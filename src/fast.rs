//! SIMD and SWAR fast paths. Each one is exact: it either returns what the
//! scalar code in csv.rs / keys.rs / ts.rs would, or declines (None) and the
//! caller uses the scalar code. Tests check this equivalence.

use std::arch::x86_64::*;

/// Bitmasks of `,` `"` `\n` over the 128 bytes at `p` (bit i = byte i).
#[inline(always)]
unsafe fn masks128(p: *const u8) -> (u128, u128, u128) {
    let (c, q, n) = (_mm256_set1_epi8(b',' as i8), _mm256_set1_epi8(b'"' as i8), _mm256_set1_epi8(b'\n' as i8));
    let mut out = [[0u32; 4]; 3];
    for k in 0..4 {
        let v = _mm256_loadu_si256(p.add(32 * k) as *const __m256i);
        out[0][k] = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, c)) as u32;
        out[1][k] = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, q)) as u32;
        out[2][k] = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, n)) as u32;
    }
    let join = |m: [u32; 4]| -> u128 {
        (m[0] as u128) | ((m[1] as u128) << 32) | ((m[2] as u128) << 64) | ((m[3] as u128) << 96)
    };
    (join(out[0]), join(out[1]), join(out[2]))
}

#[inline(always)]
fn prefix_xor(mut x: u128) -> u128 {
    x ^= x << 1;
    x ^= x << 2;
    x ^= x << 4;
    x ^= x << 8;
    x ^= x << 16;
    x ^= x << 32;
    x ^= x << 64;
    x
}

/// Bytes of lookahead the fast paths may read past a line's start.
pub const LOOKAHEAD: usize = 128 + 32;

/// Tokenizes the line starting at `p` (the caller guarantees
/// `p + LOOKAHEAD <= data.len()`). None: the line is longer than 127 bytes,
/// use the scalar splitter. Otherwise (next line start, well formed), with
/// `f` holding each field's value range relative to `p`.
///
/// The quote rule, branch-free: with Q the quote bits and X its prefix-xor
/// (1 inside a quoted run, including the opening quote), opening quotes
/// Q&X must sit at field starts, closing quotes Q&!X at field ends, no comma
/// may be inside a run (X&C), and no run may be open at the line's end.
#[inline(always)]
pub fn tokenize<const N: usize>(data: &[u8], p: usize, f: &mut [(u32, u32); N]) -> Option<(usize, bool)> {
    debug_assert!(p + LOOKAHEAD <= data.len());
    let (cm, qm, nm) = unsafe { masks128(data.as_ptr().add(p)) };
    if nm == 0 {
        return None;
    }
    let len = nm.trailing_zeros();
    let next = p + len as usize + 1;
    if len == 0 {
        return Some((next, false));
    }
    let range = (1u128 << len) - 1;
    let c = cm & range;
    if c.count_ones() != (N - 1) as u32 {
        return Some((next, false));
    }
    let mut cc = c;
    let mut fs = 0u32;
    for slot in f.iter_mut().take(N - 1) {
        let fe = cc.trailing_zeros();
        cc &= cc - 1;
        *slot = (fs, fe);
        fs = fe + 1;
    }
    f[N - 1] = (fs, len);
    let q = qm & range;
    if q != 0 {
        let x = prefix_xor(q);
        let open = q & x;
        let close = q & !x;
        let starts = (c << 1) | 1;
        let ends = (c >> 1) | (1u128 << (len - 1));
        if (open & !starts) | (close & !ends) | (x & c) | ((x >> (len - 1)) & 1) != 0 {
            return Some((next, false));
        }
        for slot in f.iter_mut() {
            if (open >> slot.0) & 1 != 0 {
                *slot = (slot.0 + 1, slot.1 - 1);
            }
        }
    }
    Some((next, true))
}

/// Prefix-xor of a 64-bit mask with one carry-less multiply.
#[inline(always)]
fn prefix_xor64(x: u64) -> u64 {
    unsafe {
        let v = _mm_clmulepi64_si128(_mm_set_epi64x(0, x as i64), _mm_set1_epi8(-1), 0);
        _mm_cvtsi128_si64(v) as u64
    }
}

/// `tokenize`, with 64-bit halves, carry-less-multiply prefix-xor, and quote
/// fixes only at opening quotes. Same contract and results as `tokenize`.
#[inline(always)]
pub fn tokenize2<const N: usize>(data: &[u8], p: usize, f: &mut [(u32, u32); N]) -> Option<(usize, bool)> {
    debug_assert!(p + LOOKAHEAD <= data.len());
    let (cm, qm, nm) = unsafe { masks128(data.as_ptr().add(p)) };
    if nm == 0 {
        return None;
    }
    let len = nm.trailing_zeros();
    let next = p + len as usize + 1;
    if len == 0 {
        return Some((next, false));
    }
    let range = (1u128 << len) - 1;
    let c = cm & range;
    let (clo, chi) = (c as u64, (c >> 64) as u64);
    if clo.count_ones() + chi.count_ones() != (N - 1) as u32 {
        return Some((next, false));
    }
    let mut i = 0usize;
    let mut fs = 0u32;
    let mut w = clo;
    while w != 0 {
        let fe = w.trailing_zeros();
        w &= w - 1;
        f[i] = (fs, fe);
        i += 1;
        fs = fe + 1;
    }
    let mut w = chi;
    while w != 0 {
        let fe = 64 + w.trailing_zeros();
        w &= w - 1;
        f[i] = (fs, fe);
        i += 1;
        fs = fe + 1;
    }
    f[N - 1] = (fs, len);
    let q = qm & range;
    if q != 0 {
        let (qlo, qhi) = (q as u64, (q >> 64) as u64);
        let xlo = prefix_xor64(qlo);
        let xhi = prefix_xor64(qhi) ^ (0u64.wrapping_sub((qlo.count_ones() & 1) as u64));
        let x = (xlo as u128) | ((xhi as u128) << 64);
        let open = q & x;
        let close = q & !x;
        let starts = (c << 1) | 1;
        let ends = (c >> 1) | (1u128 << (len - 1));
        if (open & !starts) | (close & !ends) | (x & c) | ((x >> (len - 1)) & 1) != 0 {
            return Some((next, false));
        }
        let mut o = open;
        while o != 0 {
            let pos = o.trailing_zeros();
            o &= o - 1;
            let field = (c & ((1u128 << pos) - 1)).count_ones() as usize;
            f[field].0 += 1;
            f[field].1 -= 1;
        }
    }
    Some((next, true))
}

/// Field ranges of a tokenized line: value start and end, relative to the line.
pub struct Fields<const N: usize> {
    pub start: [u32; N],
    pub end: [u32; N],
}

/// `tokenize` without data-dependent branches on the hot path: the comma
/// positions come from a fixed count of tzcnt/blsr steps on two 64-bit
/// halves, and which fields are quoted is one PEXT of the opening quotes at
/// the field starts. Same results as `tokenize`.
#[inline(always)]
pub fn tokenize3<const N: usize>(data: &[u8], p: usize, f: &mut Fields<N>) -> Option<(usize, bool)> {
    debug_assert!(p + LOOKAHEAD <= data.len());
    let (cm, qm, nm) = unsafe { masks128(data.as_ptr().add(p)) };
    if nm == 0 {
        return None;
    }
    let len = nm.trailing_zeros();
    let next = p + len as usize + 1;
    let range = (1u128 << len) - 1;
    let c = cm & range;
    let (clo, chi) = (c as u64, (c >> 64) as u64);
    let commas_ok = clo.count_ones() + chi.count_ones() == (N - 1) as u32;
    // Comma positions, branch-free: take from the low half while it has bits.
    let (mut lo, mut hi) = (clo, chi);
    let mut fs = 0u32;
    for i in 0..N - 1 {
        let use_lo = lo != 0;
        let t = if use_lo { lo.trailing_zeros() } else { 64 + hi.trailing_zeros() };
        lo = if use_lo { lo & lo.wrapping_sub(1) } else { lo };
        hi = if use_lo { hi } else { hi & hi.wrapping_sub(1) };
        f.start[i] = fs;
        f.end[i] = t;
        fs = t + 1;
    }
    f.start[N - 1] = fs;
    f.end[N - 1] = len;
    let q = qm & range;
    let (qlo, qhi) = (q as u64, (q >> 64) as u64);
    let xlo = prefix_xor64(qlo);
    let xhi = prefix_xor64(qhi) ^ (0u64.wrapping_sub((qlo.count_ones() & 1) as u64));
    let x = (xlo as u128) | ((xhi as u128) << 64);
    let open = q & x;
    let close = q & !x;
    let starts = ((c << 1) | 1) & range;
    let last = 1u128 << (len.max(1) - 1);
    let ends = (c >> 1) | last;
    let bad = (open & !starts) | (close & !ends) | (x & c) | (x & last);
    let ok = commas_ok && len != 0 && bad == 0;
    // Bit i of `quoted` = field i starts with an opening quote.
    let (slo, shi) = (starts as u64, (starts >> 64) as u64);
    let quoted = unsafe {
        use std::arch::x86_64::_pext_u64;
        _pext_u64(open as u64, slo) | (_pext_u64((open >> 64) as u64, shi) << slo.count_ones())
    };
    for i in 0..N {
        let qi = ((quoted >> i) & 1) as u32;
        f.start[i] += qi;
        f.end[i] -= qi;
    }
    Some((next, ok))
}

static LEN_MASKS: [[u8; 16]; 17] = {
    let mut t = [[0u8; 16]; 17];
    let mut n = 0;
    while n <= 16 {
        let mut i = 0;
        while i < n {
            t[n][i] = 0xFF;
            i += 1;
        }
        n += 1;
    }
    t
};

/// `keys::pack` with SSE: 16 bytes at `p` are readable; the string is the first `len`.
#[inline(always)]
pub fn pack(p: *const u8, len: usize) -> Option<u64> {
    if len == 0 {
        return Some(0);
    }
    if len > 10 {
        return None;
    }
    unsafe {
        let v = _mm_loadu_si128(p as *const __m128i);
        let gt = |k: i8| _mm_cmpgt_epi8(v, _mm_set1_epi8(k));
        let lt = |k: i8| _mm_cmplt_epi8(v, _mm_set1_epi8(k));
        let digit = _mm_and_si128(gt(0x2F), lt(0x3A));
        let upper = _mm_and_si128(gt(0x40), lt(0x5B));
        let lower = _mm_and_si128(gt(0x60), lt(0x7B));
        let under = _mm_cmpeq_epi8(v, _mm_set1_epi8(0x5F));
        let any = _mm_or_si128(_mm_or_si128(digit, upper), _mm_or_si128(lower, under));
        let want = (1u32 << len) - 1;
        if (_mm_movemask_epi8(any) as u32) & want != want {
            return None;
        }
        let code = _mm_or_si128(
            _mm_or_si128(
                _mm_and_si128(digit, _mm_sub_epi8(v, _mm_set1_epi8(0x2F))),
                _mm_and_si128(upper, _mm_sub_epi8(v, _mm_set1_epi8(0x36))),
            ),
            _mm_or_si128(
                _mm_and_si128(lower, _mm_sub_epi8(v, _mm_set1_epi8(0x3B))),
                _mm_and_si128(under, _mm_set1_epi8(37)),
            ),
        );
        let code = _mm_and_si128(code, _mm_loadu_si128(LEN_MASKS[len].as_ptr() as *const __m128i));
        let pairs = _mm_maddubs_epi16(code, _mm_set1_epi16(0x0140));
        let quads = _mm_madd_epi16(pairs, _mm_set1_epi32(0x0001_1000));
        let l0 = _mm_cvtsi128_si32(quads) as u32 as u64;
        let l1 = _mm_extract_epi32(quads, 1) as u32 as u64;
        let l2 = _mm_extract_epi32(quads, 2) as u32 as u64;
        Some((l0 << 36) | (l1 << 12) | (l2 >> 12))
    }
}

/// Value of an all-ASCII-digit string of length 1..=16 ending at `end`
/// (16 bytes before `end` are readable), or None if any byte is not a digit.
#[inline(always)]
pub fn digits16(end: *const u8, len: usize) -> Option<u64> {
    debug_assert!((1..=16).contains(&len));
    unsafe {
        let v = _mm_loadu_si128(end.sub(16) as *const __m128i);
        let d = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
        // Unsigned d <= 9 for every byte in the last `len` positions.
        let ok = _mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d);
        let want = !((1u32 << (16 - len)) - 1) & 0xFFFF;
        if (_mm_movemask_epi8(ok) as u32) & want != want {
            return None;
        }
        // Zero the leading bytes that are not part of the number.
        let keep = _mm_loadu_si128(LEN_MASKS[16 - len].as_ptr() as *const __m128i);
        let d = _mm_andnot_si128(keep, d);
        let t1 = _mm_maddubs_epi16(d, _mm_set1_epi16(0x010A));
        let t2 = _mm_madd_epi16(t1, _mm_set1_epi32(0x0001_0064));
        let t3 = _mm_packus_epi32(t2, t2);
        let t4 = _mm_madd_epi16(t3, _mm_set1_epi32(0x0001_2710));
        let hi = _mm_cvtsi128_si32(t4) as u32 as u64;
        let lo = _mm_extract_epi32(t4, 1) as u32 as u64;
        Some(hi * 100_000_000 + lo)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csv::split;
    use crate::keys;

    fn padded(s: &[u8]) -> Vec<u8> {
        let mut v = vec![b'#'; 32];
        v.extend_from_slice(s);
        v.extend_from_slice(&[b'#'; 200]);
        v
    }

    #[test]
    fn tokenize_matches_split() {
        let lines: Vec<&str> = vec![
            "a,b,c", "\"a\",b,\"\"", "a,b", "a,b,c,d", "", "a,\"b,c", "a,b\",c", "\"a\"x,b,c", "\",b,c", " a , b ,c\r",
            ",,", "\"\",\"\",\"\"", "a,\"\"\",c", "\"a,b\",c", "a,\"\",\"", "a,b,\"", "a,b,\"\"\"", "a,\"x\"\"y\",c",
            "\"\"\"\",b,c", "a,b,\"c\"", "\"a\",\"b\",\"c\"", "a,,\"\"", "\"\"x,b,c", "x\"\",b,c", ",\"\",",
        ];
        for l in &lines {
            let mut buf = padded(l.as_bytes());
            buf[32 + l.len()] = b'\n';
            let mut f = [(0u32, 0u32); 3];
            let (next, ok) = tokenize::<3>(&buf, 32, &mut f).unwrap();
            assert_eq!(next, 32 + l.len() + 1);
            let mut g = [&b""[..]; 3];
            let want = split(l.as_bytes(), &mut g);
            assert_eq!(ok, want, "{:?}", l);
            if ok {
                for i in 0..3 {
                    assert_eq!(&buf[32 + f[i].0 as usize..32 + f[i].1 as usize], g[i], "{:?} field {}", l, i);
                }
            }
        }
        // Random lines over a small alphabet, against the scalar splitter.
        let alphabet = b"a\",";
        // Lines up to 63 bytes come from the generator below; longer ones test the high half.
        let mut seed = 12345u64;
        for _ in 0..200_000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let len = if seed & 1 == 0 { (seed >> 58) as usize } else { 60 + ((seed >> 57) as usize % 68) };
            let mut line = Vec::new();
            let mut s = seed;
            for _ in 0..len {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
                line.push(alphabet[((s >> 33) % 3) as usize]);
            }
            let mut buf = padded(&line);
            buf[32 + line.len()] = b'\n';
            let mut f = [(0u32, 0u32); 3];
            let (_, ok) = tokenize::<3>(&buf, 32, &mut f).unwrap();
            let mut f2 = [(0u32, 0u32); 3];
            let (_, ok2) = tokenize2::<3>(&buf, 32, &mut f2).unwrap();
            assert_eq!(ok, ok2, "{:?}", String::from_utf8_lossy(&line));
            let mut f3 = Fields::<3> { start: [0; 3], end: [0; 3] };
            let (_, ok3) = tokenize3::<3>(&buf, 32, &mut f3).unwrap();
            assert_eq!(ok, ok3, "tokenize3 {:?}", String::from_utf8_lossy(&line));
            if ok {
                assert_eq!(f, f2);
                for i in 0..3 {
                    assert_eq!((f3.start[i], f3.end[i]), f[i]);
                }
            }
            let mut g = [&b""[..]; 3];
            let want = split(&line, &mut g);
            assert_eq!(ok, want, "{:?}", String::from_utf8_lossy(&line));
            if ok {
                for i in 0..3 {
                    assert_eq!(&buf[32 + f[i].0 as usize..32 + f[i].1 as usize], g[i]);
                }
            }
        }
    }

    #[test]
    fn pack_matches_scalar() {
        let words: Vec<&[u8]> = vec![
            b"a", b"u1", b"e42huhoug", b"U8W84O3T0", b"c_1958", b"p13649", b"zzzzzzzzzz", b"0", b"_", b"A_z9",
            b"abcdefghijk", b"u 1", b"u\r", b"\xc3\xa9", b"u-1", b"", b"user_id_a", b"\x7f", b"@", b"[", b"`", b"{",
        ];
        for w in words {
            let buf = padded(w);
            assert_eq!(pack(buf[32..].as_ptr(), w.len()), keys::pack(w), "{:?}", w);
        }
    }

    #[test]
    fn digits_match_scalar() {
        for s in ["1", "0", "09", "1791071890000", "0001791071890000", "9999999999999999", "12a4", "١٢", "1234567890123456"] {
            let b = s.as_bytes();
            if b.is_empty() || b.len() > 16 {
                continue;
            }
            let buf = padded(b);
            let end = unsafe { buf.as_ptr().add(32 + b.len()) };
            let want = if b.iter().all(|c| c.is_ascii_digit()) { Some(s.parse::<u64>().unwrap()) } else { None };
            assert_eq!(digits16(end, b.len()), want, "{}", s);
        }
    }
}
