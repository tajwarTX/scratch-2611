//! Fast line and field splitting with SIMD byte-class masks.

/// Bit i set when byte p+i is '\n', ',' or '"' respectively. Reads d[p..p+64].
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
pub fn masks64(d: &[u8], p: usize) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    debug_assert!(p + 64 <= d.len());
    unsafe {
        let ptr = d.as_ptr().add(p);
        let a = _mm256_loadu_si256(ptr as *const __m256i);
        let b = _mm256_loadu_si256(ptr.add(32) as *const __m256i);
        let f = |ch: u8| -> u64 {
            let cv = _mm256_set1_epi8(ch as i8);
            let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, cv)) as u32 as u64;
            let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, cv)) as u32 as u64;
            lo | (hi << 32)
        };
        (f(b'\n'), f(b','), f(b'"'))
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub fn masks64(d: &[u8], p: usize) -> (u64, u64, u64) {
    use std::arch::aarch64::*;
    unsafe {
        let ptr = d.as_ptr().add(p);
        const W: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
        let wv = vld1q_u8(W.as_ptr());
        let v0 = vld1q_u8(ptr);
        let v1 = vld1q_u8(ptr.add(16));
        let v2 = vld1q_u8(ptr.add(32));
        let v3 = vld1q_u8(ptr.add(48));
        let mk = |ch: u8| -> u64 {
            let cv = vdupq_n_u8(ch);
            let a0 = vandq_u8(vceqq_u8(v0, cv), wv);
            let a1 = vandq_u8(vceqq_u8(v1, cv), wv);
            let a2 = vandq_u8(vceqq_u8(v2, cv), wv);
            let a3 = vandq_u8(vceqq_u8(v3, cv), wv);
            let s0 = vpaddq_u8(a0, a1);
            let s1 = vpaddq_u8(a2, a3);
            let s = vpaddq_u8(s0, s1);
            let s = vpaddq_u8(s, s);
            vgetq_lane_u64(vreinterpretq_u64_u8(s), 0)
        };
        (mk(b'\n'), mk(b','), mk(b'"'))
    }
}

#[cfg(not(any(target_arch = "aarch64", all(target_arch = "x86_64", target_feature = "avx2"))))]
#[inline(always)]
pub fn masks64(d: &[u8], p: usize) -> (u64, u64, u64) {
    let mut n = 0u64;
    let mut c = 0u64;
    let mut q = 0u64;
    for i in 0..64 {
        let b = d[p + i];
        n |= ((b == b'\n') as u64) << i;
        c |= ((b == b',') as u64) << i;
        q |= ((b == b'"') as u64) << i;
    }
    (n, c, q)
}

#[inline]
pub fn memchr(c: u8, d: &[u8], from: usize) -> Option<usize> {
    d[from..].iter().position(|&b| b == c).map(|p| p + from)
}

#[inline(always)]
fn block_masks(d: &[u8], b: usize) -> (u64, u64, u64) {
    // The mapping is padded with zero bytes past the end (map_file), so reading a
    // whole block is always safe; bytes past len are zero and never match.
    masks64_unchecked(d, b * 64)
}

/// Calls `f` with the N unquoted fields of every well-formed line whose first byte
/// lies in [start, end). Malformed lines (rules' CSV dialect) are skipped.
/// Streams over 64-byte blocks: every byte is classified once.
#[inline(always)]
pub fn chunk_fields<'a, const N: usize>(d: &'a [u8], start: usize, end: usize, mut f: impl FnMut(&[&'a [u8]; N])) {
    let len = d.len();
    let mut p = start;
    if p > 0 && d[p - 1] != b'\n' {
        match memchr(b'\n', d, p) {
            Some(q) => p = q + 1,
            None => return,
        }
    }
    if p >= end {
        return;
    }
    let mut blk = p / 64;
    let (mut n, mut c, mut q) = block_masks(d, blk);
    let mut seps = [0usize; N];
    while p < end {
        let pb = p / 64;
        if pb != blk {
            blk = pb;
            (n, c, q) = block_masks(d, blk);
        }
        let mut off = p % 64;
        let mut ns = 0usize;
        let mut nq = 0u32;
        let e;
        loop {
            let keep = !0u64 << off;
            let nm = n & keep;
            let lim = if nm != 0 { (nm & nm.wrapping_neg()) - 1 } else { !0u64 };
            let mut cm = c & keep & lim;
            nq += (q & keep & lim).count_ones();
            while cm != 0 {
                let pos = blk * 64 + cm.trailing_zeros() as usize;
                if ns < N - 1 {
                    seps[ns] = pos;
                }
                ns += 1;
                cm &= cm - 1;
            }
            if nm != 0 {
                e = blk * 64 + nm.trailing_zeros() as usize;
                break;
            }
            blk += 1;
            if blk * 64 >= len {
                e = len;
                break;
            }
            (n, c, q) = block_masks(d, blk);
            off = 0;
        }
        if ns == N - 1 {
            seps[N - 1] = e;
            let mut fields: [&'a [u8]; N] = [&[]; N];
            let mut s = p;
            let base = d.as_ptr();
            if nq == 0 {
                for i in 0..N {
                    let fe = seps[i];
                    fields[i] = unsafe { std::slice::from_raw_parts(base.add(s), fe - s) };
                    s = fe + 1;
                }
                f(&fields);
            } else {
                // A field is quoted iff it starts and ends with '"' and has length >= 2.
                // An empty field's d[s] is its own separator and d[e-1] the previous one,
                // so neither reads as a quote; s >= 1 always (the header precedes data).
                let mut quoted = 0u32;
                let mut bad = 0usize;
                for i in 0..N {
                    let fe = seps[i];
                    let len = fe - s;
                    let qs = (unsafe { *base.add(s) } == b'"') as usize;
                    let qe = (unsafe { *base.add(fe - 1) } == b'"') as usize;
                    bad |= (qs ^ qe) | (qs & (len < 2) as usize);
                    fields[i] = unsafe { std::slice::from_raw_parts(base.add(s + qs), len.wrapping_sub(2 * qs) & !(0usize.wrapping_sub(bad))) };
                    quoted += qs as u32;
                    s = fe + 1;
                }
                if bad == 0 && nq == 2 * quoted {
                    f(&fields);
                }
            }
        }
        p = e + 1;
    }
}

#[inline(never)]
pub fn slow_line<'a, const N: usize>(d: &'a [u8], p: usize, f: &mut impl FnMut(&[&'a [u8]; N])) -> usize {
    let e = memchr(b'\n', d, p).unwrap_or(d.len());
    let mut fields: [&'a [u8]; N] = [&[]; N];
    if split(&d[p..e], &mut fields) {
        f(&fields);
    }
    e + 1
}

/// Splits one line into exactly N fields under the rules' dialect; false if malformed.
pub fn split<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let len = line.len();
    let mut n = 0;
    let mut i = 0;
    loop {
        if n == N {
            return false;
        }
        if i < len && line[i] == b'"' {
            let start = i + 1;
            let mut j = start;
            while j < len && line[j] != b'"' && line[j] != b',' {
                j += 1;
            }
            if j >= len || line[j] != b'"' {
                return false;
            }
            out[n] = &line[start..j];
            n += 1;
            i = j + 1;
            if i == len {
                break;
            }
            if line[i] != b',' {
                return false;
            }
            i += 1;
        } else {
            let start = i;
            let mut j = i;
            while j < len && line[j] != b',' && line[j] != b'"' {
                j += 1;
            }
            if j < len && line[j] == b'"' {
                return false;
            }
            out[n] = &line[start..j];
            n += 1;
            if j == len {
                break;
            }
            i = j + 1;
        }
    }
    n == N
}

#[inline(always)]
fn masks64_unchecked(d: &[u8], p: usize) -> (u64, u64, u64) {
    let (n, c, q) = masks64(unsafe { std::slice::from_raw_parts(d.as_ptr(), p + 64) }, p);
    if p + 64 <= d.len() {
        (n, c, q)
    } else {
        let keep = if p >= d.len() { 0 } else { (1u64 << (d.len() - p)) - 1 };
        (n & keep, c & keep, q & keep)
    }
}

/// Per-line variant: classifies the 128 bytes at each line start, so all per-line
/// work happens in registers. Lines of 128+ bytes take the slow path.
/// Requires a padded mapping (reads up to 128 bytes past any line start).
#[inline(always)]
pub fn chunk_fields_w<'a, const N: usize>(d: &'a [u8], start: usize, end: usize, mut f: impl FnMut(&[&'a [u8]; N])) {
    let len = d.len();
    let mut p = start;
    if p > 0 && d[p - 1] != b'\n' {
        match memchr(b'\n', d, p) {
            Some(q) => p = q + 1,
            None => return,
        }
    }
    let base = d.as_ptr();
    let full = unsafe { std::slice::from_raw_parts(base, len + 192) };
    while p < end {
        let (n0, c0, q0) = masks64(full, p);
        let (nl, cm, qm): (usize, u64, u64, );
        let (cm_hi, qm_hi): (u64, u64);
        if n0 != 0 {
            nl = n0.trailing_zeros() as usize;
            let below = (1u64 << nl) - 1;
            cm = c0 & below;
            qm = q0 & below;
            cm_hi = 0;
            qm_hi = 0;
        } else {
            let (n1, c1, q1) = masks64(full, p + 64);
            if n1 == 0 {
                p = slow_line::<N>(d, p, &mut f);
                continue;
            }
            let t = n1.trailing_zeros() as usize;
            nl = 64 + t;
            let below = (1u64 << t) - 1;
            cm = c0;
            qm = q0;
            cm_hi = c1 & below;
            qm_hi = q1 & below;
        }
        let e = p + nl;
        let line_end_in_file = e.min(len);
        if (cm.count_ones() + cm_hi.count_ones()) as usize != N - 1 || e > len {
            if e > len {
                // last line without newline, or padding: handle via the slow path
                p = slow_line::<N>(d, p, &mut f);
                continue;
            }
            p = line_end_in_file + 1;
            continue;
        }
        let mut seps = [0usize; N];
        let mut k = 0;
        let mut m = cm;
        while m != 0 {
            seps[k] = p + m.trailing_zeros() as usize;
            m &= m - 1;
            k += 1;
        }
        let mut m = cm_hi;
        while m != 0 {
            seps[k] = p + 64 + m.trailing_zeros() as usize;
            m &= m - 1;
            k += 1;
        }
        seps[N - 1] = e;
        let mut fields: [&'a [u8]; N] = [&[]; N];
        let mut s = p;
        if (qm | qm_hi) == 0 {
            for i in 0..N {
                let fe = seps[i];
                fields[i] = unsafe { std::slice::from_raw_parts(base.add(s), fe - s) };
                s = fe + 1;
            }
            f(&fields);
        } else {
            // Valid quoting <=> every quote sits at a field's first or last byte, and the
            // quote parity (prefix xor) is zero at every separator and at the line end.
            let starts_lo = (cm << 1) | 1;
            let starts_hi = (cm_hi << 1) | (cm >> 63);
            let (ends_lo, ends_hi) = if nl < 64 {
                ((cm >> 1) | (1u64 << (nl - 1).min(63)) & (if nl > 0 { !0 } else { 0 }), 0u64)
            } else {
                let t = nl - 64;
                let lo = (cm >> 1) | ((cm_hi & 1) << 63) | if t == 0 { 1u64 << 63 } else { 0 };
                let hi = (cm_hi >> 1) | if t > 0 { 1u64 << (t - 1) } else { 0 };
                (lo, hi)
            };
            let bad_pos = (qm & !(starts_lo | ends_lo)) | (qm_hi & !(starts_hi | ends_hi));
            let px_lo = prefix_xor(qm);
            let carry = 0u64.wrapping_sub((qm.count_ones() & 1) as u64);
            let px_hi = prefix_xor(qm_hi) ^ carry;
            // parity must be zero at each separator (commas) and after the last quote
            let total_odd = (qm.count_ones() + qm_hi.count_ones()) & 1;
            if bad_pos == 0 && (px_lo & cm) == 0 && (px_hi & cm_hi) == 0 && total_odd == 0 {
                for i in 0..N {
                    let fe = seps[i];
                    let qs = (unsafe { *base.add(s) } == b'"') as usize;
                    fields[i] = unsafe { std::slice::from_raw_parts(base.add(s + qs), fe - s - 2 * qs) };
                    s = fe + 1;
                }
                f(&fields);
            }
        }
        p = e + 1;
    }
}

/// Bit i of the result = xor of bits 0..=i of x.
#[inline(always)]
fn prefix_xor(mut x: u64) -> u64 {
    x ^= x << 1;
    x ^= x << 2;
    x ^= x << 4;
    x ^= x << 8;
    x ^= x << 16;
    x ^= x << 32;
    x
}

#[inline(always)]
pub fn blsr(x: u64) -> u64 {
    x & x.wrapping_sub(1)
}

/// Bits below k (k < 64).
#[inline(always)]
pub fn below(k: u32) -> u64 {
    (1u64 << k) - 1
}

/// Quoting check for the fields of one 64-byte window. `q`: quotes before the window's
/// last used separator, `seps`: the separators closing its fields, `last`: the position
/// of the last of them. Valid quoting <=> every quote is a field's first or last byte,
/// and the number of quotes before each separator is even (so a field has none, or
/// exactly its first and last byte).
#[inline(always)]
fn quotes_ok(q: u64, seps: u64, last: u32) -> bool {
    let starts = 1 | ((seps << 1) & below(last));
    let ends = seps >> 1;
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    {
        // Same test without the prefix xor: per field, "first byte is a quote" must
        // equal "last byte is a quote" (bit i of each pext is field i; an empty first
        // field has no end bit, hence the shift), and a one-byte field is not a quote.
        use core::arch::x86_64::_pext_u64;
        let qs = unsafe { _pext_u64(q, starts) };
        let qe = unsafe { _pext_u64(q, ends) } << (seps & 1);
        (q & !(starts | ends)) == 0 && qs == qe && (q & starts & ends) == 0
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        (q & !(starts | ends)) == 0 && (prefix_xor(q) & seps) == 0
    }
}

/// `quotes_ok` that also returns, bit i, whether field i of the window starts with a
/// quote (None: bad quoting).
#[inline(always)]
pub fn quote_starts(q: u64, seps: u64, last: u32) -> Option<u64> {
    let starts = 1 | ((seps << 1) & below(last));
    let ends = seps >> 1;
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    {
        use core::arch::x86_64::_pext_u64;
        let qs = unsafe { _pext_u64(q, starts) };
        let qe = unsafe { _pext_u64(q, ends) } << (seps & 1);
        if (q & !(starts | ends)) == 0 && qs == qe && (q & starts & ends) == 0 {
            Some(qs)
        } else {
            None
        }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        if (q & !(starts | ends)) == 0 && (prefix_xor(q) & seps) == 0 {
            let mut m = starts;
            let mut qs = 0u64;
            let mut i = 0;
            while m != 0 {
                qs |= ((q >> m.trailing_zeros()) & 1) << i;
                i += 1;
                m = blsr(m);
            }
            Some(qs)
        } else {
            None
        }
    }
}

/// Reference form of `quotes_ok` (prefix xor), for tests.
#[allow(dead_code)]
fn quotes_ok_ref(q: u64, seps: u64, last: u32) -> bool {
    let starts = 1 | ((seps << 1) & below(last));
    let ends = seps >> 1;
    (q & !(starts | ends)) == 0 && (prefix_xor(q) & seps) == 0
}

/// Portable form of the pext variant of `quotes_ok`, for tests on any CPU.
#[allow(dead_code)]
fn quotes_ok_pext_model(q: u64, seps: u64, last: u32) -> bool {
    fn pext(x: u64, mut m: u64) -> u64 {
        let mut r = 0u64;
        let mut k = 0;
        while m != 0 {
            let b = m & m.wrapping_neg();
            if x & b != 0 {
                r |= 1 << k;
            }
            k += 1;
            m ^= b;
        }
        r
    }
    let starts = 1 | ((seps << 1) & below(last));
    let ends = seps >> 1;
    let qs = pext(q, starts);
    let qe = pext(q, ends) << (seps & 1);
    (q & !(starts | ends)) == 0 && qs == qe && (q & starts & ends) == 0
}

/// How far ahead of the current row the input is prefetched (bytes; 0 = off).
pub static PF_DIST: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(512);

#[inline(always)]
pub fn prefetch_in(p: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_T0)
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, readonly, preserves_flags))
    }
}

/// Event rows: calls `f` with the 7 unquoted fields of every well-formed line whose
/// first byte lies in [start, end). Same results as `chunk_fields::<7>`.
/// Works in two 64-byte windows per row, one at the row start (3 fields) and one after
/// the third comma (4 fields); rows that do not fit this shape take the scalar path.
/// Requires a padded mapping (reads up to 130 bytes past a row start).
#[inline(always)]
pub fn rows7<'a>(d: &'a [u8], start: usize, end: usize, mut f: impl FnMut(&[&'a [u8]; 7])) {
    let len = d.len();
    let mut p = start;
    if p > 0 && d[p - 1] != b'\n' {
        match memchr(b'\n', d, p) {
            Some(q) => p = q + 1,
            None => return,
        }
    }
    let base = d.as_ptr();
    let full = unsafe { std::slice::from_raw_parts(base, len + 192) };
    let pf = PF_DIST.load(std::sync::atomic::Ordering::Relaxed);
    while p < end {
        if pf != 0 {
            // the mapped file is not in cache: ask for the lines a few rows ahead
            prefetch_in(base.wrapping_add(p + pf));
            prefetch_in(base.wrapping_add(p + pf + 64));
        }
        let (n0, c0, q0) = masks64(full, p);
        let s0 = n0 | c0;
        let m1 = blsr(s0);
        let m2 = blsr(m1);
        let (k0, k1, k2) = (s0.trailing_zeros(), m1.trailing_zeros(), m2.trailing_zeros());
        let low3 = s0 ^ blsr(m2);
        // three commas, no line end before them
        if m2 == 0 || (n0 & low3) != 0 {
            p = slow_line::<7>(d, p, &mut f);
            continue;
        }
        let p2 = p + k2 as usize + 1;
        let (n1, c1, q1) = masks64(full, p2);
        let s1 = n1 | c1;
        let r1 = blsr(s1);
        let r2 = blsr(r1);
        let r3 = blsr(r2);
        let (k3, k4, k5, k6) = (s1.trailing_zeros(), r1.trailing_zeros(), r2.trailing_zeros(), r3.trailing_zeros());
        let low4 = s1 ^ blsr(r3);
        // three more commas, then the line end
        if r3 == 0 || (n1 & low4) != (1u64 << k6) {
            p = slow_line::<7>(d, p, &mut f);
            continue;
        }
        let e = p2 + k6 as usize;
        let qa = q0 & below(k2);
        let qb = q1 & below(k6);
        if (qa | qb) != 0 && (!quotes_ok(qa, low3, k2) || !quotes_ok(qb, low4, k6)) {
            p = e + 1;
            continue;
        }
        // A field that starts with a quote also ends with one (checked above); an empty
        // field's first byte is its own separator, never a quote.
        let fld = |at: usize, q: u64, from: u32, to: u32| -> &'a [u8] {
            let t = ((q >> from) & 1) as usize;
            unsafe { std::slice::from_raw_parts(base.add(at + from as usize + t), (to - from) as usize - 2 * t) }
        };
        let fields: [&'a [u8]; 7] = [
            fld(p, q0, 0, k0),
            fld(p, q0, k0 + 1, k1),
            fld(p, q0, k1 + 1, k2),
            fld(p2, q1, 0, k3),
            fld(p2, q1, k3 + 1, k4),
            fld(p2, q1, k4 + 1, k5),
            fld(p2, q1, k5 + 1, k6),
        ];
        f(&fields);
        p = e + 1;
    }
}

/// Link rows (2 fields): both in one 64-byte window, or, with long IDs, one window
/// per field; anything else goes to the generic splitter.
#[inline(always)]
pub fn rows2<'a>(d: &'a [u8], start: usize, end: usize, mut f: impl FnMut(&[&'a [u8]; 2])) {
    let len = d.len();
    let mut p = start;
    if p > 0 && d[p - 1] != b'\n' {
        match memchr(b'\n', d, p) {
            Some(q) => p = q + 1,
            None => return,
        }
    }
    let base = d.as_ptr();
    let full = unsafe { std::slice::from_raw_parts(base, len + 192) };
    while p < end {
        let (n0, c0, q0) = masks64(full, p);
        let s0 = n0 | c0;
        let m1 = blsr(s0);
        let (k0, k1) = (s0.trailing_zeros(), m1.trailing_zeros());
        let low2 = s0 ^ blsr(m1);
        // a comma, then the line end
        if m1 != 0 && (n0 & low2) == (1u64 << k1) {
            if let Some(qs) = quote_starts(q0 & below(k1), low2, k1) {
                let (t0, t1) = ((qs & 1) as usize, ((qs >> 1) & 1) as usize);
                let (k0, k1) = (k0 as usize, k1 as usize);
                let fields: [&'a [u8]; 2] = unsafe {
                    [
                        std::slice::from_raw_parts(base.add(p + t0), k0 - 2 * t0),
                        std::slice::from_raw_parts(base.add(p + k0 + 1 + t1), k1 - k0 - 1 - 2 * t1),
                    ]
                };
                f(&fields);
            }
            // (bad quoting: the line is malformed and skipped)
            p += k1 as usize + 1;
            continue;
        }
        // two long IDs: one window per field
        let mut fields: [&'a [u8]; 2] = [&[]; 2];
        let mut at = p;
        let mut ok = true;
        for (i, fld) in fields.iter_mut().enumerate() {
            let (n, c, q) = masks64(full, at);
            let s = n | c;
            let k = s.trailing_zeros();
            if s == 0 || ((n >> k) & 1 == 1) != (i == 1) {
                ok = false;
                break;
            }
            let t = match quote_starts(q & below(k), 1u64 << k, k) {
                Some(x) => (x & 1) as usize,
                None => {
                    ok = false;
                    break;
                }
            };
            *fld = unsafe { std::slice::from_raw_parts(base.add(at + t), k as usize - 2 * t) };
            at += k as usize + 1;
        }
        if ok {
            f(&fields);
            p = at;
        } else {
            p = slow_line::<2>(d, p, &mut f);
        }
    }
}

/// Click rows: like `rows7` for 4 fields, all in one 64-byte window.
#[inline(always)]
pub fn rows4<'a>(d: &'a [u8], start: usize, end: usize, mut f: impl FnMut(&[&'a [u8]; 4])) {
    let len = d.len();
    let mut p = start;
    if p > 0 && d[p - 1] != b'\n' {
        match memchr(b'\n', d, p) {
            Some(q) => p = q + 1,
            None => return,
        }
    }
    let base = d.as_ptr();
    let full = unsafe { std::slice::from_raw_parts(base, len + 192) };
    let pf = PF_DIST.load(std::sync::atomic::Ordering::Relaxed);
    while p < end {
        prefetch_in(base.wrapping_add(p + pf));
        let (n0, c0, q0) = masks64(full, p);
        let s0 = n0 | c0;
        let m1 = blsr(s0);
        let m2 = blsr(m1);
        let m3 = blsr(m2);
        let (k0, k1, k2, k3) = (s0.trailing_zeros(), m1.trailing_zeros(), m2.trailing_zeros(), m3.trailing_zeros());
        let low4 = s0 ^ blsr(m3);
        // three commas, then the line end
        if m3 == 0 || (n0 & low4) != (1u64 << k3) {
            // A long click_id or user_id pushes the line end out of the window: take
            // the fields one by one, each from a window that starts at the field
            // (the generic splitter handles whatever this cannot).
            let mut fields: [&'a [u8]; 4] = [&[]; 4];
            let mut at = p;
            let mut ok = true;
            for (i, fld) in fields.iter_mut().enumerate() {
                let (n, c, q) = masks64(full, at);
                let s = n | c;
                let k = s.trailing_zeros();
                // the first separator: a comma after fields 0..2, the line end after field 3
                if s == 0 || ((n >> k) & 1 == 1) != (i == 3) {
                    ok = false;
                    break;
                }
                let t = match quote_starts(q & below(k), 1u64 << k, k) {
                    Some(x) => (x & 1) as usize,
                    None => {
                        ok = false;
                        break;
                    }
                };
                *fld = unsafe { std::slice::from_raw_parts(base.add(at + t), k as usize - 2 * t) };
                at += k as usize + 1;
            }
            if ok {
                f(&fields);
                p = at;
            } else {
                p = slow_line::<4>(d, p, &mut f);
            }
            continue;
        }
        let e = p + k3 as usize;
        // no "any quote?" test first: that branch is a coin flip per row
        let qs = match quote_starts(q0 & below(k3), low4, k3) {
            Some(x) => x,
            None => {
                p = e + 1;
                continue;
            }
        };
        let fld = |i: u32, from: u32, to: u32| -> &'a [u8] {
            let t = ((qs >> i) & 1) as usize;
            unsafe { std::slice::from_raw_parts(base.add(p + from as usize + t), (to - from) as usize - 2 * t) }
        };
        let fields: [&'a [u8]; 4] = [fld(0, 0, k0), fld(1, k0 + 1, k1), fld(2, k1 + 1, k2), fld(3, k2 + 1, k3)];
        f(&fields);
        p = e + 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check<const N: usize>(seed: u64, alphabet: &[u8], lines: usize) {
        let mut rng = seed;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut data = b"header\n".to_vec();
        for _ in 0..lines {
            let len = (next() % 140) as usize;
            for _ in 0..len {
                data.push(alphabet[(next() % alphabet.len() as u64) as usize]);
            }
            data.push(b'\n');
        }
        let len = data.len();
        data.extend(std::iter::repeat(0u8).take(512));
        let d = &data[..len];
        let mut got: Vec<Vec<Vec<u8>>> = Vec::new();
        chunk_fields_w::<N>(d, 7, len, |f| got.push(f.iter().map(|x| x.to_vec()).collect()));
        let mut want: Vec<Vec<Vec<u8>>> = Vec::new();
        for line in d[7..].split(|&b| b == b'\n') {
            let mut f: [&[u8]; N] = [&[]; N];
            if split(line, &mut f) {
                want.push(f.iter().map(|x| x.to_vec()).collect());
            }
        }
        // the trailing empty piece after the final newline is not a line
        assert_eq!(got, want);
    }

    fn reference<const N: usize>(d: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut want: Vec<Vec<Vec<u8>>> = Vec::new();
        let mut lines: Vec<&[u8]> = d[7..].split(|&b| b == b'\n').collect();
        if lines.last().map(|l| l.is_empty()).unwrap_or(false) {
            lines.pop(); // the piece after the final newline is not a line
        }
        for line in lines {
            let mut f: [&[u8]; N] = [&[]; N];
            if split(line, &mut f) {
                want.push(f.iter().map(|x| x.to_vec()).collect());
            }
        }
        want
    }

    fn gen(seed: u64, alphabet: &[u8], lines: usize, maxlen: u64, final_newline: bool) -> (Vec<u8>, usize) {
        let mut rng = seed;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut data = b"header\n".to_vec();
        for i in 0..lines {
            let len = (next() % maxlen) as usize;
            for _ in 0..len {
                data.push(alphabet[(next() % alphabet.len() as u64) as usize]);
            }
            if i + 1 < lines || final_newline {
                data.push(b'\n');
            }
        }
        let len = data.len();
        data.extend(std::iter::repeat(0u8).take(512));
        (data, len)
    }

    #[test]
    fn quote_checks_agree() {
        // exhaustive over small windows: separators and quotes on disjoint positions
        let mut rng = 99u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut n = 0u64;
        for _ in 0..3_000_000 {
            let width = 1 + (next() % 24) as u32;
            let dens = 1 + next() % 6;
            let mut seps = 0u64;
            for i in 0..width {
                if next() % dens == 0 {
                    seps |= 1 << i;
                }
            }
            if seps == 0 {
                continue;
            }
            let last = 63 - seps.leading_zeros();
            let mut q = 0u64;
            let qd = 1 + next() % 5;
            for i in 0..last {
                if seps >> i & 1 == 0 && next() % qd == 0 {
                    q |= 1 << i;
                }
            }
            let a = quotes_ok_ref(q, seps, last);
            assert_eq!(a, quotes_ok_pext_model(q, seps, last), "q={q:b} seps={seps:b}");
            assert_eq!(a, quotes_ok(q, seps, last), "q={q:b} seps={seps:b}");
            n += a as u64;
        }
        assert!(n > 100_000);
    }

    #[test]
    fn row_splitters_match_reference() {
        for s in 1..60u64 {
            for (alpha, maxlen) in [(&b"ab,\"\" ,\r"[..], 140u64), (&b"abcdefgh,,\"\"\"x"[..], 90), (&b"aaaaaaaaaaaaaaaa,,\""[..], 200), (&b"a,"[..], 30)] {
                for fin in [true, false] {
                    let (data, len) = gen(s * 7919 + maxlen, alpha, 2000, maxlen, fin);
                    let d = &data[..len];
                    let mut got7: Vec<Vec<Vec<u8>>> = Vec::new();
                    rows7(d, 7, len, |f| got7.push(f.iter().map(|x| x.to_vec()).collect()));
                    assert_eq!(got7, reference::<7>(d), "rows7 seed {} maxlen {}", s, maxlen);
                    let mut got4: Vec<Vec<Vec<u8>>> = Vec::new();
                    rows4(d, 7, len, |f| got4.push(f.iter().map(|x| x.to_vec()).collect()));
                    assert_eq!(got4, reference::<4>(d), "rows4 seed {} maxlen {}", s, maxlen);
                    let mut got2: Vec<Vec<Vec<u8>>> = Vec::new();
                    rows2(d, 7, len, |f| got2.push(f.iter().map(|x| x.to_vec()).collect()));
                    assert_eq!(got2, reference::<2>(d), "rows2 seed {} maxlen {}", s, maxlen);
                    // split into chunks at arbitrary byte positions: same rows, each once
                    let mut parts: Vec<Vec<Vec<u8>>> = Vec::new();
                    let mut a = 7;
                    while a < len {
                        let b = (a + 1 + (s as usize * 131 + a) % 300).min(len);
                        rows7(d, a, b, |f| parts.push(f.iter().map(|x| x.to_vec()).collect()));
                        a = b;
                    }
                    assert_eq!(parts, got7, "rows7 chunked seed {}", s);
                }
            }
        }
    }

    /// Rows built from fields: mostly well-formed, with every kind of quoting slip.
    fn gen_rows(seed: u64, nfields: u64, lines: usize) -> (Vec<u8>, usize) {
        let mut rng = seed;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut data = b"header\n".to_vec();
        for _ in 0..lines {
            let n = match next() % 40 {
                0 => nfields - 1,
                1 => nfields + 1,
                2 => 0,
                _ => nfields,
            };
            for i in 0..n {
                if i > 0 {
                    data.push(b',');
                }
                let len = match next() % 10 {
                    0 => 0,
                    1 => 1,
                    2 => 25 + next() % 40,
                    _ => 1 + next() % 12,
                };
                let quoted = next() % 4 == 0;
                if quoted {
                    data.push(b'"');
                }
                for _ in 0..len {
                    let r = next();
                    data.push(match r % 60 {
                        0 => b'"',
                        1 => b'\r',
                        2 => b' ',
                        _ => b"abcxyz0189_-:TZ+"[(r >> 8) as usize % 16],
                    });
                }
                if quoted && next() % 30 != 0 {
                    data.push(b'"');
                }
            }
            data.push(b'\n');
        }
        let len = data.len();
        data.extend(std::iter::repeat(0u8).take(512));
        (data, len)
    }

    #[test]
    fn row_splitters_match_reference_on_rows() {
        let mut accepted = 0usize;
        for s in 1..80u64 {
            let (data, len) = gen_rows(s * 104729, 7, 3000);
            let d = &data[..len];
            let mut got: Vec<Vec<Vec<u8>>> = Vec::new();
            rows7(d, 7, len, |f| got.push(f.iter().map(|x| x.to_vec()).collect()));
            accepted += got.len();
            assert_eq!(got, reference::<7>(d), "rows7 seed {}", s);
            let mut old: Vec<Vec<Vec<u8>>> = Vec::new();
            chunk_fields::<7>(d, 7, len, |f| old.push(f.iter().map(|x| x.to_vec()).collect()));
            assert_eq!(got, old);
            let (data, len) = gen_rows(s * 31337, 4, 3000);
            let d = &data[..len];
            let mut got: Vec<Vec<Vec<u8>>> = Vec::new();
            rows4(d, 7, len, |f| got.push(f.iter().map(|x| x.to_vec()).collect()));
            accepted += got.len();
            assert_eq!(got, reference::<4>(d), "rows4 seed {}", s);
            let (data, len) = gen_rows(s * 7717, 2, 3000);
            let d = &data[..len];
            let mut got: Vec<Vec<Vec<u8>>> = Vec::new();
            rows2(d, 7, len, |f| got.push(f.iter().map(|x| x.to_vec()).collect()));
            accepted += got.len();
            assert_eq!(got, reference::<2>(d), "rows2 seed {}", s);
        }
        assert!(accepted > 100_000, "too few well-formed rows: {}", accepted);
    }

    #[test]
    fn window_splitter_matches_reference() {
        for s in 1..40u64 {
            check::<4>(s * 7919, b"ab,\"\" ,\r", 3000);
            check::<7>(s * 104729, b"abc,,\"\",x\"", 3000);
            check::<2>(s * 31337, b"a,\"", 3000);
        }
    }
}
