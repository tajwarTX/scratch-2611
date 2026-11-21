//! The CSV dialect of rules.md: every comma separates fields; a field is
//! either free of quotes or wrapped whole in one pair of them.

/// Splits `line` into exactly N fields with quotes removed, or returns false
/// for a malformed line.
#[inline(always)]
pub fn split<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let n = line.len();
    if n == 0 {
        return false;
    }
    let mut i = 0usize;
    let mut f = 0usize;
    loop {
        let start = i;
        let mut quotes = 0u32;
        while i < n {
            let b = unsafe { *line.get_unchecked(i) };
            if b == b',' {
                break;
            }
            quotes += (b == b'"') as u32;
            i += 1;
        }
        let (mut fs, mut fe) = (start, i);
        if quotes != 0 {
            if quotes != 2 || fe - fs < 2 || line[fs] != b'"' || line[fe - 1] != b'"' {
                return false;
            }
            fs += 1;
            fe -= 1;
        }
        if f == N {
            return false;
        }
        out[f] = &line[fs..fe];
        f += 1;
        if i == n {
            break;
        }
        i += 1;
    }
    f == N
}

/// Calls `each` on every line of `data` (pieces between `\n`; a final piece
/// with no `\n` is a line too).
#[inline(always)]
pub fn for_each_line<'a>(data: &'a [u8], mut each: impl FnMut(&'a [u8])) {
    let mut p = 0usize;
    let e = data.len();
    while p < e {
        let nl = match memchr::memchr(b'\n', &data[p..]) {
            Some(x) => p + x,
            None => e,
        };
        each(&data[p..nl]);
        p = nl + 1;
    }
}

/// The byte offset after a file's header line.
pub fn after_header(data: &[u8]) -> usize {
    match memchr::memchr(b'\n', data) {
        Some(p) => p + 1,
        None => data.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s<const N: usize>(line: &str) -> Option<Vec<String>> {
        let mut out = [&b""[..]; N];
        if split(line.as_bytes(), &mut out) {
            Some(out.iter().map(|f| String::from_utf8(f.to_vec()).unwrap()).collect())
        } else {
            None
        }
    }

    #[test]
    fn dialect() {
        assert_eq!(s::<2>("a,b"), Some(vec!["a".into(), "b".into()]));
        assert_eq!(s::<2>("\"a\",\"\""), Some(vec!["a".into(), "".into()]));
        assert_eq!(s::<2>("a,"), Some(vec!["a".into(), "".into()]));
        assert_eq!(s::<2>(",b"), Some(vec!["".into(), "b".into()]));
        assert_eq!(s::<2>(" a , b"), Some(vec![" a ".into(), " b".into()]));
        assert_eq!(s::<2>("a,b,c"), None);
        assert_eq!(s::<2>("a"), None);
        assert_eq!(s::<2>(""), None);
        assert_eq!(s::<2>("a,b\""), None);
        assert_eq!(s::<2>("\"a,b\""), None);
        assert_eq!(s::<2>("\"a\"x,b"), None);
        assert_eq!(s::<2>("\",b"), None);
        assert_eq!(s::<2>("a\"\"b,c"), None);
        assert_eq!(s::<2>("a,b\r"), Some(vec!["a".into(), "b\r".into()]));
    }
}

/// Bitmasks of `,` `"` `\n` over the 32 * B bytes at `p` (bit i = byte i).
#[inline(always)]
unsafe fn masks<const B: usize>(p: *const u8) -> (u128, u128, u128) {
    use std::arch::x86_64::*;
    let (c, q, n) = (_mm256_set1_epi8(b',' as i8), _mm256_set1_epi8(b'"' as i8), _mm256_set1_epi8(b'\n' as i8));
    let (mut cm, mut qm, mut nm) = (0u128, 0u128, 0u128);
    for k in 0..B {
        let v = _mm256_loadu_si256(p.add(32 * k) as *const __m256i);
        cm |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, c)) as u32 as u128) << (32 * k);
        qm |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, q)) as u32 as u128) << (32 * k);
        nm |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, n)) as u32 as u128) << (32 * k);
    }
    (cm, qm, nm)
}

/// Splits the line starting at `p`, like `split`, using 32 * B bytes of
/// lookahead (the caller guarantees `p + 32 * B <= data.len()`).
/// Returns None when the line is too long for the window (use `split`);
/// otherwise the next line's start and whether the line is well formed, with
/// `out` holding absolute field ranges.
#[inline(always)]
pub fn split_simd<const N: usize, const B: usize>(data: &[u8], p: usize, out: &mut [(usize, usize); N]) -> Option<(usize, bool)> {
    debug_assert!(p + 32 * B <= data.len());
    let (cm, qm, nm) = unsafe { masks::<B>(data.as_ptr().add(p)) };
    if nm == 0 {
        return None;
    }
    let len = nm.trailing_zeros();
    let next = p + len as usize + 1;
    let range = (1u128 << len) - 1;
    let (cm, qm) = (cm & range, qm & range);
    if len == 0 || cm.count_ones() != (N - 1) as u32 {
        return Some((next, false));
    }
    let mut c = cm;
    let mut fs = 0u32;
    for slot in out.iter_mut().take(N - 1) {
        let fe = c.trailing_zeros();
        c &= c - 1;
        *slot = (fs as usize, fe as usize);
        fs = fe + 1;
    }
    out[N - 1] = (fs as usize, len as usize);
    if qm != 0 {
        for slot in out.iter_mut() {
            let (fs, fe) = (slot.0 as u32, slot.1 as u32);
            let field = ((1u128 << fe) - 1) & !((1u128 << fs) - 1);
            let fq = qm & field;
            if fq != 0 {
                if fe - fs < 2 || fq != (1u128 << fs) | (1u128 << (fe - 1)) {
                    return Some((next, false));
                }
                *slot = (fs as usize + 1, fe as usize - 1);
            }
        }
    }
    for slot in out.iter_mut() {
        slot.0 += p;
        slot.1 += p;
    }
    Some((next, true))
}

/// Calls `each` with the fields of every well-formed N-field line in
/// `data[start..end]`, where `data` is the whole file (so the SIMD window may
/// read past `end` but never past the file).
#[inline(always)]
pub fn for_each_record<'a, const N: usize, const B: usize>(
    data: &'a [u8],
    start: usize,
    end: usize,
    mut each: impl FnMut(&[&'a [u8]; N]),
) {
    let mut p = start;
    let mut ranges = [(0usize, 0usize); N];
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let fast_limit = data.len().saturating_sub(32 * B);
    while p < end {
        if p <= fast_limit {
            if let Some((next, ok)) = split_simd::<N, B>(data, p, &mut ranges) {
                if ok {
                    for i in 0..N {
                        fields[i] = unsafe { data.get_unchecked(ranges[i].0..ranges[i].1) };
                    }
                    each(&fields);
                }
                p = next;
                continue;
            }
        }
        let nl = match memchr::memchr(b'\n', &data[p..end]) {
            Some(x) => p + x,
            None => end,
        };
        if split(&data[p..nl], &mut fields) {
            each(&fields);
        }
        p = nl + 1;
    }
}

#[cfg(test)]
mod simd_tests {
    use super::*;

    #[test]
    fn simd_matches_scalar() {
        let lines = [
            "a,b,c", "\"a\",b,\"\"", "a,b", "a,b,c,d", "", "a,\"b,c", "a,b\",c", "\"a\"x,b,c", "\",b,c", " a , b ,c\r",
            ",,", "\"\",\"\",\"\"", "a,\"\"\",c", "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz,b,c",
        ];
        let mut data = Vec::new();
        for l in &lines {
            data.extend_from_slice(l.as_bytes());
            data.push(b'\n');
        }
        let mut padded = data.clone();
        padded.extend_from_slice(&[b'x'; 256]);
        let mut scalar = Vec::new();
        for_each_line(&data, |line| {
            let mut f = [&b""[..]; 3];
            if split(line, &mut f) {
                scalar.push(f.iter().map(|x| x.to_vec()).collect::<Vec<_>>());
            }
        });
        for b in [2usize, 4] {
            let mut simd = Vec::new();
            match b {
                2 => for_each_record::<3, 2>(&padded, 0, data.len(), |f| simd.push(f.iter().map(|x| x.to_vec()).collect::<Vec<_>>())),
                _ => for_each_record::<3, 4>(&padded, 0, data.len(), |f| simd.push(f.iter().map(|x| x.to_vec()).collect::<Vec<_>>())),
            }
            assert_eq!(simd, scalar);
        }
        let mut tail = Vec::new();
        for_each_record::<3, 4>(&data, 0, data.len(), |f| tail.push(f.iter().map(|x| x.to_vec()).collect::<Vec<_>>()));
        assert_eq!(tail, scalar);
    }
}
