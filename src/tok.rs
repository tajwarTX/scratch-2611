//! Streaming CSV tokenizer: every input byte is classified once, 64 bytes
//! at a time, and separator positions are flattened into a small index the
//! line loop reads. Same results as `csv::split` on every line.
//!
//! Stage 1 (per 64-byte block): masks of `,` `"` `\n`; X = quote parity
//! (prefix-xor, 1 from an opening quote up to its closing quote); a line is
//! bad if an opening quote is not at a field start, a closing quote is not
//! followed by a separator, or a separator is inside quotes. Bad bits are
//! carried to the line's newline with one add (a carry runs through the
//! non-newline bits up to the next newline), so each newline entry says
//! whether its line is bad.
//!
//! Stage 2 (per line): a line is well formed iff it is not bad and its N-th
//! separator is its newline.

use std::arch::x86_64::*;

/// Input bytes per stage-1 batch.
const BATCH: usize = 16 << 10;
/// Bytes after a field's end and before its start that callers may read.
pub const PAD: usize = 64;
const BAD: u32 = 1 << 31;

#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let m = |v: __m256i, c: u8| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(c as i8))) as u32 as u64;
    (
        m(a, b',') | (m(b, b',') << 32),
        m(a, b'"') | (m(b, b'"') << 32),
        m(a, b'\n') | (m(b, b'\n') << 32),
    )
}

#[inline(always)]
fn prefix_xor(x: u64) -> u64 {
    unsafe {
        let v = _mm_clmulepi64_si128(_mm_set_epi64x(0, x as i64), _mm_set1_epi8(-1), 0);
        _mm_cvtsi128_si64(v) as u64
    }
}

/// Quote parity with a reset after every newline (the rare case of an
/// unclosed quote): bit i = inside quotes at byte i. Returns (X, parity out).
#[cold]
fn parity_slow(q: u64, n: u64, mut par: u64) -> (u64, u64) {
    let mut x = 0u64;
    for i in 0..64 {
        par ^= (q >> i) & 1;
        x |= par << i;
        if (n >> i) & 1 != 0 {
            par = 0;
        }
    }
    (x, par)
}

pub struct Scanner {
    idx: Vec<u32>,
    nl: Vec<u32>,
}

impl Scanner {
    pub fn new() -> Self {
        Scanner { idx: vec![0; BATCH + 64], nl: vec![0; BATCH + 64] }
    }

    /// Calls `each(starts, ends)` with the value ranges (absolute offsets,
    /// quotes removed) of every well-formed line of N fields in
    /// `[s, e)`. `s` is a line start; `e` is a line end + 1 or the end of
    /// `data`. Bytes from `s - PAD` to `e + PAD` must be readable, and a line
    /// that runs to `e` without `\n` must have one at `e` (callers pad).
    #[inline(always)]
    pub unsafe fn scan<const N: usize>(&mut self, data: *const u8, s: usize, e: usize, mut each: impl FnMut(&[u32; N], &[u32; N])) {
        let idx = self.idx.as_mut_ptr();
        let nl = self.nl.as_mut_ptr();
        let mut pos = s;
        while pos < e {
            let lim = (pos + BATCH).min(e);
            // ---- stage 1 over [pos, lim)
            let (mut wi, mut wn) = (0usize, 0usize);
            let (mut cx, mut ps, mut cl, mut carry) = (0u64, 1u64, 0u64, false);
            let mut b = pos;
            while b < lim {
                let (mut c, mut q, mut n) = masks(data.add(b));
                if lim - b < 64 {
                    let keep = (1u64 << (lim - b)) - 1;
                    c &= keep;
                    q &= keep;
                    n &= keep;
                }
                let sep = c | n;
                let mut x = prefix_xor(q) ^ cx;
                if x & n != 0 {
                    let (xs, par) = parity_slow(q, n, cx & 1);
                    x = xs;
                    cx = 0u64.wrapping_sub(par);
                } else {
                    cx = ((x as i64) >> 63) as u64;
                }
                let open = q & x;
                let close = q & !x;
                let prev = (sep << 1) | ps;
                ps = sep >> 63;
                let after = (close << 1) | cl;
                cl = close >> 63;
                let bad = (open & !prev) | (after & !sep) | (x & sep);
                // Carry each bad bit to the next newline: ones at non-newlines.
                let bnl = bad & n;
                let (s1, o1) = (!n).overflowing_add(bad & !n);
                let (s2, o2) = s1.overflowing_add(carry as u64);
                carry = o1 | o2;
                let badnl = ((s2 & n) | bnl) & n;
                // Flatten separators (8 at a time, unconditionally).
                let off = (b - pos) as u32;
                let cnt = sep.count_ones() as usize;
                let mut m = sep;
                for k in 0..8 {
                    *idx.add(wi + k) = off + m.trailing_zeros();
                    m &= m.wrapping_sub(1);
                }
                if cnt > 8 {
                    let mut k = 8;
                    while m != 0 {
                        *idx.add(wi + k) = off + m.trailing_zeros();
                        m &= m - 1;
                        k += 1;
                    }
                }
                wi += cnt;
                // Flatten newlines with their bad flag.
                let ncnt = n.count_ones() as usize;
                let mut m = n;
                for k in 0..2 {
                    let t = m.trailing_zeros();
                    *nl.add(wn + k) = off + t | (((badnl >> (t & 63)) & 1) as u32) << 31;
                    m &= m.wrapping_sub(1);
                }
                if ncnt > 2 {
                    let mut k = 2;
                    while m != 0 {
                        let t = m.trailing_zeros();
                        *nl.add(wn + k) = off + t | (((badnl >> t) & 1) as u32) << 31;
                        m &= m - 1;
                        k += 1;
                    }
                }
                wn += ncnt;
                b += 64;
            }
            for k in 0..N {
                *idx.add(wi + k) = u32::MAX;
            }
            // ---- stage 2: complete lines
            let mut k = 0usize;
            let mut ls = 0u32;
            let mut st = [0u32; N];
            let mut en = [0u32; N];
            let base = pos as u32;
            for i in 0..wn {
                let ent = *nl.add(i);
                let end = ent & !BAD;
                if ent & BAD == 0 && *idx.add(k + N - 1) == end {
                    let mut f0 = ls;
                    for f in 0..N {
                        let fe = *idx.add(k + f);
                        let q = (*data.add(pos + f0 as usize) == b'"') as u32;
                        st[f] = base + f0 + q;
                        en[f] = base + fe - q;
                        f0 = fe + 1;
                    }
                    k += N;
                    each(&st, &en);
                } else {
                    while *idx.add(k) != end {
                        k += 1;
                    }
                    k += 1;
                }
                ls = end + 1;
            }
            if wn == 0 {
                // A line longer than a batch: hand it to the slow path.
                let line_end = match memchr::memchr(b'\n', std::slice::from_raw_parts(data.add(pos), e - pos)) {
                    Some(x) => pos + x,
                    None => e,
                };
                let line = std::slice::from_raw_parts(data.add(pos), line_end - pos);
                let mut out = [&b""[..]; N];
                if crate::csv::split(line, &mut out) {
                    for f in 0..N {
                        st[f] = (out[f].as_ptr() as usize - data as usize) as u32;
                        en[f] = st[f] + out[f].len() as u32;
                    }
                    each(&st, &en);
                }
                pos = line_end + 1;
            } else {
                pos += ls as usize;
            }
        }
    }
}

/// Runs `scan` over `[s, e)` of a whole file: the part far from the file's
/// end directly, the rest from a padded copy (with a `\n` added when the
/// file does not end with one). `each` gets the line's base pointer.
#[inline(always)]
pub fn scan_file<const N: usize>(sc: &mut Scanner, file: &[u8], s: usize, e: usize, mut each: impl FnMut(*const u8, &[u32; N], &[u32; N])) {
    let len = file.len();
    let safe = len.saturating_sub(PAD + 64);
    let mut mid = e;
    if e > safe {
        // Split at the last line start at or before `safe`.
        mid = if safe <= s {
            s
        } else {
            match memchr::memrchr(b'\n', &file[s..safe]) {
                Some(x) => s + x + 1,
                None => s,
            }
        };
    }
    if s.max(PAD) == s && mid > s {
        let p = file.as_ptr();
        unsafe { sc.scan::<N>(p, s, mid, |a, b| each(p, a, b)) };
    } else {
        mid = s;
    }
    if mid < e {
        let n = e - mid;
        let mut buf = vec![0u8; PAD + n + 1 + PAD + 64];
        buf[PAD..PAD + n].copy_from_slice(&file[mid..e]);
        let mut end = PAD + n;
        if file[e - 1] != b'\n' {
            buf[end] = b'\n';
            end += 1;
        }
        let p = buf.as_ptr();
        unsafe { sc.scan::<N>(p, PAD, end, |a, b| each(p, a, b)) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(text: &[u8]) {
        // Reference: split every line.
        let mut want: Vec<Vec<Vec<u8>>> = Vec::new();
        crate::csv::for_each_line(text, |line| {
            let mut out = [&b""[..]; 3];
            if crate::csv::split(line, &mut out) {
                want.push(out.iter().map(|f| f.to_vec()).collect());
            }
        });
        let mut file = vec![b'h'; PAD];
        file.push(b'\n');
        let s = file.len();
        file.extend_from_slice(text);
        let mut got: Vec<Vec<Vec<u8>>> = Vec::new();
        let mut sc = Scanner::new();
        scan_file::<3>(&mut sc, &file, s, file.len(), |p, st, en| {
            got.push((0..3).map(|f| unsafe { std::slice::from_raw_parts(p.add(st[f] as usize), (en[f] - st[f]) as usize).to_vec() }).collect());
        });
        assert_eq!(got, want, "text {:?}", String::from_utf8_lossy(text));
    }

    #[test]
    fn matches_split() {
        let pieces: [&[u8]; 16] = [
            b"a", b"\"b\"", b"", b"\"\"", b"\"", b"x\"y", b"\"q\"r", b"\r", b"  ", b"\"a,b\"", b"\"\"\"", b"abcdefghijklmnopqrstuvwxyz0123456789",
            b"\"long quoted value with spaces .........................................\"", b",", b"\n", b"z",
        ];
        let mut rng = 12345u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for it in 0..3000 {
            let mut text = Vec::new();
            let lines = if it % 100 == 0 { 3000 } else { next() % 40 };
            if it % 300 == 7 {
                text.extend_from_slice(b"a,");
                text.extend(std::iter::repeat(b'x').take(40000));
                text.extend_from_slice(b",b\n\"");
                text.extend(std::iter::repeat(b'y').take(20000));
                text.extend_from_slice(b"\",,\n");
            }
            for _ in 0..lines {
                let fields = next() % 5;
                for f in 0..fields {
                    if f > 0 {
                        text.push(b',');
                    }
                    text.extend_from_slice(pieces[(next() % 16) as usize]);
                }
                text.push(b'\n');
            }
            if next() % 3 == 0 {
                text.pop();
            }
            check(&text);
        }
    }
}
