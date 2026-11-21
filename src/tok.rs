//! Streaming CSV tokenizer: every input byte is classified once, 64 bytes
//! at a time, and separator positions are flattened into a small index the
//! line loop reads. Same results as `csv::split` on every line.
//!
//! Stage 1 (per 64-byte block): masks of `,` `"` `\n`; S = separators. A
//! line is well formed only if every quote is either the first or the last
//! byte of its field (exactly one of: separator before it, separator after
//! it), and every field has an opening quote iff it has a closing one. The
//! first test is `Qs & !(S ^ S2)`, where Qs marks the byte after each quote
//! and S2 is S shifted by two bytes. For the second, opening quotes are
//! added to !S so each carry lands on the separator ending its field; those
//! landings must be exactly the separators preceded by a quote. No prefix
//! xor, so an unclosed quote needs no per-line reset. Bad bits are carried to
//! the line's newline with one more add (a carry runs through the
//! non-newline bits up to the next newline); the newline entries of bad
//! lines (rare) are flagged off the hot path.
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
            // Byte pos - 1 is a line end (a separator); no quote, no carries.
            let (mut ps, mut pq, mut cl, mut cb) = (1u64 << 63, 0u64, 0u8, 0u8);
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
                let s1 = (sep << 1) | (ps >> 63);
                let s2 = (sep << 2) | (ps >> 62);
                let qs = (q << 1) | (pq >> 63);
                ps = sep;
                pq = q;
                // A quote neither first nor last (or both) in its field.
                let bad1 = qs & !(sep ^ s2);
                // Opening quotes carried to the separator ending their field.
                let mut land = 0u64;
                cl = _addcarry_u64(cl, !sep, q & s1, &mut land);
                let bad = bad1 | ((land ^ qs) & sep);
                // Carry each bad bit to the next newline: ones at non-newlines.
                let mut t = 0u64;
                cb = _addcarry_u64(cb, !n, bad & !n, &mut t);
                let badnl = (t | bad) & n;
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
                // Flatten newlines (2 at a time); flag bad ones (rare).
                let ncnt = n.count_ones() as usize;
                let mut m = n;
                for k in 0..2 {
                    *nl.add(wn + k) = off + m.trailing_zeros();
                    m &= m.wrapping_sub(1);
                }
                if ncnt > 2 {
                    let mut k = 2;
                    while m != 0 {
                        *nl.add(wn + k) = off + m.trailing_zeros();
                        m &= m - 1;
                        k += 1;
                    }
                }
                if badnl != 0 {
                    mark_bad(nl.add(wn), n, badnl);
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
                if ent & BAD == 0 && *idx.add(k + N - 1) == end && (N > 1 || end != ls) {
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

/// Flags the newline entries (flattened from `n` at `nl`) of the lines with
/// a bit in `badnl`.
#[cold]
#[inline(never)]
unsafe fn mark_bad(nl: *mut u32, n: u64, badnl: u64) {
    let mut m = badnl;
    while m != 0 {
        let t = m.trailing_zeros();
        *nl.add((n & ((1u64 << t) - 1)).count_ones() as usize) |= BAD;
        m &= m - 1;
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

    type Lines = Vec<Vec<Vec<u8>>>;

    fn want<const N: usize>(text: &[u8]) -> Lines {
        let mut want = Vec::new();
        crate::csv::for_each_line(text, |line| {
            let mut out = [&b""[..]; N];
            if crate::csv::split(line, &mut out) {
                want.push(out.iter().map(|f| f.to_vec()).collect());
            }
        });
        want
    }

    /// Scans `text` (placed after a header line) as one range, and again as
    /// a few ranges split at line starts, and compares with `csv::split`.
    fn check_n<const N: usize>(text: &[u8], cuts: &[usize]) {
        let want = want::<N>(text);
        let mut file = vec![b'h'; PAD];
        file.push(b'\n');
        let s = file.len();
        file.extend_from_slice(text);
        let mut sc = Scanner::new();
        let mut run = |a: usize, b: usize, got: &mut Lines| {
            scan_file::<N>(&mut sc, &file, a, b, |p, st, en| {
                got.push((0..N).map(|f| unsafe { std::slice::from_raw_parts(p.add(st[f] as usize), (en[f] - st[f]) as usize).to_vec() }).collect());
            });
        };
        let mut got = Vec::new();
        run(s, file.len(), &mut got);
        assert_eq!(got, want, "N {} text {:?}", N, String::from_utf8_lossy(text));
        // Ranges split at line starts (like the parallel callers).
        let mut bounds = vec![s];
        for &c in cuts {
            let c = s + c % (text.len() + 1);
            let c = match memchr::memchr(b'\n', &file[c..]) {
                Some(x) => c + x + 1,
                None => file.len(),
            };
            if c > *bounds.last().unwrap() && c < file.len() {
                bounds.push(c);
            }
        }
        bounds.push(file.len());
        let mut got = Vec::new();
        for w in bounds.windows(2) {
            run(w[0], w[1], &mut got);
        }
        assert_eq!(got, want, "N {} cuts {:?} text {:?}", N, bounds, String::from_utf8_lossy(text));
    }

    fn check(text: &[u8], cuts: &[usize]) {
        check_n::<1>(text, cuts);
        check_n::<2>(text, cuts);
        check_n::<3>(text, cuts);
        check_n::<4>(text, cuts);
        check_n::<7>(text, cuts);
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
        for it in 0..4000 {
            let mut text = Vec::new();
            let lines = if it % 100 == 0 { 3000 } else { next() % 40 };
            if it % 300 == 7 {
                text.extend_from_slice(b"a,");
                text.extend(std::iter::repeat(b'x').take(40000));
                text.extend_from_slice(b",b\n\"");
                text.extend(std::iter::repeat(b'y').take(20000));
                text.extend_from_slice(b"\",,\n");
            }
            let fmax = [3, 5, 8, 10][it % 4];
            for _ in 0..lines {
                let fields = next() % fmax;
                for f in 0..fields {
                    if f > 0 {
                        text.push(b',');
                    }
                    let r = next();
                    if r % 4 == 0 {
                        // A field of random length, often quoted, to put
                        // quotes and separators at every block offset.
                        let quoted = r & 16 != 0;
                        if quoted {
                            text.push(b'"');
                        }
                        let len = (r >> 8) % 70;
                        text.extend(std::iter::repeat(b'v').take(len as usize));
                        if (r >> 20) % 50 == 0 {
                            text.push(b'"');
                        }
                        if quoted && (r >> 30) % 30 != 0 {
                            text.push(b'"');
                        }
                    } else {
                        text.extend_from_slice(pieces[(r >> 8) as usize % 16]);
                    }
                }
                text.push(b'\n');
            }
            if it % 500 == 3 {
                // Long lines (longer than a batch), good and bad.
                for k in 0..3 {
                    text.extend_from_slice(b"\"");
                    text.extend(std::iter::repeat(b'w').take(BATCH + 100 * k));
                    text.extend_from_slice(if k == 1 { b"\"x,b,c\n" } else { b"\",b,c\n" });
                }
            }
            if next() % 3 == 0 {
                text.pop();
            }
            let cuts = [next() as usize, next() as usize, next() as usize];
            check(&text, &cuts[..(it % 4)]);
        }
    }

    #[test]
    fn edges() {
        for t in [&b""[..], b"\n", b"\n\n", b"a", b"\"", b"\"\"", b"a,b,c", b"\"a\",\"\",\"c\"\n", b"\"a,b\nc\"\n1,2,3\n", b"1,2,3\n\"\n1,2,3\n\"\n1,2,3"] {
            check(t, &[]);
        }
    }
}
