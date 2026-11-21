//! Streaming CSV tokenizer: every input byte is classified once, 64 bytes
//! at a time, and the value ranges are written straight into the arrays the
//! callback reads, so the line loop does no per-field work. Same results as
//! `csv::split` on every line.
//!
//! Per 64-byte block: masks of separators S (`,` or `\n`), quotes Q and
//! newlines. Each separator yields exactly one content end (itself, or the
//! closing quote just before it) and one content start for the next field
//! (the byte after it, or the byte after an opening quote). Both masks are
//! flattened into arrays whose j-th entry belongs to the j-th separator of
//! the batch, so the fields of a line are N consecutive entries of each.
//!
//! A line is well formed only if every quote is either the first or the last
//! byte of its field (exactly one of: separator before it, separator after
//! it), and every field has an opening quote iff it has a closing one. The
//! first test is `Qs & !(S ^ S2)`, where Qs marks the byte after each quote
//! and S2 is S shifted by two bytes. For the second, opening quotes are
//! added to !S so each carry lands on the separator ending its field; those
//! landings must be exactly the separators preceded by a quote. Bad bits are
//! carried to the line's newline with one more add.
//!
//! Each newline records the batch index of its separator (flagged if its
//! line is bad): a line is well formed iff it is not flagged and has exactly
//! N separators, i.e. its index is N more than the previous newline's.

use std::arch::x86_64::*;

/// Input bytes per batch.
const BATCH: usize = 16 << 10;
/// Bytes after a field's end and before its start that callers may read.
pub const PAD: usize = 64;
const BAD: u32 = 1 << 31;

/// Separator, quote and newline masks of the 64 bytes at `p`.
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let eq = |v: __m256i, c: u8| _mm256_cmpeq_epi8(v, _mm256_set1_epi8(c as i8));
    let mm = |v: __m256i| _mm256_movemask_epi8(v) as u32 as u64;
    let (na, nb) = (eq(a, b'\n'), eq(b, b'\n'));
    let sa = _mm256_or_si256(eq(a, b','), na);
    let sb = _mm256_or_si256(eq(b, b','), nb);
    (mm(sa) | (mm(sb) << 32), mm(eq(a, b'"')) | (mm(eq(b, b'"')) << 32), mm(na) | (mm(nb) << 32))
}

/// `masks` with AVX-512BW: one load, three compares straight into mask registers.
#[target_feature(enable = "avx512f,avx512bw")]
#[inline]
unsafe fn masks512(p: *const u8) -> (u64, u64, u64) {
    let v = _mm512_loadu_si512(p as *const _);
    let n = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'\n' as i8));
    let c = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b',' as i8));
    let q = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'"' as i8));
    (c | n, q, n)
}

/// `masks` of the block at `b`, cleared from `lim` on.
#[inline(always)]
unsafe fn block<const W: bool>(data: *const u8, b: usize, lim: usize) -> (u64, u64, u64) {
    let (mut s, mut q, mut n) = if W { masks512(data.add(b)) } else { masks(data.add(b)) };
    if lim - b < 64 {
        let keep = (1u64 << (lim - b)) - 1;
        s &= keep;
        q &= keep;
        n &= keep;
    }
    (s, q, n)
}

/// Appends `off + i` for every set bit i of `m` to `out` at `*w`.
#[inline(always)]
unsafe fn flatten(out: *mut u32, w: &mut usize, m: u64, off: u32) {
    let cnt = m.count_ones() as usize;
    let p = out.add(*w);
    let mut x = m;
    for k in 0..7 {
        *p.add(k) = off.wrapping_add(x.trailing_zeros());
        x &= x.wrapping_sub(1);
    }
    if cnt > 7 {
        let mut k = 7;
        while x != 0 {
            *p.add(k) = off.wrapping_add(x.trailing_zeros());
            x &= x - 1;
            k += 1;
        }
    }
    *w += cnt;
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

pub struct Scanner {
    st: Vec<u32>,
    en: Vec<u32>,
    nl: Vec<u32>,
}

impl Scanner {
    pub fn new() -> Self {
        Scanner { st: vec![0; BATCH + 128], en: vec![0; BATCH + 128], nl: vec![0; BATCH + 128] }
    }

    /// Calls `each(starts, ends)` with the value ranges (absolute offsets,
    /// quotes removed) of every well-formed line of N fields in
    /// `[s, e)`. `s` is a line start; `e` is a line end + 1 or the end of
    /// `data`. Bytes from `s - PAD` to `e + PAD` must be readable, and a line
    /// that runs to `e` without `\n` must have one at `e` (callers pad).
    #[inline(always)]
    pub unsafe fn scan<const N: usize, const W: bool>(&mut self, data: *const u8, s: usize, e: usize, mut each: impl FnMut(&[u32; N], &[u32; N])) {
        let stp = self.st.as_mut_ptr();
        let enp = self.en.as_mut_ptr();
        let nlp = self.nl.as_mut_ptr();
        let mut pos = s;
        while pos < e {
            let lim = (pos + BATCH).min(e);
            // Entry j of st/en belongs to separator j of the batch; separator
            // 0 is the (virtual) line end at pos - 1.
            let (mut ws, mut we, mut wn) = (0usize, 1usize, 0usize);
            let mut nsep = 0u32;
            let (mut ps, mut pq, mut pn, mut po) = (1u64 << 63, 0u64, 1u64 << 63, 0u64);
            let (mut cl, mut cb) = (0u8, 0u8);
            let mut b = pos;
            let mut cur = block::<W>(data, b, lim);
            while b < lim {
                let nxt = if b + 64 < lim { block::<W>(data, b + 64, lim) } else { (0, 0, 0) };
                let (sep, q, n) = cur;
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
                let mut bad = bad1 | ((land ^ qs) & sep);
                if N == 1 {
                    // An empty line.
                    bad |= n & ((n << 1) | (pn >> 63));
                    pn = n;
                }
                // Carry each bad bit to the next newline: ones at non-newlines.
                let mut t = 0u64;
                cb = _addcarry_u64(cb, !n, bad & !n, &mut t);
                let badnl = (t | bad) & n;
                // Content starts: after each separator, past an opening quote.
                let o = s1 & q;
                let stm = (s1 & !q) | (o << 1) | (po >> 63);
                po = o;
                // Content ends: each separator, or the closing quote before it.
                let enm = (sep & !qs) | (q & ((sep >> 1) | (nxt.0 << 63)));
                let off = b as u32;
                flatten(stp, &mut ws, stm, off);
                flatten(enp, &mut we, enm, off);
                // Newlines: their separator's index; bad lines (rare) flagged off the hot path.
                let ncnt = n.count_ones() as usize;
                let p = nlp.add(wn);
                let nlu = if N >= 5 { 1 } else { 2 };
                let mut m = n;
                for k in 0..nlu {
                    *p.add(k) = nsep + (sep & (m ^ m.wrapping_sub(1))).count_ones();
                    m &= m.wrapping_sub(1);
                }
                if ncnt > nlu {
                    let mut k = nlu;
                    while m != 0 {
                        *p.add(k) = nsep + (sep & (m ^ (m - 1))).count_ones();
                        m &= m - 1;
                        k += 1;
                    }
                }
                if badnl != 0 {
                    mark_bad(p, n, badnl);
                }
                wn += ncnt;
                nsep += sep.count_ones();
                b += 64;
                cur = nxt;
            }
            // Complete lines.
            let mut prev = 0u32;
            for i in 0..wn {
                let raw = *nlp.add(i);
                if raw.wrapping_sub(prev) == N as u32 {
                    each(&*(stp.add(prev as usize) as *const [u32; N]), &*(enp.add(prev as usize + 1) as *const [u32; N]));
                }
                prev = raw & !BAD;
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
                    let (mut st, mut en) = ([0u32; N], [0u32; N]);
                    for f in 0..N {
                        st[f] = (out[f].as_ptr() as usize - data as usize) as u32;
                        en[f] = st[f] + out[f].len() as u32;
                    }
                    each(&st, &en);
                }
                pos = line_end + 1;
            } else {
                pos += memchr::memrchr(b'\n', std::slice::from_raw_parts(data.add(pos), lim - pos)).unwrap() + 1;
            }
        }
    }
}

/// Runs `scan` over `[s, e)` of a whole file: the part far from the file's
/// end directly, the rest from a padded copy (with a `\n` added when the
/// file does not end with one). `each` gets the line's base pointer.
#[inline(always)]
pub fn scan_file<const N: usize>(sc: &mut Scanner, file: &[u8], s: usize, e: usize, each: impl FnMut(*const u8, &[u32; N], &[u32; N])) {
    scan_file_w::<N, false>(sc, file, s, e, each)
}

/// `scan_file`, with AVX-512BW block masks when `W` (the caller must run on
/// such a CPU and be compiled with that feature for the masks to inline).
#[inline(always)]
pub fn scan_file_w<const N: usize, const W: bool>(sc: &mut Scanner, file: &[u8], s: usize, e: usize, mut each: impl FnMut(*const u8, &[u32; N], &[u32; N])) {
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
        unsafe { sc.scan::<N, W>(p, s, mid, |a, b| each(p, a, b)) };
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
        unsafe { sc.scan::<N, W>(p, PAD, end, |a, b| each(p, a, b)) };
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
    fn quotes_at_block_edges() {
        // Quoted and empty fields ending at every offset around 64-byte
        // block boundaries (separator and closing quote in different blocks).
        for pre in 0..140 {
            let mut text = Vec::new();
            for k in 0..6 {
                text.extend(std::iter::repeat(b'p').take(pre));
                text.extend_from_slice([&b",\"x\",\"\",,\"\"\n"[..], b",\"\"\n", b",a\"\n", b",\"\n", b"\n", b",,\n"][k]);
            }
            check(&text, &[pre, 3 * pre]);
        }
    }

    #[test]
    fn edges() {
        for t in [&b""[..], b"\n", b"\n\n", b"a", b"\"", b"\"\"", b"a,b,c", b"\"a\",\"\",\"c\"\n", b"\"a,b\nc\"\n1,2,3\n", b"1,2,3\n\"\n1,2,3\n\"\n1,2,3"] {
            check(t, &[]);
        }
    }
}
