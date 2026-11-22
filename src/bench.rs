//! Single-thread micro-benchmarks of the parse stages (BENCH=1): less
//! sensitive to other load than whole runs. Prints ns per line per stage.

use std::time::Instant;

use crate::csv::split;
use crate::fast;
use crate::legacy::{list_csv, map_file};

pub fn run() {
    // Match production defaults rather than Scanner's standalone default.
    crate::tok::TOK.store(
        std::env::var("TOK").unwrap_or_else(|_| "1".into()).parse().expect("TOK"),
        std::sync::atomic::Ordering::Relaxed,
    );
    crate::tok::PREFETCH.store(
        std::env::var("PF").unwrap_or_else(|_| "0".into()) == "1",
        std::sync::atomic::Ordering::Relaxed,
    );
    if std::env::var("BENCH").unwrap_or_default().starts_with("mt") {
        return mt();
    }
    let data = std::path::PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let maps: Vec<_> = list_csv(&data.join("events")).iter().filter_map(|p| map_file(p)).collect();
    // The largest file only, so the run is a few seconds.
    let m = maps.iter().max_by_key(|m| m.len()).unwrap();
    let file: &[u8] = &m[..];
    let start = crate::csv::after_header(file);
    let limit = file.len() - fast::LOOKAHEAD;
    let mut lines = 0u64;
    // Warm the page cache and count lines.
    let t = Instant::now();
    let mut f = [(0u32, 0u32); 7];
    for &b in file {
        lines += (b == b'\n') as u64;
    }
    eprintln!("lines {} ({:.0} ns/line to count bytes)", lines, t.elapsed().as_nanos() as f64 / lines as f64);
    let per = |name: &str, t: Instant, sink: u64| {
        eprintln!("{:<12} {:>6.1} ns/line   (sink {})", name, t.elapsed().as_nanos() as f64 / lines as f64, sink % 7);
    };
    let only = std::env::var("BENCH").unwrap_or_default();
    let on = |n: &str| only == "1" || only == n;
    {
        // 1. Tokenize only.
        if on("tok") {
        let t = Instant::now();
        let mut p = start;
        let mut sink = 0u64;
        while p < limit {
            match fast::tokenize::<7>(file, p, &mut f) {
                Some((next, ok)) => {
                    sink += ok as u64 + f[3].0 as u64;
                    p = next;
                }
                None => p += memchr::memchr(b'\n', &file[p..]).unwrap() + 1,
            }
        }
        per("tokenize", t, sink);
        }
        // 2. Tokenize + pack id + pack user.
        if on("pack") {
        let t = Instant::now();
        let mut p = start;
        let mut sink = 0u64;
        while p < limit {
            match fast::tokenize::<7>(file, p, &mut f) {
                Some((next, ok)) => {
                    if ok {
                        let a = fast::pack(unsafe { file.as_ptr().add(p + f[0].0 as usize) }, (f[0].1 - f[0].0) as usize);
                        let b = fast::pack(unsafe { file.as_ptr().add(p + f[1].0 as usize) }, (f[1].1 - f[1].0) as usize);
                        sink = sink.wrapping_add(a.unwrap_or(1) ^ b.unwrap_or(3));
                    }
                    p = next;
                }
                None => p += memchr::memchr(b'\n', &file[p..]).unwrap() + 1,
            }
        }
        per("+pack x2", t, sink);
        }
        // 2b. tokenize2.
        if on("tok2") {
        let t = Instant::now();
        let mut p = start;
        let mut sink = 0u64;
        while p < limit {
            match fast::tokenize2::<7>(file, p, &mut f) {
                Some((next, ok)) => {
                    sink += ok as u64 + f[3].0 as u64;
                    p = next;
                }
                None => p += memchr::memchr(b'\n', &file[p..]).unwrap() + 1,
            }
        }
        per("tokenize2", t, sink);
        }
        // 4. tokenize2 + packs + both timestamps (+ validation-ish checks).
        let manifest = std::fs::read_to_string(data.join("manifest.json")).unwrap();
        let ws = crate::ts::parse_ts(crate::legacy::manifest_value(&manifest, "window_start").unwrap().as_bytes()).unwrap();
        let fts = crate::ts::FastTs::new(ws - 17 * 86_400_000, ws + 24 * 86_400_000);
        let tsf = |s: &[u8]| -> Option<i64> {
            let n = s.len();
            if (1..=16).contains(&n) {
                return fast::digits16(unsafe { s.as_ptr().add(n) }, n).map(|v| v as i64);
            }
            fts.parse(s)
        };
        if on("ts") || on("ts1") {
            let both = on("ts");
            let t = Instant::now();
            let mut p = start;
            let mut sink = 0u64;
            while p < limit {
                match fast::tokenize2::<7>(file, p, &mut f) {
                    Some((next, ok)) => {
                        if ok {
                            let fld = |i: usize| &file[p + f[i].0 as usize..p + f[i].1 as usize];
                            let a = fast::pack(fld(0).as_ptr(), fld(0).len()).unwrap_or(1);
                            let b = fast::pack(fld(1).as_ptr(), fld(1).len()).unwrap_or(3);
                            let ing = if both { tsf(fld(3)).unwrap_or(5) } else { 0 };
                            let ts = tsf(fld(2)).unwrap_or(7);
                            let ty = fld(4);
                            let v = (ty == b"view") as u64 + (ty == b"cart") as u64 + (ty == b"purchase") as u64;
                            sink = sink.wrapping_add(a ^ b ^ (ing as u64) ^ (ts as u64) ^ v);
                        }
                        p = next;
                    }
                    None => p += memchr::memchr(b'\n', &file[p..]).unwrap() + 1,
                }
            }
            per(if both { "+ts x2" } else { "+ts x1" }, t, sink);
        }
        // REPS=k repeats the scan benches and reports the fastest run.
        let reps: usize = std::env::var("REPS").ok().and_then(|s| s.parse().ok()).unwrap_or(1);
        let best = |name: &str, run: &mut dyn FnMut() -> u64| {
            let mut min = f64::MAX;
            let mut sink = 0;
            for _ in 0..reps {
                let t = Instant::now();
                sink = run();
                min = min.min(t.elapsed().as_nanos() as f64 / lines as f64);
            }
            eprintln!("{:<12} {:>6.1} ns/line   (sink {}, min of {})", name, min, sink % 7, reps);
        };
        if on("scan") {
            let mut sc = crate::tok::Scanner::new();
            best("scan", &mut || {
                let mut sink = 0u64;
                crate::tok::scan_file::<7>(&mut sc, file, start, file.len(), |_, a, b| {
                    sink += (a[3] ^ b[5] ^ a[0] ^ b[6]) as u64;
                });
                sink
            });
        }
        if on("scanall") {
            // Like scan, but the callback reads every field (as real callers do).
            let mut sc = crate::tok::Scanner::new();
            best("scanall", &mut || {
                let mut sink = 0u64;
                crate::tok::scan_file::<7>(&mut sc, file, start, file.len(), |_, a, b| {
                    for i in 0..7 {
                        sink = sink.wrapping_add((a[i] ^ b[i]) as u64);
                    }
                });
                sink
            });
        }
        if on("read") {
            // Memory floor: one AVX2 pass over the bytes.
            let t = Instant::now();
            let sink = unsafe {
                use std::arch::x86_64::*;
                let mut acc = _mm256_setzero_si256();
                let mut i = start;
                while i + 64 <= file.len() {
                    let a = _mm256_loadu_si256(file.as_ptr().add(i) as *const __m256i);
                    let b = _mm256_loadu_si256(file.as_ptr().add(i + 32) as *const __m256i);
                    acc = _mm256_add_epi64(acc, _mm256_sad_epu8(_mm256_xor_si256(a, b), _mm256_setzero_si256()));
                    i += 64;
                }
                _mm256_extract_epi64(acc, 0) as u64
            };
            per("read", t, sink);
        }
        if on("tok3") {
            let t = Instant::now();
            let mut p = start;
            let mut sink = 0u64;
            let mut g = fast::Fields::<7> { start: [0; 7], end: [0; 7] };
            while p < limit {
                match fast::tokenize3::<7>(file, p, &mut g) {
                    Some((next, ok)) => {
                        sink += ok as u64 + g.start[3] as u64;
                        p = next;
                    }
                    None => p += memchr::memchr(b'\n', &file[p..]).unwrap() + 1,
                }
            }
            per("tokenize3", t, sink);
        }
        // 3. Scalar split for comparison.
        if on("split") {
        let t = Instant::now();
        let mut sink = 0u64;
        let mut g = [&b""[..]; 7];
        crate::csv::for_each_line(&file[start..], |line| {
            sink += split(line, &mut g) as u64;
        });
        per("scalar split", t, sink);
        }
    }
}

/// Multithreaded floors over all event and click files (BENCH=mt-touch, mt-sum, mt-tok3).
fn mt() {
    use crate::legacy::{chunks, run_jobs, Job};
    let mode = std::env::var("BENCH").unwrap();
    let data = std::path::PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let threads: usize = std::env::var("NCPU").ok().and_then(|s| s.parse().ok()).unwrap_or(8);
    let t0 = Instant::now();
    let mut maps: Vec<Option<memmap2::Mmap>> = list_csv(&data.join("events")).iter().map(|p| map_file(p)).collect();
    maps.extend(list_csv(&data.join("clicks")).iter().map(|p| map_file(p)));
    let mut jobs = Vec::new();
    chunks(&maps, Job::Events, &mut jobs);
    let t1 = Instant::now();
    let mut sinks: Vec<(u64, crate::tok::Scanner)> = (0..threads).map(|_| (0u64, crate::tok::Scanner::new())).collect();
    run_jobs(&mut sinks, jobs.len(), |(sink, sc), j| {
        let Job::Events(fi, s, e) = jobs[j] else { return };
        let file: &[u8] = maps[fi].as_ref().unwrap();
        match mode.as_str() {
            "mt-touch" => {
                let mut i = s;
                while i < e {
                    *sink += file[i] as u64;
                    i += 4096;
                }
            }
            "mt-sum" => {
                unsafe {
                    use std::arch::x86_64::*;
                    let mut acc = _mm256_setzero_si256();
                    let mut i = s;
                    while i + 32 <= e {
                        acc = _mm256_add_epi64(acc, _mm256_sad_epu8(_mm256_loadu_si256(file.as_ptr().add(i) as *const __m256i), _mm256_setzero_si256()));
                        i += 32;
                    }
                    *sink += _mm256_extract_epi64(acc, 0) as u64;
                }
            }
            "mt-scan" => {
                crate::tok::scan_file::<7>(sc, file, s, e, |_, st, en| {
                    *sink += (st[3] ^ en[1]) as u64;
                });
            }
            _ => {
                let limit = file.len().saturating_sub(fast::LOOKAHEAD);
                let mut g = fast::Fields::<7> { start: [0; 7], end: [0; 7] };
                let mut p = s;
                while p < e && p < limit {
                    match fast::tokenize3::<7>(file, p, &mut g) {
                        Some((next, ok)) => {
                            *sink += ok as u64 + g.start[3] as u64;
                            p = next;
                        }
                        None => p += memchr::memchr(b'\n', &file[p..]).unwrap() + 1,
                    }
                }
            }
        }
    });
    eprintln!("{} map {:.3}s run {:.3}s sink {}", mode, (t1 - t0).as_secs_f64(), t1.elapsed().as_secs_f64(), sinks.iter().map(|x| x.0).sum::<u64>() % 7);
}
