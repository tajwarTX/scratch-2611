//! Single-thread micro-benchmarks of the parse stages (BENCH=1): less
//! sensitive to other load than whole runs. Prints ns per line per stage.

use std::time::Instant;

use crate::csv::split;
use crate::fast;
use crate::legacy::{list_csv, map_file};

pub fn run() {
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
