//! Probe helper: page-cache residency of every file under a folder (mincore),
//! and with --drop, evicts them (posix_fadvise DONTNEED) to simulate a cold read.
use std::os::unix::io::AsRawFd;

extern "C" {
    fn mincore(addr: *mut u8, len: usize, vec: *mut u8) -> i32;
    fn posix_fadvise(fd: i32, offset: i64, len: i64, advice: i32) -> i32;
}

fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else {
                out.push(p);
            }
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let drop = args.iter().any(|a| a == "--drop");
    let mut files = Vec::new();
    walk(std::path::Path::new(&args[1]), &mut files);
    let (mut total, mut resident) = (0u64, 0u64);
    for f in &files {
        let Ok(file) = std::fs::File::open(f) else { continue };
        let len = file.metadata().map(|m| m.len()).unwrap_or(0) as usize;
        if len == 0 {
            continue;
        }
        if drop {
            unsafe { posix_fadvise(file.as_raw_fd(), 0, 0, 4) };
        }
        let Ok(m) = (unsafe { memmap2::Mmap::map(&file) }) else { continue };
        let pages = len.div_ceil(4096);
        let mut v = vec![0u8; pages];
        if unsafe { mincore(m.as_ptr() as *mut u8, len, v.as_mut_ptr()) } == 0 {
            resident += v.iter().filter(|&&b| b & 1 == 1).count() as u64 * 4096;
        }
        total += len as u64;
    }
    eprintln!("{} files, {} MB, {:.1}% resident", files.len(), total >> 20, 100.0 * resident as f64 / total.max(1) as f64);
}
