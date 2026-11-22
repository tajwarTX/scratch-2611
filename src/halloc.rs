//! Global allocator. Blocks of `BIG` bytes or more are mapped directly, 2 MB
//! aligned and marked for transparent huge pages, and (HPOP=1, the default)
//! faulted in with one MADV_POPULATE_WRITE call by the allocating thread
//! instead of one page fault at a time, all threads queueing on the
//! process's memory lock. Smaller blocks go to mimalloc.

use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicU8, Ordering};

extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn munmap(addr: *mut u8, len: usize) -> i32;
    fn mremap(old: *mut u8, old_len: usize, new_len: usize, flags: i32, ...) -> *mut u8;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
}

const PROT_READ_WRITE: i32 = 0x1 | 0x2;
const MAP_PRIVATE: i32 = 0x02;
const MAP_ANONYMOUS: i32 = 0x20;
const MAP_FAILED: *mut u8 = !0usize as *mut u8;
const MADV_HUGEPAGE: i32 = 14;
const MADV_POPULATE_WRITE: i32 = 23;
const MREMAP_MAYMOVE: i32 = 1;

const HUGE: usize = 2 << 20;
/// Threshold for mapped blocks: HBIG (MB, 0 = never), 4 MB by default. Read
/// once, before the first large allocation, so every block keeps its kind.
static BIG: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[inline]
fn big() -> usize {
    let b = BIG.load(Ordering::Relaxed);
    if b != 0 {
        return b;
    }
    let v = match std::env::var_os("HBIG") {
        Some(v) if v == "0" => usize::MAX,
        Some(v) => v.to_str().and_then(|t| t.parse::<usize>().ok()).map_or(4 << 20, |m| m << 20),
        None => 4 << 20,
    };
    BIG.store(v, Ordering::Relaxed);
    v
}

/// 0: not read yet, 1: populate, 2: do not.
static POPULATE: AtomicU8 = AtomicU8::new(0);

fn populate() -> bool {
    match POPULATE.load(Ordering::Relaxed) {
        1 => true,
        2 => false,
        _ => {
            // Reading the variable allocates only small blocks (mimalloc), never re-entering here.
            let on = std::env::var_os("HPOP").map_or(true, |v| v != "0");
            POPULATE.store(if on { 1 } else { 2 }, Ordering::Relaxed);
            on
        }
    }
}

#[inline]
fn round(n: usize) -> usize {
    (n + HUGE - 1) & !(HUGE - 1)
}

unsafe fn map_big(size: usize) -> *mut u8 {
    let len = round(size);
    let raw = mmap(std::ptr::null_mut(), len + HUGE, PROT_READ_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if raw == MAP_FAILED {
        return std::ptr::null_mut();
    }
    // Trim to a 2 MB aligned block of `len` bytes.
    let start = round(raw as usize) as *mut u8;
    let head = start as usize - raw as usize;
    if head != 0 {
        munmap(raw, head);
    }
    let tail = HUGE - head;
    if tail != 0 {
        munmap(start.add(len), tail);
    }
    madvise(start, len, MADV_HUGEPAGE);
    if populate() {
        // Older kernels reject the advice: pages then fault in as usual.
        madvise(start, len, MADV_POPULATE_WRITE);
    }
    start
}

pub struct HugeAlloc;

unsafe impl GlobalAlloc for HugeAlloc {
    #[inline]
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if l.size() >= big() && l.align() <= HUGE {
            return map_big(l.size());
        }
        mimalloc::MiMalloc.alloc(l)
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        if l.size() >= big() && l.align() <= HUGE {
            // Fresh anonymous memory reads as zero.
            return map_big(l.size());
        }
        mimalloc::MiMalloc.alloc_zeroed(l)
    }

    #[inline]
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        if l.size() >= big() && l.align() <= HUGE {
            munmap(p, round(l.size()));
            return;
        }
        mimalloc::MiMalloc.dealloc(p, l)
    }

    unsafe fn realloc(&self, p: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
        let old_big = l.size() >= big() && l.align() <= HUGE;
        let new_big = new_size >= big() && l.align() <= HUGE;
        if old_big && new_big {
            let (ol, nl) = (round(l.size()), round(new_size));
            if ol == nl {
                return p;
            }
            let q = mremap(p, ol, nl, MREMAP_MAYMOVE);
            if q == MAP_FAILED {
                return std::ptr::null_mut();
            }
            if nl > ol {
                madvise(q.add(ol), nl - ol, MADV_HUGEPAGE);
                if populate() {
                    madvise(q.add(ol), nl - ol, MADV_POPULATE_WRITE);
                }
            }
            return q;
        }
        if !old_big && !new_big {
            return mimalloc::MiMalloc.realloc(p, l, new_size);
        }
        let nl = Layout::from_size_align_unchecked(new_size, l.align());
        let q = self.alloc(nl);
        if !q.is_null() {
            std::ptr::copy_nonoverlapping(p, q, l.size().min(new_size));
            self.dealloc(p, l);
        }
        q
    }
}
