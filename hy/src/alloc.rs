//! Global allocator: big blocks come straight from mmap, 2 MB aligned, with
//! MADV_HUGEPAGE, and grow with mremap. Small blocks go to the system allocator.

use std::alloc::{GlobalAlloc, Layout, System};

extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn munmap(addr: *mut u8, len: usize) -> i32;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
    fn mremap(old: *mut u8, old_len: usize, new_len: usize, flags: i32, ...) -> *mut u8;
}

const BIG: usize = 1 << 20;
const HUGE: usize = 1 << 21;
const PROT_RW: i32 = 3;
const MAP_PRIVATE_ANON: i32 = 0x02 | 0x20;
const MADV_HUGEPAGE: i32 = 14;
const MREMAP_MAYMOVE: i32 = 1;

pub struct HugeAlloc;
pub static NOHUGE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
pub static POPULATE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
/// Ask for ordinary 4 KB pages explicitly (timing comparisons).
pub static SMALLPAGES: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
const MADV_NOHUGEPAGE: i32 = 15;
const MADV_POPULATE_WRITE: i32 = 23;

#[inline]
fn round(n: usize) -> usize {
    (n + HUGE - 1) & !(HUGE - 1)
}

unsafe fn big_alloc(size: usize) -> *mut u8 {
    let len = round(size);
    let raw = mmap(std::ptr::null_mut(), len + HUGE, PROT_RW, MAP_PRIVATE_ANON, -1, 0);
    if raw as isize == -1 {
        return std::ptr::null_mut();
    }
    let addr = raw as usize;
    let aligned = (addr + HUGE - 1) & !(HUGE - 1);
    let head = aligned - addr;
    if head > 0 {
        munmap(raw, head);
    }
    let tail = HUGE - head;
    if tail > 0 {
        munmap((aligned + len) as *mut u8, tail);
    }
    if SMALLPAGES.load(std::sync::atomic::Ordering::Relaxed) {
        madvise(aligned as *mut u8, len, MADV_NOHUGEPAGE);
    } else if !NOHUGE.load(std::sync::atomic::Ordering::Relaxed) {
        madvise(aligned as *mut u8, len, MADV_HUGEPAGE);
    }
    if POPULATE.load(std::sync::atomic::Ordering::Relaxed) {
        madvise(aligned as *mut u8, len, MADV_POPULATE_WRITE);
    }
    aligned as *mut u8
}

unsafe impl GlobalAlloc for HugeAlloc {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() >= BIG && layout.align() <= 4096 {
            big_alloc(layout.size())
        } else {
            System.alloc(layout)
        }
    }
    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if layout.size() >= BIG && layout.align() <= 4096 {
            big_alloc(layout.size()) // fresh anonymous memory is zeroed
        } else {
            System.alloc_zeroed(layout)
        }
    }
    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if layout.size() >= BIG && layout.align() <= 4096 {
            munmap(ptr, round(layout.size()));
        } else {
            System.dealloc(ptr, layout)
        }
    }
    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let old_big = layout.size() >= BIG && layout.align() <= 4096;
        let new_big = new_size >= BIG && layout.align() <= 4096;
        if old_big && new_big {
            let ol = round(layout.size());
            let nl = round(new_size);
            if ol == nl {
                return ptr;
            }
            if nl < ol {
                munmap(ptr.add(nl), ol - nl);
                return ptr;
            }
            // grow: allocate aligned region and move pages there with mremap(FIXED) is fiddly;
            // MAYMOVE keeps page alignment of the start (2 MB aligned target not guaranteed).
            let p = mremap(ptr, ol, nl, MREMAP_MAYMOVE);
            if p as isize == -1 {
                return std::ptr::null_mut();
            }
            if SMALLPAGES.load(std::sync::atomic::Ordering::Relaxed) {
                madvise(p, nl, MADV_NOHUGEPAGE);
            } else if !NOHUGE.load(std::sync::atomic::Ordering::Relaxed) {
                madvise(p, nl, MADV_HUGEPAGE);
            }
            return p;
        }
        if !old_big && !new_big {
            return System.realloc(ptr, layout, new_size);
        }
        let nlay = Layout::from_size_align_unchecked(new_size, layout.align());
        let np = self.alloc(nlay);
        if !np.is_null() {
            std::ptr::copy_nonoverlapping(ptr, np, layout.size().min(new_size));
            self.dealloc(ptr, layout);
        }
        np
    }
}
