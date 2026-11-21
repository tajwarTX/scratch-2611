//! Radix scatter for 16-byte records with software write-combining.
//!
//! Each partition stages records in one 128-byte (8-record) block of a small
//! contiguous staging area. A full block is copied to the partition's block
//! list with non-temporal stores, so the destination is never read first.
//! Counts live in a dense array, so a push touches only that array and the
//! staging block, and the push's final index in its partition is known.

use std::arch::x86_64::*;

pub const K: usize = 8;

/// Experiment switch: non-temporal block stores (default on).
pub static NT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);
/// Experiment switch: store each record straight into its partition (no
/// staging block, no copy-out branch); default off.
pub static DIRECT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[derive(Clone, Copy)]
#[repr(C, align(64))]
pub struct Block<T: Copy> {
    pub recs: [T; K],
}

pub struct Scatter16<T: Copy> {
    stage: Vec<Block<T>>,
    count: Vec<u32>,
    pub blocks: Vec<Vec<Block<T>>>,
}

impl<T: Copy + Default> Scatter16<T> {
    pub fn new(nparts: usize, reserve_recs: usize) -> Self {
        assert!(std::mem::size_of::<T>() == 16);
        let blank = Block { recs: [T::default(); K] };
        Scatter16 {
            stage: vec![blank; nparts],
            count: vec![0; nparts],
            blocks: (0..nparts).map(|_| Vec::with_capacity(reserve_recs / K + 1)).collect(),
        }
    }

    /// Stages `rec` for partition `p` and returns its index in that partition.
    /// A full staging block is copied out only when the partition's next
    /// record arrives, long after its last store, so the copy's wide loads
    /// never wait on store forwarding.
    #[inline(always)]
    pub fn push(&mut self, p: usize, rec: T) -> u32 {
        unsafe {
            if DIRECT.load(std::sync::atomic::Ordering::Relaxed) {
                let idx = *self.count.get_unchecked(p);
                let dst = self.blocks.get_unchecked_mut(p);
                if idx as usize == dst.capacity() * K {
                    // Keep what is written (whole blocks) across the reallocation.
                    dst.set_len(idx as usize / K);
                    dst.reserve(dst.len().max(16));
                }
                *(dst.as_mut_ptr() as *mut T).add(idx as usize) = rec;
                *self.count.get_unchecked_mut(p) = idx + 1;
                return idx;
            }
            let c = self.count.get_unchecked_mut(p);
            let idx = *c;
            *c = idx + 1;
            let slot = idx as usize % K;
            if slot == 0 && idx != 0 {
                self.copy_out(p);
            }
            *self.stage.get_unchecked_mut(p).recs.get_unchecked_mut(slot) = rec;
            idx
        }
    }

    #[inline(always)]
    unsafe fn copy_out(&mut self, p: usize) {
        let block = self.stage.get_unchecked(p) as *const Block<T> as *const __m256i;
        let dst = self.blocks.get_unchecked_mut(p);
        if dst.len() == dst.capacity() {
            dst.reserve(dst.len().max(16));
        }
        let to = dst.as_mut_ptr().add(dst.len()) as *mut __m256i;
        if NT.load(std::sync::atomic::Ordering::Relaxed) {
            for i in 0..4 {
                _mm256_stream_si256(to.add(i), _mm256_load_si256(block.add(i)));
            }
        } else {
            for i in 0..4 {
                _mm256_store_si256(to.add(i), _mm256_load_si256(block.add(i)));
            }
        }
        dst.set_len(dst.len() + 1);
    }

    /// Moves every staged block (full or partial) into its partition (call once, at the end).
    pub fn flush(&mut self) {
        if DIRECT.load(std::sync::atomic::Ordering::Relaxed) {
            for p in 0..self.count.len() {
                let blocks = (self.count[p] as usize).div_ceil(K);
                unsafe { self.blocks[p].set_len(blocks) };
            }
            return;
        }
        for p in 0..self.count.len() {
            if self.count[p] != 0 {
                let b = self.stage[p];
                self.blocks[p].push(b);
            }
        }
        unsafe { _mm_sfence() };
    }

    pub fn len(&self, p: usize) -> usize {
        self.count[p] as usize
    }

    /// The records of partition `p`, in push order.
    #[inline(always)]
    pub fn records(&self, p: usize) -> &[T] {
        let b = &self.blocks[p];
        unsafe { std::slice::from_raw_parts(b.as_ptr() as *const T, self.count[p] as usize) }
    }

    /// Raw pointer to the first record of partition `p` (records are contiguous).
    pub fn base_ptr(&mut self, p: usize) -> *mut T {
        self.blocks[p].as_mut_ptr() as *mut T
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, Default, PartialEq, Debug)]
    #[repr(C)]
    struct R {
        a: u64,
        b: u64,
    }

    #[test]
    fn scatter_keeps_order_and_indexes() {
        let mut s = Scatter16::<R>::new(5, 3);
        let mut want: Vec<Vec<R>> = vec![Vec::new(); 5];
        for i in 0..1000u64 {
            let p = ((i * 7919) % 5) as usize;
            let r = R { a: i, b: i * 3 };
            let idx = s.push(p, r);
            assert_eq!(idx as usize, want[p].len());
            want[p].push(r);
        }
        s.flush();
        for p in 0..5 {
            assert_eq!(s.records(p), &want[p][..]);
            assert_eq!(s.len(p), want[p].len());
        }
    }
}
