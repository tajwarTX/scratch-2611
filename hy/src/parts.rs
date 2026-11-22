//! Radix-partition buffers: per partition a list of fixed-size chunks carved
//! from big (huge-page) blocks, so pushes never reallocate or copy.

pub struct Parts<T: Copy> {
    ch: usize,
    blocks: Vec<Vec<T>>,
    free: *mut T,
    free_left: usize,
    block_records: usize,
    /// per partition: current chunk and how many records it holds (one cache line per
    /// four partitions, so a write touches a single line of this state)
    st: Vec<(*mut T, u32)>,
    done: Vec<Vec<(*const T, u32)>>,
    /// memory handed over by `donate`: used for new chunks before any new block
    spare: Vec<(*mut T, usize)>,
}

unsafe impl<T: Copy + Send> Send for Parts<T> {}
unsafe impl<T: Copy + Sync> Sync for Parts<T> {}

impl<T: Copy> Parts<T> {
    pub fn new(nparts: usize, block_records: usize) -> Self {
        Self::with_chunk(nparts, block_records, 1024)
    }

    pub fn with_chunk(nparts: usize, block_records: usize, ch: usize) -> Self {
        Parts {
            ch,
            blocks: Vec::new(),
            free: std::ptr::null_mut(),
            free_left: 0,
            block_records: block_records.max(ch * 4),
            st: vec![(std::ptr::null_mut(), ch as u32); nparts],
            done: (0..nparts).map(|_| Vec::new()).collect(),
            spare: Vec::new(),
        }
    }

    /// Gives this writer `bytes` of memory at `ptr` (aligned for T) that nobody reads
    /// any more and that stays allocated for the rest of the run.
    pub fn donate(&mut self, ptr: *mut u8, bytes: usize) {
        let n = bytes / std::mem::size_of::<T>();
        if n >= self.ch && (ptr as usize) % std::mem::align_of::<T>() == 0 {
            self.spare.push((ptr as *mut T, n));
        }
    }

    /// The memory regions (start, bytes) of partition p's chunks, full or not.
    pub fn regions(&self, p: usize) -> impl Iterator<Item = (*mut u8, usize)> + '_ {
        let bytes = self.ch * std::mem::size_of::<T>();
        self.done[p].iter().map(move |&(ptr, _)| (ptr as *mut u8, bytes))
    }

    #[inline(always)]
    pub fn push(&mut self, p: usize, x: T) {
        let (c, l) = unsafe { *self.st.get_unchecked(p) };
        if l as usize == self.ch {
            self.new_chunk(p);
            unsafe { self.st[p].0.write(x) };
            self.st[p].1 = 1;
        } else {
            unsafe {
                c.add(l as usize).write(x);
                self.st.get_unchecked_mut(p).1 = l + 1;
            }
        }
    }

    /// `push` that also asks for the cache line this partition writes `ahead` records
    /// from now: the next writes to the same partition come many rows later, so the
    /// line (fresh memory, otherwise a miss on every new line) is there by then.
    #[inline(always)]
    pub fn push_pf(&mut self, p: usize, x: T, ahead: usize) {
        let (c, l) = unsafe { *self.st.get_unchecked(p) };
        if l as usize == self.ch {
            self.new_chunk(p);
            unsafe { self.st[p].0.write(x) };
            self.st[p].1 = 1;
            prefetch_w(self.st[p].0.wrapping_add(1));
        } else {
            unsafe {
                let d = c.add(l as usize);
                d.write(x);
                self.st.get_unchecked_mut(p).1 = l + 1;
                prefetch_w(d.wrapping_add(ahead));
            }
        }
    }

    /// Where partition p's next record goes, with room made for it. The record
    /// counts only once `commit_pf` is called; until then it may be written in pieces.
    #[inline(always)]
    pub fn slot(&mut self, p: usize) -> *mut T {
        let (c, l) = unsafe { *self.st.get_unchecked(p) };
        if l as usize == self.ch {
            return self.slot_new(p);
        }
        unsafe { c.add(l as usize) }
    }

    #[inline(never)]
    fn slot_new(&mut self, p: usize) -> *mut T {
        self.new_chunk(p);
        self.st[p].1 = 0;
        self.st[p].0
    }

    /// Counts the record written at `slot(p)` (= d) and asks for the cache line this
    /// partition writes `ahead` records from now (see `push_pf`).
    #[inline(always)]
    pub fn commit_pf(&mut self, p: usize, d: *mut T, ahead: usize) {
        unsafe { self.st.get_unchecked_mut(p).1 += 1 };
        prefetch_w(d.wrapping_add(ahead));
    }

    /// Address of partition p's write state (to prefetch it).
    #[inline(always)]
    pub fn state_ptr(&self, p: usize) -> *const u8 {
        self.st.as_ptr().wrapping_add(p) as *const u8
    }

    /// Where partition p's next record will be written (meaningless before its first chunk).
    #[inline(always)]
    pub fn peek(&self, p: usize) -> *const T {
        let (c, l) = unsafe { *self.st.get_unchecked(p) };
        c.wrapping_add(l as usize) as *const T
    }

    #[inline(never)]
    fn new_chunk(&mut self, p: usize) {
        if !self.st[p].0.is_null() {
            self.done[p].push((self.st[p].0 as *const T, self.ch as u32));
        }
        if self.free_left < self.ch {
            if let Some((ptr, n)) = self.spare.pop() {
                self.free = ptr;
                self.free_left = n;
            } else {
                let mut v: Vec<T> = Vec::with_capacity(self.block_records);
                self.free = v.as_mut_ptr();
                self.free_left = v.capacity();
                self.blocks.push(v);
            }
        }
        self.st[p].0 = self.free;
        self.free = unsafe { self.free.add(self.ch) };
        self.free_left -= self.ch;
    }

    /// Moves partially filled chunks to the done lists. Call once after pushing.
    pub fn finish(&mut self) {
        for p in 0..self.st.len() {
            if !self.st[p].0.is_null() && self.st[p].1 > 0 {
                self.done[p].push((self.st[p].0 as *const T, self.st[p].1));
            }
            self.st[p] = (std::ptr::null_mut(), self.ch as u32);
        }
    }

    #[inline]
    pub fn chunks(&self, p: usize) -> impl Iterator<Item = &[T]> + '_ {
        self.done[p].iter().map(|&(ptr, n)| unsafe { std::slice::from_raw_parts(ptr, n as usize) })
    }

    pub fn count(&self, p: usize) -> usize {
        self.done[p].iter().map(|&(_, n)| n as usize).sum()
    }
}

/// Prefetch for writing (falls back to a plain prefetch where the CPU feature is not enabled).
#[inline(always)]
pub fn prefetch_w<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_ET0)
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!("prfm pstl1keep, [{0}]", in(reg) p, options(nostack, readonly, preserves_flags))
    }
}

#[inline(always)]
pub fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_T0)
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, readonly, preserves_flags))
    }
}
