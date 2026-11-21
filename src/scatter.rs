//! Radix scatter with software write-combining: records for each partition
//! are staged in a small contiguous buffer (so the hot working set is a few
//! hundred KB and few pages) and copied to the partition in bursts of K.

pub struct Scatter<T: Copy, const K: usize> {
    stage: Vec<T>,
    fill: Vec<u8>,
    pub parts: Vec<Vec<T>>,
}

impl<T: Copy + Default, const K: usize> Scatter<T, K> {
    pub fn new(nparts: usize, reserve: usize) -> Self {
        Scatter {
            stage: vec![T::default(); nparts * K],
            fill: vec![0; nparts],
            parts: (0..nparts).map(|_| Vec::with_capacity(reserve)).collect(),
        }
    }

    #[inline(always)]
    pub fn push(&mut self, p: usize, rec: T) {
        unsafe {
            let f = self.fill.get_unchecked_mut(p);
            let base = p * K;
            *self.stage.get_unchecked_mut(base + *f as usize) = rec;
            *f += 1;
            if *f as usize == K {
                *f = 0;
                self.parts.get_unchecked_mut(p).extend_from_slice(self.stage.get_unchecked(base..base + K));
            }
        }
    }

    /// Moves every staged record to its partition.
    pub fn flush(&mut self) {
        for p in 0..self.fill.len() {
            let n = self.fill[p] as usize;
            if n > 0 {
                let base = p * K;
                self.parts[p].extend_from_slice(&self.stage[base..base + n]);
                self.fill[p] = 0;
            }
        }
    }
}
