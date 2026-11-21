//! Open-addressing hash tables keyed by non-zero u64 keys.

#[inline(always)]
pub fn mix(k: u64) -> u64 {
    let mut x = k;
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^= x >> 33;
    x
}

/// u64 -> u64 map with linear probing; key 0 marks an empty slot.
pub struct U64Map {
    slots: Vec<(u64, u64)>,
    mask: usize,
    len: usize,
}

impl U64Map {
    pub fn with_capacity(n: usize) -> Self {
        let cap = (n.max(8) * 2).next_power_of_two();
        U64Map { slots: vec![(0, 0); cap], mask: cap - 1, len: 0 }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn get(&self, k: u64) -> Option<u64> {
        let mut i = mix(k) as usize & self.mask;
        loop {
            let (sk, sv) = unsafe { *self.slots.get_unchecked(i) };
            if sk == k {
                return Some(sv);
            }
            if sk == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Starts loading the slot `k` hashes to, ahead of a `get`.
    #[inline(always)]
    pub fn prefetch(&self, k: u64) {
        let i = mix(k) as usize & self.mask;
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(self.slots.as_ptr().add(i) as *const i8, _MM_HINT_T0);
        }
    }

    /// Returns the value for `k`, inserting `make()` first if absent.
    #[inline]
    pub fn get_or_insert(&mut self, k: u64, make: impl FnOnce() -> u64) -> u64 {
        if (self.len + 1) * 2 > self.slots.len() {
            self.grow();
        }
        let mut i = mix(k) as usize & self.mask;
        loop {
            let (sk, sv) = self.slots[i];
            if sk == k {
                return sv;
            }
            if sk == 0 {
                let v = make();
                self.slots[i] = (k, v);
                self.len += 1;
                return v;
            }
            i = (i + 1) & self.mask;
        }
    }

    pub fn insert(&mut self, k: u64, v: u64) {
        if (self.len + 1) * 2 > self.slots.len() {
            self.grow();
        }
        let mut i = mix(k) as usize & self.mask;
        loop {
            let sk = self.slots[i].0;
            if sk == k {
                self.slots[i].1 = v;
                return;
            }
            if sk == 0 {
                self.slots[i] = (k, v);
                self.len += 1;
                return;
            }
            i = (i + 1) & self.mask;
        }
    }

    fn grow(&mut self) {
        let old = std::mem::take(&mut self.slots);
        let cap = old.len() * 2;
        self.slots = vec![(0, 0); cap];
        self.mask = cap - 1;
        self.len = 0;
        for (k, v) in old {
            if k != 0 {
                self.insert(k, v);
            }
        }
    }

    /// Rewrites every value in place.
    pub fn map_values(&mut self, mut f: impl FnMut(u64, u64) -> u64) {
        for slot in self.slots.iter_mut() {
            if slot.0 != 0 {
                slot.1 = f(slot.0, slot.1);
            }
        }
    }
}

/// Reusable table from u64 key to a u32 index, cleared between uses.
pub struct IndexTable {
    keys: Vec<u64>,
    vals: Vec<u32>,
    mask: usize,
}

impl IndexTable {
    pub fn new() -> Self {
        IndexTable { keys: Vec::new(), vals: Vec::new(), mask: 0 }
    }

    pub fn reset(&mut self, n: usize) {
        let cap = (n.max(8) * 2).next_power_of_two();
        if self.keys.len() < cap {
            self.keys = vec![0; cap];
            self.vals = vec![0; cap];
        } else {
            self.keys[..cap].iter_mut().for_each(|k| *k = 0);
        }
        self.mask = cap - 1;
    }

    /// The index stored for `k`, or stores `next` and returns None.
    #[inline(always)]
    pub fn find_or_insert(&mut self, k: u64, hash: u64, next: u32) -> Option<u32> {
        let mut i = hash as usize & self.mask;
        loop {
            let sk = unsafe { *self.keys.get_unchecked(i) };
            if sk == k {
                return Some(unsafe { *self.vals.get_unchecked(i) });
            }
            if sk == 0 {
                unsafe {
                    *self.keys.get_unchecked_mut(i) = k;
                    *self.vals.get_unchecked_mut(i) = next;
                }
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
}
