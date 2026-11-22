//! IDs as 64-bit keys.
//!
//! IDs of at most 9 bytes, each in 0x01..=0x7F, pack into 63 bits: seven bits
//! a byte, first byte highest, zero-padded. Padding is below every byte, so
//! comparing two packed keys as integers is exactly byte-wise string
//! comparison, and equal keys mean equal strings.
//!
//! Anything else (longer, NUL, non-ASCII) is interned in a global table and
//! gets a key with the top bit set. Interned keys are equal only for equal
//! strings too, but ordering them needs the bytes: `cmp_key`.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Mutex;

pub const INTERN_BIT: u64 = 1 << 63;
const MAX_PACKED: usize = 9;

#[inline(always)]
pub fn pack(s: &[u8]) -> Option<u64> {
    let n = s.len();
    if n > MAX_PACKED {
        return None;
    }
    let mut v = 0u64;
    for &b in s {
        if b == 0 || b >= 0x80 {
            return None;
        }
        v = (v << 7) | b as u64;
    }
    Some(v << (7 * (MAX_PACKED - n)))
}

/// Strings that do not pack, interned without locks.
///
/// `slots` is an open-addressing table of `tag << 40 | offset` (0: empty),
/// where the offset points into `arena` at `[len: u16][bytes]`. The first
/// thread to claim a slot (CAS from 0) owns the string; a loser that finds
/// its string already there adopts the winner's offset, so equal strings
/// always get one key, `INTERN_BIT | offset`. The arena only grows (an
/// atomic bump) and is sized to the input, which every interned string is a
/// substring of; published offsets are read without any lock.
///
/// If the table gets too full (an input where most IDs are long), inserts
/// stop and later strings go through `extra`, a locked map. A string is in
/// at most one of the two, so keys stay unique: a lookup always tries the
/// table first, and the table takes no new strings once `extra` is in use.
struct Interner {
    slots: Vec<AtomicU64>,
    mask: usize,
    arena: Vec<u8>,
    bump: AtomicUsize,
    used: AtomicUsize,
    limit: usize,
    full: AtomicBool,
    extra: Mutex<HashMap<Vec<u8>, u64>>,
}

const OFF_BITS: u32 = 40;
const OFF_MASK: u64 = (1 << OFF_BITS) - 1;

static INTERNER: AtomicPtr<Interner> = AtomicPtr::new(std::ptr::null_mut());
static ANY_INTERNED: AtomicBool = AtomicBool::new(false);

/// Prepares the intern table for about `strings` distinct strings whose bytes
/// total at most `bytes` (the input size is always enough). Replaces any
/// earlier table, so call it only before parsing.
pub fn init_interner(strings: usize, bytes: usize) {
    let cap = (strings.max(1) * 4).max(1 << 12).next_power_of_two();
    // Zeroed lazily by the kernel: untouched pages cost nothing.
    let slots: Vec<AtomicU64> = unsafe {
        let layout = std::alloc::Layout::array::<AtomicU64>(cap).unwrap();
        let p = std::alloc::alloc_zeroed(layout) as *mut AtomicU64;
        assert!(!p.is_null());
        Vec::from_raw_parts(p, cap, cap)
    };
    let arena_bytes = (bytes + 64).min((OFF_MASK as usize) - 64);
    let mut arena = Vec::with_capacity(arena_bytes);
    // Offset 0 marks an empty slot: start the data at 8.
    arena.resize(8, 0);
    let it = Box::new(Interner {
        slots,
        mask: cap - 1,
        arena,
        bump: AtomicUsize::new(8),
        used: AtomicUsize::new(0),
        limit: cap / 2,
        full: AtomicBool::new(false),
        extra: Mutex::new(HashMap::new()),
    });
    INTERNER.store(Box::into_raw(it), AtomicOrdering::Release);
}

#[inline(always)]
fn interner() -> &'static Interner {
    let p = INTERNER.load(AtomicOrdering::Acquire);
    if p.is_null() {
        init_default();
        return interner();
    }
    unsafe { &*p }
}

#[cold]
fn init_default() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        if INTERNER.load(AtomicOrdering::Acquire).is_null() {
            init_interner(1 << 20, 1 << 30);
        }
    });
}

#[inline(always)]
fn hash_bytes(s: &[u8]) -> u64 {
    const K: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut h = (s.len() as u64).wrapping_mul(K);
    let mut p = s;
    while p.len() >= 8 {
        let w = u64::from_le_bytes(p[..8].try_into().unwrap());
        h = (h ^ w).wrapping_mul(K);
        h ^= h >> 29;
        p = &p[8..];
    }
    if !p.is_empty() {
        let mut b = [0u8; 8];
        b[..p.len()].copy_from_slice(p);
        h = (h ^ u64::from_le_bytes(b)).wrapping_mul(K);
        h ^= h >> 29;
    }
    h.wrapping_mul(0xD6E8_FEB8_6659_7F4B) ^ (h >> 32)
}

impl Interner {
    #[inline(always)]
    fn bytes_at(&self, off: usize) -> &[u8] {
        unsafe {
            let p = self.arena.as_ptr().add(off);
            let len = u16::from_le_bytes([*p, *p.add(1)]) as usize;
            std::slice::from_raw_parts(p.add(2), len)
        }
    }

    /// Appends `s` to the arena; its offset, or None when the arena is full.
    /// Threads take the arena in 64 KB chunks, so the shared bump pointer is
    /// touched rarely.
    fn append(&self, s: &[u8]) -> Option<usize> {
        thread_local! {
            static CHUNK: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
        }
        if s.len() > u16::MAX as usize {
            return None;
        }
        let size = (2 + s.len() + 7) & !7;
        let off = CHUNK.with(|c| {
            let (cur, end) = c.get();
            let off = if cur + size <= end {
                cur
            } else {
                let grab = size.max(1 << 16);
                let o = self.bump.fetch_add(grab, AtomicOrdering::Relaxed);
                c.set((o, o + grab));
                o
            };
            c.set((off + size, c.get().1));
            off
        });
        if off + size > self.arena.capacity() {
            return None;
        }
        unsafe {
            let p = self.arena.as_ptr().add(off) as *mut u8;
            let len = (s.len() as u16).to_le_bytes();
            *p = len[0];
            *p.add(1) = len[1];
            std::ptr::copy_nonoverlapping(s.as_ptr(), p.add(2), s.len());
        }
        Some(off)
    }

    /// Counts an insert; the shared counter moves once per 128 of a thread's.
    #[inline(always)]
    fn count_insert(&self) {
        thread_local! {
            static N: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
        }
        let n = N.with(|c| {
            let n = c.get() + 1;
            c.set(n & 127);
            n
        });
        if n == 128 && self.used.fetch_add(128, AtomicOrdering::Relaxed) + 128 >= self.limit {
            self.full.store(true, AtomicOrdering::Relaxed);
        }
    }

    fn intern(&self, s: &[u8]) -> u64 {
        let h = hash_bytes(s);
        let tag = h & !OFF_MASK;
        let mut i = h as usize & self.mask;
        let mut mine: Option<usize> = None;
        let mut probes = 0usize;
        loop {
            let v = self.slots[i].load(AtomicOrdering::Acquire);
            if v == 0 {
                if self.full.load(AtomicOrdering::Relaxed) {
                    break;
                }
                if mine.is_none() {
                    match self.append(s) {
                        Some(off) => mine = Some(off),
                        None => break,
                    }
                }
                let cand = tag | mine.unwrap() as u64;
                match self.slots[i].compare_exchange(0, cand, AtomicOrdering::AcqRel, AtomicOrdering::Acquire) {
                    Ok(_) => {
                        self.count_insert();
                        return INTERN_BIT | mine.unwrap() as u64;
                    }
                    Err(actual) => {
                        if actual & !OFF_MASK == tag && self.bytes_at((actual & OFF_MASK) as usize) == s {
                            return INTERN_BIT | (actual & OFF_MASK);
                        }
                    }
                }
            } else if v & !OFF_MASK == tag && self.bytes_at((v & OFF_MASK) as usize) == s {
                return INTERN_BIT | (v & OFF_MASK);
            }
            i = (i + 1) & self.mask;
            probes += 1;
            if probes > self.mask {
                break;
            }
        }
        // The locked path: strings the table did not take.
        let mut extra = self.extra.lock().unwrap();
        if let Some(&k) = extra.get(s) {
            return k;
        }
        let off = match mine.or_else(|| self.append(s)) {
            Some(off) => off,
            None => {
                // No room in the arena at all (not reachable with an arena sized
                // to the input); keep exactness by storing the string on the heap
                // behind a key above every arena offset.
                let k = INTERN_BIT | (1u64 << 62) | extra.len() as u64;
                extra.insert(s.to_vec(), k);
                HEAP.lock().unwrap().push(s.to_vec());
                return k;
            }
        };
        let k = INTERN_BIT | off as u64;
        extra.insert(s.to_vec(), k);
        k
    }

    #[inline(always)]
    fn bytes_of(&self, k: u64) -> &[u8] {
        if k & (1 << 62) != 0 {
            // Heap strings: rare, and only when the arena was too small.
            let v = HEAP.lock().unwrap();
            let s = &v[(k & OFF_MASK) as usize];
            unsafe { std::slice::from_raw_parts(s.as_ptr(), s.len()) }
        } else {
            self.bytes_at((k & OFF_MASK) as usize)
        }
    }
}

static HEAP: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());

#[cold]
pub fn intern(s: &[u8]) -> u64 {
    ANY_INTERNED.store(true, AtomicOrdering::Relaxed);
    interner().intern(s)
}

/// The number of interned strings so far (for diagnostics).
pub fn interned_count() -> usize {
    let it = interner();
    it.slots.iter().filter(|x| x.load(AtomicOrdering::Relaxed) != 0).count() + it.extra.lock().unwrap().len()
}

/// The bytes of an interned key.
#[inline]
pub fn interned_bytes(k: u64) -> &'static [u8] {
    interner().bytes_of(k)
}

/// Whether any key so far needed the intern table (so integer order is not byte order).
pub fn any_interned() -> bool {
    ANY_INTERNED.load(AtomicOrdering::Relaxed)
}

#[inline(always)]
pub fn key_of(s: &[u8]) -> u64 {
    match pack(s) {
        Some(k) => k,
        None => intern(s),
    }
}

pub fn key_bytes(k: u64, out: &mut Vec<u8>) {
    if k & INTERN_BIT != 0 {
        out.extend_from_slice(interned_bytes(k));
        return;
    }
    for i in 0..MAX_PACKED {
        let c = (k >> (7 * (MAX_PACKED - 1 - i))) & 0x7F;
        if c == 0 {
            break;
        }
        out.push(c as u8);
    }
}

/// Byte-wise comparison of the strings behind two keys.
#[inline]
pub fn cmp_key(a: u64, b: u64) -> Ordering {
    if (a | b) & INTERN_BIT == 0 {
        return a.cmp(&b);
    }
    cmp_slow(a, b)
}

#[cold]
fn cmp_slow(a: u64, b: u64) -> Ordering {
    let mut buf = [0u8; 16];
    let side = |k: u64, buf: &mut [u8; 16]| -> &[u8] {
        if k & INTERN_BIT != 0 {
            interned_bytes(k)
        } else {
            let mut n = 0;
            for i in 0..MAX_PACKED {
                let c = (k >> (7 * (MAX_PACKED - 1 - i))) & 0x7F;
                if c == 0 {
                    break;
                }
                buf[n] = c as u8;
                n += 1;
            }
            unsafe { std::slice::from_raw_parts(buf.as_ptr(), n) }
        }
    };
    let mut buf2 = [0u8; 16];
    let x = side(a, &mut buf);
    let y = side(b, &mut buf2);
    x.cmp(y)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interning_is_exact_and_concurrent() {
        init_interner(1 << 10, 1 << 24);
        let words: Vec<Vec<u8>> = (0..20_000u32).map(|i| format!("u{:x}{}", i * 2654435761u32, "f".repeat((i % 40) as usize)).into_bytes()).collect();
        let keys: Vec<Vec<u64>> = std::thread::scope(|sc| {
            let hs: Vec<_> = (0..8).map(|t| {
                let words = &words;
                sc.spawn(move || words.iter().skip(t % 3).map(|w| intern(w)).collect::<Vec<u64>>())
            }).collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        // Every thread got the same key for the same word, and bytes round-trip.
        for (t, ks) in keys.iter().enumerate() {
            for (w, &k) in words.iter().skip(t % 3).zip(ks) {
                assert_eq!(interned_bytes(k), &w[..]);
                assert_eq!(intern(w), k);
            }
        }
        // Distinct words, distinct keys; byte order preserved by cmp_key.
        let mut all: Vec<u64> = words.iter().map(|w| intern(w)).collect();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), words.len());
        for a in words.iter().take(200) {
            for b in words.iter().take(200) {
                assert_eq!(cmp_key(intern(a), intern(b)), a.cmp(b));
                assert_eq!(cmp_key(intern(a), pack(b"u1").unwrap()), a.as_slice().cmp(&b"u1"[..]));
            }
        }
        // The table was small (4096 slots for 20k strings): the locked path kept things exact.
        assert!(interned_count() == words.len());
    }

    #[test]
    fn packed_order_is_byte_order() {
        let words: Vec<&[u8]> = vec![
            b"0", b"09", b"1", b"A", b"AB", b"U1", b"Z", b"_", b"a", b"a0", b"aa", b"u", b"u1", b"u10", b"u2",
            b"user_id_a", b"zzzzzzzzz", b" ", b"u 1", b"\x7f", b"\x01",
        ];
        for a in &words {
            for b in &words {
                let (ka, kb) = (pack(a).unwrap(), pack(b).unwrap());
                assert_eq!(ka.cmp(&kb), a.cmp(b), "{:?} {:?}", a, b);
                let mut back = Vec::new();
                key_bytes(ka, &mut back);
                assert_eq!(&back[..], *a);
            }
        }
        assert!(pack(b"u 1").is_some());
        assert!(pack(b"abcdefghij").is_none());
        assert!(pack(b"a\x80").is_none());
        assert!(pack(b"a\x00").is_none());
    }
}
