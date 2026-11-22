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
/// With INTERN_BIT: the key's low 40 bits are an offset into the input (see
/// `set_source`), and the string runs to the next `,`, `"` or newline.
pub const SRC_BIT: u64 = 1 << 62;
/// With INTERN_BIT: a string on the heap (only when the arena was too small).
const HEAP_BIT: u64 = 1 << 61;
/// With INTERN_BIT: the low 60 bits are a hash of the string, whose bytes are
/// found through the names table (see `name_insert`).
pub const FP_BIT: u64 = 1 << 60;
const FP_MASK: u64 = (1 << 60) - 1;

/// Names of fingerprint keys: key -> (check hash << 40 | source offset).
struct Names {
    slots: Vec<[AtomicU64; 2]>,
    mask: usize,
    used: AtomicUsize,
}
static NAMES: AtomicPtr<Names> = AtomicPtr::new(std::ptr::null_mut());
/// Set when two different strings were seen under one key (or the table is full):
/// the fast pipeline gives up.
pub static NAMES_FAIL: AtomicBool = AtomicBool::new(false);

pub fn init_names(expect: usize) {
    let cap = (expect.max(1024) * 4).next_power_of_two();
    let slots = unsafe {
        let layout = std::alloc::Layout::array::<[AtomicU64; 2]>(cap).unwrap();
        let p = std::alloc::alloc_zeroed(layout) as *mut [AtomicU64; 2];
        Vec::from_raw_parts(p, cap, cap)
    };
    let n = Box::new(Names { slots, mask: cap - 1, used: AtomicUsize::new(0) });
    let old = NAMES.swap(Box::into_raw(n), AtomicOrdering::AcqRel);
    if !old.is_null() {
        std::mem::forget(old);
    }
    NAMES_FAIL.store(false, AtomicOrdering::Relaxed);
}

/// A second, independent hash of an ID (24 bits), to check that equal keys are equal strings.
#[inline(always)]
pub fn check_hash(s: &[u8]) -> u64 {
    let mut h: u64 = 0x243F_6A88_85A3_08D3 ^ s.len() as u64;
    for c in s.chunks(8) {
        let mut b = [0u8; 8];
        b[..c.len()].copy_from_slice(c);
        h = (h.rotate_left(23) ^ u64::from_le_bytes(b)).wrapping_mul(0xA076_1D64_78BD_642F);
    }
    (h ^ (h >> 31)).wrapping_mul(0xE703_7ED1_A0B4_28DB) >> 48
}

/// A name word: check hash (16 bits) << 48 | length (8) << 40 | source offset (40).
/// Two words with different top 24 bits are different strings.
#[inline(always)]
pub fn name_word(s: &[u8], src: u64) -> u64 {
    if s.len() > 255 {
        NAMES_FAIL.store(true, AtomicOrdering::Relaxed);
    }
    check_hash(s) << 48 | ((s.len() as u64 & 0xFF) << 40) | src
}

/// The `len` bytes at source offset `off`.
#[inline]
pub fn source_bytes(off: u64, len: usize) -> &'static [u8] {
    let tab = unsafe { &*SOURCE.load(AtomicOrdering::Acquire) };
    let off = off as usize;
    let i = tab.bases.partition_point(|&b| b <= off) - 1;
    unsafe { std::slice::from_raw_parts(tab.files[i].0.add(off - tab.bases[i]), len) }
}

/// The fingerprint key of an ID that does not pack.
#[inline(always)]
pub fn fp_key(s: &[u8]) -> u64 {
    INTERN_BIT | FP_BIT | (hash_bytes(s) & FP_MASK & FP_TEST_MASK.load(AtomicOrdering::Relaxed)) | 1
}

/// Test hook (FP_TEST_BITS): narrows fingerprints so that collisions happen.
pub static FP_TEST_MASK: AtomicU64 = AtomicU64::new(u64::MAX);

/// Records `w` = check_hash << 40 | source offset as the name of `key` unless it has one;
/// flags a failure when the existing name has another check hash.
pub fn name_insert(key: u64, w: u64) {
    let t = unsafe { &*NAMES.load(AtomicOrdering::Acquire) };
    let mut i = (key.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 20) as usize & t.mask;
    loop {
        let s = &t.slots[i];
        let k = s[0].load(AtomicOrdering::Acquire);
        if k == 0 {
            if t.used.fetch_add(1, AtomicOrdering::Relaxed) > t.mask / 2 {
                NAMES_FAIL.store(true, AtomicOrdering::Relaxed);
                return;
            }
            // The value first, then the key: a reader that sees the key sees its value.
            match s[0].compare_exchange(0, key | 0, AtomicOrdering::AcqRel, AtomicOrdering::Acquire) {
                Ok(_) => {
                    s[1].store(w, AtomicOrdering::Release);
                    return;
                }
                Err(_) => continue,
            }
        }
        if k == key {
            let mut v = s[1].load(AtomicOrdering::Acquire);
            while v == 0 {
                std::hint::spin_loop();
                v = s[1].load(AtomicOrdering::Acquire);
            }
            if v >> 40 != w >> 40 {
                NAMES_FAIL.store(true, AtomicOrdering::Relaxed);
            }
            return;
        }
        i = (i + 1) & t.mask;
    }
}

fn name_of(key: u64) -> u64 {
    let t = unsafe { &*NAMES.load(AtomicOrdering::Acquire) };
    let mut i = (key.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 20) as usize & t.mask;
    loop {
        let s = &t.slots[i];
        let k = s[0].load(AtomicOrdering::Acquire);
        if k == key {
            return s[1].load(AtomicOrdering::Acquire);
        }
        assert!(k != 0, "a fingerprint key without a name");
        i = (i + 1) & t.mask;
    }
}
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

/// The input files, for keys that point into them.
struct SourceTab {
    bases: Vec<usize>,
    files: Vec<(*const u8, usize)>,
}
unsafe impl Sync for SourceTab {}
unsafe impl Send for SourceTab {}
static SOURCE: AtomicPtr<SourceTab> = AtomicPtr::new(std::ptr::null_mut());

/// Registers the input files (they must stay mapped for the run) and returns
/// each file's base in one shared offset space: a key with SRC_BIT holds
/// `base + offset in the file`.
pub fn set_source(files: &[(*const u8, usize)]) -> Vec<usize> {
    let mut bases = Vec::with_capacity(files.len());
    let mut b = 0usize;
    for &(_, len) in files {
        bases.push(b);
        b += len;
    }
    assert!(b < (1usize << 40));
    let tab = Box::new(SourceTab { bases: bases.clone(), files: files.to_vec() });
    SOURCE.store(Box::into_raw(tab), AtomicOrdering::Release);
    bases
}

/// The field value starting at source offset `off`: it ends at the next `,`,
/// `"` or newline (a value never holds one), or at the file's end.
#[inline]
pub fn source_slice(off: u64) -> &'static [u8] {
    let tab = unsafe { &*SOURCE.load(AtomicOrdering::Acquire) };
    let off = off as usize;
    let i = tab.bases.partition_point(|&b| b <= off) - 1;
    let (p, len) = tab.files[i];
    let at = off - tab.bases[i];
    let rest = unsafe { std::slice::from_raw_parts(p.add(at), len - at) };
    let n = memchr::memchr3(b',', b'"', b'\n', rest).unwrap_or(rest.len());
    &rest[..n]
}

/// Records that some key is not a packed one (so integer order is not byte order).
#[inline(always)]
pub fn note_nonpacked() {
    ANY_INTERNED.store(true, AtomicOrdering::Relaxed);
}

#[inline(always)]
pub fn hash_bytes(s: &[u8]) -> u64 {
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
                let k = INTERN_BIT | HEAP_BIT | extra.len() as u64;
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
        if k & FP_BIT != 0 {
            let w = name_of(k);
            return source_bytes(w & OFF_MASK, ((w >> 40) & 0xFF) as usize);
        }
        if k & SRC_BIT != 0 {
            return source_slice(k & OFF_MASK);
        }
        if k & HEAP_BIT != 0 {
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

/// Starts loading the table slot `s` hashes to, ahead of `intern(s)`.
#[inline(always)]
pub fn prefetch(s: &[u8]) {
    let it = interner();
    let i = hash_bytes(s) as usize & it.mask;
    unsafe {
        std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(it.slots.as_ptr().add(i) as *const i8);
    }
}

#[cold]
pub fn intern(s: &[u8]) -> u64 {
    ANY_INTERNED.store(true, AtomicOrdering::Relaxed);
    interner().intern(s)
}

/// (table entries, table slots, strings in the locked overflow map), for diagnostics.
pub fn interner_stats() -> (usize, usize, usize) {
    let it = interner();
    (it.slots.iter().filter(|x| x.load(AtomicOrdering::Relaxed) != 0).count(), it.slots.len(), it.extra.lock().unwrap().len())
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

/// Byte length of the string behind an interned key.
pub fn interned_len(k: u64) -> usize {
    if k & FP_BIT != 0 {
        return ((name_of(k) >> 40) & 0xFF) as usize;
    }
    interned_bytes(k).len()
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
