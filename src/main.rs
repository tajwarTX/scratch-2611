//! Shopping Sessions pipeline: CSV click-stream -> user_daily / campaign_daily Parquet.
mod alloc;
mod parquet;
mod parts;
mod scan;

#[global_allocator]
static GLOBAL: alloc::HugeAlloc = alloc::HugeAlloc;

use parts::{prefetch, prefetch_w, Parts};
use scan::{chunk_fields, chunk_fields_w, memchr, rows4, rows7, split};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const NPART: usize = 256;

// ------------------------------------------------------------------ mmap
extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
}

/// Drops the page-table entries of the whole pages inside [start, start + len) of a
/// read-only file mapping (MADV_DONTNEED: the pages stay in the page cache). Process
/// exit unmaps what is still mapped on one thread; the worker threads do it here in
/// parallel once the input has been parsed.
fn unmap_pages(start: usize, len: usize) {
    let a = (start + 4095) & !4095;
    let b = (start + len) & !4095;
    if b > a {
        unsafe { madvise(a as *mut u8, b - a, 4) };
    }
}

/// Maps a file read-only with at least PAD readable zero bytes after its end,
/// so SIMD/SWAR code may over-read past any field.
const PAD: usize = 1 << 16;
fn map_file(path: &Path) -> &'static [u8] {
    use std::os::unix::io::AsRawFd;
    let f = std::fs::File::open(path).expect("open");
    let len = f.metadata().expect("meta").len() as usize;
    unsafe {
        let total = ((len + 4095) & !4095) + PAD;
        let base = mmap(std::ptr::null_mut(), total, 1, 0x02 | 0x20, -1, 0);
        if base as isize == -1 {
            panic!("mmap failed");
        }
        if len > 0 {
            let p = mmap(base, len, 1, 0x02 | 0x10, f.as_raw_fd(), 0);
            if p as isize == -1 || p != base {
                panic!("mmap file failed");
            }
        }
        std::slice::from_raw_parts(base, len)
    }
}

// ------------------------------------------------------------------ keys
// Strings of at most 10 chars over [0-9A-Z_a-z] pack order-preservingly into 60 bits.
// Anything else is interned and gets FALLBACK | index.
const FALLBACK: u64 = 1 << 63;
const EMPTY: u64 = u64::MAX;

static CODE: [u8; 256] = {
    let mut t = [0u8; 256];
    let mut c = 1u8;
    let mut b = 0usize;
    while b < 256 {
        let ok = (b >= b'0' as usize && b <= b'9' as usize)
            || (b >= b'A' as usize && b <= b'Z' as usize)
            || b == b'_' as usize
            || (b >= b'a' as usize && b <= b'z' as usize);
        if ok {
            t[b] = c;
            c += 1;
        }
        b += 1;
    }
    t
};
static DECODE: [u8; 64] = {
    let mut t = [0u8; 64];
    let mut b = 0usize;
    while b < 256 {
        if CODE[b] != 0 {
            t[CODE[b] as usize] = b as u8;
        }
        b += 1;
    }
    t
};

#[inline]
fn pack(s: &[u8]) -> Option<u64> {
    if s.len() > 10 {
        return None;
    }
    let mut k = 0u64;
    for i in 0..10 {
        let c = if i < s.len() {
            let c = CODE[s[i] as usize];
            if c == 0 {
                return None;
            }
            c
        } else {
            0
        };
        k = (k << 6) | c as u64;
    }
    Some(k)
}

struct Interner {
    map: Mutex<(HashMap<Vec<u8>, u64>, Vec<Vec<u8>>)>,
}

static INTERN: std::sync::OnceLock<Interner> = std::sync::OnceLock::new();

fn interner() -> &'static Interner {
    INTERN.get_or_init(|| Interner { map: Mutex::new((HashMap::new(), Vec::new())) })
}

/// SWAR version of `pack` for fields up to 10 bytes; reads 16 bytes from s
/// (safe: inputs are padded mappings). Same result as `pack`.
#[inline(always)]
fn pack_fast(s: &[u8]) -> Option<u64> {
    const ONES: u64 = 0x0101_0101_0101_0101;
    const H: u64 = 0x8080_8080_8080_8080;
    let n = s.len();
    if n > 10 {
        return None;
    }
    let p = s.as_ptr();
    let x0 = unsafe { std::ptr::read_unaligned(p as *const u64) };
    let x1 = unsafe { std::ptr::read_unaligned(p.add(8) as *const u16) } as u64;
    let m0: u64 = if n >= 8 { !0 } else { (1u64 << (8 * n)) - 1 };
    let m1: u64 = if n > 8 { (1u64 << (8 * (n - 8))) - 1 } else { 0 };
    #[inline(always)]
    fn conv(x: u64, m: u64) -> (u64, bool) {
        let t = x | H;
        let ge = |c: u64| t.wrapping_sub(c * ONES) & H;
        let (g48, g58, g65, g91, g95, g96, g97, g123) =
            (ge(48), ge(58), ge(65), ge(91), ge(95), ge(96), ge(97), ge(123));
        let valid = (g48 & !g58) | (g65 & !g91) | (g95 & !g96) | (g97 & !g123);
        let mh = m & H;
        let ok = (valid & mh) == mh && (x & mh) == 0;
        let code = x
            .wrapping_sub(47 * ONES)
            .wrapping_sub((g65 >> 7) * 7)
            .wrapping_sub((g95 >> 7) * 4)
            .wrapping_sub(g97 >> 7);
        (code & m, ok)
    }
    let (c0, ok0) = conv(x0, m0);
    let (c1, ok1) = conv(x1, m1);
    if !(ok0 && ok1) {
        return None;
    }
    let lo = ((c1 & 0x3F) << 6) | ((c1 >> 8) & 0x3F);
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    let hi = unsafe { core::arch::x86_64::_pext_u64(c0.swap_bytes(), 0x3F3F_3F3F_3F3F_3F3F) };
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    let hi = {
        let mut k = 0u64;
        for i in 0..8 {
            k = (k << 6) | ((c0 >> (8 * i)) & 0x3F);
        }
        k
    };
    Some((hi << 12) | lo)
}

/// Order-preserving reversible key for IDs of 1..=9 ASCII bytes without NUL:
/// 7 bits per char, first char most significant, zero-padded (63 bits).
/// Reads 8 bytes from s (inputs are padded mappings or long-lived buffers checked by caller).
#[inline(always)]
fn pack7(s: &[u8]) -> Option<u64> {
    const ONES: u64 = 0x0101_0101_0101_0101;
    const H: u64 = 0x8080_8080_8080_8080;
    let n = s.len();
    if n.wrapping_sub(1) >= 9 {
        return None;
    }
    let x = unsafe { std::ptr::read_unaligned(s.as_ptr() as *const u64) };
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    let m: u64 = unsafe { core::arch::x86_64::_bzhi_u64(!0, 8 * n as u32) };
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    let m: u64 = if n >= 8 { !0 } else { (1u64 << (8 * n)) - 1 };
    let v = x & m;
    let v2 = v | (ONES & !m);
    if (v & H) != 0 || (v2.wrapping_sub(ONES) & !v2 & H) != 0 {
        return None;
    }
    let c9 = if n == 9 {
        let c = s[8];
        if c == 0 || c >= 0x80 {
            return None;
        }
        c as u64
    } else {
        0
    };
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    let hi = unsafe { core::arch::x86_64::_pext_u64(v.swap_bytes(), 0x7F7F_7F7F_7F7F_7F7F) };
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    let hi = {
        let b = v.swap_bytes();
        let mut k = 0u64;
        for i in (0..8).rev() {
            k = (k << 7) | ((b >> (8 * i)) & 0x7F);
        }
        k
    };
    Some((hi << 7) | c9)
}

fn unpack7(k: u64, out: &mut Vec<u8>) {
    for i in (0..9).rev() {
        let c = ((k >> (7 * i)) & 0x7F) as u8;
        if c == 0 {
            break;
        }
        out.push(c);
    }
}

/// Key of an ID: "<pfx>" followed by 1..=8 bytes, the last one not NUL and the eighth
/// (if any) ASCII, maps to those bytes as a little-endian number (so the key is not
/// zero and bit 63 is clear; the length is where the last non-zero byte is, so the
/// mapping is one-to-one). Longer IDs get keys with FALLBACK set: see `key_user`,
/// `key_click` and `eid_key`. Each kind of ID (users 'u', events 'e', clicks 'k')
/// uses its own prefix everywhere, so equal keys of one kind mean equal IDs.
/// None: the ID is not of the short form. Reads 9 bytes at `s` (callers pass fields
/// of padded mappings).
#[inline(always)]
fn key_short(s: &[u8], pfx: u8) -> Option<u64> {
    let n = s.len().wrapping_sub(2);
    if n <= 7 {
        let p = s.as_ptr();
        // the n + 1 payload bytes, zero above the last one (top = its bit offset)
        let x = rd64(unsafe { p.add(1) });
        let top = 8 * n as u32;
        #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
        let k = unsafe { core::arch::x86_64::_bzhi_u64(x, top + 8) };
        #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
        let k = x & (u64::MAX >> (56 - top));
        if unsafe { *p } == pfx && (k >> top) != 0 && (k as i64) >= 0 {
            return Some(k);
        }
    }
    None
}

const PFX_USER: u8 = b'u';
const PFX_EVENT: u8 = b'e';
const PFX_CLICK: u8 = b'k';

/// Second flag of a FALLBACK key: not an interner index (see `key_user`, `key_click`).
const LONG: u64 = 1 << 62;
/// Length of the other usual ID form: the prefix and 32 lowercase hex digits.
const HEX_ID_LEN: usize = 33;

/// Value of 8 lowercase hex digits given as a big-endian word (first digit in the top
/// byte); None if a byte is not in [0-9a-f].
#[inline(always)]
fn hex8(x: u64) -> Option<u64> {
    const ONE: u64 = 0x0101_0101_0101_0101;
    // letters have bit 6 set: their value is the low nibble + 9
    let v = (x & (0x0F * ONE)) + ((x >> 6) & ONE) * 9;
    // back to text: digits below ten are '0' + v, the others 'a' + v - 10
    let ge10 = ((v + 6 * ONE) >> 4) & ONE;
    let back = v + 0x30 * ONE + ge10 * 0x27;
    if back != x || (v & (0xF0 * ONE)) != 0 {
        return None;
    }
    // the eight nibbles, first digit most significant
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    {
        Some(unsafe { core::arch::x86_64::_pext_u64(v, 0x0F * ONE) })
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let a = (v | (v >> 4)) & 0x00FF_00FF_00FF_00FF;
        let b = (a | (a >> 8)) & 0x0000_FFFF_0000_FFFF;
        Some((b | (b >> 16)) & 0xFFFF_FFFF)
    }
}

/// The 32 lowercase hex digits at `p` as two numbers (first 16 digits, last 16
/// digits); their order as numbers is the byte order of the text.
#[inline(always)]
fn hex32(p: *const u8) -> Option<(u64, u64)> {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        hex32_avx2(p)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        hex32_words(p)
    }
}

/// `hex32` eight digits at a time.
#[inline(always)]
fn hex32_words(p: *const u8) -> Option<(u64, u64)> {
    let w = |i: usize| hex8(rd64(unsafe { p.add(i) }).swap_bytes());
    let (a, b, c, d) = (w(0)?, w(8)?, w(16)?, w(24)?);
    Some((a << 32 | b, c << 32 | d))
}

/// `hex32` with one 32-byte vector: digit values by range, pairs of digits to bytes.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
fn hex32_avx2(p: *const u8) -> Option<(u64, u64)> {
    use core::arch::x86_64::*;
    unsafe {
        let v = _mm256_loadu_si256(p as *const __m256i);
        // '0'..'9' -> 0..9 and 'a'..'f' -> 0..5 (as unsigned bytes: anything else is larger)
        let d = _mm256_sub_epi8(v, _mm256_set1_epi8(b'0' as i8));
        let a = _mm256_sub_epi8(v, _mm256_set1_epi8(b'a' as i8));
        let is_d = _mm256_cmpeq_epi8(_mm256_max_epu8(d, _mm256_set1_epi8(9)), _mm256_set1_epi8(9));
        let is_a = _mm256_cmpeq_epi8(_mm256_max_epu8(a, _mm256_set1_epi8(5)), _mm256_set1_epi8(5));
        if _mm256_movemask_epi8(_mm256_or_si256(is_d, is_a)) != -1 {
            return None;
        }
        let nib = _mm256_blendv_epi8(_mm256_add_epi8(a, _mm256_set1_epi8(10)), d, is_d);
        // two digits -> one byte (first digit * 16 + second), then the 16 bytes of
        // each half next to each other
        let pairs = _mm256_maddubs_epi16(nib, _mm256_set1_epi16(0x0110));
        let bytes = _mm256_packus_epi16(pairs, pairs);
        let hi = _mm256_extract_epi64::<0>(bytes) as u64;
        let lo = _mm256_extract_epi64::<2>(bytes) as u64;
        // the first digit is the most significant
        Some((hi.swap_bytes(), lo.swap_bytes()))
    }
}

fn hex16_into(out: &mut Vec<u8>, v: u64) {
    for i in (0..16).rev() {
        out.push(b"0123456789abcdef"[((v >> (4 * i)) & 15) as usize]);
    }
}

/// Every distinct ID of the hex form of one kind, shared by all threads: a slot holds
/// the two numbers of `hex32`, and the slot number is the ID's key. Lock-free: a slot
/// is claimed with a compare-and-swap, written, then published.
struct LongTab {
    slots: Vec<[std::sync::atomic::AtomicU64; 3]>, // hi, lo, state (0 empty, 1 being written, 2 ready)
    mask: usize,
    count: AtomicUsize,
    limit: usize,
}

/// Set when a long-ID table or side list is full: the parse stage then runs again in
/// a process that interns every long ID (LONGIDS=0), which is slow but has no limits.
static LONG_OVERFLOW: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static LONG_OFF: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static LONG_USERS: std::sync::OnceLock<LongTab> = std::sync::OnceLock::new();

fn long_users() -> &'static LongTab {
    LONG_USERS.get_or_init(|| LongTab::new(1 << 16))
}

impl LongTab {
    fn new(cap: usize) -> Self {
        let cap = cap.next_power_of_two().max(1 << 12);
        // fresh zero pages (all slots empty), touched only where used; an AtomicU64
        // has the layout of a u64
        let mut plain = std::mem::ManuallyDrop::new(vec![[0u64; 3]; cap]);
        let slots = unsafe {
            Vec::from_raw_parts(plain.as_mut_ptr() as *mut [std::sync::atomic::AtomicU64; 3], plain.len(), plain.capacity())
        };
        LongTab { slots, mask: cap - 1, count: AtomicUsize::new(0), limit: cap / 4 * 3 }
    }
    #[inline(always)]
    fn home(&self, hi: u64, lo: u64) -> usize {
        ((hi ^ lo.rotate_left(29)).wrapping_mul(GOLD) >> 20) as usize & self.mask
    }
    /// Asks for the cache line where (hi, lo) is or will be.
    #[inline(always)]
    fn prefetch(&self, hi: u64, lo: u64) {
        prefetch(self.slots.as_ptr().wrapping_add(self.home(hi, lo)));
    }
    /// The slot of (hi, lo), adding it if new. None: the table is full.
    #[inline(always)]
    fn intern(&self, hi: u64, lo: u64) -> Option<usize> {
        let mut i = self.home(hi, lo);
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            match s[2].load(Ordering::Acquire) {
                2 => {
                    if s[0].load(Ordering::Relaxed) == hi && s[1].load(Ordering::Relaxed) == lo {
                        return Some(i);
                    }
                    i = (i + 1) & self.mask;
                }
                0 => {
                    if self.count.load(Ordering::Relaxed) >= self.limit {
                        return None;
                    }
                    if s[2].compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                        s[0].store(hi, Ordering::Relaxed);
                        s[1].store(lo, Ordering::Relaxed);
                        s[2].store(2, Ordering::Release);
                        self.count.fetch_add(1, Ordering::Relaxed);
                        return Some(i);
                    }
                    // lost the race for this slot: look at it again
                }
                _ => std::hint::spin_loop(),
            }
        }
    }
    fn get(&self, slot: usize) -> (u64, u64) {
        let s = &self.slots[slot];
        (s[0].load(Ordering::Relaxed), s[1].load(Ordering::Relaxed))
    }
}

/// Key of a user ID. Short form: `key_short`. 'u' + 32 lowercase hex digits:
/// FALLBACK | LONG | its slot in the shared table. Anything else: interned.
#[inline(always)]
fn key_user(s: &[u8]) -> u64 {
    match key_short(s, PFX_USER) {
        Some(k) => k,
        None => key_user_long(s),
    }
}

#[inline(never)]
fn key_user_long(s: &[u8]) -> u64 {
    match user_hex(s) {
        Some((hi, lo)) => key_user_hex(hi, lo),
        None => key_slow(s),
    }
}

/// The two numbers of a user ID of the hex form (None: another form, or LONGIDS=0).
#[inline(always)]
fn user_hex(s: &[u8]) -> Option<(u64, u64)> {
    if s.len() == HEX_ID_LEN && s[0] == PFX_USER && !LONG_OFF.load(Ordering::Relaxed) {
        hex32(unsafe { s.as_ptr().add(1) })
    } else {
        None
    }
}

/// Key of the user ID 'u' + hex(hi) + hex(lo): its slot in the shared table, or, if
/// the table is full, what `key_slow` gives for that text.
#[inline(always)]
fn key_user_hex(hi: u64, lo: u64) -> u64 {
    match long_users().intern(hi, lo) {
        Some(slot) => FALLBACK | LONG | slot as u64,
        None => {
            LONG_OVERFLOW.store(true, Ordering::Relaxed);
            let mut id = vec![PFX_USER];
            hex16_into(&mut id, hi);
            hex16_into(&mut id, lo);
            key_slow(&id)
        }
    }
}

/// What the parser writes for a user ID that is not of the short form until its key
/// is known (no real key: its table slot would be 2^62 - 1).
const USER_PENDING: u64 = u64::MAX;

/// Keys of long user IDs to write a few rows later: the table slot of such an ID is a
/// cache miss, so it is asked for when the ID is seen and read when it has arrived.
struct Pending {
    ring: [(*mut u64, u64, u64); PEND],
    n: usize,
}
const PEND: usize = 8;

impl Pending {
    fn new() -> Self {
        Pending { ring: [(std::ptr::null_mut(), 0, 0); PEND], n: 0 }
    }
    /// `*dst` gets the key of the user ID with the numbers (hi, lo) by the next
    /// `drain` at the latest. `dst` must stay valid until then.
    #[inline(always)]
    fn push(&mut self, dst: *mut u64, hi: u64, lo: u64) {
        long_users().prefetch(hi, lo);
        let j = self.n % PEND;
        if self.n >= PEND {
            let (d, h, l) = self.ring[j];
            unsafe { *d = key_user_hex(h, l) };
        }
        self.ring[j] = (dst, hi, lo);
        self.n += 1;
    }
    fn drain(&mut self) {
        for k in (self.n - self.n.min(PEND))..self.n {
            let (d, h, l) = self.ring[k % PEND];
            unsafe { *d = key_user_hex(h, l) };
        }
        self.n = 0;
    }
}

/// Key of a click ID. Short form: `key_short`. Any other ID of fewer than 2^14 bytes:
/// FALLBACK | LONG | length << 48 | its address in the mapped file (click IDs are only
/// ever compared with each other, and rarely: `key_cmp` reads the bytes there).
#[inline(always)]
fn key_click(s: &[u8]) -> u64 {
    match key_short(s, PFX_CLICK) {
        Some(k) => k,
        None => {
            let a = s.as_ptr() as u64;
            if s.len() < (1 << 14) && a < (1 << 48) {
                FALLBACK | LONG | (s.len() as u64) << 48 | a
            } else {
                key_slow(s)
            }
        }
    }
}

/// Key of a user or click ID.
#[inline(always)]
fn key_p(s: &[u8], pfx: u8) -> u64 {
    if pfx == PFX_USER {
        key_user(s)
    } else {
        debug_assert_eq!(pfx, PFX_CLICK);
        key_click(s)
    }
}

#[inline(never)]
fn key_slow(s: &[u8]) -> u64 {
    let mut g = interner().map.lock().unwrap();
    if let Some(&k) = g.0.get(s) {
        return k;
    }
    let k = FALLBACK | g.1.len() as u64;
    g.1.push(s.to_vec());
    g.0.insert(s.to_vec(), k);
    k
}

/// The ID behind a user or click key.
fn key_bytes(k: u64, pfx: u8, out: &mut Vec<u8>) {
    if k & FALLBACK == 0 {
        out.push(pfx);
        // the payload runs up to the last non-zero byte
        let n = 8 - (k.leading_zeros() / 8) as usize;
        out.extend_from_slice(&k.to_le_bytes()[..n]);
    } else if k & LONG == 0 {
        let g = interner().map.lock().unwrap();
        out.extend_from_slice(&g.1[(k & !FALLBACK) as usize]);
    } else if pfx == PFX_USER {
        let (hi, lo) = long_users().get((k & !(FALLBACK | LONG)) as usize);
        out.push(pfx);
        hex16_into(out, hi);
        hex16_into(out, lo);
    } else {
        let len = ((k >> 48) & 0x3FFF) as usize;
        let at = (k & ((1 << 48) - 1)) as *const u8;
        out.extend_from_slice(unsafe { std::slice::from_raw_parts(at, len) });
    }
}

/// Byte-wise order of the IDs behind two keys of the same kind.
fn key_cmp(a: u64, b: u64, pfx: u8) -> std::cmp::Ordering {
    if (a | b) & FALLBACK == 0 {
        // first byte most significant, zero-padded: the order of the byte strings
        return a.swap_bytes().cmp(&b.swap_bytes());
    }
    if pfx == PFX_USER {
        let t = long_users();
        let slot = |k: u64| (k & !(FALLBACK | LONG)) as usize;
        let is_long = |k: u64| k & (FALLBACK | LONG) == (FALLBACK | LONG);
        if is_long(a) && is_long(b) {
            // two hex IDs: the order of their numbers
            return t.get(slot(a)).cmp(&t.get(slot(b)));
        }
        let short_long = |sk: u64, lk: u64| {
            // 'u' + up to 8 bytes against 'u' + 32 hex digits: the short one's bytes,
            // zero-padded, against the first 8 digits; equal means it is a prefix
            let hi = t.get(slot(lk)).0;
            let mut w = 0u64;
            for i in 0..8 {
                w = w << 8 | b"0123456789abcdef"[((hi >> (60 - 4 * i)) & 15) as usize] as u64;
            }
            sk.swap_bytes().cmp(&w).then(std::cmp::Ordering::Less)
        };
        if a & FALLBACK == 0 && is_long(b) {
            return short_long(a, b);
        }
        if b & FALLBACK == 0 && is_long(a) {
            return short_long(b, a).reverse();
        }
    }
    let mut x = Vec::new();
    let mut y = Vec::new();
    key_bytes(a, pfx, &mut x);
    key_bytes(b, pfx, &mut y);
    x.cmp(&y)
}

/// Bits of a long event's reference (`eid_key`): thread number above, entry + 1 below.
const EREF_IDX_BITS: u32 = 26;
const EREF_MAX_THREADS: usize = 32;

/// An event ID -> (the value stored in records, reference). Short form: the hashed
/// key and 0. 'e' + 32 lowercase hex digits: a hash of its two numbers, and a
/// reference (thread << 26 | entry + 1) to the first number in the thread's side
/// list: for a given first number the hash is one-to-one in the second, so two such
/// IDs are equal iff hashes and first numbers are (`same_event`). Anything else: the
/// hashed interner key and 0.
#[inline(always)]
fn eid_key(s: &[u8], dc: &mut DateCache) -> (u64, u64) {
    match key_short(s, PFX_EVENT) {
        Some(k) => (eid_hash(k), 0),
        None => eid_key_long(s, dc),
    }
}

#[inline(never)]
fn eid_key_long(s: &[u8], dc: &mut DateCache) -> (u64, u64) {
    if s.len() == HEX_ID_LEN && s[0] == PFX_EVENT && !LONG_OFF.load(Ordering::Relaxed) {
        if let Some((hi, lo)) = hex32(unsafe { s.as_ptr().add(1) }) {
            let idx = dc.long_hi.len() as u64;
            if idx + 1 < (1 << EREF_IDX_BITS) {
                dc.long_hi.push(hi);
                return ((hi ^ lo.rotate_left(32)).wrapping_mul(GOLD), dc.tid << EREF_IDX_BITS | (idx + 1));
            }
            LONG_OVERFLOW.store(true, Ordering::Relaxed);
        }
    }
    (eid_hash(key_slow(s)), 0)
}

/// Event key -> the value stored in records: a multiplication by an odd constant, so
/// it is one-to-one; its top bits choose the dedup partition and bucket.
#[inline(always)]
fn eid_hash(k: u64) -> u64 {
    k.wrapping_mul(GOLD)
}

#[inline]
fn mix(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^= x >> 33;
    x
}

// Open-addressing u64 -> u64 map (keys never EMPTY).
struct U64Map {
    keys: Vec<u64>,
    vals: Vec<u64>,
    mask: usize,
}

impl U64Map {
    fn with_capacity(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        U64Map { keys: vec![EMPTY; cap], vals: vec![0; cap], mask: cap - 1 }
    }
    #[inline]
    fn get(&self, k: u64) -> Option<u64> {
        let mut i = mix(k) as usize & self.mask;
        loop {
            let kk = self.keys[i];
            if kk == k {
                return Some(self.vals[i]);
            }
            if kk == EMPTY {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    /// Returns (slot, inserted)
    #[inline]
    fn slot(&mut self, k: u64) -> (usize, bool) {
        let mut i = mix(k) as usize & self.mask;
        loop {
            let kk = self.keys[i];
            if kk == k {
                return (i, false);
            }
            if kk == EMPTY {
                self.keys[i] = k;
                return (i, true);
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ------------------------------------------------------------------ time
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[inline]
fn dig(b: u8) -> Option<i64> {
    if b.is_ascii_digit() {
        Some((b - b'0') as i64)
    } else {
        None
    }
}

#[inline]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    Some(dig(s[i])? * 10 + dig(s[i + 1])?)
}

fn parse_dt(s: &[u8]) -> Option<i64> {
    // YYYY-MM-DDTHH:MM:SS
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let mo = d2(s, 5)?;
    let d = d2(s, 8)?;
    let h = d2(s, 11)?;
    let mi = d2(s, 14)?;
    let se = d2(s, 17)?;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || se > 59 || d < 1 {
        return None;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let dim = match mo {
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
        return None;
    }
    Some((days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + se) * 1000)
}

/// Some(ms) for a valid timestamp (F3 values that overflow become i64::MAX: outside any window).
fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if s.iter().all(|b| b.is_ascii_digit()) {
        let mut i = 0;
        while i < n && s[i] == b'0' {
            i += 1;
        }
        if n - i > 19 {
            return Some(i64::MAX);
        }
        let mut v: u64 = 0;
        for &b in &s[i..] {
            v = v * 10 + (b - b'0') as u64;
        }
        return Some(if v > i64::MAX as u64 { i64::MAX } else { v as i64 });
    }
    if n == 20 && s[19] == b'Z' {
        return parse_dt(&s[..19]);
    }
    if n == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
        let t = parse_dt(&s[..19])?;
        let hh = d2(s, 20)?;
        let mm = d2(s, 23)?;
        if hh > 23 || mm > 59 {
            return None;
        }
        let off = (hh * 60 + mm) * 60_000;
        return Some(if s[19] == b'+' { t - off } else { t + off });
    }
    None
}

fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() || !s.iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut i = 0;
    while i < s.len() && s[i] == b'0' {
        i += 1;
    }
    if s.len() - i > 8 {
        return None;
    }
    let mut v: u32 = 0;
    for &b in &s[i..] {
        v = v * 10 + (b - b'0') as u32;
    }
    if (1..=10_000_000).contains(&v) {
        Some(v)
    } else {
        None
    }
}


/// Faster dispatch: F1 first (most common), then F3, then F2.
#[inline(always)]
fn ts_fast(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 20 {
        return match f1_swar(s) {
            Some(v) => Some(v),
            None => parse_ts(s),
        };
    }
    if n >= 1 && n <= 16 {
        return f3_swar(s);
    }
    if n == 25 {
        return match f2_swar(s) {
            Some(v) => Some(v),
            None => parse_ts(s), // e.g. a zero-padded 25-digit F3
        };
    }
    parse_ts(s)
}

const ONES: u64 = 0x0101_0101_0101_0101;
const HIGH: u64 = 0x8080_8080_8080_8080;

#[inline(always)]
fn rd64(p: *const u8) -> u64 {
    unsafe { std::ptr::read_unaligned(p as *const u64) }
}

/// True when every byte of v is an ASCII digit.
#[inline(always)]
fn all_digits(v: u64) -> bool {
    ((v.wrapping_add(0x46 * ONES)) | (v.wrapping_sub(0x30 * ONES))) & HIGH == 0
}

/// 8 digit values (0..=9, most significant in byte 0) -> number.
#[inline(always)]
fn parse8(v: u64) -> u64 {
    let v = v.wrapping_mul(10).wrapping_add(v >> 8);
    (((v & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000 << 32)))
        .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32))))
        >> 32
}

/// F3 of 1..=16 chars: digits only (leading zeros fine). Reads up to 16 bytes from s.
#[inline(always)]
fn f3_swar(s: &[u8]) -> Option<i64> {
    let n = s.len();
    let p = s.as_ptr();
    let lo = rd64(p) as u128 | ((rd64(unsafe { p.add(8) }) as u128) << 64);
    let keep: u128 = if n == 16 { !0 } else { (1u128 << (8 * n)) - 1 };
    let v = ((lo & keep) << (8 * (16 - n))) | ((0x30 * (ONES as u128 | ((ONES as u128) << 64))) & !(keep << (8 * (16 - n))));
    let a = v as u64;
    let b = (v >> 64) as u64;
    if !all_digits(a) || !all_digits(b) {
        return if s.iter().all(|c| c.is_ascii_digit()) { parse_ts(s) } else { None };
    }
    let hi = parse8(a.wrapping_sub(0x30 * ONES));
    let lo = parse8(b.wrapping_sub(0x30 * ONES));
    Some((hi * 100_000_000 + lo) as i64)
}

/// ingest_ts: the rules guarantee valid F1, so only the value is computed.
#[inline(always)]
fn ingest_fast(s: &[u8]) -> i64 {
    if s.len() != 20 {
        return parse_ts(s).unwrap_or(i64::MAX);
    }
    let p = s.as_ptr();
    let a = rd64(p);
    let b = rd64(unsafe { p.add(8) });
    let c = unsafe { std::ptr::read_unaligned(p.add(16) as *const u32) } as u64;
    // Replace separators by '0' before subtracting so no borrow crosses bytes.
    const MA: u64 = 0x00FF_FF00_FFFF_FFFF;
    const MB: u64 = 0xFFFF_00FF_FF00_FFFF;
    const MC: u64 = 0x00FF_FF00;
    let da = ((a & MA) | (0x30 * ONES & !MA)).wrapping_sub(0x30 * ONES);
    let db = ((b & MB) | (0x30 * ONES & !MB)).wrapping_sub(0x30 * ONES);
    let dc = ((c & MC) | (0x3030_3030 & !MC)).wrapping_sub(0x3030_3030);
    let y = (da & 0xFF) * 1000 + ((da >> 8) & 0xFF) * 100 + ((da >> 16) & 0xFF) * 10 + ((da >> 24) & 0xFF);
    let mo = ((da >> 40) & 0xFF) * 10 + ((da >> 48) & 0xFF);
    let d = (db & 0xFF) * 10 + ((db >> 8) & 0xFF);
    let h = ((db >> 24) & 0xFF) * 10 + ((db >> 32) & 0xFF);
    let mi = ((db >> 48) & 0xFF) * 10 + ((db >> 56) & 0xFF);
    let se = ((dc >> 8) & 0xFF) * 10 + ((dc >> 16) & 0xFF);
    if mo.wrapping_sub(1) >= 12 || y > 9999 || d > 31 || h > 99 || mi > 99 || se > 99 {
        return parse_ts(s).unwrap_or(i64::MAX);
    }
    let days = if (y as usize).wrapping_sub(MS_Y0) < MS_NY {
        MSTART[(y as usize - MS_Y0) * 12 + mo as usize - 1] as i64 + d as i64 - 1
    } else {
        dfc(y as i64, mo as i64, d as i64)
    };
    (days * 86_400 + (h * 3600 + mi * 60 + se) as i64) * 1000
}


/// "HH:MM:SS" as a little-endian u64 -> its three two-digit numbers in bytes 0, 3, 6.
#[inline(always)]
fn hms_lanes(w: u64) -> u64 {
    let d = w & 0x0F0F_000F_0F00_0F0F;
    d.wrapping_mul(10).wrapping_add(d >> 8)
}

/// Seconds of the day from `hms_lanes` output (no range checks).
#[inline(always)]
fn tod_of_lanes(v: u64) -> u64 {
    // (HH + MM << 24) * (3600 << 24 + 60): bits 24..48 hold HH*3600 + MM*60
    let hm = (v & 0x0000_0000_FF00_00FF).wrapping_mul((3600u64 << 24) + 60);
    ((hm >> 24) & 0x00FF_FFFF) + ((v >> 48) & 0xFF)
}

/// Seconds of the day of a valid "HH:MM:SS" (digits, colons, ranges), else None.
#[inline(always)]
fn tod_checked(w: u64) -> Option<u64> {
    // high nibbles of the digits are 3 and the separators are ':'
    if (w & 0xF0F0_FFF0_F0FF_F0F0) != 0x3030_3A30_303A_3030 {
        return None;
    }
    let d = w & 0x0F0F_000F_0F00_0F0F;
    // low nibbles at most 9
    if (d.wrapping_add(0x0606_0006_0600_0606) & 0x1010_0010_1000_1010) != 0 {
        return None;
    }
    let v = d.wrapping_mul(10).wrapping_add(d >> 8);
    // HH <= 23, MM <= 59, SS <= 59 (each lane is at most 99)
    if ((v & 0x00FF_0000_FF00_00FF).wrapping_add(0x0044_0000_4400_0068) & 0x0080_0000_8000_0080) != 0 {
        return None;
    }
    Some(tod_of_lanes(v))
}

/// Per-thread cache of validated dates ("YYYY-MM-DD" -> day number). A dataset has
/// only a few dozen distinct dates, so each timestamp mostly parses only its time.
struct DateCache {
    keys: [u64; 256],
    days: [i64; 256],
    /// first numbers of the long event IDs this thread parsed (`eid_key`)
    long_hi: Vec<u64>,
    /// this thread's number, below EREF_MAX_THREADS
    tid: u64,
    /// long user IDs of event records whose keys are still to be written
    pend: Pending,
}

impl DateCache {
    fn new() -> Self {
        DateCache { keys: [0; 256], days: [i64::MIN; 256], long_hi: Vec::new(), tid: 0, pend: Pending::new() }
    }

    /// Same result as dt19_swar(p).
    #[inline(always)]
    fn dt19(&mut self, p: *const u8) -> Option<i64> {
        const MA: u64 = 0x00FF_FF00_FFFF_FFFF;
        const SA: u64 = 0x2D00_002D_0000_0000;
        // bytes 8..16 = D D T H H : M M ; time digits at 3,4,6,7
        const SBT: u64 = 0x0000_3A00_0054_0000;
        const MBT: u64 = 0xFFFF_00FF_FF00_0000;
        const MBX: u64 = 0xFFFF_00FF_FF00_FFFF;
        const MC: u64 = 0x00FF_FF00;
        const SC: u64 = 0x0000_003A;
        let a = rd64(p);
        if (a & !MA) != SA {
            return None;
        }
        let b = rd64(unsafe { p.add(8) });
        let key = (a & MA) | ((b & 0xFF) << 32) | (((b >> 8) & 0xFF) << 56);
        let idx = (key.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 56) as usize;
        let days = if self.keys[idx] == key && self.days[idx] != i64::MIN {
            self.days[idx]
        } else {
            // validate the whole value the slow way, then cache its date
            let v = dt19_swar(p)?;
            let d = v.div_euclid(86_400_000);
            self.keys[idx] = key;
            self.days[idx] = d;
            d
        };
        // byte 10 is 'T', bytes 11..19 are "HH:MM:SS"
        if (b & 0x00FF_0000) != (b'T' as u64) << 16 {
            return None;
        }
        let tod = tod_checked(rd64(unsafe { p.add(11) }))?;
        Some(days * 86_400_000 + (tod * 1000) as i64)
    }

    #[inline(always)]
    fn ts(&mut self, s: &[u8]) -> Option<i64> {
        let n = s.len();
        if n == 20 {
            if s[19] == b'Z' {
                if let Some(v) = self.dt19(s.as_ptr()) {
                    return Some(v);
                }
            }
            return parse_ts(s);
        }
        if n == 13 {
            // the usual F3: 13 digits = 8 digits * 10^5 + 5 digits
            let p = s.as_ptr();
            let a = rd64(p);
            let b = (rd64(unsafe { p.add(8) }) << 24) | 0x30_3030;
            if all_digits(a) && all_digits(b) {
                let hi = parse8(a.wrapping_sub(0x30 * ONES));
                let lo = parse8(b.wrapping_sub(0x30 * ONES));
                return Some((hi * 100_000 + lo) as i64);
            }
            return None;
        }
        if n >= 1 && n <= 16 {
            return f3_swar(s);
        }
        if n == 25 {
            let sign = s[19];
            if (sign == b'+' || sign == b'-') && s[22] == b':' {
                let g = |i: usize| s[i].wrapping_sub(b'0') as i64;
                let (h0, h1, m0, m1) = (g(20), g(21), g(23), g(24));
                if h0 <= 9 && h1 <= 9 && m0 <= 9 && m1 <= 9 {
                    let hh = h0 * 10 + h1;
                    let mm = m0 * 10 + m1;
                    if hh <= 23 && mm <= 59 {
                        if let Some(t) = self.dt19(s.as_ptr()) {
                            let off = (hh * 60 + mm) * 60_000;
                            return Some(if sign == b'+' { t - off } else { t + off });
                        }
                        return None;
                    }
                }
            }
            return parse_ts(s);
        }
        parse_ts(s)
    }

    /// ingest_ts (always valid F1 per the rules): cached date, time read without checks.
    /// Returns (whole seconds, milliseconds).
    #[inline(always)]
    fn ingest(&mut self, s: &[u8]) -> (i64, i64) {
        const MA: u64 = 0x00FF_FF00_FFFF_FFFF;
        if s.len() == 20 {
            let p = s.as_ptr();
            let a = rd64(p);
            let b = rd64(unsafe { p.add(8) });
            let key = (a & MA) | ((b & 0xFF) << 32) | (((b >> 8) & 0xFF) << 56);
            let idx = (key.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 56) as usize;
            if self.keys[idx] == key && self.days[idx] != i64::MIN {
                let tod = tod_of_lanes(hms_lanes(rd64(unsafe { p.add(11) })));
                let secs = self.days[idx] * 86_400 + tod as i64;
                return (secs, secs * 1000);
            }
            if let Some(v) = self.dt19(p) {
                return (v.div_euclid(1000), v);
            }
        }
        let v = ingest_fast(s);
        (v.div_euclid(1000), v)
    }
}

/// "YYYY-MM-DDTHH:MM:SS" (first 19 bytes at p) via SWAR; validates the calendar.
#[inline(always)]
fn dt19_swar(p: *const u8) -> Option<i64> {
    let a = rd64(p);
    let b = rd64(unsafe { p.add(8) });
    let c = (unsafe { std::ptr::read_unaligned(p.add(16) as *const u32) } as u64) & 0x00FF_FFFF;
    const MA: u64 = 0x00FF_FF00_FFFF_FFFF;
    const SA: u64 = 0x2D00_002D_0000_0000;
    const MB: u64 = 0xFFFF_00FF_FF00_FFFF;
    const SB: u64 = 0x0000_3A00_0054_0000;
    const MC: u64 = 0x00FF_FF00;
    const SC: u64 = 0x0000_003A;
    if (a & !MA) != SA || (b & !MB) != SB || (c & !MC) != SC {
        return None;
    }
    let av = (a & MA) | (0x30 * ONES & !MA);
    let bv = (b & MB) | (0x30 * ONES & !MB);
    let cv = (c & MC) | (0x30 * ONES & !MC);
    if !all_digits(av) || !all_digits(bv) || !all_digits(cv) {
        return None;
    }
    let da = av - 0x30 * ONES;
    let db = bv - 0x30 * ONES;
    let dc = cv - 0x30 * ONES;
    let pa = da * 10 + (da >> 8);
    let pb = db * 10 + (db >> 8);
    let pc = dc * 10 + (dc >> 8);
    let y = (pa & 0xFF) * 100 + ((pa >> 16) & 0xFF);
    let mo = (pa >> 40) & 0xFF;
    let d = pb & 0xFF;
    let h = (pb >> 24) & 0xFF;
    let mi = (pb >> 48) & 0xFF;
    let se = (pc >> 8) & 0xFF;
    if mo.wrapping_sub(1) >= 12 || h > 23 || mi > 59 || se > 59 || d == 0 {
        return None;
    }
    if d > 28 {
        let leap = (y % 4 == 0) & ((y % 100 != 0) | (y % 400 == 0));
        if d > DIM[(leap as usize) * 12 + mo as usize - 1] as u64 {
            return None;
        }
    }
    let days = if (y as usize).wrapping_sub(MS_Y0) < MS_NY {
        MSTART[(y as usize - MS_Y0) * 12 + mo as usize - 1] as i64 + d as i64 - 1
    } else {
        dfc(y as i64, mo as i64, d as i64)
    };
    Some((days * 86_400 + (h * 3600 + mi * 60 + se) as i64) * 1000)
}

/// F1 "YYYY-MM-DDTHH:MM:SSZ" (exactly 20 bytes).
#[inline(always)]
fn f1_swar(s: &[u8]) -> Option<i64> {
    if s[19] != b'Z' {
        return None;
    }
    dt19_swar(s.as_ptr())
}

/// F2 "YYYY-MM-DDTHH:MM:SS+HH:MM" (exactly 25 bytes).
#[inline(always)]
fn f2_swar(s: &[u8]) -> Option<i64> {
    let sign = s[19];
    if (sign != b'+' && sign != b'-') || s[22] != b':' {
        return None;
    }
    let g = |i: usize| s[i].wrapping_sub(b'0') as i64;
    let (h0, h1, m0, m1) = (g(20), g(21), g(23), g(24));
    if h0 > 9 || h1 > 9 || m0 > 9 || m1 > 9 {
        return None;
    }
    let hh = h0 * 10 + h1;
    let mm = m0 * 10 + m1;
    if hh > 23 || mm > 59 {
        return None;
    }
    let t = dt19_swar(s.as_ptr())?;
    let off = (hh * 60 + mm) * 60_000;
    Some(if sign == b'+' { t - off } else { t + off })
}

/// SWAR canon_num for fields inside padded mappings (reads 8 bytes past the prefix).
#[inline(always)]
fn canon_num_fast(s: &[u8], prefix: &[u8]) -> Option<usize> {
    let pl = prefix.len();
    let n = s.len().wrapping_sub(pl);
    if n.wrapping_sub(1) >= 7 || &s[..pl] != prefix || s[pl] == b'0' {
        return None;
    }
    let x = rd64(unsafe { s.as_ptr().add(pl) });
    let sh = 8 * (8 - n);
    let v = (x << sh) | (0x30 * ONES & ((1u64 << sh) - 1));
    if !all_digits(v) {
        return None;
    }
    Some(parse8(v.wrapping_sub(0x30 * ONES)) as usize)
}

/// Parses "p<digits>"-style canonical IDs: prefix then a decimal number without leading zero.
#[inline(always)]
fn canon_num(s: &[u8], prefix: &[u8]) -> Option<usize> {
    let pl = prefix.len();
    if s.len() <= pl || s.len() > pl + 7 || &s[..pl] != prefix || s[pl] == b'0' {
        return None;
    }
    let mut v = 0usize;
    for &b in &s[pl..] {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as usize;
    }
    Some(v)
}

// ------------------------------------------------------------------ records
const KIND_INVALID: u8 = 3;

#[derive(Clone, Copy)]
#[repr(C)]
struct EvRec {
    eid: u64,
    user: u64,
    /// ingest_ts: seconds since the epoch (saturated to 32 bits), or, with ING_LAZY
    /// set, its unparsed time of day (see `ing_secs`)
    ing: u64,
    ts: u32, // ms since TB
    /// price (24 bits) | category << 24 (6 bits) | kind << 30
    pck: u32,
}

const ING_LAZY: u64 = 1 << 63;
const PCK_INVALID: u32 = (KIND_INVALID as u32) << 30;

impl EvRec {
    #[inline(always)]
    fn valid(&self) -> bool {
        self.pck < PCK_INVALID
    }
}

/// The reference of a record's long event ID (`eid_key`), 0 for any other ID: bits
/// 32..63 of a parsed ingest_ts, bits 24..55 of a lazy one.
#[inline(always)]
fn long_ref(r: &EvRec) -> u64 {
    if r.ing & ING_LAZY != 0 {
        (r.ing >> 24) & 0x7FFF_FFFF
    } else {
        r.ing >> 32
    }
}

/// The six digits of the "HH:MM:SS" bytes `w` as nibbles (first digit lowest): what
/// a lazy ingest_ts keeps of its time of day.
#[inline(always)]
fn tod_nibbles(w: u64) -> u64 {
    const M: u64 = 0x0F0F_000F_0F00_0F0F;
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    {
        unsafe { core::arch::x86_64::_pext_u64(w, M) }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let d = w & M;
        (d & 0xF) | (d >> 4 & 0xF0) | (d >> 16 & 0xF00) | (d >> 20 & 0xF000) | (d >> 32 & 0xF_0000) | (d >> 36 & 0xF0_0000)
    }
}

/// `tod_nibbles` back to the digit positions of "HH:MM:SS" (low nibbles only).
#[inline(always)]
fn tod_from_nibbles(v: u64) -> u64 {
    (v & 0xF) | (v & 0xF0) << 4 | (v & 0xF00) << 16 | (v & 0xF000) << 20 | (v & 0xF_0000) << 32 | (v & 0xF0_0000) << 36
}

/// Whether two records with equal `eid` are copies of one event: both IDs short (or
/// interned), or both long with the same first number.
#[inline(always)]
fn same_event(a: &EvRec, b: &EvRec, sides: &[&[u64]]) -> bool {
    let (x, y) = (long_ref(a), long_ref(b));
    if (x | y) == 0 {
        return true;
    }
    if x == 0 || y == 0 {
        return false;
    }
    unsafe { *side_entry(sides, x) == *side_entry(sides, y) }
}

/// Where the first number of the long event ID with reference `r` (not 0) is.
#[inline(always)]
fn side_entry(sides: &[&[u64]], r: u64) -> *const u64 {
    &sides[(r >> EREF_IDX_BITS) as usize][(r & ((1 << EREF_IDX_BITS) - 1)) as usize - 1]
}

/// The ingest_ts seconds of a record. A lazy one (only valid events have them) holds
/// the "HH:MM:SS" digits (`tod_nibbles`) of an ingest_ts on the calendar day of the
/// event's ts.
#[inline(always)]
fn ing_secs(r: &EvRec, tb: i64) -> u64 {
    if r.ing & ING_LAZY == 0 {
        return r.ing & 0xFFFF_FFFF;
    }
    let day = (tb + r.ts as i64).div_euclid(DAY);
    (day * 86_400) as u64 + tod_of_lanes(hms_lanes(tod_from_nibbles(r.ing & 0xFF_FFFF)))
}

#[derive(Clone, Copy)]
#[repr(C)]
struct PEv {
    person: u64,
    ts: u32,
    /// price (24 bits) | category << 24 (6 bits) | kind << 30
    pck: u32,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct PClick {
    person: u64,
    cid: u64,
    ts: u32,
    camp: u32,
}

// ------------------------------------------------------------------ inputs
struct Lookups {
    prod_direct: Vec<u16>, // by number for canonical "p<n>"; u16::MAX = absent
    prod_packed: U64Map,
    prod_str: HashMap<Vec<u8>, u16>,
    unknown_cat: u16,
    cat_names: Vec<Vec<u8>>,
    camp_direct: Vec<u32>, // by number for canonical "c_<n>"; u32::MAX = absent
    camp_packed: U64Map,
    camp_str: HashMap<Vec<u8>, u32>,
    /// every campaign ID of 1..=8 bytes: [its bytes as a number, length << 32 | index],
    /// open addressing, an empty slot has u64::MAX in the second word
    camp_fast: Vec<[u64; 2]>,
    camp_ids: Vec<Vec<u8>>,
    camp_channel: Vec<Vec<u8>>,
}

/// Slot of `camp_fast` where (k, n) is or would go.
#[inline(always)]
fn camp_fast_slot(t: &[[u64; 2]], k: u64, n: usize) -> usize {
    let mask = t.len() - 1;
    let mut i = ((k ^ n as u64).wrapping_mul(GOLD) >> 40) as usize & mask;
    loop {
        let s = unsafe { *t.get_unchecked(i) };
        if s[1] == u64::MAX || (s[0] == k && (s[1] >> 32) as usize == n) {
            return i;
        }
        i = (i + 1) & mask;
    }
}

impl Lookups {
    #[inline(always)]
    fn product_cat(&self, s: &[u8]) -> u16 {
        if let Some(n) = canon_num_fast(s, b"p").filter(|&n| n < DIRECT_MAX) {
            if n < self.prod_direct.len() {
                let c = self.prod_direct[n];
                if c != u16::MAX {
                    return c;
                }
            }
            return self.unknown_cat;
        }
        self.product_cat_slow(s)
    }
    #[inline(never)]
    fn product_cat_slow(&self, s: &[u8]) -> u16 {
        match pack(s) {
            Some(k) => self.prod_packed.get(k).map(|c| c as u16),
            None => self.prod_str.get(s).copied(),
        }
        .unwrap_or(self.unknown_cat)
    }
    #[inline(always)]
    /// The campaign with ID `s`. `s` must have eight readable bytes (it is a field
    /// of a mapped, padded file).
    #[inline(always)]
    fn campaign(&self, s: &[u8]) -> Option<u32> {
        let n = s.len();
        if n.wrapping_sub(1) < 8 {
            // an ID of 1..=8 bytes is in `camp_fast` or is no campaign
            let k = rd64(s.as_ptr()) & (u64::MAX >> (64 - 8 * n));
            let e = unsafe { *self.camp_fast.get_unchecked(camp_fast_slot(&self.camp_fast, k, n)) };
            return if e[1] == u64::MAX { None } else { Some(e[1] as u32) };
        }
        self.campaign_long(s)
    }
    /// `campaign` by the older tables (any length; the reference for the fast path).
    fn campaign_long(&self, s: &[u8]) -> Option<u32> {
        if let Some(n) = canon_num_fast(s, b"c_").filter(|&n| n < DIRECT_MAX) {
            if n < self.camp_direct.len() {
                let c = self.camp_direct[n];
                if c != u32::MAX {
                    return Some(c);
                }
            }
            return None;
        }
        self.campaign_slow(s)
    }
    #[inline(never)]
    fn campaign_slow(&self, s: &[u8]) -> Option<u32> {
        match pack(s) {
            Some(k) => self.camp_packed.get(k).map(|c| c as u32),
            None => self.camp_str.get(s).copied(),
        }
    }
}

fn file_lines(data: &[u8]) -> Vec<&[u8]> {
    let mut v = Vec::new();
    let mut p = match memchr(b'\n', data, 0) {
        Some(p) => p + 1,
        None => data.len(),
    };
    while p < data.len() {
        let e = memchr(b'\n', data, p).unwrap_or(data.len());
        v.push(&data[p..e]);
        p = e + 1;
    }
    v
}

const DIRECT_MAX: usize = 1 << 22;

fn load_lookups(data: &Path) -> Lookups {
    // products.csv: one pass over the padded mapping, no per-row allocation.
    let ppath = data.join("products.csv");
    let pdata: &[u8] = if ppath.exists() { map_file(&ppath) } else { &[] };
    let hdr = memchr(b'\n', pdata, 0).map(|p| p + 1).unwrap_or(pdata.len());
    let mut rows: Vec<(&[u8], u16)> = Vec::with_capacity(pdata.len() / 12 + 16);
    let mut tmp_names: Vec<&[u8]> = Vec::new();
    // category name -> index: a small cache keyed by the name's length and first bytes
    // in front of the list of names (a hit is checked against the full name)
    let mut cache = [(u64::MAX, 0usize); 64];
    chunk_fields_w::<2>(pdata, hdr, pdata.len(), |f| {
        let c = f[1];
        let mut w = [0u8; 8];
        let m = c.len().min(8);
        w[..m].copy_from_slice(&c[..m]);
        let key = u64::from_le_bytes(w) ^ ((c.len() as u64) << 56);
        let slot = &mut cache[(key.wrapping_mul(GOLD) >> 58) as usize];
        let ci = if slot.0 == key && slot.1 < tmp_names.len() && tmp_names[slot.1] == c {
            slot.1
        } else {
            let i = match tmp_names.iter().position(|&n| n == c) {
                Some(i) => i,
                None => {
                    tmp_names.push(c);
                    tmp_names.len() - 1
                }
            };
            *slot = (key, i);
            i
        };
        rows.push((f[0], ci as u16));
    });
    let mut names: Vec<Vec<u8>> = tmp_names.iter().map(|n| n.to_vec()).collect();
    names.push(b"unknown".to_vec());
    names.sort();
    names.dedup();
    let remap: Vec<u16> = tmp_names.iter().map(|n| names.iter().position(|m| m.as_slice() == *n).unwrap() as u16).collect();
    let unknown_cat = names.iter().position(|m| m.as_slice() == b"unknown").unwrap() as u16;
    let mut prod_direct: Vec<u16> = Vec::new();
    let mut prod_packed = U64Map::with_capacity(16);
    let mut prod_str = HashMap::new();
    let mut packed_rows = Vec::new();
    for &(p, t) in &rows {
        let ci = remap[t as usize];
        if let Some(n) = canon_num(p, b"p").filter(|&n| n < DIRECT_MAX) {
            if prod_direct.len() <= n {
                prod_direct.resize(n + 1, u16::MAX);
            }
            prod_direct[n] = ci;
        } else {
            match pack(p) {
                Some(k) => packed_rows.push((k, ci)),
                None => {
                    prod_str.insert(p.to_vec(), ci);
                }
            }
        }
    }
    if !packed_rows.is_empty() {
        prod_packed = U64Map::with_capacity(packed_rows.len());
        for (k, ci) in packed_rows {
            let (s, _) = prod_packed.slot(k);
            prod_packed.vals[s] = ci as u64;
        }
    }
    let cdata = std::fs::read(data.join("campaigns.csv")).unwrap_or_default();
    let mut camp_ids = Vec::new();
    let mut camp_channel = Vec::new();
    let mut camp_direct: Vec<u32> = Vec::new();
    let mut camp_packed = U64Map::with_capacity(4096);
    let mut camp_str = HashMap::new();
    for line in file_lines(&cdata) {
        let mut f = [&b""[..]; 2];
        if split(line, &mut f) {
            let i = camp_ids.len() as u32;
            camp_ids.push(f[0].to_vec());
            camp_channel.push(f[1].to_vec());
            if let Some(n) = canon_num(f[0], b"c_").filter(|&n| n < DIRECT_MAX) {
                if camp_direct.len() <= n {
                    camp_direct.resize(n + 1, u32::MAX);
                }
                camp_direct[n] = i;
            } else {
                match pack(f[0]) {
                    Some(k) => {
                        let (s, _) = camp_packed.slot(k);
                        camp_packed.vals[s] = i as u64;
                    }
                    None => {
                        camp_str.insert(f[0].to_vec(), i);
                    }
                }
            }
        }
    }
    let mut camp_fast = vec![[0u64, u64::MAX]; (camp_ids.len() * 4).next_power_of_two().max(64)];
    for (i, id) in camp_ids.iter().enumerate() {
        let n = id.len();
        if n >= 1 && n <= 8 {
            let mut w = [0u8; 8];
            w[..n].copy_from_slice(id);
            let k = u64::from_le_bytes(w);
            let sl = camp_fast_slot(&camp_fast, k, n);
            // the last row of an ID wins, as in the other tables
            camp_fast[sl] = [k, (n as u64) << 32 | i as u64];
        }
    }
    Lookups {
        prod_direct,
        prod_packed,
        prod_str,
        unknown_cat,
        cat_names: names,
        camp_direct,
        camp_packed,
        camp_str,
        camp_fast,
        camp_ids,
        camp_channel,
    }
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false))
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

struct Chunk {
    kind: u8, // 0 events 1 clicks 2 links
    data: &'static [u8],
    start: usize,
    end: usize,
}

fn make_chunks(kind: u8, files: &[PathBuf], size: usize, out: &mut Vec<Chunk>) {
    for f in files {
        let d = map_file(f);
        let hdr = match memchr(b'\n', d, 0) {
            Some(p) => p + 1,
            None => d.len(),
        };
        let mut s = hdr;
        while s < d.len() {
            let e = (s + size).min(d.len());
            out.push(Chunk { kind, data: d, start: s, end: e });
            s = e;
        }
    }
}

fn parse_manifest(data: &Path) -> (i64, i64) {
    let s = std::fs::read(data.join("manifest.json")).expect("manifest");
    let find = |name: &str| -> i64 {
        let pat = format!("\"{}\"", name);
        let i = s.windows(pat.len()).position(|w| w == pat.as_bytes()).expect("key") + pat.len();
        let rest = &s[i..];
        let q1 = rest.iter().position(|&b| b == b'"').unwrap() + 1;
        let q2 = rest[q1..].iter().position(|&b| b == b'"').unwrap() + q1;
        parse_ts(&rest[q1..q2]).expect("manifest ts")
    };
    (find("window_start"), find("window_end"))
}

/// Growable open-addressing map u64 -> u32 for per-partition grouping.
/// One slot of a `LocalMap`: key and value side by side (one cache line per lookup).
#[derive(Clone, Copy)]
#[repr(C)]
struct LSlot {
    k: u64,
    v: u32,
    pad: u32,
}

const LEMPTY: LSlot = LSlot { k: EMPTY, v: 0, pad: 0 };

struct LocalMap {
    slots: Vec<LSlot>,
    mask: usize,
    len: usize,
    /// bits of `k * GOLD` already used to choose the partition (all keys here share them)
    skip: u32,
    shift: u32,
}

impl LocalMap {
    fn new(skip: u32) -> Self {
        LocalMap { slots: vec![LEMPTY; 1024], mask: 1023, len: 0, skip, shift: 64 - 10 }
    }
    fn clear_for(&mut self, n: usize) {
        let cap = (n * 2).next_power_of_two().max(1024);
        if cap != self.slots.len() {
            self.slots = vec![LEMPTY; cap];
            self.mask = cap - 1;
            self.shift = 64 - cap.trailing_zeros();
        } else {
            self.slots.fill(LEMPTY);
        }
        self.len = 0;
    }
    #[inline(always)]
    fn home(&self, k: u64) -> usize {
        ((k.wrapping_mul(GOLD) << self.skip) >> self.shift) as usize
    }
    /// Returns the value for k, inserting `next` if absent.
    #[inline(always)]
    fn get_or_insert(&mut self, k: u64, next: u32) -> (u32, bool) {
        let mut i = self.home(k);
        loop {
            // i <= mask < slots.len()
            let s = unsafe { self.slots.get_unchecked_mut(i) };
            if s.k == k {
                return (s.v, false);
            }
            if s.k == EMPTY {
                s.k = k;
                s.v = next;
                self.len += 1;
                return (next, true);
            }
            i = (i + 1) & self.mask;
        }
    }
    /// Asks for the cache line that a lookup of k will read first.
    #[inline(always)]
    fn prefetch(&self, k: u64) {
        prefetch(self.slots.as_ptr().wrapping_add(self.home(k)));
    }
    #[inline(always)]
    fn get(&self, k: u64) -> Option<u32> {
        let mut i = self.home(k);
        loop {
            let s = unsafe { *self.slots.get_unchecked(i) };
            if s.k == k {
                return Some(s.v);
            }
            if s.k == EMPTY {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(never)]
    fn grow(&mut self) {
        let old = std::mem::take(&mut self.slots);
        let cap = old.len() * 2;
        self.slots = vec![LEMPTY; cap];
        self.mask = cap - 1;
        self.shift = 64 - cap.trailing_zeros();
        self.len = 0;
        for s in old {
            if s.k != EMPTY {
                self.get_or_insert(s.k, s.v);
            }
        }
    }
}


#[derive(Clone, Copy)]
#[repr(C)]
struct CRec {
    user: u64,
    cid: u64,
    ts: u32,
    camp: u32,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct Slot {
    eid: u64,
    idx: u32,
    ing: u32,
}

const BATCH: usize = 16;
/// Records ahead to prefetch in sequential scans of 32-byte records (16-byte: twice that).
const PF_RECS: usize = 16;
const GOLD: u64 = 0x9E37_79B9_7F4A_7C15;
/// Experiment switches for timing ablations only (outputs are wrong when set).
static XP: AtomicUsize = AtomicUsize::new(0);
#[inline(always)]
fn xp(bit: usize) -> bool {
    XP.load(Ordering::Relaxed) & bit != 0
}

/// user key -> person key, only for IDs whose person name differs from themselves.
struct PMap {
    slots: Vec<[u64; 2]>,
    mask: usize,
    shift: u32,
}

/// Key of an empty `PMap` slot.
const PEMPTY: u64 = 0;

/// The value of the slot whose key is k, or k itself if neither slot has it. On
/// x86-64 this is written as two conditional moves: the compiler otherwise turns the
/// choice into branches, and it is a coin flip per row.
#[inline(always)]
fn pick2(k: u64, s0: [u64; 2], s1: [u64; 2]) -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        let mut r = k;
        unsafe {
            core::arch::asm!(
                "cmp {a}, {k}",
                "cmove {r}, {v0}",
                "cmp {b}, {k}",
                "cmove {r}, {v1}",
                a = in(reg) s0[0],
                b = in(reg) s1[0],
                k = in(reg) k,
                v0 = in(reg) s0[1],
                v1 = in(reg) s1[1],
                r = inout(reg) r,
                options(pure, nomem, nostack)
            );
        }
        r
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let v = if s1[0] == k { s1[1] } else { k };
        if s0[0] == k {
            s0[1]
        } else {
            v
        }
    }
}

impl PMap {
    fn new(n: usize) -> Self {
        // Buckets of two slots (one cache line), sparse on purpose (at most one key
        // per four buckets): most lookups are of users that are not in the map, and a
        // lookup only has to look further when its bucket is full, which is rare.
        // An empty slot has key 0 (no ID has key 0), so the table starts as fresh
        // zero pages and needs no initialization pass.
        let mul: usize = std::env::var("PMUL").ok().and_then(|v| v.parse().ok()).unwrap_or(8);
        let cap = (n * mul).next_power_of_two().max(16);
        PMap { slots: vec![[PEMPTY, 0]; cap], mask: cap - 1, shift: 64 - cap.trailing_zeros() }
    }
    fn insert(&mut self, k: u64, v: u64) {
        let mut i = self.slot_of(k);
        loop {
            for j in [i, i + 1] {
                if self.slots[j][0] == PEMPTY || self.slots[j][0] == k {
                    self.slots[j] = [k, v];
                    return;
                }
            }
            i = (i + 2) & self.mask;
        }
    }
    /// First slot of k's bucket (an even index).
    #[inline(always)]
    fn slot_of(&self, k: u64) -> usize {
        (k.wrapping_mul(GOLD) >> self.shift) as usize & !1
    }
    #[inline(always)]
    fn prefetch(&self, i: usize) {
        prefetch(self.slots.as_ptr().wrapping_add(i));
    }
    /// The person of user key k (k itself when it is not in the map); i = slot_of(k).
    /// Which slot holds k, if any, is a coin flip per row, so that choice is made
    /// without branches (`pick2`); only a full bucket without k takes a branch.
    #[inline(always)]
    fn lookup_from(&self, i: usize, k: u64) -> u64 {
        let (s0, s1) = unsafe { (*self.slots.get_unchecked(i), *self.slots.get_unchecked(i + 1)) };
        // a bucket fills its first slot first
        if (s0[0] != k) & (s1[0] != k) & (s1[0] != PEMPTY) {
            return self.lookup_far(i, k);
        }
        pick2(k, s0, s1)
    }
    #[inline(never)]
    fn lookup_far(&self, mut i: usize, k: u64) -> u64 {
        loop {
            i = (i + 2) & self.mask;
            for j in [i, i + 1] {
                let s = self.slots[j];
                if s[0] == k {
                    return s[1];
                }
                if s[0] == PEMPTY {
                    return k;
                }
            }
        }
    }
}

const RING: usize = 16;
/// Records ahead whose person-map slot is requested in the dedup stage.
const PLOOK: usize = 12;

/// Sorts by the high 32 bits (the timestamp) with four byte-wise counting passes.
/// The order of equal timestamps is not defined (it does not matter to the rules).
#[inline(always)]
fn insertion_sort(e: &mut [u64]) {
    let ep = e.as_mut_ptr();
    for a in 1..e.len() {
        unsafe {
            let x = *ep.add(a);
            let mut b = a;
            while b > 0 && *ep.add(b - 1) > x {
                *ep.add(b) = *ep.add(b - 1);
                b -= 1;
            }
            *ep.add(b) = x;
        }
    }
}

/// How many of the n keys at `kp` are below each of the four keys in `x` (keys are
/// biased: signed order).
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn below4(x: *const i64, kp: *const i64, n: usize) -> [u64; 4] {
    use core::arch::x86_64::*;
    let x = _mm256_loadu_si256(x as *const __m256i);
    let mut c = _mm256_setzero_si256();
    for j in 0..n {
        let y = _mm256_set1_epi64x(*kp.add(j));
        c = _mm256_sub_epi64(c, _mm256_cmpgt_epi64(x, y));
    }
    let mut r = [0u64; 4];
    _mm256_storeu_si256(r.as_mut_ptr() as *mut __m256i, c);
    r
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline(always)]
unsafe fn below4(x: *const i64, kp: *const i64, n: usize) -> [u64; 4] {
    let mut r = [0u64; 4];
    for j in 0..n {
        let y = *kp.add(j);
        for t in 0..4 {
            r[t] += (*x.add(t) > y) as u64;
        }
    }
    r
}

/// Sorts 2..=32 keys without a data-dependent branch: every key goes to the place
/// given by the number of keys below it (four keys against all others per step).
/// An insertion sort mispredicts its loop exit about once per key on unordered
/// input, which costs more than these compares. Equal keys get the same place; the
/// places after it that stay empty take the same value.
#[inline(always)]
fn rank_sort(e: &mut [u64]) {
    const B: u64 = 1 << 63;
    let n = e.len();
    debug_assert!(n >= 2 && n <= 32);
    let n4 = (n + 3) & !3;
    // the padding keys are the largest: below no key, and at place n or later
    let mut k = [i64::MAX; 32];
    let mut out = [0u64; 36];
    let mut seen = 0u64;
    unsafe {
        for i in 0..n {
            *k.get_unchecked_mut(i) = (*e.get_unchecked(i) ^ B) as i64;
        }
        let kp = k.as_ptr();
        let mut i = 0;
        while i < n4 {
            let r = below4(kp.add(i), kp, n);
            for t in 0..4 {
                let rk = r[t] as usize;
                *out.get_unchecked_mut(rk) = (*kp.add(i + t) as u64) ^ B;
                seen |= 1u64 << rk;
            }
            i += 4;
        }
        let mut holes = !seen & ((1u64 << n) - 1);
        while holes != 0 {
            let h = holes.trailing_zeros() as usize;
            *out.get_unchecked_mut(h) = *out.get_unchecked(h - 1);
            holes &= holes - 1;
        }
        std::ptr::copy_nonoverlapping(out.as_ptr(), e.as_mut_ptr(), n);
    }
}

/// Insertion sort of clicks by (ts, click_id).
#[inline(always)]
fn click_insertion_sort(c: &mut [PClick]) {
    let cp = c.as_mut_ptr();
    for a in 1..c.len() {
        unsafe {
            let x = *cp.add(a);
            let mut b = a;
            while b > 0 {
                let y = &*cp.add(b - 1);
                if y.ts < x.ts || (y.ts == x.ts && key_cmp(y.cid, x.cid, PFX_CLICK) != std::cmp::Ordering::Greater) {
                    break;
                }
                *cp.add(b) = *y;
                b -= 1;
            }
            *cp.add(b) = x;
        }
    }
}

/// Sorts 2..=32 clicks by (ts, click_id) like `rank_sort`: each click goes to the place
/// given by the number of (ts, position) keys below its own, which orders them by ts;
/// clicks with equal ts (rare) are then put in click_id order by an insertion sort.
#[inline(always)]
fn click_rank_sort(c: &mut [PClick]) {
    const B: u64 = 1 << 63;
    let n = c.len();
    debug_assert!(n >= 2 && n <= 32);
    let n4 = (n + 3) & !3;
    let mut k = [i64::MAX; 32];
    let mut out = [PClick { person: 0, cid: 0, ts: 0, camp: 0 }; 32];
    let mut tie = false;
    unsafe {
        for i in 0..n {
            *k.get_unchecked_mut(i) = ((((*c.get_unchecked(i)).ts as u64) << 32 | i as u64) ^ B) as i64;
        }
        let kp = k.as_ptr();
        let mut i = 0;
        while i < n4 {
            let r = below4(kp.add(i), kp, n);
            // keys are distinct: the places of the n clicks are 0..n, each once
            // (a padding key's place is n or more and is skipped)
            for t in 0..4 {
                if i + t < n {
                    *out.get_unchecked_mut(r[t] as usize) = *c.get_unchecked(i + t);
                }
            }
            i += 4;
        }
        std::ptr::copy_nonoverlapping(out.as_ptr(), c.as_mut_ptr(), n);
        for i in 1..n {
            tie |= (*c.get_unchecked(i)).ts == (*c.get_unchecked(i - 1)).ts;
        }
    }
    if tie {
        click_insertion_sort(c);
    }
}

/// A person's few clicks (2..=48) in (ts, click_id) order.
#[inline(always)]
fn small_click_sort(c: &mut [PClick]) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        if c.len() <= 32 {
            return click_rank_sort(c);
        }
    }
    click_insertion_sort(c);
}

/// A person's few events (at most 32): `rank_sort` where its compares are vector
/// instructions, else an insertion sort.
#[inline(always)]
fn small_sort(e: &mut [u64]) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        if e.len() >= 2 {
            rank_sort(e);
        }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    insertion_sort(e);
}

/// Sorts 9..=255 keys (timestamp in the high 32 bits, below `32 << bshift`): one
/// counting pass into at most 32 buckets of the timestamp's top bits, then an insertion
/// sort that only has to order each bucket (a person's events of about a day), so its
/// inner loop is short and predictable.
#[inline(always)]
fn bucket_sort_ts(e: &mut [u64], tmp: &mut [u64; 256], bshift: u32) {
    let n = e.len();
    debug_assert!(n < 256);
    let sh = 32 + bshift;
    let mut cnt = [0u64; 4];
    let cp = cnt.as_mut_ptr() as *mut u8;
    for &x in e.iter() {
        unsafe { *cp.add(((x >> sh) & 31) as usize) += 1 };
    }
    // exclusive prefix sums of the 32 byte counters (their total is n < 256)
    let mut pos = [0u64; 4];
    let mut carry = 0u64;
    for i in 0..4 {
        let incl = cnt[i].wrapping_mul(0x0101_0101_0101_0101).wrapping_add(carry.wrapping_mul(0x0101_0101_0101_0101));
        pos[i] = (incl << 8) | carry;
        carry = incl >> 56;
    }
    let pp = pos.as_mut_ptr() as *mut u8;
    for &x in e.iter() {
        unsafe {
            let b = pp.add(((x >> sh) & 31) as usize);
            *tmp.get_unchecked_mut(*b as usize) = x;
            *b += 1;
        }
    }
    e.copy_from_slice(&tmp[..n]);
    insertion_sort(e);
}

fn radix_sort_ts(e: &mut [u64], tmp: &mut Vec<u64>) {
    let n = e.len();
    if tmp.len() < n {
        tmp.resize(n, 0);
    }
    let mut cnt = [[0u32; 256]; 4];
    for &x in e.iter() {
        let t = (x >> 32) as usize;
        cnt[0][t & 255] += 1;
        cnt[1][(t >> 8) & 255] += 1;
        cnt[2][(t >> 16) & 255] += 1;
        cnt[3][t >> 24] += 1;
    }
    let mut src: *mut u64 = e.as_mut_ptr();
    let mut dst: *mut u64 = tmp.as_mut_ptr();
    for d in 0..4 {
        let c = &mut cnt[d];
        let mut acc = 0u32;
        for v in c.iter_mut() {
            let k = *v;
            *v = acc;
            acc += k;
        }
        let sh = 32 + 8 * d;
        for i in 0..n {
            unsafe {
                let x = *src.add(i);
                let b = ((x >> sh) & 255) as usize;
                let at = c.get_unchecked_mut(b);
                *dst.add(*at as usize) = x;
                *at += 1;
            }
        }
        std::mem::swap(&mut src, &mut dst);
    }
    // four passes: the result is back in e
}


/// Per-campaign-day counters of one thread. The two hit for every session (credited
/// sessions, assisted sessions) are small arrays of their own so that they stay in
/// cache; purchases touch the third.
struct CampAcc {
    sess: Vec<u32>,
    assist: Vec<u32>,
    purch: Vec<(u32, i64)>,
}

impl CampAcc {
    fn new(cells: usize) -> Self {
        CampAcc { sess: vec![0; cells], assist: vec![0; cells], purch: vec![(0, 0); cells] }
    }
    #[inline(always)]
    fn credit(&mut self, cell: usize, purch: bool, rev: i64) {
        unsafe {
            *self.sess.get_unchecked_mut(cell) += 1;
            if purch {
                let a = self.purch.get_unchecked_mut(cell);
                a.0 += 1;
                a.1 += rev;
            }
        }
    }
    /// `n` more sessions of this (campaign, day) cell had the campaign in their window.
    #[inline(always)]
    fn assist_n(&mut self, cell: usize, n: u32) {
        unsafe { *self.assist.get_unchecked_mut(cell) += n };
    }
    fn get(&self, cell: usize) -> [i64; 4] {
        [self.sess[cell] as i64, self.purch[cell].0 as i64, self.purch[cell].1, self.assist[cell] as i64]
    }
}

/// Where the event parser puts its records: `slot` gives room for the record of an
/// event id, the parser fills it field by field, `commit` counts it.
trait EvSink {
    /// Whether a record stays where `slot` put it (then the key of a long user ID may
    /// be written into it a few rows later, see `Pending`).
    const DEFER: bool;
    fn slot(&mut self, eid: u64) -> *mut EvRec;
    fn commit(&mut self, eid: u64, d: *mut EvRec);
    #[inline(always)]
    fn put(&mut self, r: EvRec) {
        let d = self.slot(r.eid);
        unsafe { d.write(r) };
        self.commit(r.eid, d);
    }
}

/// The dedup partitions of one parse thread (partition = top bits of the hashed id).
struct PartSink {
    ev: Parts<EvRec>,
    eshift: u32,
}

impl EvSink for PartSink {
    const DEFER: bool = true;
    #[inline(always)]
    fn slot(&mut self, eid: u64) -> *mut EvRec {
        self.ev.slot((eid >> self.eshift) as usize)
    }
    #[inline(always)]
    fn commit(&mut self, eid: u64, d: *mut EvRec) {
        // 32-byte records: ask for the line after the one just written
        self.ev.commit_pf((eid >> self.eshift) as usize, d, 2);
    }
}

/// Records in file order (self-check).
struct VecSink {
    v: Vec<EvRec>,
    tmp: EvRec,
}

impl EvSink for VecSink {
    const DEFER: bool = false;
    fn slot(&mut self, _eid: u64) -> *mut EvRec {
        &mut self.tmp
    }
    fn commit(&mut self, _eid: u64, _d: *mut EvRec) {
        self.v.push(self.tmp);
    }
}

/// Partition writer that asks for each record's destination line first and writes the
/// record RING records later, so the store does not wait for memory.
struct Pusher<T: Copy> {
    ring: [(u32, T); RING],
    n: usize,
    parts: Parts<T>,
}

impl<T: Copy> Pusher<T> {
    fn new(parts: Parts<T>, zero: T) -> Self {
        Pusher { ring: [(0, zero); RING], n: 0, parts }
    }
    #[inline(always)]
    fn offer(&mut self, part: usize, r: T) {
        prefetch_w(self.parts.peek(part));
        let j = self.n % RING;
        if self.n >= RING {
            let (p, o) = self.ring[j];
            self.parts.push(p as usize, o);
        }
        self.ring[j] = (part as u32, r);
        self.n += 1;
    }
    fn drain(&mut self) {
        let m = self.n.min(RING);
        for k in (self.n - m)..self.n {
            let (p, o) = self.ring[k % RING];
            self.parts.push(p as usize, o);
        }
        self.n = 0;
    }
}

#[derive(Clone, Copy)]
struct PendC {
    user: u64,
    cid: u64,
    slot: usize,
    ts: u32,
    camp: u32,
}

/// Clicks: person lookup RING clicks after asking for the map slot, then a delayed write.
struct CSink<'a> {
    ring: [PendC; RING],
    n: usize,
    out: Pusher<PClick>,
    pmap: &'a PMap,
    pshift: u32,
}

impl<'a> CSink<'a> {
    #[inline(always)]
    fn offer(&mut self, c: &CRec) {
        let slot = self.pmap.slot_of(c.user);
        self.pmap.prefetch(slot);
        let j = self.n % RING;
        if self.n >= RING {
            let o = self.ring[j];
            self.emit(&o);
        }
        self.ring[j] = PendC { user: c.user, cid: c.cid, slot, ts: c.ts, camp: c.camp };
        self.n += 1;
    }
    #[inline(always)]
    fn emit(&mut self, o: &PendC) {
        let person = self.pmap.lookup_from(o.slot, o.user);
        self.out.offer((person.wrapping_mul(GOLD) >> self.pshift) as usize, PClick { person, cid: o.cid, ts: o.ts, camp: o.camp });
    }
    fn drain(&mut self) {
        let m = self.n.min(RING);
        for k in (self.n - m)..self.n {
            let o = self.ring[k % RING];
            self.emit(&o);
        }
        self.n = 0;
        self.out.drain();
    }
}

#[derive(Clone, Copy)]
struct PendP {
    user: u64,
    slot: usize,
    ts: u32,
    pck: u32,
}

/// Delays each kept event by RING rows so its person-map slot is in cache when read,
/// then writes it to its person partition.
struct PSink<'a> {
    ring: [PendP; RING],
    n: usize,
    out: Pusher<PEv>,
    pmap: &'a PMap,
    pshift: u32,
}

impl<'a> PSink<'a> {
    #[inline(always)]
    fn offer(&mut self, user: u64, ts: u32, pck: u32) {
        let slot = self.pmap.slot_of(user);
        self.pmap.prefetch(slot);
        let j = self.n % RING;
        if self.n >= RING {
            let o = self.ring[j];
            self.emit(&o);
        }
        self.ring[j] = PendP { user, slot, ts, pck };
        self.n += 1;
    }
    #[inline(always)]
    fn emit(&mut self, o: &PendP) {
        let person = self.pmap.lookup_from(o.slot, o.user);
        self.out.offer((person.wrapping_mul(GOLD) >> self.pshift) as usize, PEv { person, ts: o.ts, pck: o.pck });
    }
    fn drain(&mut self) {
        let m = self.n.min(RING);
        for k in (self.n - m)..self.n {
            let o = self.ring[k % RING];
            self.emit(&o);
        }
        self.n = 0;
        self.out.drain();
    }
}

fn build_links(chunks: &[Chunk], t0: Instant) -> PMap {
    let mut pairs: Vec<[u64; 2]> = Vec::new();
    // Long IDs get their keys a few rows later (their table slots are cache misses,
    // asked for when the ID is read): (pair, side, the ID's two numbers).
    let mut ring = [(0usize, 0usize, 0u64, 0u64); PEND];
    let mut nring = 0usize;
    for c in chunks {
        scan::rows2(c.data, c.start, c.end, |f| {
            if !f[0].is_empty() && !f[1].is_empty() {
                let at = pairs.len();
                pairs.push([0, 0]);
                for side in 0..2 {
                    let id = f[side];
                    let k = match key_short(id, PFX_USER) {
                        Some(k) => k,
                        None => match user_hex(id) {
                            Some((hi, lo)) => {
                                long_users().prefetch(hi, lo);
                                let j = nring % PEND;
                                if nring >= PEND {
                                    let (p, sd, h, l) = ring[j];
                                    pairs[p][sd] = key_user_hex(h, l);
                                }
                                ring[j] = (at, side, hi, lo);
                                nring += 1;
                                USER_PENDING
                            }
                            None => key_slow(id),
                        },
                    };
                    pairs[at][side] = k;
                }
            }
        });
    }
    for k in (nring - nring.min(PEND))..nring {
        let (p, sd, h, l) = ring[k % PEND];
        pairs[p][sd] = key_user_hex(h, l);
    }
    let npairs = pairs.len();
    let t_pairs = t0.elapsed();
    // ID -> dense number. Key and number share a slot (one cache line per lookup); every
    // table and array below is far larger than the cache and hit at random, so each
    // loop asks for what it needs AHEAD items before it uses it.
    const AHEAD: usize = 16;
    let cap = (npairs * 4 + 16).next_power_of_two();
    let shift = 64 - cap.trailing_zeros();
    let mask = cap - 1;
    // an empty slot has key 0 (no ID has key 0): fresh zero pages, no fill pass
    let mut slots: Vec<[u64; 2]> = vec![[PEMPTY, 0]; cap];
    let home = |k: u64| (k.wrapping_mul(GOLD) >> shift) as usize;
    let mut keys: Vec<u64> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }
    let mut ring = [(0u32, 0u32); AHEAD];
    for i in 0..npairs {
        if i + AHEAD < npairs {
            let [a, b] = pairs[i + AHEAD];
            prefetch(slots.as_ptr().wrapping_add(home(a)));
            prefetch(slots.as_ptr().wrapping_add(home(b)));
        }
        let [a, b] = pairs[i];
        let mut ids = [0u32; 2];
        for (j, k) in [a, b].into_iter().enumerate() {
            let mut s = home(k);
            loop {
                let kk = slots[s][0];
                if kk == k {
                    break;
                }
                if kk == PEMPTY {
                    slots[s] = [k, keys.len() as u64];
                    parent.push(keys.len() as u32);
                    keys.push(k);
                    break;
                }
                s = (s + 1) & mask;
            }
            ids[j] = slots[s][1] as u32;
        }
        prefetch(parent.as_ptr().wrapping_add(ids[0] as usize));
        prefetch(parent.as_ptr().wrapping_add(ids[1] as usize));
        // join the pair seen AHEAD pairs ago
        let j = i % AHEAD;
        if i >= AHEAD {
            let (x, y) = ring[j];
            let ra = find(&mut parent, x);
            let rb = find(&mut parent, y);
            if ra != rb {
                parent[ra.max(rb) as usize] = ra.min(rb);
            }
        }
        ring[j] = (ids[0], ids[1]);
    }
    for k in (npairs - npairs.min(AHEAD))..npairs {
        let (x, y) = ring[k % AHEAD];
        let ra = find(&mut parent, x);
        let rb = find(&mut parent, y);
        if ra != rb {
            parent[ra.max(rb) as usize] = ra.min(rb);
        }
    }
    drop(slots);
    let t_groups = t0.elapsed();
    let n = keys.len();
    let mut best: Vec<u64> = vec![EMPTY; n];
    let lt = long_users();
    for i in 0..n {
        if i + AHEAD < n {
            let p = parent[i + AHEAD] as usize;
            prefetch(parent.as_ptr().wrapping_add(p));
            prefetch(best.as_ptr().wrapping_add(p));
            // comparing a long ID reads its numbers in the shared table
            let k = keys[i + AHEAD];
            if k & (FALLBACK | LONG) == (FALLBACK | LONG) {
                prefetch(lt.slots.as_ptr().wrapping_add((k & !(FALLBACK | LONG)) as usize & lt.mask));
            }
        }
        let r = find(&mut parent, i as u32) as usize;
        if best[r] == EMPTY || key_cmp(keys[i], best[r], PFX_USER) == std::cmp::Ordering::Less {
            best[r] = keys[i];
        }
    }
    let mut nmap = 0;
    for i in 0..n {
        if i + AHEAD < n {
            prefetch(best.as_ptr().wrapping_add(parent[i + AHEAD] as usize));
        }
        if best[parent[i] as usize] != keys[i] {
            nmap += 1;
        }
    }
    let t_names = t0.elapsed();
    let mut pm = PMap::new(nmap);
    for i in 0..n {
        if i + AHEAD < n {
            prefetch(best.as_ptr().wrapping_add(parent[i + AHEAD] as usize));
            pm.prefetch(pm.slot_of(keys[i + AHEAD]));
        }
        let b = best[parent[i] as usize];
        if b != keys[i] {
            pm.insert(keys[i], b);
        }
    }
    eprintln!(
        "links: {} pairs, {} ids, {} mapped, {:?} (rows read at {:?}, groups at {:?}, names at {:?})",
        npairs,
        n,
        nmap,
        t0.elapsed(),
        t_pairs,
        t_groups,
        t_names
    );
    pm
}

const fn dfc(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

const MS_Y0: usize = 1970;
const MS_NY: usize = 137;
static MSTART: [i32; MS_NY * 12] = {
    let mut t = [0i32; MS_NY * 12];
    let mut i = 0;
    while i < MS_NY * 12 {
        t[i] = dfc((MS_Y0 + i / 12) as i64, (i % 12) as i64 + 1, 1) as i32;
        i += 1;
    }
    t
};
static DIM: [u8; 24] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

/// YYYY-MM-DDTHH:MM:SS (exactly 19 bytes) -> ms; branch-light.
#[inline(always)]
fn dt_fast(s: &[u8]) -> Option<i64> {
    let s: &[u8; 19] = s.try_into().ok()?;
    let g = |i: usize| s[i].wrapping_sub(b'0') as u32;
    let (y0, y1, y2, y3) = (g(0), g(1), g(2), g(3));
    let (m0, m1, d0, d1) = (g(5), g(6), g(8), g(9));
    let (h0, h1, i0, i1, s0, s1) = (g(11), g(12), g(14), g(15), g(17), g(18));
    let bad = (y0 > 9)
        | (y1 > 9)
        | (y2 > 9)
        | (y3 > 9)
        | (m0 > 9)
        | (m1 > 9)
        | (d0 > 9)
        | (d1 > 9)
        | (h0 > 9)
        | (h1 > 9)
        | (i0 > 9)
        | (i1 > 9)
        | (s0 > 9)
        | (s1 > 9)
        | (s[4] != b'-')
        | (s[7] != b'-')
        | (s[10] != b'T')
        | (s[13] != b':')
        | (s[16] != b':');
    if bad {
        return None;
    }
    let y = y0 * 1000 + y1 * 100 + y2 * 10 + y3;
    let mo = m0 * 10 + m1;
    let d = d0 * 10 + d1;
    let h = h0 * 10 + h1;
    let mi = i0 * 10 + i1;
    let se = s0 * 10 + s1;
    if mo.wrapping_sub(1) >= 12 || h > 23 || mi > 59 || se > 59 || d == 0 {
        return None;
    }
    let leap = (y % 4 == 0) & ((y % 100 != 0) | (y % 400 == 0));
    if d > DIM[(leap as usize) * 12 + mo as usize - 1] as u32 {
        return None;
    }
    let days = if (y as usize).wrapping_sub(MS_Y0) < MS_NY {
        MSTART[(y as usize - MS_Y0) * 12 + mo as usize - 1] as i64 + d as i64 - 1
    } else {
        dfc(y as i64, mo as i64, d as i64)
    };
    Some((days * 86_400 + (h * 3600 + mi * 60 + se) as i64) * 1000)
}


#[repr(C)]
struct Timespec {
    sec: i64,
    nsec: i64,
}
extern "C" {
    fn clock_gettime(clk: i32, ts: *mut Timespec) -> i32;
}
/// Process CPU time in seconds (all threads).
fn cpu_now() -> f64 {
    let mut ts = Timespec { sec: 0, nsec: 0 };
    unsafe { clock_gettime(2, &mut ts) };
    ts.sec as f64 + ts.nsec as f64 * 1e-9
}

/// user_daily rows of one thread, already in the layout the Parquet page needs.
struct UserCols {
    uid: Vec<u8>,
    day: Vec<i32>,
    sess: Vec<i32>,
    ev: Vec<i32>,
    rev: Vec<i64>,
    top: Vec<u8>,
    /// one bit per row: top_category is present
    defs: Vec<u8>,
    n: usize,
}

impl UserCols {
    fn new() -> Self {
        UserCols { uid: Vec::new(), day: Vec::new(), sess: Vec::new(), ev: Vec::new(), rev: Vec::new(), top: Vec::new(), defs: Vec::new(), n: 0 }
    }
    #[inline(always)]
    fn push(&mut self, name: &[u8], day: i32, s: i64, ev: i64, rev: i64, top: Option<&[u8]>) {
        self.uid.extend_from_slice(&(name.len() as u32).to_le_bytes());
        self.uid.extend_from_slice(name);
        self.day.push(day);
        self.sess.push(s as i32);
        self.ev.push(ev as i32);
        self.rev.push(rev);
        if self.n & 7 == 0 {
            self.defs.push(0);
        }
        if let Some(c) = top {
            self.top.extend_from_slice(&(c.len() as u32).to_le_bytes());
            self.top.extend_from_slice(c);
            *self.defs.last_mut().unwrap() |= 1 << (self.n & 7);
        }
        self.n += 1;
    }
}

/// What a parse thread needs to turn an event row into a record.
struct PCtx<'a> {
    lk: &'a Lookups,
    ws: i64,
    we: i64,
    tb: i64,
    /// start (ms) of the UTC day that holds `tb`
    day_base: i64,
    /// per UTC day from that one to the end of the window: its "YYYY-MM-DD" text as
    /// [first eight bytes, last two bytes | DAY_TEXT_OK] (see `day_texts`)
    days: &'a [[u64; 2]],
}

const DAY_TEXT_OK: u64 = 1 << 32;

/// Year, month, day of a day number (days since 1970-01-01); inverse of `days_from_civil`.
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + (m <= 2) as i64, m, d)
}

/// The "YYYY-MM-DD" text of every UTC day from the one holding `tb` to the one holding
/// `we - 1`, in the form `PCtx::days` wants. An ingest_ts whose first ten bytes are
/// the text of the day of the event's ts is on that calendar day. A year outside
/// 0000..=9999 has no such text: its entry never matches.
fn day_texts(tb: i64, we: i64) -> Vec<[u64; 2]> {
    let d0 = tb.div_euclid(DAY);
    let d1 = (we - 1).div_euclid(DAY).max(d0);
    (d0..=d1)
        .map(|z| {
            let (y, m, d) = civil_from_days(z);
            if !(0..=9999).contains(&y) {
                return [0, 0];
            }
            let t = format!("{:04}-{:02}-{:02}", y, m, d).into_bytes();
            let a = u64::from_le_bytes([t[0], t[1], t[2], t[3], t[4], t[5], t[6], t[7]]);
            let b = u16::from_le_bytes([t[8], t[9]]) as u64;
            [a, b | DAY_TEXT_OK]
        })
        .collect()
}

#[inline(always)]
fn event_kind(t: &[u8]) -> u8 {
    match t.len() {
        4 => {
            let w = u32::from_le_bytes([t[0], t[1], t[2], t[3]]);
            if w == u32::from_le_bytes(*b"view") {
                0
            } else if w == u32::from_le_bytes(*b"cart") {
                1
            } else {
                KIND_INVALID
            }
        }
        8 if rd64(t.as_ptr()) == u64::from_le_bytes(*b"purchase") => 2,
        _ => KIND_INVALID,
    }
}

/// The record of one event row from its seven unquoted fields (the reference form of
/// what `parse_events` computes); None for an empty event_id.
#[inline(always)]
fn event_rec(f: &[&[u8]; 7], cx: &PCtx, dc: &mut DateCache) -> Option<EvRec> {
    if f[0].is_empty() {
        return None;
    }
    let (eid, eref) = eid_key(f[0], dc);
    let (ingest_s, ingest_ms) = dc.ingest(f[3]);
    let ing = ingest_s.clamp(0, u32::MAX as i64) as u64 | eref << 32;
    let mut r = EvRec { eid, user: 0, ing, ts: 0, pck: PCK_INVALID };
    'v: {
        if f[1].is_empty() || f[5].is_empty() {
            break 'v;
        }
        let Some(ts) = dc.ts(f[2]) else { break 'v };
        if ts < cx.ws || ts >= cx.we {
            break 'v;
        }
        let kind = event_kind(f[4]);
        if kind == KIND_INVALID {
            break 'v;
        }
        // price and category only matter for purchases
        let (price, cat) = if kind == 2 {
            match parse_price(f[6]) {
                Some(p) => (p, cx.lk.product_cat(f[5])),
                None => break 'v,
            }
        } else {
            (0, 0)
        };
        if ingest_ms.saturating_sub(ts) > DAY {
            break 'v;
        }
        r.user = key_user(f[1]);
        r.ts = (ts - cx.tb) as u32;
        r.pck = price | (cat as u32) << 24 | (kind as u32) << 30;
    }
    Some(r)
}

/// Event rows whose first byte lies in [start, end) -> `sink`, same records as
/// `rows7` + `event_rec`. Written out by hand: the fields of the first 64-byte window
/// (event_id, user_id, ts) are used before the second window is split, and each value
/// goes straight into the record's place in its partition, so few values are alive at
/// once. Any row that is not of the usual shape goes through the generic splitter and
/// `event_rec`.
#[inline(always)]
fn parse_events<S: EvSink>(d: &[u8], start: usize, end: usize, cx: &PCtx, dc: &mut DateCache, sink: &mut S) {
    use scan::{below, blsr, masks64, prefetch_in, quote_starts, slow_line};
    let len = d.len();
    let mut p = start;
    if p > 0 && d[p - 1] != b'\n' {
        match memchr(b'\n', d, p) {
            Some(q) => p = q + 1,
            None => return,
        }
    }
    let base = d.as_ptr();
    let full = unsafe { std::slice::from_raw_parts(base, len + 192) };
    let pf = scan::PF_DIST.load(Ordering::Relaxed);
    let (ws, tb) = (cx.ws, cx.tb);
    let span = (cx.we - cx.ws) as u64;
    let sl = |at: *const u8, n: usize| -> &[u8] { unsafe { std::slice::from_raw_parts(at, n) } };
    while p < end {
        macro_rules! slow {
            () => {{
                p = slow_line::<7>(d, p, &mut |f| {
                    if let Some(r) = event_rec(f, cx, dc) {
                        sink.put(r)
                    }
                });
                continue;
            }};
        }
        prefetch_in(base.wrapping_add(p + pf));
        prefetch_in(base.wrapping_add(p + pf + 64));
        // window 0: event_id, user_id, ts
        let (n0, c0, q0) = masks64(full, p);
        let s0 = n0 | c0;
        let m1 = blsr(s0);
        let m2 = blsr(m1);
        let (k0, k1, k2) = (s0.trailing_zeros(), m1.trailing_zeros(), m2.trailing_zeros());
        let low3 = s0 ^ blsr(m2);
        // where the three fields are (without their quotes), and where the row goes on
        let (eid_at, l0, user_at, l1, ts_at, l2, p2);
        if m2 != 0 && (n0 & low3) == 0 {
            // no "any quote?" test first: that branch is a coin flip (half of the rows)
            let qa = q0 & below(k2);
            let qs = match quote_starts(qa, low3, k2) {
                Some(x) => x,
                None => slow!(),
            };
            let (k0, k1, k2) = (k0 as usize, k1 as usize, k2 as usize);
            let (t0, t1, t2) = ((qs & 1) as usize, ((qs >> 1) & 1) as usize, ((qs >> 2) & 1) as usize);
            let a = unsafe { base.add(p) };
            eid_at = unsafe { a.add(t0) };
            l0 = k0 - 2 * t0;
            user_at = unsafe { a.add(k0 + 1 + t1) };
            l1 = k1 - k0 - 1 - 2 * t1;
            ts_at = unsafe { a.add(k1 + 1 + t2) };
            l2 = k2 - k1 - 1 - 2 * t2;
            p2 = p + k2 + 1;
        } else {
            // A long event_id (or user_id) pushes the third comma out of the window:
            // take the event_id from this window and user_id and ts from a window
            // that starts at the user_id.
            let first = s0 & s0.wrapping_neg();
            if s0 == 0 || (n0 & first) != 0 {
                slow!();
            }
            let qs = match quote_starts(q0 & below(k0), first, k0) {
                Some(x) => x,
                None => slow!(),
            };
            let (k0, t0) = (k0 as usize, (qs & 1) as usize);
            let pb = p + k0 + 1;
            let (nb, cb, qb) = masks64(full, pb);
            let sb = nb | cb;
            let mb = blsr(sb);
            let (j0, j1) = (sb.trailing_zeros(), mb.trailing_zeros());
            let low2 = sb ^ blsr(mb);
            if mb == 0 || (nb & low2) != 0 {
                slow!();
            }
            let qs = match quote_starts(qb & below(j1), low2, j1) {
                Some(x) => x,
                None => slow!(),
            };
            let (j0, j1) = (j0 as usize, j1 as usize);
            let (t1, t2) = ((qs & 1) as usize, ((qs >> 1) & 1) as usize);
            eid_at = unsafe { base.add(p + t0) };
            l0 = k0 - 2 * t0;
            user_at = unsafe { base.add(pb + t1) };
            l1 = j0 - 2 * t1;
            ts_at = unsafe { base.add(pb + j0 + 1 + t2) };
            l2 = j1 - j0 - 1 - 2 * t2;
            p2 = pb + j1 + 1;
        }
        if l0 == 0 {
            slow!();
        }
        let (eid, eref) = eid_key(sl(eid_at, l0), dc);
        let rec = sink.slot(eid);
        // a user ID that is not of the short form gets its key at the end of the row
        let user = if l1 != 0 {
            match key_short(sl(user_at, l1), PFX_USER) {
                Some(k) => k,
                None => USER_PENDING,
            }
        } else {
            0
        };
        // a missing ts counts as one far before the window
        let tsv = match dc.ts(sl(ts_at, l2)) {
            Some(t) => t,
            None => i64::MIN,
        };
        let inw = (tsv.wrapping_sub(ws) as u64) < span;
        unsafe {
            (*rec).eid = eid;
            (*rec).user = user;
            (*rec).ts = tsv.wrapping_sub(tb) as u32;
        }
        // window 1: ingest_ts, event_type, product_id, price_cents
        let (n1, c1, q1) = masks64(full, p2);
        let s1 = n1 | c1;
        let r1 = blsr(s1);
        let r2 = blsr(r1);
        let r3 = blsr(r2);
        let (k3, k4, k5, k6) = (s1.trailing_zeros(), r1.trailing_zeros(), r2.trailing_zeros(), r3.trailing_zeros());
        let low4 = s1 ^ blsr(r3);
        if r3 == 0 || (n1 & low4) != (1u64 << k6) {
            slow!();
        }
        let qb = q1 & below(k6);
        let qs = match quote_starts(qb, low4, k6) {
            Some(x) => x,
            None => slow!(),
        };
        let (k3, k4, k5, k6) = (k3 as usize, k4 as usize, k5 as usize, k6 as usize);
        let (t3, t4, t5, t6) = ((qs & 1) as usize, ((qs >> 1) & 1) as usize, ((qs >> 2) & 1) as usize, ((qs >> 3) & 1) as usize);
        let b = unsafe { base.add(p2) };
        p = p2 + k6 + 1;
        // ingest_ts on the calendar day of a ts inside the window (its first ten bytes
        // are that day's text) is not late, and its value only matters if the event
        // has duplicates: a valid event keeps the eight "HH:MM:SS" bytes instead of
        // the parsed value.
        let ing_at = unsafe { b.add(t3) };
        let l3 = k3 - 2 * t3;
        // inw: day_base <= tb <= ws <= tsv < we, so the day is in the table
        let di = if inw { (tsv.wrapping_sub(cx.day_base) as u64 / DAY as u64) as usize } else { 0 };
        let dt = unsafe { *cx.days.get_unchecked(di) };
        let same_day = inw
            & (l3 == 20)
            & (rd64(ing_at) == dt[0])
            & (unsafe { std::ptr::read_unaligned(ing_at.add(8) as *const u16) } as u64 | DAY_TEXT_OK == dt[1]);
        let (ing, late) = if same_day {
            (0, false)
        } else {
            let (s, ms) = dc.ingest(sl(ing_at, l3));
            (s.clamp(0, u32::MAX as i64) as u64, ms.saturating_sub(tsv) > DAY)
        };
        let mut pck = PCK_INVALID;
        'v: {
            let l5 = k5 - k4 - 1 - 2 * t5;
            if !inw | late | (l5 == 0) | (user == 0) {
                break 'v;
            }
            let kind = event_kind(sl(unsafe { b.add(k3 + 1 + t4) }, k4 - k3 - 1 - 2 * t4));
            if kind == KIND_INVALID {
                break 'v;
            }
            let (price, cat) = if kind == 2 {
                match parse_price(sl(unsafe { b.add(k5 + 1 + t6) }, k6 - k5 - 1 - 2 * t6)) {
                    Some(pr) => (pr, cx.lk.product_cat(sl(unsafe { b.add(k4 + 1 + t5) }, l5))),
                    None => break 'v,
                }
            } else {
                (0, 0)
            };
            pck = price | (cat as u32) << 24 | (kind as u32) << 30;
        }
        unsafe {
            (*rec).pck = pck;
            if pck < PCK_INVALID {
                (*rec).ing = if same_day { ING_LAZY | eref << 24 | tod_nibbles(rd64(ing_at.add(11))) } else { ing | eref << 32 };
                if user == USER_PENDING {
                    let id = sl(user_at, l1);
                    match user_hex(id) {
                        Some((hi, lo)) if S::DEFER => dc.pend.push(std::ptr::addr_of_mut!((*rec).user), hi, lo),
                        Some((hi, lo)) => (*rec).user = key_user_hex(hi, lo),
                        None => (*rec).user = key_slow(id),
                    }
                }
            } else {
                (*rec).user = 0;
                (*rec).ts = 0;
                (*rec).ing = (if same_day { dc.ingest(sl(ing_at, l3)).0.clamp(0, u32::MAX as i64) as u64 } else { ing }) | eref << 32;
            }
        }
        sink.commit(eid, rec);
    }
    dc.pend.drain();
}

fn same_rec(a: &EvRec, b: &EvRec, tb: i64) -> bool {
    a.eid == b.eid && a.user == b.user && a.ts == b.ts && ing_secs(a, tb) == ing_secs(b, tb) && a.pck == b.pck
}

fn main() {
    let t0 = Instant::now();
    // Calibration only: DRY=1 runs the whole pipeline once more in a child first
    // (output under the scratch dir), to measure the scorer's fixed overhead.
    if std::env::var_os("DRY").is_some() {
        let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("/sessions"));
        let scratch = std::env::var("SCRATCH_DIR").unwrap_or("/scratch".into());
        let _ = std::process::Command::new(exe).env_remove("DRY").env("OUTPUT_DIR", format!("{}/dry_out", scratch)).status();
    }
    XP.store(std::env::var("XP").ok().and_then(|v| v.parse().ok()).unwrap_or(0), Ordering::Relaxed);
    if let Some(v) = std::env::var_os("NOHUGE") {
        alloc::NOHUGE.store(true, std::sync::atomic::Ordering::Relaxed);
        if v == "2" {
            alloc::SMALLPAGES.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }
    if std::env::var_os("POPULATE").is_some() {
        alloc::POPULATE.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let nthreads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
        .clamp(1, EREF_MAX_THREADS);
    // LONGIDS=0: every ID that is not of the short form is interned (no size limits;
    // the fallback when a long-ID table overflows, see LONG_OVERFLOW)
    if std::env::var("LONGIDS").map(|v| v == "0").unwrap_or(false) {
        LONG_OFF.store(true, Ordering::Relaxed);
    }
    let sprof: u32 = std::env::var("SPROF").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    let prof: u32 = std::env::var("PROF").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    let (ws, we) = parse_manifest(&data);
    let chunk_kb: usize = std::env::var("CHUNK_KB").ok().and_then(|v| v.parse().ok()).unwrap_or(2048);
    // The lookup tables load on a thread of their own while this one lists and maps
    // the input files (both are a few milliseconds of serial start-up).
    let (lk, link_chunks, chunks) = std::thread::scope(|sc| {
        let h = sc.spawn(|| load_lookups(&data));
        let mut link_chunks = Vec::new();
        make_chunks(2, &list_csv(&data.join("links")), 1 << 20, &mut link_chunks);
        let mut chunks = Vec::new();
        make_chunks(0, &list_csv(&data.join("events")), chunk_kb << 10, &mut chunks);
        make_chunks(1, &list_csv(&data.join("clicks")), chunk_kb << 10, &mut chunks);
        (h.join().unwrap(), link_chunks, chunks)
    });
    let ncamp = lk.camp_ids.len();
    let ncat = lk.cat_names.len();
    assert!(ncat <= 64, "too many categories for the packed layout");
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;
    // Time base: all kept times are u32 ms offsets from TB.
    let tb = ws - WEEK;
    assert!(we - tb < u32::MAX as i64, "window too long");

    // ---------------- chunks
    let total_bytes: usize = chunks.iter().map(|c| c.end - c.start).sum();
    // ~10-20k event rows per dedup partition: its bucket bitmap stays in L2 and the
    // scatter has few enough targets to keep their lines in cache.
    let est_rows = total_bytes / 85;
    let auto_bits = (64 - ((est_rows / 16000).max(1) as u64).leading_zeros()).clamp(8, 12);
    let ebits: u32 = std::env::var("EBITS").ok().and_then(|v| v.parse().ok()).unwrap_or(auto_bits);
    let npe: usize = 1 << ebits;
    let pch: Option<usize> = std::env::var("PCH").ok().and_then(|v| v.parse().ok());
    // 256 person partitions at the practice size (measured best on the scorer: more
    // make the scatter slower); 512 for larger inputs, where the tables of a partition
    // would otherwise outgrow the cache (60M events: sessions stage -3 to -6%, dedup
    // stage +2 to +3% on two kinds of CPU).
    let auto_pbits: u32 = if est_rows < 32_000_000 { 8 } else { 9 };
    let pbits: u32 = std::env::var("PBITS").ok().and_then(|v| v.parse().ok()).unwrap_or(auto_pbits);
    let npp: usize = 1 << pbits;
    let pshift: u32 = 64 - pbits;
    let eshift: u32 = 64 - ebits;
    let block_recs = (total_bytes / 80 / nthreads / 4).clamp(1 << 14, 1 << 22);

    // ---------------- parse (thread 0 resolves links first)
    let selfcheck = std::env::var_os("SELFCHECK").is_some();
    let checked = AtomicUsize::new(0);
    let checked = &checked;
    if let Some(v) = std::env::var("PFD").ok().and_then(|v| v.parse::<usize>().ok()) {
        scan::PF_DIST.store(v, Ordering::Relaxed);
    }
    // room for one long user ID per 32 rows or more, used up to three quarters (the
    // generator makes about one per 110 rows: one user ID in eight, many rows per ID)
    let long_cap: usize = std::env::var("LONGCAP").ok().and_then(|v| v.parse().ok()).unwrap_or(est_rows / 32);
    let _ = LONG_USERS.set(LongTab::new(long_cap));
    let day_text_v = day_texts(tb, we);
    let day_text = &day_text_v[..];
    eprintln!("start: {:?}", t0.elapsed());
    let next = AtomicUsize::new(0);
    struct Parsed {
        ev: Parts<EvRec>,
        cl: Vec<CRec>,
        pmap: Option<PMap>,
        /// side list of the long event IDs (`eid_key`)
        long_hi: Vec<u64>,
    }
    // Calibration only: RP=1 / RS=1 run the parse / session stage once more first.
    let rp: usize = std::env::var("RP").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    let rs: usize = std::env::var("RS").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    let mut parsed_keep: Option<Vec<Parsed>> = None;
    for _rep in 0..=rp {
    next.store(0, Ordering::Relaxed);
    if let Some(old) = parsed_keep.take() {
        std::mem::forget(old);
    }
    let parsed: Vec<Parsed> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nthreads)
            .map(|tid| {
                let link_chunks = &link_chunks;
                let chunks = &chunks;
                let next = &next;
                let lk = &lk;
                sc.spawn(move || {
                    let pmap = if tid == 0 { Some(build_links(link_chunks, t0)) } else { None };
                    let mut dcache = DateCache::new();
                    dcache.tid = tid as u64;
                    let cx = PCtx { lk, ws, we, tb, day_base: tb.div_euclid(DAY) * DAY, days: day_text };
                    let ev: Parts<EvRec> = Parts::with_chunk(npe, block_recs, pch.unwrap_or(if ebits <= 10 { 1024 } else { 512 }));
                    let zero = EvRec { eid: 0, user: 0, ing: 0, ts: 0, pck: PCK_INVALID };
                    let mut es = PartSink { ev, eshift };
                    let mut cl: Vec<CRec> = Vec::new();
                    let mut sink = 0u64;
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= chunks.len() {
                            break;
                        }
                        let c = &chunks[i];
                        if prof == 10 && c.kind == 0 {
                            continue;
                        }
                        if prof == 9 {
                            let mut b = c.start / 64;
                            while b * 64 < c.end { let (n, cc, q) = scan::masks64(c.data, b * 64); sink = sink.wrapping_add((n.count_ones() + cc.count_ones() + q.count_ones()) as u64); b += 1; }
                            continue;
                        }
                        if prof > 0 && prof != 10 {
                            if c.kind == 0 {
                                chunk_fields::<7>(c.data, c.start, c.end, |f| {
                                    let mut x = f[0].len() as u64 ^ f[6].len() as u64;
                                    if prof >= 2 { x ^= eid_key(f[0], &mut dcache).0; }
                                    if prof >= 3 { x ^= key_user(f[1]); }
                                    if prof >= 4 { x ^= ts_fast(f[2]).unwrap_or(0) as u64; }
                                    if prof >= 5 { x ^= ts_fast(f[3]).unwrap_or(0) as u64; }
                                    if prof >= 6 { x ^= lk.product_cat(f[5]) as u64 ^ parse_price(f[6]).unwrap_or(0) as u64 ^ (f[4] == b"view") as u64; }
                                    if prof >= 7 { es.ev.push((mix(x) >> eshift) as usize, EvRec { eid: x, user: x, ing: 0, ts: 0, pck: 0 }); }
                                    sink = sink.wrapping_add(x);
                                });
                            }
                            continue;
                        }
                        if c.kind == 0 {
                            if selfcheck {
                                // both forms of the event parser must give the same records
                                let mut xs = VecSink { v: Vec::new(), tmp: zero };
                                let mut y: Vec<EvRec> = Vec::new();
                                parse_events(c.data, c.start, c.end, &cx, &mut dcache, &mut xs);
                                let x = xs.v;
                                scan::rows7(c.data, c.start, c.end, |f| {
                                    if let Some(r) = event_rec(f, &cx, &mut dcache) {
                                        y.push(r)
                                    }
                                });
                                assert_eq!(x.len(), y.len(), "selfcheck: record counts differ");
                                for (i, (u, v)) in x.iter().zip(&y).enumerate() {
                                    assert!(same_rec(u, v, tb), "selfcheck: record {} differs", i);
                                }
                                checked.fetch_add(x.len(), Ordering::Relaxed);
                            }
                            parse_events(c.data, c.start, c.end, &cx, &mut dcache, &mut es);
                        } else {
                            rows4(c.data, c.start, c.end, #[inline(always)] |f| {
                                if f[0].is_empty() || f[1].is_empty() {
                                    return;
                                }
                                let ts = match dcache.ts(f[3]) {
                                    Some(t) if t >= tb && t < we => t,
                                    _ => return,
                                };
                                if selfcheck {
                                    assert_eq!(lk.campaign(f[2]), lk.campaign_long(f[2]), "selfcheck: campaign lookup");
                                }
                                let Some(camp) = lk.campaign(f[2]) else { return };
                                cl.push(CRec { user: key_user(f[1]), cid: key_click(f[0]), ts: (ts - tb) as u32, camp });
                            });
                        }
                    }
                    let mut ev = es.ev;
                    ev.finish();
                    if sink == 7 { eprintln!("."); }
                    Parsed { ev, cl, pmap, long_hi: std::mem::take(&mut dcache.long_hi) }
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    parsed_keep = Some(parsed);
    }
    let mut parsed = parsed_keep.unwrap();
    let pmap = parsed[0].pmap.take().unwrap();
    let pmap = &pmap;
    eprintln!("parsed: {:?} cpu {:.3}", t0.elapsed(), cpu_now());
    if LONG_OVERFLOW.load(Ordering::Relaxed) {
        // More long IDs than the tables hold: nothing has been written yet, so do
        // the whole run again in the mode without those tables.
        eprintln!("long-ID tables are full: running again with LONGIDS=0");
        let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("/sessions"));
        let st = std::process::Command::new(exe).env("LONGIDS", "0").status();
        std::process::exit(st.ok().and_then(|s| s.code()).unwrap_or(1));
    }
    if selfcheck {
        eprintln!("selfcheck: {} records equal", checked.load(Ordering::Relaxed));
    }

    // ---------------- dedup by event id (+ clicks) -> person partitions
    const CLICK_TASK: usize = 1 << 16;
    let mut tasks: Vec<(usize, usize, usize)> = (0..npe).map(|p| (0, p, 0)).collect();
    for (t, pr) in parsed.iter().enumerate() {
        let mut s = 0;
        while s < pr.cl.len() {
            tasks.push((1 + t, s, (s + CLICK_TASK).min(pr.cl.len())));
            s += CLICK_TASK;
        }
    }
    // The parsed input files, in pieces of 32 MB: page tables to drop (see `unmap_pages`).
    const ZAP_TASK: usize = 32 << 20;
    if std::env::var_os("NOZAP").is_none() {
        let mut seen = 0usize;
        for c in chunks.iter() {
            let base = c.data.as_ptr() as usize;
            if base == seen {
                continue;
            }
            seen = base;
            let mut s = 0;
            while s < c.data.len() {
                let e = (s + ZAP_TASK).min(c.data.len());
                tasks.push((usize::MAX, base + s, e - s));
                s = e;
            }
        }
    }
    let reuse = std::env::var_os("NOREUSE").is_none();
    // per-person sort: buckets of the timestamp so that (we - tb) >> bshift < 32
    let bshift: u32 = (32 - ((we - tb) as u32).leading_zeros()).saturating_sub(5);
    let bsmin: usize = std::env::var("BSMIN").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(32).min(32);
    let rank = std::env::var_os("NORANK").is_none();
    let plook: usize = std::env::var("PLOOK").ok().and_then(|v| v.parse().ok()).unwrap_or(PLOOK);
    let lmd: usize = std::env::var("LMD").ok().and_then(|v| v.parse().ok()).unwrap_or(16);
    let bfac: usize = std::env::var("BFAC").ok().and_then(|v| v.parse().ok()).unwrap_or(16);
    let next = AtomicUsize::new(0);
    let parsed = &parsed;
    let tasks = &tasks;
    let sides: Vec<&[u64]> = parsed.iter().map(|t| &t.long_hi[..]).collect();
    let sides = &sides[..];
    // Calibration only: RD=n runs the dedup stage n more times first (those runs leave
    // the parsed records alone).
    let rd: usize = std::env::var("RD").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    let mut stage2_keep: Option<Vec<(Parts<PEv>, Parts<PClick>)>> = None;
    for rep in 0..=rd {
    next.store(0, Ordering::Relaxed);
    if let Some(old) = stage2_keep.take() {
        std::mem::forget(old);
    }
    let reuse = reuse && rep == rd;
    let stage2: Vec<(Parts<PEv>, Parts<PClick>)> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                let next = &next;
                sc.spawn(move || {
                    let mut oe: Parts<PEv> = Parts::with_chunk(npp, block_recs, if pbits <= 9 { 1024 } else { 256 });
                    let mut bm: Vec<u64> = Vec::new();
                    let mut cands: Vec<(u64, *const EvRec)> = Vec::new();
                    let mut ctab: Vec<(u64, *const EvRec)> = Vec::new();
                    let mut order: Vec<u32> = Vec::new();
                    let mut oc: Parts<PClick> = Parts::with_chunk(npp, 1 << 16, if pbits <= 9 { 1024 } else { 256 });
                    loop {
                        let ti = next.fetch_add(1, Ordering::Relaxed);
                        if ti >= tasks.len() {
                            break;
                        }
                        let (kind, a, b) = tasks[ti];
                        if kind == 0 {
                            let p = a;
                            let total: usize = parsed.iter().map(|t| t.ev.count(p)).sum();
                            // Two bits per hash bucket ("seen", "seen twice"), about 16 buckets
                            // per row: a row whose bucket was seen once is unique for sure; the
                            // others (duplicates and a few collisions) are resolved exactly below.
                            let lb = (total * bfac).next_power_of_two().max(1024).trailing_zeros();
                            let bsh = 64 - lb;
                            bm.clear();
                            bm.resize((1usize << lb) >> 5, 0);
                            let bp = bm.as_mut_ptr();
                            for t in parsed.iter() {
                                for ch in t.ev.chunks(p) {
                                    for r in ch {
                                        prefetch((r as *const EvRec).wrapping_add(PF_RECS));
                                        // r.eid is mixed: its top `ebits` bits chose the partition
                                        let b = ((r.eid << ebits) >> bsh) as usize;
                                        unsafe {
                                            let w = bp.add(b >> 5);
                                            let m = 1u64 << ((b & 31) * 2);
                                            let old = *w;
                                            *w = old | m | ((old & m) << 1);
                                        }
                                    }
                                }
                            }
                            cands.clear();
                            for t in parsed.iter() {
                                for ch in t.ev.chunks(p) {
                                    // plain index loops (an iterator chain here costs a
                                    // dozen instructions per record); the first `head`
                                    // records also ask for the person-map bucket of the
                                    // record `plook` places ahead
                                    let n = ch.len();
                                    let base = ch.as_ptr();
                                    let head = n.saturating_sub(plook);
                                    macro_rules! record {
                                        ($r:expr) => {{
                                            let r: &EvRec = $r;
                                            let b = ((r.eid << ebits) >> bsh) as usize;
                                            let twice = unsafe { *bp.add(b >> 5) } >> ((b & 31) * 2 + 1) & 1;
                                            if twice == 0 {
                                                if r.valid() {
                                                    let person = pmap.lookup_from(pmap.slot_of(r.user), r.user);
                                                    oe.push((person.wrapping_mul(GOLD) >> pshift) as usize, PEv { person, ts: r.ts, pck: r.pck });
                                                }
                                            } else {
                                                cands.push((r.eid, r as *const EvRec));
                                                // a long event ID is compared through its
                                                // thread's side list: ask for that entry now
                                                let lr = long_ref(r);
                                                if lr != 0 {
                                                    prefetch(side_entry(sides, lr));
                                                }
                                            }
                                        }};
                                    }
                                    let mut j = 0usize;
                                    while j < head {
                                        unsafe {
                                            prefetch(base.wrapping_add(j + PF_RECS));
                                            pmap.prefetch(pmap.slot_of((*base.add(j + plook)).user));
                                            record!(&*base.add(j));
                                        }
                                        j += 1;
                                    }
                                    while j < n {
                                        record!(unsafe { &*base.add(j) });
                                        j += 1;
                                    }
                                }
                            }
                            if !cands.is_empty() {
                                // exact: per event id the copy with the smallest ingest_ts
                                // (equal ingest_ts: identical copies, the first one stays).
                                // A slot holds the hashed id next to the record, so a probe
                                // only reads a record that may be a copy; `order` lists the
                                // used slots, so the winners are found without a scan.
                                let cap = (cands.len() * 2).next_power_of_two().max(16);
                                let mask = cap - 1;
                                ctab.clear();
                                ctab.resize(cap, (0u64, std::ptr::null()));
                                order.clear();
                                for &(eid, c) in cands.iter() {
                                    // bits below the partition and bucket bits; the lowest are weak
                                    let mut i = (eid >> 24) as usize & mask;
                                    loop {
                                        let (e, s) = unsafe { *ctab.get_unchecked(i) };
                                        if s.is_null() {
                                            unsafe { *ctab.get_unchecked_mut(i) = (eid, c) };
                                            order.push(i as u32);
                                            let r = unsafe { &*c };
                                            if r.valid() {
                                                pmap.prefetch(pmap.slot_of(r.user));
                                            }
                                            break;
                                        }
                                        if e == eid {
                                            let (o, r) = unsafe { (&*s, &*c) };
                                            if same_event(o, r, sides) {
                                                if ing_secs(r, tb) < ing_secs(o, tb) {
                                                    unsafe { ctab.get_unchecked_mut(i).1 = c };
                                                    if r.valid() {
                                                        pmap.prefetch(pmap.slot_of(r.user));
                                                    }
                                                }
                                                break;
                                            }
                                        }
                                        i = (i + 1) & mask;
                                    }
                                }
                                for &i in order.iter() {
                                    let r = unsafe { &*ctab.get_unchecked(i as usize).1 };
                                    if r.valid() {
                                        let person = pmap.lookup_from(pmap.slot_of(r.user), r.user);
                                        oe.push((person.wrapping_mul(GOLD) >> pshift) as usize, PEv { person, ts: r.ts, pck: r.pck });
                                    }
                                }
                            }
                            // This partition's records are not read again: their memory
                            // (still in cache) takes the person-partition chunks from now on.
                            if reuse {
                                for t in parsed.iter() {
                                    for (ptr, bytes) in t.ev.regions(p) {
                                        oe.donate(ptr, bytes);
                                    }
                                }
                            }
                        } else if kind == usize::MAX {
                            if rep == rd {
                                unmap_pages(a, b);
                            }
                        } else {
                            let cl = &parsed[kind - 1].cl[a..b];
                            for (j, c) in cl.iter().enumerate() {
                                if j + plook < cl.len() {
                                    let u = unsafe { cl.get_unchecked(j + plook) }.user;
                                    pmap.prefetch(pmap.slot_of(u));
                                }
                                let person = pmap.lookup_from(pmap.slot_of(c.user), c.user);
                                oc.push(
                                    (person.wrapping_mul(GOLD) >> pshift) as usize,
                                    PClick { person, cid: c.cid, ts: c.ts, camp: c.camp },
                                );
                            }
                        }
                    }
                    oe.finish();
                    oc.finish();
                    (oe, oc)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    stage2_keep = Some(stage2);
    }
    let stage2 = stage2_keep.unwrap();
    eprintln!("dedup: {:?} cpu {:.3}", t0.elapsed(), cpu_now());

    // ---------------- sessions per person partition
    let next = AtomicUsize::new(0);
    struct Res {
        cols: UserCols,
        camp: CampAcc,
    }
    let gap = GAP as u32;
    let week = WEEK as u32;
    let stage2 = &stage2;
    // Largest partitions first (whales and shared devices make some much heavier).
    let mut porder: Vec<usize> = (0..npp).collect();
    {
        let sz: Vec<usize> = (0..npp).map(|p| stage2.iter().map(|t| t.0.count(p) + t.1.count(p)).sum()).collect();
        porder.sort_unstable_by_key(|&p| std::cmp::Reverse(sz[p]));
    }
    let porder = &porder;
    let cat_names = &lk.cat_names;
    let mut res_keep: Option<Vec<Res>> = None;
    for _rep in 0..=rs {
    next.store(0, Ordering::Relaxed);
    if let Some(old) = res_keep.take() {
        std::mem::forget(old);
    }
    let res: Vec<Res> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                let next = &next;
                sc.spawn(move || {
                    let mut cols = UserCols::new();
                    let mut name: Vec<u8> = Vec::new();
                    let mut cacc = CampAcc::new((ncamp + 1) * ndays);
                    let mut lmap = LocalMap::new(pbits);
                    let mut sort_tmp: Vec<u64> = Vec::new();
                    let mut bucket_tmp = [0u64; 256];
                    let mut pkeys: Vec<u64> = Vec::new();
                    let mut ecount: Vec<u32> = Vec::new();
                    let mut ccount: Vec<u32> = Vec::new();
                    let mut evs: Vec<u64> = Vec::new();
                    let mut cls: Vec<PClick> = Vec::new();
                    let mut eidx: Vec<u32> = Vec::new();
                    let mut cidx: Vec<u32> = Vec::new();
                    let mut catrev = vec![0i64; ncat];
                    let mut wcnt = vec![0u32; ncamp + 1];
                    let mut active: Vec<u32> = Vec::new();
                    let mut apos = vec![0u32; ncamp + 1];
                    // assists are counted per stretch of sessions, not per session: a campaign
                    // that becomes active notes how many sessions the day had before, and
                    // gets the difference when it leaves the window or the day ends
                    let mut amark = vec![0u32; ncamp + 1];
                    let mut stm = [0f64; 2];
                    loop {
                        let pi = next.fetch_add(1, Ordering::Relaxed);
                        if pi >= npp {
                            break;
                        }
                        let p = porder[pi];
                        let st0 = Instant::now();
                        let ne: usize = stage2.iter().map(|t| t.0.count(p)).sum();
                        lmap.clear_for(ne / lmd + 16);
                        pkeys.clear();
                        ecount.clear();
                        eidx.clear();
                        eidx.reserve(ne);
                        // capacity for ne entries was reserved above
                        let eip = eidx.as_mut_ptr();
                        let mut en = 0usize;
                        for t in stage2.iter() {
                            for ch in t.0.chunks(p) {
                                // plain index loops; all but the last LAHEAD events of a
                                // chunk also ask for the table slot of the event LAHEAD
                                // places ahead (the table can exceed a thread's cache share)
                                const LAHEAD: usize = 8;
                                let n = ch.len();
                                let base = ch.as_ptr();
                                let head = n.saturating_sub(LAHEAD);
                                macro_rules! event {
                                    ($e:expr) => {{
                                        let e: &PEv = $e;
                                        let (li, ins) = lmap.get_or_insert(e.person, pkeys.len() as u32);
                                        if ins {
                                            pkeys.push(e.person);
                                            ecount.push(0);
                                            if lmap.len * 2 > lmap.slots.len() {
                                                lmap.grow();
                                            }
                                        }
                                        unsafe {
                                            *ecount.get_unchecked_mut(li as usize) += 1;
                                            eip.add(en).write(li);
                                        }
                                        en += 1;
                                    }};
                                }
                                let mut j = 0usize;
                                while j < head {
                                    unsafe {
                                        prefetch(base.wrapping_add(j + 2 * PF_RECS));
                                        lmap.prefetch((*base.add(j + LAHEAD)).person);
                                        event!(&*base.add(j));
                                    }
                                    j += 1;
                                }
                                while j < n {
                                    event!(unsafe { &*base.add(j) });
                                    j += 1;
                                }
                            }
                        }
                        debug_assert_eq!(en, ne);
                        unsafe { eidx.set_len(en) };
                        let np = pkeys.len();
                        let mut estart: Vec<u32> = Vec::with_capacity(np + 1);
                        let mut acc = 0u32;
                        for &c in &ecount {
                            estart.push(acc);
                            acc += c;
                        }
                        estart.push(acc);
                        evs.clear();
                        evs.reserve(ne);
                        unsafe { evs.set_len(ne) };
                        {
                            let mut fill: Vec<u32> = estart[..np].to_vec();
                            let mut k = 0usize;
                            for t in stage2.iter() {
                                for ch in t.0.chunks(p) {
                                    for e in ch {
                                        prefetch((e as *const PEv).wrapping_add(2 * PF_RECS));
                                        unsafe {
                                            if k + 8 < ne {
                                                let lj = *eidx.get_unchecked(k + 8) as usize;
                                                prefetch_w(evs.as_ptr().wrapping_add(*fill.get_unchecked(lj) as usize));
                                            }
                                            let li = *eidx.get_unchecked(k) as usize;
                                            k += 1;
                                            let fp = fill.get_unchecked_mut(li);
                                            *evs.get_unchecked_mut(*fp as usize) = ((e.ts as u64) << 32) | e.pck as u64;
                                            *fp += 1;
                                        }
                                    }
                                }
                            }
                        }
                        ccount.clear();
                        ccount.resize(np, 0);
                        cidx.clear();
                        for t in stage2.iter() {
                            for ch in t.1.chunks(p) {
                                for c in ch {
                                    prefetch((c as *const PClick).wrapping_add(PF_RECS));
                                    match lmap.get(c.person) {
                                        Some(li) => {
                                            ccount[li as usize] += 1;
                                            cidx.push(li);
                                        }
                                        None => cidx.push(u32::MAX),
                                    }
                                }
                            }
                        }
                        let mut cstart: Vec<u32> = Vec::with_capacity(np + 1);
                        let mut acc = 0u32;
                        for &c in &ccount {
                            cstart.push(acc);
                            acc += c;
                        }
                        cstart.push(acc);
                        cls.clear();
                        cls.reserve(acc as usize);
                        unsafe { cls.set_len(acc as usize) };
                        {
                            let mut fill: Vec<u32> = cstart[..np].to_vec();
                            let mut k = 0usize;
                            for t in stage2.iter() {
                                for ch in t.1.chunks(p) {
                                    for c in ch {
                                        let li = cidx[k];
                                        k += 1;
                                        if li != u32::MAX {
                                            cls[fill[li as usize] as usize] = *c;
                                            fill[li as usize] += 1;
                                        }
                                    }
                                }
                            }
                        }
                        stm[0] += st0.elapsed().as_secs_f64();
                        let st1 = Instant::now();
                        for li in 0..np {
                            let pk = pkeys[li];
                            name.clear();
                            key_bytes(pk, PFX_USER, &mut name);
                            let name = &name[..];
                            let e = &mut evs[estart[li] as usize..estart[li + 1] as usize];
                            if xp(16) { continue; }
                            if e.len() <= bsmin {
                                if rank { small_sort(e) } else { insertion_sort(e) }
                            } else if e.len() < 256 {
                                bucket_sort_ts(e, &mut bucket_tmp, bshift);
                            } else {
                                radix_sort_ts(e, &mut sort_tmp);
                            }
                            let c = &mut cls[cstart[li] as usize..cstart[li + 1] as usize];
                            if xp(8) { continue; }
                            if c.len() > 1 && c.len() <= 48 {
                                if rank { small_click_sort(c) } else { click_insertion_sort(c) }
                            } else if c.len() > 48 {
                                if c.iter().all(|x| x.cid & FALLBACK == 0) {
                                    c.sort_unstable_by_key(|x| ((x.ts as u128) << 64) | x.cid.swap_bytes() as u128);
                                } else {
                                    c.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| key_cmp(a.cid, b.cid, PFX_CLICK)));
                                }
                            }
                            let e = &*e;
                            let c = &*c;
                            let mut q = 0usize;
                            let mut lo = 0usize;
                            let mut cur_day = i64::MIN;
                            let mut d_sess = 0i64;
                            let mut d_ev = 0i64;
                            let mut d_rev = 0i64;
                            let mut k = 0usize;
                            let flush = |cols: &mut UserCols, catrev: &mut Vec<i64>, day: i64, s: i64, ev: i64, rev: i64| {
                                let mut top: Option<&[u8]> = None;
                                let mut bestv = 0i64;
                                if rev > 0 {
                                    for (ci, v) in catrev.iter_mut().enumerate() {
                                        if *v > bestv {
                                            bestv = *v;
                                            top = Some(&cat_names[ci][..]);
                                        }
                                        *v = 0;
                                    }
                                }
                                cols.push(name, day as i32, s, ev, rev, top);
                            };
                            let en = e.len();
                            let ep = e.as_ptr();
                            while k < en {
                                let s0 = (unsafe { *ep.add(k) } >> 32) as u32;
                                // close the previous day first: its category totals are reset there
                                let day = (tb + s0 as i64).div_euclid(DAY);
                                if day != cur_day {
                                    if cur_day != i64::MIN {
                                        let odi = (cur_day - day0) as usize;
                                        for &cc in &active {
                                            let cc = cc as usize;
                                            unsafe {
                                                cacc.assist_n(cc * ndays + odi, d_sess as u32 - *amark.get_unchecked(cc));
                                                *amark.get_unchecked_mut(cc) = 0;
                                            }
                                        }
                                        flush(&mut cols, &mut catrev, cur_day, d_sess, d_ev, d_rev);
                                    }
                                    cur_day = day;
                                    d_sess = 0;
                                    d_ev = 0;
                                    d_rev = 0;
                                }
                                // one pass over the session's events: its end, revenue and categories
                                let mut prev = s0;
                                let mut rev = 0i64;
                                let mut purch = false;
                                let mut m = k;
                                loop {
                                    let x = unsafe { *ep.add(m) };
                                    let t = (x >> 32) as u32;
                                    if t - prev > gap {
                                        break;
                                    }
                                    prev = t;
                                    let pck = x as u32;
                                    if pck >= 2 << 30 {
                                        let pr = (pck & 0x00FF_FFFF) as i64;
                                        rev += pr;
                                        unsafe { *catrev.get_unchecked_mut(((pck >> 24) & 63) as usize) += pr };
                                        purch = true;
                                    }
                                    m += 1;
                                    if m == en {
                                        break;
                                    }
                                }
                                d_sess += 1;
                                d_ev += (m - k) as i64;
                                d_rev += rev;
                                let di = (day - day0) as usize;
                                // clicks of the person up to the session start, and the 7 days before
                                // (campaign indexes are below wcnt.len() == apos.len() by construction)
                                unsafe {
                                    let cn = c.len();
                                    let cp = c.as_ptr();
                                    while q < cn && (*cp.add(q)).ts <= s0 {
                                        let cc = (*cp.add(q)).camp as usize;
                                        let w = wcnt.get_unchecked_mut(cc);
                                        if *w == 0 {
                                            *apos.get_unchecked_mut(cc) = active.len() as u32;
                                            active.push(cc as u32);
                                            *amark.get_unchecked_mut(cc) = d_sess as u32 - 1;
                                        }
                                        *w += 1;
                                        q += 1;
                                    }
                                    while lo < q && ((*cp.add(lo)).ts as i64) < s0 as i64 - week as i64 {
                                        let cc = (*cp.add(lo)).camp as usize;
                                        let w = wcnt.get_unchecked_mut(cc);
                                        *w -= 1;
                                        if *w == 0 {
                                            cacc.assist_n(cc * ndays + di, d_sess as u32 - 1 - *amark.get_unchecked(cc));
                                            let pos = *apos.get_unchecked(cc) as usize;
                                            let last = *active.get_unchecked(active.len() - 1);
                                            *active.get_unchecked_mut(pos) = last;
                                            *apos.get_unchecked_mut(last as usize) = pos as u32;
                                            active.pop();
                                        }
                                        lo += 1;
                                    }
                                }
                                let credited = if lo < q {
                                    unsafe {
                                        let cp = c.as_ptr();
                                        let t = (*cp.add(q - 1)).ts;
                                        let mut g = q - 1;
                                        while g > lo && (*cp.add(g - 1)).ts == t {
                                            g -= 1;
                                        }
                                        (*cp.add(g)).camp as usize
                                    }
                                } else {
                                    ncamp
                                };
                                cacc.credit(credited * ndays + di, purch, rev);
                                k = m;
                            }
                            if cur_day != i64::MIN {
                                let odi = (cur_day - day0) as usize;
                                for &cc in &active {
                                    let cc = cc as usize;
                                    cacc.assist_n(cc * ndays + odi, d_sess as u32 - amark[cc]);
                                }
                                flush(&mut cols, &mut catrev, cur_day, d_sess, d_ev, d_rev);
                            }
                            for x in &c[lo..q] {
                                wcnt[x.camp as usize] = 0;
                            }
                            active.clear();
                        }
                        stm[1] += st1.elapsed().as_secs_f64();
                    }
                    eprintln!("sessions timers: group {:.3} persons {:.3}", stm[0], stm[1]);
                    Res { cols, camp: cacc }
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    res_keep = Some(res);
    }
    let res = res_keep.unwrap();
    eprintln!("sessions: {:?} cpu {:.3}", t0.elapsed(), cpu_now());

    // ---------------- write
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();
    std::thread::scope(|sc| {
        for (t, r) in res.iter().enumerate() {
            let ud = &ud;
            sc.spawn(move || {
                let c = &r.cols;
                let cols = [
                    parquet::ColRef { name: "user_id", ty: parquet::Ty::Str, defs: None, data: &c.uid },
                    parquet::ColRef { name: "day", ty: parquet::Ty::Date, defs: None, data: parquet::bytes_of(&c.day) },
                    parquet::ColRef { name: "sessions", ty: parquet::Ty::I32, defs: None, data: parquet::bytes_of(&c.sess) },
                    parquet::ColRef { name: "events", ty: parquet::Ty::I32, defs: None, data: parquet::bytes_of(&c.ev) },
                    parquet::ColRef { name: "revenue_cents", ty: parquet::Ty::I64, defs: None, data: parquet::bytes_of(&c.rev) },
                    parquet::ColRef { name: "top_category", ty: parquet::Ty::Str, defs: Some(&c.defs), data: &c.top },
                ];
                parquet::write_cols(&ud.join(format!("part-{:03}.parquet", t)), &cols, c.n).unwrap();
            });
        }
    // campaign_daily on this thread meanwhile
    let mut cid = Vec::new();
    let mut chan = Vec::new();
    let mut days = Vec::new();
    let mut vals: [Vec<i64>; 4] = Default::default();
    for c in 0..=ncamp {
        for d in 0..ndays {
            let mut a = [0i64; 4];
            for r in &res {
                let x = r.camp.get(c * ndays + d);
                for z in 0..4 {
                    a[z] += x[z];
                }
            }
            if a[0] == 0 && a[3] == 0 {
                continue;
            }
            let (id, ch): (&[u8], &[u8]) =
                if c == ncamp { (b"organic", b"organic") } else { (&lk.camp_ids[c], &lk.camp_channel[c]) };
            cid.extend_from_slice(&(id.len() as u32).to_le_bytes());
            cid.extend_from_slice(id);
            chan.extend_from_slice(&(ch.len() as u32).to_le_bytes());
            chan.extend_from_slice(ch);
            days.push((day0 + d as i64) as i32);
            for z in 0..4 {
                vals[z].push(a[z]);
            }
        }
    }
    let nrows = days.len();
    let [v0, v1, v2, v3] = vals;
    let cols = vec![
        parquet::Column { name: "campaign_id", values: parquet::Values::Str { plain: cid, defs: None } },
        parquet::Column { name: "channel", values: parquet::Values::Str { plain: chan, defs: None } },
        parquet::Column { name: "day", values: parquet::Values::Date(days) },
        parquet::Column { name: "sessions", values: parquet::Values::I64(v0) },
        parquet::Column { name: "purchasing_sessions", values: parquet::Values::I64(v1) },
        parquet::Column { name: "revenue_cents", values: parquet::Values::I64(v2) },
        parquet::Column { name: "assisted_sessions", values: parquet::Values::I64(v3) },
    ];
    parquet::write(&cd.join("part-000.parquet"), &cols, nrows).unwrap();
    });
    eprintln!("done: {:?} cpu {:.3}", t0.elapsed(), cpu_now());
    // Output files are complete and closed; skip destructors (unmapping GBs of buffers).
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn f3_thirteen_matches_parse_ts() {
        let mut rng = 2025u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut dc = DateCache::new();
        for i in 0..2_000_000u32 {
            let mut buf = [0u8; 40];
            for c in buf.iter_mut() {
                *c = (next() >> 8) as u8;
            }
            for c in buf[..13].iter_mut() {
                *c = b'0' + (next() % 10) as u8;
            }
            if i % 3 == 0 {
                buf[(next() % 13) as usize] = b"-+.e x:/"[(next() % 8) as usize];
            }
            let s = &buf[..13];
            assert_eq!(dc.ts(s), parse_ts(s), "{:?}", std::str::from_utf8(s));
        }
    }

    #[test]
    fn day_texts_match_the_date_parser() {
        for z in (-800_000i64..3_000_000).step_by(37).chain(19_000..23_000) {
            let (y, m, d) = civil_from_days(z);
            assert_eq!(days_from_civil(y, m, d), z);
        }
        // a window across a month end, a leap day and a year end
        for (y, m, d) in [(2027, 12, 20), (2028, 2, 20), (2024, 2, 25), (1999, 12, 28), (2100, 2, 25)] {
            let tb = days_from_civil(y, m, d) * DAY + 12_345;
            let we = tb + 21 * DAY;
            let tab = day_texts(tb, we);
            assert_eq!(tab.len(), 22);
            for (i, e) in tab.iter().enumerate() {
                let mut t = e[0].to_le_bytes().to_vec();
                t.extend_from_slice(&(e[1] as u16).to_le_bytes());
                assert_eq!(e[1] >> 16, DAY_TEXT_OK >> 16);
                t.extend_from_slice(b"T00:00:00Z");
                assert_eq!(parse_ts(&t), Some((tb.div_euclid(DAY) + i as i64) * DAY), "{}", String::from_utf8_lossy(&t));
            }
        }
        assert_eq!(day_texts(days_from_civil(12_000, 1, 1) * DAY, days_from_civil(12_000, 1, 2) * DAY)[0], [0, 0]);
    }

    #[test]
    fn person_map_finds_every_key() {
        let mut rng = 99u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for n in [1usize, 2, 3, 50, 1000, 20_000] {
            let keys: Vec<u64> = (0..n).map(|_| next() | 1).collect();
            let mut pm = PMap::new(n);
            for (i, &k) in keys.iter().enumerate() {
                pm.insert(k, i as u64 + 7);
            }
            let mut want = std::collections::HashMap::new();
            for (i, &k) in keys.iter().enumerate() {
                want.insert(k, i as u64 + 7);
            }
            for &k in &keys {
                assert_eq!(pm.lookup_from(pm.slot_of(k), k), want[&k]);
            }
            for _ in 0..n * 4 + 100 {
                let k = next() | 1;
                let w = want.get(&k).copied().unwrap_or(k);
                assert_eq!(pm.lookup_from(pm.slot_of(k), k), w);
            }
        }
        assert_eq!(pick2(5, [5, 9], [0, 0]), 9);
        assert_eq!(pick2(5, [4, 9], [5, 8]), 8);
        assert_eq!(pick2(5, [4, 9], [6, 8]), 5);
        assert_eq!(pick2(5, [0, 0], [0, 0]), 5);
    }

    #[test]
    fn lazy_time_of_day_keeps_the_digits() {
        for (h, m, sec) in [(0u64, 0u64, 0u64), (23, 59, 59), (9, 5, 7), (12, 34, 56), (20, 0, 9)] {
            let text = format!("{:02}:{:02}:{:02}", h, m, sec);
            let w = rd64(text.as_ptr());
            let v = tod_nibbles(w);
            assert!(v < 1 << 24);
            assert_eq!(tod_from_nibbles(v), w & 0x0F0F_000F_0F00_0F0F);
            assert_eq!(tod_of_lanes(hms_lanes(tod_from_nibbles(v))), h * 3600 + m * 60 + sec);
            let r = EvRec { eid: 1, user: 1, ing: ING_LAZY | 0x7FFF_FFFF << 24 | v, ts: 5 * 86_400_000 + 77, pck: 0 };
            assert_eq!(long_ref(&r), 0x7FFF_FFFF);
            assert_eq!(ing_secs(&r, 0), 5 * 86_400 + h * 3600 + m * 60 + sec);
            let r = EvRec { eid: 1, user: 1, ing: 123_456 | 0x7FFF_FFFF << 32, ts: 0, pck: 0 };
            assert_eq!((long_ref(&r), ing_secs(&r, 0)), (0x7FFF_FFFF, 123_456));
        }
    }

    #[test]
    fn hex_digits_parse_exactly() {
        let mut rng = 2024u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let reference = |b: &[u8]| -> Option<u64> {
            let mut v = 0u64;
            for &c in b {
                let d = match c {
                    b'0'..=b'9' => c - b'0',
                    b'a'..=b'f' => c - b'a' + 10,
                    _ => return None,
                };
                v = v << 4 | d as u64;
            }
            Some(v)
        };
        // every byte value at every position of an otherwise valid word
        for pos in 0..8 {
            for c in 0..=255u8 {
                let mut b = *b"0f3a9c7e";
                b[pos] = c;
                assert_eq!(hex8(u64::from_be_bytes(b)), reference(&b), "{:?}", b);
            }
        }
        for _ in 0..2_000_000 {
            let mut b = [0u8; 8];
            for c in b.iter_mut() {
                let r = next();
                *c = if r % 23 == 0 { (r >> 8) as u8 } else { b"0123456789abcdefABg`/:"[(r >> 8) as usize % if r % 5 == 0 { 22 } else { 16 }] };
            }
            assert_eq!(hex8(u64::from_be_bytes(b)), reference(&b), "{:?}", b);
        }
        for _ in 0..500_000 {
            let mut b = [0u8; 32];
            let bad = next() % 3 == 0;
            for c in b.iter_mut() {
                let r = next();
                *c = if bad && r % 29 == 0 { (r >> 8) as u8 } else { b"0123456789abcdef"[(r >> 8) as usize % 16] };
            }
            let want = match (reference(&b[..16]), reference(&b[16..])) {
                (Some(x), Some(y)) => Some((x, y)),
                _ => None,
            };
            assert_eq!(hex32(b.as_ptr()), want, "{:?}", String::from_utf8_lossy(&b));
            assert_eq!(hex32_words(b.as_ptr()), want);
        }
        let id = b"0123456789abcdeffedcba9876543210";
        assert_eq!(hex32(id.as_ptr()), Some((0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210)));
        let mut out = Vec::new();
        hex16_into(&mut out, 0x0123_4567_89ab_cdef);
        hex16_into(&mut out, 0xfedc_ba98_7654_3210);
        assert_eq!(&out[..], &id[..]);
        assert_eq!(hex32(b"0123456789abcdeffedcba987654321G".as_ptr()), None);
    }

    /// Random IDs of every form: short, 32 hex digits, and neither.
    fn random_ids(n: usize, pfx: u8, seed: u64) -> Vec<Vec<u8>> {
        let mut rng = seed;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        (0..n)
            .map(|_| {
                let r = next();
                let mut id = vec![if r % 11 == 0 { b'U' } else { pfx }];
                match (r >> 8) % 6 {
                    0 | 1 => {
                        // hex form, a small space so that equal IDs and shared prefixes occur
                        let hi = next() % 5;
                        let lo = next() % 7;
                        id.extend_from_slice(format!("{:016x}{:016x}", hi.wrapping_mul(0x1111_0000_0000_0001), lo.wrapping_mul(0x0101_0101_0101_0101)).as_bytes());
                        if r % 13 == 0 {
                            id[5] = b'F'; // not lowercase: not the hex form
                        }
                        if r % 17 == 0 {
                            id.pop(); // 31 digits
                        }
                    }
                    2 => {
                        let v = next() % 40;
                        id.extend_from_slice(format!("{:x}", v).as_bytes());
                    }
                    3 => {
                        for _ in 0..(1 + next() % 8) {
                            id.push(b"0123456789abcdefxyz"[(next() % 19) as usize]);
                        }
                    }
                    4 => {
                        for _ in 0..(9 + next() % 40) {
                            id.push(b"01abxyz_-"[(next() % 9) as usize]);
                        }
                    }
                    _ => {
                        id.extend_from_slice(format!("{:016x}{:016x}", next(), next()).as_bytes());
                    }
                }
                // room to read past the end, as in the padded mappings
                id
            })
            .collect()
    }

    fn padded(id: &[u8]) -> Vec<u8> {
        let mut v = id.to_vec();
        v.resize(id.len() + 16, 0);
        v
    }

    #[test]
    fn long_user_keys_are_exact_and_ordered() {
        let ids = random_ids(6000, PFX_USER, 99);
        let bufs: Vec<Vec<u8>> = ids.iter().map(|i| padded(i)).collect();
        let keys: Vec<u64> = ids.iter().zip(&bufs).map(|(i, b)| key_user(&b[..i.len()])).collect();
        let mut seen: std::collections::HashMap<u64, &[u8]> = std::collections::HashMap::new();
        let mut n_long = 0;
        for (id, &k) in ids.iter().zip(&keys) {
            if let Some(o) = seen.insert(k, &id[..]) {
                assert_eq!(o, &id[..], "one key for two IDs");
            }
            let mut back = Vec::new();
            key_bytes(k, PFX_USER, &mut back);
            assert_eq!(&back[..], &id[..]);
            let hex = id.len() == 33 && id[0] == b'u' && id[1..].iter().all(|c| b"0123456789abcdef".contains(c));
            assert_eq!(k & (FALLBACK | LONG) == (FALLBACK | LONG), hex, "{:?}", String::from_utf8_lossy(id));
            n_long += hex as usize;
        }
        assert!(n_long > 500);
        // the same ID gives the same key again
        for (id, (&k, b)) in ids.iter().zip(keys.iter().zip(&bufs)).take(500) {
            assert_eq!(key_user(&b[..id.len()]), k);
        }
        for i in 0..ids.len() {
            for j in [i / 2, (i * 7 + 3) % ids.len(), ids.len() - 1 - i] {
                assert_eq!(key_cmp(keys[i], keys[j], PFX_USER), ids[i].cmp(&ids[j]), "{:?} {:?}", String::from_utf8_lossy(&ids[i]), String::from_utf8_lossy(&ids[j]));
            }
        }
    }

    #[test]
    fn long_click_keys_compare_by_their_bytes() {
        let ids = random_ids(3000, PFX_CLICK, 5);
        let bufs: Vec<Vec<u8>> = ids.iter().map(|i| padded(i)).collect();
        let keys: Vec<u64> = ids.iter().zip(&bufs).map(|(i, b)| key_click(&b[..i.len()])).collect();
        for i in 0..ids.len() {
            let mut back = Vec::new();
            key_bytes(keys[i], PFX_CLICK, &mut back);
            assert_eq!(&back[..], &ids[i][..]);
            for j in [i / 3, (i * 5 + 1) % ids.len()] {
                assert_eq!(key_cmp(keys[i], keys[j], PFX_CLICK), ids[i].cmp(&ids[j]));
            }
        }
    }

    #[test]
    fn event_keys_identify_events() {
        let ids = random_ids(5000, PFX_EVENT, 31);
        let bufs: Vec<Vec<u8>> = ids.iter().map(|i| padded(i)).collect();
        // two "threads", each with its own side list
        let mut dcs = [DateCache::new(), DateCache::new()];
        dcs[1].tid = 1;
        let mut recs: Vec<EvRec> = Vec::new();
        let mut n_long = 0;
        for (n, (id, b)) in ids.iter().zip(&bufs).enumerate() {
            let (eid, eref) = eid_key(&b[..id.len()], &mut dcs[n % 2]);
            n_long += (eref != 0) as usize;
            // half of them with a lazy ingest_ts, as the parser stores them
            let ing = if n % 4 < 2 { ING_LAZY | eref << 24 | tod_nibbles(0x3935_3A39_353A_3332) } else { 12_345 | eref << 32 };
            recs.push(EvRec { eid, user: 0, ing, ts: 0, pck: 0 });
        }
        assert!(n_long > 500);
        let sides: Vec<&[u64]> = dcs.iter().map(|d| &d.long_hi[..]).collect();
        for i in 0..ids.len() {
            for j in [i, i / 2, (i * 13 + 5) % ids.len(), ids.len() - 1 - i] {
                let same = recs[i].eid == recs[j].eid && same_event(&recs[i], &recs[j], &sides);
                assert_eq!(same, ids[i] == ids[j], "{:?} {:?}", String::from_utf8_lossy(&ids[i]), String::from_utf8_lossy(&ids[j]));
            }
        }
        // equal hash and unequal first numbers are different events; a short and a
        // long ID are different events even with equal hashes
        let mut dc = DateCache::new();
        dc.long_hi = vec![7, 7, 8];
        let side = [&dc.long_hi[..]];
        let r = |eref: u64| EvRec { eid: 42, user: 0, ing: 99 | eref << 32, ts: 0, pck: 0 };
        assert!(same_event(&r(1), &r(2), &side));
        assert!(!same_event(&r(1), &r(3), &side));
        assert!(!same_event(&r(0), &r(3), &side));
        assert!(same_event(&r(0), &r(0), &side));
    }

    #[test]
    fn click_rank_sort_sorts() {
        let mut rng = 777u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for round in 0..200_000u32 {
            let n = 2 + (next() % 31) as usize;
            let mut v: Vec<PClick> = (0..n)
                .map(|i| {
                    let ts = match round % 4 {
                        0 => next() as u32,
                        1 => (next() % 3) as u32,
                        2 => u32::MAX - (next() % 4) as u32,
                        _ => (next() % 1_814_400_000) as u32,
                    };
                    // distinct ids: raw keys, some with the fallback bit
                    let cid = if next() % 9 == 0 { key_slow(format!("click-{}-{}", round, i).as_bytes()) } else { key_p(format!("k{:07}", next() % 5_000_000).as_bytes(), PFX_CLICK) };
                    PClick { person: 1, cid, ts, camp: i as u32 }
                })
                .collect();
            let mut want = v.clone();
            want.sort_by(|a, b| a.ts.cmp(&b.ts).then_with(|| key_cmp(a.cid, b.cid, PFX_CLICK)));
            click_rank_sort(&mut v);
            for (a, b) in v.iter().zip(&want) {
                assert!(a.ts == b.ts && a.cid == b.cid, "n {}", n);
            }
            let mut w = want.clone();
            w.reverse();
            click_insertion_sort(&mut w);
            for (a, b) in w.iter().zip(&want) {
                assert!(a.ts == b.ts && a.cid == b.cid);
            }
        }
    }

    #[test]
    fn rank_sort_sorts() {
        let mut rng = 4242u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for round in 0..300_000u32 {
            let n = 2 + (next() % 31) as usize;
            let mut v: Vec<u64> = (0..n)
                .map(|_| match round % 5 {
                    0 => next(),
                    1 => next() % 4,
                    2 => (next() % 5) << 32 | (next() % 3),
                    3 => u64::MAX - next() % 3,
                    _ => (next() % 1_814_400_000) << 32 | (next() & 0xBFFF_FFFF),
                })
                .collect();
            let mut want = v.clone();
            want.sort_unstable();
            rank_sort(&mut v);
            assert_eq!(v, want, "n {}", n);
        }
    }

    #[test]
    fn bucket_sort_sorts() {
        let mut rng = 31337u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut tmp = [0u64; 256];
        for round in 0..200_000u32 {
            let span: u32 = match round % 4 {
                0 => 1_814_400_000,
                1 => 1 + (next() % 4_000_000_000) as u32,
                2 => 1 + (next() % 5000) as u32,
                _ => u32::MAX - 1,
            };
            let bshift = (32 - span.leading_zeros()).saturating_sub(5);
            assert!((span >> bshift) < 32);
            let n = 9 + (next() % 247) as usize;
            let mut v: Vec<u64> = (0..n)
                .map(|_| {
                    let ts = if round % 3 == 0 { (next() % 7) * (span as u64 / 7) } else { next() % (span as u64 + 1) };
                    (ts.min(span as u64) << 32) | (next() & 0xFFFF_FFFF)
                })
                .collect();
            let mut want = v.clone();
            want.sort_unstable();
            bucket_sort_ts(&mut v, &mut tmp, bshift);
            assert_eq!(v, want);
        }
    }

    #[test]
    fn prefix_keys_are_exact_and_ordered() {
        let mut rng = 777u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut seen: std::collections::HashMap<u64, Vec<u8>> = std::collections::HashMap::new();
        let mut fast: Vec<(Vec<u8>, u64)> = Vec::new();
        for _ in 0..300_000 {
            let n = (next() % 12) as usize;
            let mut buf = vec![0u8; 32];
            for (i, c) in buf.iter_mut().enumerate() {
                let r = next();
                *c = match r % 8 {
                    0 => (r >> 8) as u8,
                    1 => b" u\rU_e\""[(r >> 8) as usize % 7],
                    _ => b"0123abcxyz"[(r >> 8) as usize % 10],
                };
                if i == 0 && r % 3 != 0 {
                    *c = b'u';
                }
            }
            let s = &buf[..n];
            let k = key_p(s, PFX_USER);
            // equal keys <=> equal strings
            if let Some(o) = seen.get(&k) {
                assert_eq!(&o[..], s);
            } else {
                seen.insert(k, s.to_vec());
            }
            let mut back = Vec::new();
            key_bytes(k, PFX_USER, &mut back);
            assert_eq!(&back[..], s);
            let is_fast = n >= 2 && n <= 9 && s[0] == b'u' && s[n - 1] != 0 && (n < 9 || s[8] < 0x80);
            assert_eq!(k & FALLBACK == 0, is_fast, "{:?}", s);
            if is_fast && fast.len() < 3000 {
                fast.push((s.to_vec(), k));
            }
        }
        for w in fast.windows(2) {
            assert_eq!(key_cmp(w[0].1, w[1].1, PFX_USER), w[0].0.cmp(&w[1].0), "{:?} {:?}", w[0].0, w[1].0);
        }
        // mixed fast / interned keys compare by their bytes
        let a = key_p(b"u1\0\0\0\0\0\0\0\0\0\0", PFX_USER);
        let _ = a;
        let (x, y) = (b"U1xxxxxxxxxxxx".to_vec(), b"u1xxxxxxxxxxxx".to_vec());
        let (kx, ky) = (key_p(&x[..2], PFX_USER), key_p(&y[..2], PFX_USER));
        assert_eq!(key_cmp(kx, ky, PFX_USER), std::cmp::Ordering::Less);
    }

    #[test]
    fn tod_matches_plain_parse() {
        let mut rng = 4242u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let alphabet = b"0123456789:;/ T-Zz.+";
        let mut seen_ok = 0;
        for i in 0..4_000_000u32 {
            let mut s = [0u8; 8];
            if i % 3 == 0 {
                // well-formed shape with random digits
                for (j, c) in s.iter_mut().enumerate() {
                    *c = if j == 2 || j == 5 { b':' } else { b'0' + (next() % 10) as u8 };
                }
            } else if i % 3 == 1 {
                let (h, m, sec) = (next() % 24, next() % 60, next() % 60);
                s.copy_from_slice(format!("{:02}:{:02}:{:02}", h, m, sec).as_bytes());
                if next() % 4 == 0 {
                    s[(next() % 8) as usize] = alphabet[(next() % alphabet.len() as u64) as usize];
                }
            } else {
                for c in s.iter_mut() {
                    *c = if next() % 5 == 0 { (next() >> 8) as u8 } else { alphabet[(next() % alphabet.len() as u64) as usize] };
                }
            }
            let dg = |k: usize| if s[k].is_ascii_digit() { Some((s[k] - b'0') as u64) } else { None };
            let want = (|| {
                if s[2] != b':' || s[5] != b':' {
                    return None;
                }
                let (h, m, sec) = (dg(0)? * 10 + dg(1)?, dg(3)? * 10 + dg(4)?, dg(6)? * 10 + dg(7)?);
                if h > 23 || m > 59 || sec > 59 {
                    return None;
                }
                Some(h * 3600 + m * 60 + sec)
            })();
            let w = u64::from_le_bytes(s);
            assert_eq!(tod_checked(w), want, "{:?}", std::str::from_utf8(&s));
            if let Some(v) = want {
                assert_eq!(tod_of_lanes(hms_lanes(w)), v);
                seen_ok += 1;
            }
        }
        assert!(seen_ok > 500_000);
    }
    #[test]
    fn pack7_roundtrip_and_order() {
        let mut rng = 12345u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut prev: Option<(Vec<u8>, u64)> = None;
        for _ in 0..2_000_000 {
            let n = 1 + (next() % 11) as usize;
            let mut buf = vec![0u8; 32];
            for i in 0..n {
                let r = next();
                buf[i] = match r % 5 {
                    0 => (r >> 8) as u8,
                    1 => b" \r_-\"\t"[(r >> 8) as usize % 6],
                    _ => b"0123456789ABCXYZabcxyz"[(r >> 8) as usize % 22],
                };
            }
            for i in n..32 { buf[i] = (next() >> 8) as u8; }
            let s = &buf[..n];
            let ok = n <= 9 && s.iter().all(|&c| c != 0 && c < 0x80);
            let k = pack7(s);
            assert_eq!(k.is_some(), ok, "{:?}", s);
            if let Some(k) = k {
                let mut o = Vec::new();
                unpack7(k, &mut o);
                assert_eq!(&o[..], s);
                if let Some((ps, pk)) = &prev {
                    assert_eq!(ps.as_slice().cmp(s), pk.cmp(&k), "{:?} {:?}", ps, s);
                }
                prev = Some((s.to_vec(), k));
            }
        }
    }
}
