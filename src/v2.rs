//! The fast pipeline.
//!
//! L (parallel): links. A concurrent hash table gives every linked ID a slot;
//!   a lock-free union-find over the slots always hangs the root with the
//!   larger ID under the one with the smaller, so each set's root is its
//!   byte-wise smallest ID: the person's name.
//! A (parallel): every event and click line. A valid event copy goes straight
//!   to its person's partition as 16 bytes; every event copy also leaves a
//!   16-byte dedup record (event_id, ingest_ts, where its event record went)
//!   in a partition by event_id. Valid clicks go to their person's partition.
//! B (parallel): per event_id partition, the event_ids seen more than once
//!   are found with a small counter table; of each one's copies all but the
//!   earliest are tombstoned in place. An invalid earliest copy has no event
//!   record, so tombstoning the rest drops the event (dedup before validation).
//! C (parallel): per person partition, group by person, sort each person's
//!   few events, then sessions, credit, assists and the daily rows.
//!
//! Compact records need: window + 7 days < 2^32 ms, at most 256 categories,
//! ingest_ts in whole seconds within i32 seconds of the window. A dataset
//! that breaks one makes `run` return false and the general pipeline runs.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Instant;

use memmap2::Mmap;

use crate::tok::{self, Scanner};
use crate::fast;
use crate::keys::{self, any_interned, cmp_key, key_bytes};
use crate::legacy::{add_cat, chunks, env, list_csv, manifest_value, map_file, prefix, read_pairs, run_jobs, top_category, Job};
use crate::scatter::Scatter;
use crate::scatter2::Scatter16;
use crate::table::{mix, LocalMap, U64Map};
use crate::ts::{parse_price, parse_ts, FastTs};

const GAP: u32 = 1_800_000;
const REACH: i64 = 604_800_000;
const DAY: i64 = 86_400_000;
const LATE: i64 = 86_400_000;
const NONE: u32 = u32::MAX;
const TOMB: u32 = u32::MAX;
const RING: usize = 16;

macro_rules! log {
    ($t0:expr, $($arg:tt)*) => {
        eprintln!("[{:>7.3}s] {} (rss {} MB, anon {} MB)", $t0.elapsed().as_secs_f64(), format!($($arg)*), rss_mb().0, rss_mb().1)
    };
}

/// Resident and anonymous-resident memory in MB (from /proc/self/statm; 0 if unavailable).
fn rss_mb() -> (u64, u64) {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let v: Vec<u64> = s.split_whitespace().filter_map(|x| x.parse().ok()).collect();
    if v.len() < 3 {
        return (0, 0);
    }
    (v[1] * 4096 >> 20, (v[1] - v[2]) * 4096 >> 20)
}

/// A valid event, in its person's partition. `ts` is ms since `base`
/// (window_start - 7 days); TOMB marks a copy dedup dropped.
/// `pc` = price | category << 24 (price 0: not a purchase).
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct PEv {
    person: u64,
    ts: u32,
    pc: u32,
}

/// A dedup record for one event copy: ingest_ts in seconds from the window
/// start, and `loc` = where its PEv went (thread, partition, index), or NONE.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Dd {
    /// A packed event_id; or, for a long one, INTERN_BIT | fingerprint(31) << 32
    /// | source offset >> 8 (the low 8 bits of the offset are in `word`).
    id: u64,
    /// ingest (24 bits, signed) | low 8 bits of the source offset << 24.
    word: u32,
    loc: u32,
}

/// The dedup key of a record: the whole packed id, or the fingerprint part of a long one.
#[inline(always)]
fn dkey(id: u64) -> u64 {
    if id & keys::INTERN_BIT != 0 { id & 0xFFFF_FFFF_0000_0000 } else { id }
}
#[inline(always)]
fn dsrc(id: u64, word: u32) -> u64 {
    ((id & 0xFFFF_FFFF) << 8) | (word >> 24) as u64
}
#[inline(always)]
fn dingest(word: u32) -> i32 {
    ((word << 8) as i32) >> 8
}
#[inline(always)]
fn dword(ingest: i32, src: u64) -> u32 {
    ((src & 0xFF) as u32) << 24 | (ingest as u32 & 0x00FF_FFFF)
}
/// Whether two long event ids (same fingerprint) are the same string.
#[inline(never)]
fn same_long_id(a: u64, aw: u32, b: u64, bw: u32) -> bool {
    let (sa, sb) = (dsrc(a, aw), dsrc(b, bw));
    sa == sb || keys::source_slice(sa) == keys::source_slice(sb)
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct PCk {
    person: u64,
    id: u64,
    ts: u32,
    camp: u32,
}

struct UserRow {
    person: u64,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: u64,
    top: u32,
}

struct Ctx {
    /// The window start in whole seconds.
    ws_s: i64,
    ws: i64,
    we: i64,
    base: i64,
    day_lo: i64,
    ndays: usize,
    categories: Vec<Vec<u8>>,
    unknown_cat: u32,
    product_fast: U64Map,
    product_slow: HashMap<Vec<u8>, u32>,
    camp_fast: U64Map,
    /// Campaigns with IDs of at most 7 bytes: open addressing over (raw
    /// bytes | length << 56, index); u64::MAX marks an empty slot.
    camp_short: Vec<(u64, u32)>,
    camp_slow: HashMap<Vec<u8>, u32>,
    campaign_ids: Vec<Vec<u8>>,
    channels: Vec<Vec<u8>>,
    p1_bits: u32,
    p2_bits: u32,
    idx_bits: u32,
    fast_ts: FastTs,
}

impl Ctx {
    #[inline]
    fn category(&self, product: &[u8]) -> u32 {
        if product.is_empty() {
            return self.unknown_cat;
        }
        let found = match fast::pack(product.as_ptr(), product.len()) {
            Some(k) => self.product_fast.get(k).map(|v| v as u32),
            None => self.product_slow.get(product).copied(),
        };
        found.unwrap_or(self.unknown_cat)
    }

    /// The campaign's index (`c` has 8 readable bytes).
    #[inline(always)]
    fn campaign(&self, c: &[u8]) -> Option<u32> {
        let n = c.len();
        if n <= 7 {
            // Every campaign this short is in `camp_short`.
            let k = unsafe { std::arch::x86_64::_bzhi_u64((c.as_ptr() as *const u64).read_unaligned(), 8 * n as u32) } | (n as u64) << 56;
            let mask = self.camp_short.len() - 1;
            let mut i = (k.wrapping_mul(crate::table::PHI) >> 40) as usize & mask;
            loop {
                let (sk, v) = unsafe { *self.camp_short.get_unchecked(i) };
                if sk == k {
                    return Some(v);
                }
                if sk == u64::MAX {
                    return None;
                }
                i = (i + 1) & mask;
            }
        }
        match fast::pack(c.as_ptr(), n) {
            Some(k) => self.camp_fast.get(k).map(|v| v as u32),
            None => self.camp_slow.get(c).copied(),
        }
    }
}

/// The partition of a key: the top bits of one multiplication (every key
/// bit reaches them). Tables inside a partition index with `mix`, which is
/// independent of this.
#[inline(always)]
fn part(k: u64, bits: u32) -> usize {
    (k.wrapping_mul(crate::table::PHI) >> (64 - bits)) as usize
}

// ---------------------------------------------------------------- links

#[repr(C)]
struct Slot {
    key: AtomicU64,
    val: AtomicU64,
}

/// Concurrent ID table and union-find. `parent` holds parent slot + 1, or 0
/// for a root, so freshly zeroed memory is a forest of singletons.
struct Links {
    slots: Vec<Slot>,
    parent: Vec<AtomicU32>,
    mask: usize,
}

fn zeroed<T>(n: usize) -> Vec<T> {
    // Slot and AtomicU32/U64 are valid when all-zero; calloc'd memory is lazily zeroed.
    let layout = std::alloc::Layout::array::<T>(n).unwrap();
    unsafe {
        let p = std::alloc::alloc_zeroed(layout) as *mut T;
        assert!(!p.is_null());
        Vec::from_raw_parts(p, n, n)
    }
}

impl Links {
    fn new(expect: usize) -> Self {
        let cap = (expect.max(16) * 2).next_power_of_two();
        Links { slots: zeroed(cap), parent: zeroed(cap), mask: cap - 1 }
    }

    #[inline]
    fn insert(&self, k: u64) -> u32 {
        let mut i = mix(k) as usize & self.mask;
        loop {
            let cur = self.slots[i].key.load(Ordering::Acquire);
            if cur == k {
                return i as u32;
            }
            if cur == 0 {
                match self.slots[i].key.compare_exchange(0, k, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => return i as u32,
                    Err(actual) if actual == k => return i as u32,
                    Err(_) => {}
                }
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline]
    fn key_at(&self, i: u32) -> u64 {
        self.slots[i as usize].key.load(Ordering::Relaxed)
    }

    #[inline]
    fn find(&self, mut x: u32) -> u32 {
        loop {
            let p = self.parent[x as usize].load(Ordering::Acquire);
            if p == 0 {
                return x;
            }
            let p = p - 1;
            let gp = self.parent[p as usize].load(Ordering::Acquire);
            if gp == 0 {
                return p;
            }
            let _ = self.parent[x as usize].compare_exchange(p + 1, gp, Ordering::AcqRel, Ordering::Relaxed);
            x = gp - 1;
        }
    }

    fn union(&self, a: u32, b: u32) {
        loop {
            let (ra, rb) = (self.find(a), self.find(b));
            if ra == rb {
                return;
            }
            let (child, par) =
                if cmp_key(self.key_at(ra), self.key_at(rb)) == std::cmp::Ordering::Less { (rb, ra) } else { (ra, rb) };
            if self.parent[child as usize]
                .compare_exchange(0, par + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    #[inline(always)]
    fn prefetch(&self, k: u64) {
        let i = mix(k) as usize & self.mask;
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(self.slots.as_ptr().add(i) as *const i8, _MM_HINT_T0);
        }
    }

    /// The person for an ID: its set's smallest ID, or itself if unlinked.
    #[inline(always)]
    fn person(&self, k: u64) -> u64 {
        let mut i = mix(k) as usize & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            let sk = s.key.load(Ordering::Relaxed);
            if sk == k {
                return s.val.load(Ordering::Relaxed);
            }
            if sk == 0 {
                return k;
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// The key of a linked ID; a long one's name is recorded now (links are compared by bytes).
#[inline(always)]
unsafe fn link_key(p: *const u8, n: usize, src: u64) -> u64 {
    if n <= 9 {
        if let Some(k) = fast::pack9(p, n) {
            return k;
        }
    }
    let b = std::slice::from_raw_parts(p, n);
    let key = keys::fp_key(b);
    keys::note_nonpacked();
    keys::name_insert(key, keys::name_word(b, src));
    key
}

fn parse_links(file: &[u8], start: usize, end: usize, links: &Links, sc: &mut Scanner, fbase: usize) {
    tok::scan_file_src::<2>(sc, file, start, end, #[inline(always)]
    |p, origin, s, e| unsafe {
        let (na, nb) = ((e[0] - s[0]) as usize, (e[1] - s[1]) as usize);
        if na == 0 || nb == 0 {
            return;
        }
        let src = |i: usize| (fbase as isize + origin + s[i] as isize) as u64;
        let (ka, kb) = (link_key(p.add(s[0] as usize), na, src(0)), link_key(p.add(s[1] as usize), nb, src(1)));
        if ka != kb {
            let (a, b) = (links.insert(ka), links.insert(kb));
            links.union(a, b);
        }
    }, |p, n| keys::prefetch(unsafe { std::slice::from_raw_parts(p, n) }));
}

// ---------------------------------------------------------------- parse

struct AState {
    tid: u32,
    pe: Scatter16<PEv>,
    dd: Scatter16<Dd>,
    pc: Scatter<PCk, 8>,
    lu: Scatter16<Lu>,
    sc: Scanner,
    buf: Vec<u8>,
    bad: bool,
}

const VIEW: u32 = u32::from_le_bytes(*b"view");
const CART: u32 = u32::from_le_bytes(*b"cart");
const PURCHASE: u64 = u64::from_le_bytes(*b"purchase");

/// A key for the `n` bytes at `p` (16 readable).
#[inline(always)]
unsafe fn key_at(p: *const u8, n: usize) -> u64 {
    if n == 9 {
        // The usual ID length, in a dozen instructions (same value as `pack9`).
        const M7: u64 = 0x7F7F_7F7F_7F7F_7F7F;
        let v = (p as *const u64).read_unaligned();
        let c = *p.add(8) as u64;
        let u = (v & M7).wrapping_add(M7);
        if ((!u | v) & 0x8080_8080_8080_8080 == 0) & (c.wrapping_sub(1) < 0x7F) {
            return (std::arch::x86_64::_pext_u64(v.swap_bytes(), M7) << 7) | c;
        }
    }
    match fast::pack9(p, n) {
        Some(k) => k,
        None => keys::intern(std::slice::from_raw_parts(p, n)),
    }
}

/// A ts value at `p` (the 16 bytes before its end readable).
#[inline(always)]
unsafe fn ts_at(ctx: &Ctx, p: *const u8, n: usize) -> Option<i64> {
    if (1..=16).contains(&n) {
        return fast::digits16(p.add(n), n).map(|v| v as i64);
    }
    if n == 20 {
        if let Some(s) = ctx.fast_ts.f1_fast(p) {
            return Some(s * 1000);
        }
    }
    if n == 20 || n == 25 {
        if let Some(s) = ctx.fast_ts.dt_secs_simd(p, n) {
            return Some(s * 1000);
        }
    }
    ctx.fast_ts.parse(std::slice::from_raw_parts(p, n))
}

/// An ingest_ts as whole seconds since the window start; None if it is not
/// a whole second within i32 range (the fast pipeline then gives up).
#[inline(always)]
unsafe fn ingest_at(ctx: &Ctx, p: *const u8, n: usize) -> Option<i32> {
    let ws_s = ctx.ws_s;
    if n == 20 {
        if let Some(s) = ctx.fast_ts.f1_fast(p) {
            return i32::try_from(s - ws_s).ok();
        }
        if let Some(s) = ctx.fast_ts.dt_secs_simd(p, n) {
            return i32::try_from(s - ws_s).ok();
        }
    }
    let ms = ts_at(ctx, p, n)?;
    if ms % 1000 != 0 {
        return None;
    }
    i32::try_from(ms / 1000 - ws_s).ok()
}

/// One event line's values: the dedup key and ingest (None: not whole
/// seconds in range, the fast pipeline gives up), and the event record if
/// the copy is valid.
struct EvOut {
    id: u64,
    /// The source offset of a long id (0 for a packed one).
    src: u64,
    ingest: Option<i32>,
    ev: Option<PEv>,
}

/// Converts one well-formed event line (field ranges `s`/`e` from `p`, PAD
/// readable around each field). None: empty event_id, the line is dropped.
#[inline(always)]
unsafe fn convert_event(ctx: &Ctx, p: *const u8, origin: isize, fbase: usize, s: &[u32; 7], e: &[u32; 7], lu: &mut Scatter16<Lu>) -> Option<EvOut> {
    let (ws, we, base) = (ctx.ws, ctx.we, ctx.base);
    let at = |i: usize| p.add(s[i] as usize);
    let ln = |i: usize| (e[i] - s[i]) as usize;
    if ln(0) == 0 {
        return None;
    }
    let (id, src) = match fast::pack9(at(0), ln(0)) {
        Some(k) => (k, 0u64),
        None => {
            // A long id: a fingerprint for dedup's tables, and where its bytes are.
            let bytes = std::slice::from_raw_parts(at(0), ln(0));
            let src = (fbase as isize + origin + (at(0) as isize - p as isize)) as u64;
            (keys::INTERN_BIT | ((keys::hash_padded(bytes) & 0x7FFF_FFFF) << 32) | (src >> 8), src)
        }
    };
    // ingest must fit 24 signed bits (the fast pipeline gives up otherwise).
    let ingest = ingest_at(ctx, at(3), ln(3)).filter(|v| (-(1 << 23)..(1 << 23)).contains(v));
    let ingest_ms = ws + ingest.unwrap_or(0) as i64 * 1000;
    let ev = 'valid: {
        if ln(1) == 0 || ln(5) == 0 {
            break 'valid None;
        }
        let ts = match ts_at(ctx, at(2), ln(2)) {
            Some(t) if t >= ws && t < we => t,
            _ => break 'valid None,
        };
        let tl = ln(4);
        let tw = (at(4) as *const u32).read_unaligned();
        let purchase = tl == 8 && (at(4) as *const u64).read_unaligned() == PURCHASE;
        if !(purchase | ((tl == 4) & ((tw == VIEW) | (tw == CART)))) {
            break 'valid None;
        }
        let mut pc = 0u32;
        if purchase {
            match parse_price(std::slice::from_raw_parts(at(6), ln(6))) {
                Some(p) => pc = p,
                None => break 'valid None,
            }
        }
        if ingest_ms.saturating_sub(ts) > LATE {
            break 'valid None;
        }
        if purchase {
            pc |= ctx.category(std::slice::from_raw_parts(at(5), ln(5))) << 24;
        }
        let usrc = (fbase as isize + origin + (at(1) as isize - p as isize)) as u64;
        Some(PEv { person: user_key(at(1), ln(1), usrc, lu, ctx.p2_bits), ts: (ts - base) as u32, pc })
    };
    Some(EvOut { id, src, ingest, ev })
}

#[inline(never)]
fn parse_events(file: &[u8], start: usize, end: usize, ctx: &Ctx, st: &mut AState, fbase: usize) {
    let (p1, p2, ib) = (ctx.p1_bits, ctx.p2_bits, ctx.idx_bits);
    let loc_hi = st.tid << (p2 + ib);
    let idx_max = (1usize << ib) - 2;
    let AState { pe, dd, sc, bad, lu, .. } = st;
    tok::scan_file_src::<7>(sc, file, start, end, #[inline(always)]
    |p, origin, s, e| unsafe {
        let Some(o) = convert_event(ctx, p, origin, fbase, s, e, lu) else { return };
        let ingest = o.ingest.unwrap_or_else(|| {
            *bad = true;
            0
        });
        let mut loc = NONE;
        if let Some(ev) = o.ev {
            let pp = part(ev.person, p2);
            let idx = pe.push(pp, ev) as usize;
            if idx > idx_max {
                *bad = true;
            }
            loc = loc_hi | ((pp as u32) << ib) | (idx as u32 & ((1 << ib) - 1));
        }
        dd.push(part(dkey(o.id), p1), Dd { id: o.id, word: dword(ingest, o.src), loc });
    }, |p, n| keys::prefetch(unsafe { std::slice::from_raw_parts(p, n) }));
}

/// CBENCH=1: single-thread cost of `convert_event` alone, over the largest
/// event file's lines pre-tokenized in memory.
fn conv_bench(ctx: &Ctx, maps: &[Option<Mmap>]) {
    let m = maps.iter().flatten().max_by_key(|m| m.len()).unwrap();
    let file: &[u8] = &m[..];
    let mut lines: Vec<([u32; 7], [u32; 7])> = Vec::new();
    let mut sc = Scanner::new();
    let base = file.as_ptr();
    tok::scan_file::<7>(&mut sc, file, crate::csv::after_header(file), file.len(), |p, s, e| {
        if p == base {
            lines.push((*s, *e));
        }
    });
    let mode: u32 = env("CBENCH", "9").parse().unwrap();
    let mut bench_lu: Scatter16<Lu> = Scatter16::new(1 << ctx.p2_bits, 1024);
    for round in 0..3 {
        let t = Instant::now();
        let mut sink = 0u64;
        for (s, e) in &lines {
            if mode < 9 {
                unsafe {
                    let p = base;
                    let at = |i: usize| p.add(s[i] as usize);
                    let ln = |i: usize| (e[i] - s[i]) as usize;
                    let v = match mode {
                        0 => (s[3] ^ e[2]) as u64,
                        1 => key_at(at(0), ln(0)),
                        2 => key_at(at(0), ln(0)) ^ key_at(at(1), ln(1)),
                        3 => ingest_at(ctx, at(3), ln(3)).unwrap_or(0) as u64,
                        4 => ts_at(ctx, at(2), ln(2)).unwrap_or(0) as u64,
                        5 => {
                            let tl = ln(4);
                            let tw = (at(4) as *const u32).read_unaligned();
                            let purchase = tl == 8 && (at(4) as *const u64).read_unaligned() == PURCHASE;
                            (purchase | ((tl == 4) & ((tw == VIEW) | (tw == CART)))) as u64
                        }
                        6 => ctx.fast_ts.dt_secs(at(3), 20).unwrap_or(0) as u64,
                        _ => ctx.fast_ts.dt_secs_simd(at(3), 20).unwrap_or(0) as u64,
                    };
                    sink = sink.wrapping_add(v);
                }
                continue;
            }
            if let Some(o) = unsafe { convert_event(ctx, base, 0, 0, s, e, &mut bench_lu) } {
                sink = sink.wrapping_add(o.id ^ o.ingest.unwrap_or(0) as u64);
                if let Some(ev) = o.ev {
                    sink = sink.wrapping_add(ev.person ^ ev.ts as u64 ^ ev.pc as u64);
                }
            }
        }
        eprintln!("conv round {} {:.1} ns/line ({} lines, sink {})", round, t.elapsed().as_nanos() as f64 / lines.len() as f64, lines.len(), sink % 7);
    }
}

#[inline(never)]
fn parse_clicks(file: &[u8], start: usize, end: usize, ctx: &Ctx, st: &mut AState, fbase: usize) {
    let (lo, hi, base, p2) = (ctx.ws - REACH, ctx.we, ctx.base, ctx.p2_bits);
    let AState { pc, sc, lu, .. } = st;
    tok::scan_file_src::<4>(sc, file, start, end, #[inline(always)]
    |p, origin, s, e| unsafe {
        let at = |i: usize| p.add(s[i] as usize);
        let ln = |i: usize| (e[i] - s[i]) as usize;
        if ln(0) == 0 || ln(1) == 0 {
            return;
        }
        let ts = match ts_at(ctx, at(3), ln(3)) {
            Some(t) if t >= lo && t < hi => t,
            _ => return,
        };
        let Some(camp) = ctx.campaign(std::slice::from_raw_parts(at(2), ln(2))) else { return };
        let user = user_key(at(1), ln(1), (fbase as isize + origin + (at(1) as isize - p as isize)) as u64, lu, p2);
        // A click id is only ever ordered (ties on ts): a long one points at its bytes.
        let id = match fast::pack9(at(0), ln(0)) {
            Some(k) => k,
            None => {
                keys::note_nonpacked();
                keys::INTERN_BIT | keys::SRC_BIT | (fbase as isize + origin + (at(0) as isize - p as isize)) as u64
            }
        };
        pc.push(part(user, p2), PCk { person: user, id, ts: (ts - base) as u32, camp });
    }, |p, n| keys::prefetch(unsafe { std::slice::from_raw_parts(p, n) }));
}

// ---------------------------------------------------------------- dedup

struct BState {
    bits: Vec<u64>,
    cand: Vec<Dd>,
    /// Exact table over the candidates: dedup key (0: empty) and the best copy so far.
    keys: Vec<u64>,
    best: Vec<Dd>,
}

/// Raw base pointers of every (thread, person partition) event buffer.
struct Bases(Vec<Vec<*mut PEv>>);
unsafe impl Sync for Bases {}

/// The filter slot of an id: the bits of `part`'s product just below the
/// ones that chose the partition (`skip` of them), so they still vary freely
/// inside it.
#[inline(always)]
fn dd_hash(id: u64, skip: u32, shift: u32) -> usize {
    ((id.wrapping_mul(0x9E37_79B9_7F4A_7C15) << skip) >> shift) as usize
}

#[inline(never)]
fn dedup_partition(a: &[AState], j: usize, bases: &Bases, p1: u32, p2: u32, ib: u32, st: &mut BState) {
    let n: usize = a.iter().map(|s| s.dd.len(j)).sum();
    if n < 2 {
        return;
    }
    // Pass 1: a 2-bit filter (bit 0: seen, bit 1: seen again), ~8 slots per record.
    let slots = (n * 8).max(1 << 10).next_power_of_two();
    let shift = 64 - slots.trailing_zeros();
    st.bits.clear();
    st.bits.resize(slots / 32, 0);
    let bits = &mut st.bits[..];
    for s in a {
        for r in s.dd.records(j) {
            let h = dd_hash(dkey(r.id), p1, shift);
            let w = unsafe { bits.get_unchecked_mut(h >> 5) };
            let b = (h & 31) * 2;
            let seen = (*w >> b) & 1;
            *w |= (1 | (seen << 1)) << b;
        }
    }
    // Pass 2: every copy of an id whose slot was hit twice (the duplicates
    // plus a few hash collisions), appended branch-free.
    st.cand.clear();
    st.cand.reserve(n);
    let cand = st.cand.as_mut_ptr();
    let mut nc = 0usize;
    for s in a {
        for r in s.dd.records(j) {
            let h = dd_hash(dkey(r.id), p1, shift);
            let dup = unsafe { (*bits.get_unchecked(h >> 5) >> ((h & 31) * 2 + 1)) & 1 };
            unsafe { cand.add(nc).write(*r) };
            nc += dup as usize;
        }
    }
    unsafe { st.cand.set_len(nc) };
    if nc == 0 {
        return;
    }
    // Exact pass: per id keep the copy with the smallest ingest (equal ingest:
    // identical copies, either will do) and tombstone every other copy.
    let cap = (nc * 2).max(16).next_power_of_two();
    st.keys.clear();
    st.keys.resize(cap, 0);
    if st.best.len() < cap {
        st.best.resize(cap, Dd::default());
    }
    let tomb = |loc: u32| {
        if loc != NONE {
            let t = (loc >> (p2 + ib)) as usize;
            let pp = ((loc >> ib) & ((1 << p2) - 1)) as usize;
            let idx = (loc & ((1 << ib) - 1)) as usize;
            unsafe { (*bases.0[t][pp].add(idx)).ts = TOMB };
        }
    };
    let (keys, best) = (&mut st.keys[..], &mut st.best[..]);
    for r in &st.cand {
        // Keys are never 0 (packed IDs are non-empty, long ones have the top bit).
        // mix's low bits: unrelated to both the partition and the filter slot.
        let dk = dkey(r.id);
        let long = dk & keys::INTERN_BIT != 0;
        let mut i = mix(dk) as usize & (cap - 1);
        loop {
            let k = keys[i];
            if k == dk && (!long || same_long_id(r.id, r.word, best[i].id, best[i].word)) {
                let b = &mut best[i];
                if dingest(r.word) < dingest(b.word) {
                    tomb(b.loc);
                    *b = *r;
                } else {
                    tomb(r.loc);
                }
                break;
            }
            if k == 0 {
                keys[i] = dk;
                best[i] = *r;
                break;
            }
            i = (i + 1) & (cap - 1);
        }
    }
}

// ---------------------------------------------------------------- sessions

/// A click in its person's group: id, ts, campaign.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Ck {
    id: u64,
    ts: u32,
    camp: u32,
}

struct CState {
    /// Events grouped by person as 8-byte sort keys: ts << 32 | pc.
    evs: Vec<u64>,
    cks: Vec<Ck>,
    ev_tmp: Vec<u64>,
    ck_tmp: Vec<Ck>,
    pmap: LocalMap,
    roots: Vec<u64>,
    persons: Vec<u64>,
    local: Vec<u32>,
    ev_start: Vec<u32>,
    ck_start: Vec<u32>,
    cursor: Vec<u32>,
    cursor2: Vec<u32>,
    runs: Vec<[u32; 6]>,
    rows: Vec<UserRow>,
    acc: Vec<[u64; 4]>,
    asst: Vec<u64>,
    win: Vec<u32>,
    active: Vec<u32>,
    pos: Vec<u32>,
    act: Vec<[u32; 2]>,
    segs: Vec<(u32, u32)>,
    cats: Vec<(u32, u64)>,
}

struct Walk<'a> {
    rows: &'a mut Vec<UserRow>,
    acc: &'a mut Vec<[u64; 4]>,
    asst: &'a mut Vec<u64>,
    win: &'a mut Vec<u32>,
    active: &'a mut Vec<u32>,
    pos: &'a mut Vec<u32>,
    act: &'a mut Vec<[u32; 2]>,
    segs: &'a mut Vec<(u32, u32)>,
    cats: &'a mut Vec<(u32, u64)>,
}

/// Credits campaign `c` an assist for each of the person's sessions in
/// [act[c][0], b): per day, from the day segments `segs` (day index, first
/// session), starting at segment act[c][1].
#[inline(always)]
fn close_assist(asst: &mut [u64], segs: &[(u32, u32)], act: [u32; 2], b: u32, c: usize, nc: usize) {
    let a = act[0];
    if a >= b {
        return;
    }
    let mut g = act[1] as usize;
    loop {
        let (di, first) = unsafe { *segs.get_unchecked(g) };
        let end = if g + 1 < segs.len() { unsafe { segs.get_unchecked(g + 1).1 }.min(b) } else { b };
        let from = a.max(first);
        if end > from {
            unsafe { *asst.get_unchecked_mut(di as usize * nc + c) += (end - from) as u64 };
        }
        if end >= b {
            return;
        }
        g += 1;
    }
}

/// Below this many records a comparison sort beats the radix sort.
const RADIX_MIN: usize = 384;

/// Sorts `v` by a 32-bit key: LSD radix, three 11-bit digits, skipping
/// digits every record shares. `tmp` is scratch.
fn radix_sort<T: Copy>(v: &mut [T], tmp: &mut Vec<T>, key: impl Fn(&T) -> u32) {
    const B: u32 = 11;
    const R: usize = 1 << B;
    let n = v.len();
    if n < 2 {
        return;
    }
    let mut h = vec![[0u32; R]; 3];
    for x in v.iter() {
        let k = key(x);
        h[0][(k & (R as u32 - 1)) as usize] += 1;
        h[1][((k >> B) & (R as u32 - 1)) as usize] += 1;
        h[2][(k >> (2 * B)) as usize] += 1;
    }
    if tmp.len() < n {
        tmp.resize(n, v[0]);
    }
    let mut in_v = true;
    for (d, hd) in h.iter_mut().enumerate() {
        if hd.iter().any(|&c| c as usize == n) {
            continue;
        }
        let shift = d as u32 * B;
        let mut sum = 0u32;
        for c in hd.iter_mut() {
            let t = *c;
            *c = sum;
            sum += t;
        }
        // v and tmp are distinct buffers: source and destination never alias.
        let (src, dst) = unsafe {
            if in_v {
                (std::slice::from_raw_parts(v.as_ptr(), n), std::slice::from_raw_parts_mut(tmp.as_mut_ptr(), n))
            } else {
                (std::slice::from_raw_parts(tmp.as_ptr(), n), std::slice::from_raw_parts_mut(v.as_mut_ptr(), n))
            }
        };
        for x in src {
            let b = ((key(x) >> shift) as usize) & (R - 1);
            unsafe {
                let p = hd.get_unchecked_mut(b);
                *dst.get_unchecked_mut(*p as usize) = *x;
                *p += 1;
            }
        }
        in_v = !in_v;
    }
    if !in_v {
        v.copy_from_slice(&tmp[..n]);
    }
}

/// One person's events (8-byte keys ts << 32 | pc, sorted) and clicks
/// (sorted by ts; ties in any order): sessions, credit, assists, daily rows.
fn process_person(person: u64, evs: &[u64], cks: &[Ck], ctx: &Ctx, w: &mut Walk) {
    let organic = ctx.campaign_ids.len();
    let nc = organic + 1;
    let reach = REACH as u32;
    let (mut lo, mut hi) = (0usize, 0usize);
    let n = evs.len();
    let mut k = 0usize;
    let mut cur_day = i64::MIN;
    let (mut d_sessions, mut d_events, mut d_revenue) = (0u32, 0u32, 0u64);
    // Session count so far; assists are credited per interval of sessions a
    // campaign stays in the window (see close_assist).
    let mut ns = 0u32;
    w.cats.clear();
    w.segs.clear();
    while k < n {
        let start = (evs[k] >> 32) as u32;
        let day = (ctx.base + start as i64).div_euclid(DAY);
        if day != cur_day {
            if cur_day != i64::MIN {
                w.rows.push(UserRow {
                    person,
                    day: cur_day as i32,
                    sessions: d_sessions,
                    events: d_events,
                    revenue: d_revenue,
                    top: top_category(w.cats),
                });
            }
            cur_day = day;
            w.segs.push(((day - ctx.day_lo) as u32, ns));
            d_sessions = 0;
            d_events = 0;
            d_revenue = 0;
            w.cats.clear();
        }
        let mut m = k;
        let mut last = start;
        let mut s_revenue = 0u64;
        let mut s_purchase = false;
        while m < n {
            let e = unsafe { *evs.get_unchecked(m) };
            let ts = (e >> 32) as u32;
            if ts - last > GAP {
                break;
            }
            last = ts;
            let pc = e as u32;
            if pc != 0 {
                let price = pc & 0x00FF_FFFF;
                s_revenue += price as u64;
                s_purchase = true;
                add_cat(w.cats, pc >> 24, price);
            }
            m += 1;
        }
        d_sessions += 1;
        d_events += (m - k) as u32;
        d_revenue += s_revenue;

        // The window is the person's clicks with start - 7 days <= ts <= start.
        // win[c] counts campaign c's clicks in it; `active` lists the
        // campaigns with a nonzero count (pos[c] = index in `active`).
        while hi < cks.len() && cks[hi].ts <= start {
            let c = cks[hi].camp as usize;
            if w.win[c] == 0 {
                w.pos[c] = w.active.len() as u32;
                w.active.push(c as u32);
                w.act[c] = [ns, w.segs.len() as u32 - 1];
            }
            w.win[c] += 1;
            hi += 1;
        }
        while lo < hi && cks[lo].ts < start - reach {
            let c = cks[lo].camp as usize;
            w.win[c] -= 1;
            if w.win[c] == 0 {
                close_assist(w.asst, w.segs, w.act[c], ns, c, nc);
                let at = w.pos[c] as usize;
                let moved = *w.active.last().unwrap();
                w.active.swap_remove(at);
                if (moved as usize) != c {
                    w.pos[moved as usize] = at as u32;
                }
            }
            lo += 1;
        }
        let di = (day - ctx.day_lo) as usize;
        let camp = if lo < hi {
            // The latest click; on a ts tie, the smallest click_id.
            let mut b = hi - 1;
            let t = cks[b].ts;
            let mut i = b;
            while i > lo && cks[i - 1].ts == t {
                i -= 1;
                if cmp_key(cks[i].id, cks[b].id) == std::cmp::Ordering::Less {
                    b = i;
                }
            }
            cks[b].camp as usize
        } else {
            organic
        };
        let a = &mut w.acc[di * nc + camp];
        a[0] += 1;
        a[1] += s_purchase as u64;
        a[2] += s_revenue;
        ns += 1;
        k = m;
    }
    if cur_day != i64::MIN {
        w.rows.push(UserRow {
            person,
            day: cur_day as i32,
            sessions: d_sessions,
            events: d_events,
            revenue: d_revenue,
            top: top_category(w.cats),
        });
    }
    for &c in w.active.iter() {
        close_assist(w.asst, w.segs, w.act[c as usize], ns, c as usize, nc);
        w.win[c as usize] = 0;
    }
    w.active.clear();
}

/// A linked ID (one that appears in a link): its person, which is the ID
/// itself for the set's smallest ID.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Rd {
    id: u64,
    root: u64,
}

/// A long user ID seen in an event or click: its fingerprint key and
/// check_hash << 40 | source offset.
#[derive(Clone, Copy, Default)]
struct Lu {
    key: u64,
    w: u64,
}

/// The key of a user ID at `p` (n bytes, source offset `src`): packed, or a
/// fingerprint whose name goes to `lu` (checked and recorded after the parse).
#[inline(always)]
unsafe fn user_key(p: *const u8, n: usize, src: u64, lu: &mut Scatter16<Lu>, p2: u32) -> u64 {
    if n <= 9 {
        if let Some(k) = fast::pack9(p, n) {
            return k;
        }
    }
    let b = std::slice::from_raw_parts(p, n);
    let key = keys::fp_key(b);
    lu.push(part(key, p2), Lu { key, w: keys::name_word(b, src) });
    key
}

/// Checks one partition of long user IDs (same key, same check hash) and
/// records each distinct one's name.
fn check_names(srcs: &[AState], j: usize, tab: &mut Vec<u64>) {
    let n: usize = srcs.iter().map(|s| s.lu.len(j)).sum();
    if n == 0 {
        return;
    }
    let cap = (n * 2).next_power_of_two();
    tab.clear();
    tab.resize(cap * 2, 0);
    for s in srcs {
        for r in s.lu.records(j) {
            let mut i = (r.key.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize & (cap - 1);
            loop {
                let k = tab[2 * i];
                if k == r.key {
                    if tab[2 * i + 1] >> 40 != r.w >> 40 {
                        keys::NAMES_FAIL.store(true, Ordering::Relaxed);
                    }
                    break;
                }
                if k == 0 {
                    tab[2 * i] = r.key;
                    tab[2 * i + 1] = r.w;
                    keys::name_insert(r.key, r.w);
                    break;
                }
                i = (i + 1) & (cap - 1);
            }
        }
    }
}

/// Per-thread rows of linked IDs, by their person's partition, one run per
/// ID: in `ev` the person, the run's counts (events << 32 | clicks) and its
/// event keys; in `ck` its clicks.
struct MState {
    ev: Vec<Vec<u64>>,
    ck: Vec<Vec<Ck>>,
    nruns: Vec<u32>,
}

/// Turns per-group counts into group starts (`starts`, one extra at the end)
/// and leaves each group's write cursor at its start.
fn starts_of(cursor: &mut [u32], starts: &mut Vec<u32>) -> usize {
    starts.clear();
    let mut total = 0u32;
    for c in cursor.iter_mut() {
        starts.push(total);
        let n = *c;
        *c = total;
        total += n;
    }
    starts.push(total);
    total as usize
}

/// Appends group index `idx` to `local` (capacity reserved by the caller).
#[inline(always)]
unsafe fn push_local(local: &mut Vec<u32>, idx: u32) {
    let l = local.len();
    *local.as_mut_ptr().add(l) = idx;
    local.set_len(l + 1);
}

/// Sorts and walks groups `from..` of the grouped rows in `st`.
fn walk_groups(st: &mut CState, from: usize, ctx: &Ctx) {
    let CState { evs, cks, ev_tmp, ck_tmp, persons, ev_start, ck_start, rows, acc, asst, win, active, pos, act, segs, cats, .. } = st;
    let mut w = Walk { rows, acc, asst, win, active, pos, act, segs, cats };
    let skip = SKIP.load(Ordering::Relaxed);
    if skip >= 2 {
        return;
    }
    for p in from..persons.len() {
        let (es, ee) = (ev_start[p] as usize, ev_start[p + 1] as usize);
        if es == ee {
            continue;
        }
        let pe = &mut evs[es..ee];
        if pe.len() < RADIX_MIN {
            pe.sort_unstable();
        } else {
            radix_sort(pe, ev_tmp, |&e| (e >> 32) as u32);
        }
        let pc = &mut cks[ck_start[p] as usize..ck_start[p + 1] as usize];
        if pc.len() < RADIX_MIN {
            pc.sort_unstable_by_key(|c| c.ts);
        } else {
            radix_sort(pc, ck_tmp, |c| c.ts);
        }
        if skip == 0 {
            process_person(persons[p], pe, pc, ctx, &mut w);
        }
    }
}

static SKIP: AtomicU32 = AtomicU32::new(0);
/// Experiment: the grouping pass keeps each row's sort key, so the placing
/// pass does not read the rows again.
static D1TMP: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// D1: one person partition's own rows, grouped by ID with a counting sort on
/// a local index. Linked IDs (this partition's redirect records, roots
/// included) take the first groups; their rows, re-keyed by person, go to
/// the person's partition for D2. Every other ID is a person on its own and
/// is walked here.
#[inline(never)]
fn process_partition(st: &mut CState, mv: &mut MState, srcs: &[AState], rd: &[Scatter16<Rd>], j: usize, ctx: &Ctx) {
    let p2 = ctx.p2_bits;
    let nl;
    {
        let CState { evs, cks, ev_tmp, pmap, persons, roots, local, ev_start, ck_start, cursor, .. } = &mut *st;
        let nrd: usize = rd.iter().map(|r| r.len(j)).sum();
        let n: usize = srcs.iter().map(|s| s.pe.len(j)).sum();
        pmap.reset(n / 16 + nrd + 64, p2);
        persons.clear();
        roots.clear();
        cursor.clear();
        for r in rd {
            for x in r.records(j) {
                // Redirect records are unique per ID.
                pmap.get_or_insert(x.id, persons.len() as u32);
                persons.push(x.id);
                roots.push(x.root);
            }
        }
        nl = persons.len();
        cursor.resize(nl, 0);
        local.clear();
        local.reserve(n);
        // Room for every row to start a group, so the loop needs no growth
        // checks: a row writes the next group's slot and keeps it if it is new.
        persons.reserve(n + 1);
        cursor.reserve(n + 1);
        let (pp, cp, lp) = (persons.as_mut_ptr(), cursor.as_mut_ptr(), local.as_mut_ptr());
        let (mut np, mut nrow) = (nl, 0usize);
        let keep = D1TMP.load(Ordering::Relaxed);
        if keep {
            ev_tmp.clear();
            ev_tmp.reserve(n);
        }
        let tp = ev_tmp.as_mut_ptr();
        for s in srcs {
            for e in s.pe.records(j) {
                if e.ts == TOMB {
                    continue;
                }
                let (idx, inserted) = pmap.get_or_insert(e.person, np as u32);
                unsafe {
                    *pp.add(np) = e.person;
                    *cp.add(np) = 0;
                    np += inserted as usize;
                    *cp.add(idx as usize) += 1;
                    *lp.add(nrow) = idx;
                    if keep {
                        *tp.add(nrow) = (e.ts as u64) << 32 | e.pc as u64;
                    }
                    nrow += 1;
                }
            }
        }
        unsafe {
            persons.set_len(np);
            cursor.set_len(np);
            local.set_len(nrow);
        }
        let total = starts_of(cursor, ev_start);
        evs.clear();
        evs.reserve(total);
        unsafe { evs.set_len(total) };
        if keep {
            let (cp, lp, ep) = (cursor.as_mut_ptr(), local.as_ptr(), evs.as_mut_ptr());
            for k in 0..nrow {
                unsafe {
                    let c = cp.add(*lp.add(k) as usize);
                    *ep.add(*c as usize) = *tp.add(k);
                    *c += 1;
                }
            }
        } else {
            let mut k = 0usize;
            for s in srcs {
                for e in s.pe.records(j) {
                    if e.ts == TOMB {
                        continue;
                    }
                    unsafe {
                        let c = cursor.get_unchecked_mut(*local.get_unchecked(k) as usize);
                        k += 1;
                        *evs.get_unchecked_mut(*c as usize) = (e.ts as u64) << 32 | e.pc as u64;
                        *c += 1;
                    }
                }
            }
        }
        // Clicks: those of linked IDs, and of other IDs with events here.
        let nck: usize = srcs.iter().map(|s| s.pc.parts[j].len()).sum();
        local.clear();
        local.reserve(nck);
        cursor.iter_mut().for_each(|c| *c = 0);
        for s in srcs {
            for c in &s.pc.parts[j] {
                let idx = match pmap.get(c.person) {
                    Some(i) => {
                        unsafe { *cursor.get_unchecked_mut(i as usize) += 1 };
                        i
                    }
                    None => NONE,
                };
                unsafe { push_local(local, idx) };
            }
        }
        let total = starts_of(cursor, ck_start);
        cks.clear();
        cks.reserve(total);
        unsafe { cks.set_len(total) };
        let mut k = 0usize;
        for s in srcs {
            for c in &s.pc.parts[j] {
                unsafe {
                    let idx = *local.get_unchecked(k);
                    k += 1;
                    if idx != NONE {
                        let p = cursor.get_unchecked_mut(idx as usize);
                        *cks.get_unchecked_mut(*p as usize) = Ck { id: c.id, ts: c.ts, camp: c.camp };
                        *p += 1;
                    }
                }
            }
        }
        // Linked IDs' rows move to their person's partition.
        // A run: the person, its counts (events << 32 | clicks), its event keys.
        for g in 0..nl {
            let (es, ee) = (ev_start[g] as usize, ev_start[g + 1] as usize);
            let (cs, ce) = (ck_start[g] as usize, ck_start[g + 1] as usize);
            if es == ee && cs == ce {
                continue;
            }
            let root = roots[g];
            let pp = part(root, p2);
            let v = unsafe { mv.ev.get_unchecked_mut(pp) };
            v.reserve(2 + ee - es);
            v.push(root);
            v.push(((ee - es) as u64) << 32 | (ce - cs) as u64);
            v.extend_from_slice(&evs[es..ee]);
            if ce > cs {
                unsafe { mv.ck.get_unchecked_mut(pp) }.extend_from_slice(&cks[cs..ce]);
            }
            mv.nruns[pp] += 1;
        }
    }
    walk_groups(st, nl, ctx);
}

/// D2: one partition's linked persons, from the runs D1 moved here: per run
/// one person lookup, then its events and clicks are copied whole.
#[inline(never)]
fn process_moved(st: &mut CState, ms: &[MState], j: usize, ctx: &Ctx) {
    {
        let CState { evs, cks, pmap, persons, runs, ev_start, ck_start, cursor, cursor2, .. } = &mut *st;
        let nr: usize = ms.iter().map(|m| m.nruns[j] as usize).sum();
        if nr == 0 {
            return;
        }
        pmap.reset(nr / 2 + 64, ctx.p2_bits);
        persons.clear();
        cursor.clear();
        cursor2.clear();
        runs.clear();
        runs.reserve(nr);
        for (t, m) in ms.iter().enumerate() {
            let v = &m.ev[j];
            let (mut i, mut co) = (0usize, 0u32);
            while i < v.len() {
                let (root, h) = unsafe { (*v.get_unchecked(i), *v.get_unchecked(i + 1)) };
                let (nev, nck) = ((h >> 32) as u32, h as u32);
                let (idx, inserted) = pmap.get_or_insert(root, persons.len() as u32);
                if inserted {
                    persons.push(root);
                    cursor.push(0);
                    cursor2.push(0);
                }
                unsafe {
                    *cursor.get_unchecked_mut(idx as usize) += nev;
                    *cursor2.get_unchecked_mut(idx as usize) += nck;
                }
                runs.push([idx, t as u32, i as u32 + 2, nev, co, nck]);
                i += 2 + nev as usize;
                co += nck;
            }
        }
        let total = starts_of(cursor, ev_start);
        evs.clear();
        evs.reserve(total);
        unsafe { evs.set_len(total) };
        let total = starts_of(cursor2, ck_start);
        cks.clear();
        cks.reserve(total);
        unsafe { cks.set_len(total) };
        for &[idx, t, eo, nev, co, nck] in runs.iter() {
            let m = &ms[t as usize];
            unsafe {
                let c = cursor.get_unchecked_mut(idx as usize);
                std::ptr::copy_nonoverlapping(m.ev[j].as_ptr().add(eo as usize), evs.as_mut_ptr().add(*c as usize), nev as usize);
                *c += nev;
                if nck != 0 {
                    let c = cursor2.get_unchecked_mut(idx as usize);
                    std::ptr::copy_nonoverlapping(m.ck[j].as_ptr().add(co as usize), cks.as_mut_ptr().add(*c as usize), nck as usize);
                    *c += nck;
                }
            }
        }
    }
    walk_groups(st, 0, ctx);
}

// ---------------------------------------------------------------- run

fn bits_for(items: usize, per_part: usize, max_bits: u32) -> u32 {
    let parts = (items / per_part).clamp(2, 1 << max_bits).next_power_of_two();
    parts.trailing_zeros()
}

/// Runs the fast pipeline; false means the dataset needs the general one
/// (nothing has been written then).
pub fn run() -> bool {
    let t0 = Instant::now();
    crate::tok::PREFETCH.store(env("PF", "0") == "1", Ordering::Relaxed);
    crate::tok::TOK.store(env("TOK", "1").parse().unwrap(), Ordering::Relaxed);
    let data = PathBuf::from(env("DATA_DIR", "/data"));
    let out_dir = PathBuf::from(env("OUTPUT_DIR", "/output"));
    let threads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n: &usize| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, |n| n.get()));

    let manifest = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let ws = parse_ts(manifest_value(&manifest, "window_start").expect("window_start").as_bytes()).expect("ws");
    let we = parse_ts(manifest_value(&manifest, "window_end").expect("window_end").as_bytes()).expect("we");
    let base = ws - REACH;
    if we <= ws || we - base >= u32::MAX as i64 - 1 || ws % 1000 != 0 {
        return false;
    }
    let day_lo = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day_lo + 1).max(1) as usize;

    let products = read_pairs(&data.join("products.csv"));
    let mut categories: Vec<Vec<u8>> = products.iter().map(|p| p.1.clone()).collect();
    categories.push(b"unknown".to_vec());
    categories.sort();
    categories.dedup();
    if categories.len() > 256 {
        return false;
    }
    let cat_index: HashMap<Vec<u8>, u32> = categories.iter().enumerate().map(|(i, c)| (c.clone(), i as u32)).collect();
    let unknown_cat = cat_index[&b"unknown"[..]];
    let mut product_fast = U64Map::with_capacity(products.len());
    let mut product_slow = HashMap::new();
    for (id, cat) in &products {
        let c = cat_index[cat];
        match keys::pack(id) {
            Some(k) => product_fast.insert(k, c as u64),
            None => {
                product_slow.insert(id.clone(), c);
            }
        }
    }
    let mut camp_fast = U64Map::with_capacity(1 << 12);
    let mut camp_slow = HashMap::new();
    let mut campaign_ids = Vec::new();
    let mut channels = Vec::new();
    for (id, channel) in read_pairs(&data.join("campaigns.csv")) {
        let i = campaign_ids.len() as u32;
        let known = match keys::pack(&id) {
            Some(k) => camp_fast.get(k).is_some(),
            None => camp_slow.contains_key(&id),
        };
        if known {
            continue;
        }
        match keys::pack(&id) {
            Some(k) => camp_fast.insert(k, i as u64),
            None => {
                camp_slow.insert(id.clone(), i);
            }
        }
        campaign_ids.push(id);
        channels.push(channel);
    }

    let event_maps: Vec<Option<Mmap>> = list_csv(&data.join("events")).iter().map(|p| map_file(p)).collect();
    let click_maps: Vec<Option<Mmap>> = list_csv(&data.join("clicks")).iter().map(|p| map_file(p)).collect();
    let link_maps: Vec<Option<Mmap>> = list_csv(&data.join("links")).iter().map(|p| map_file(p)).collect();
    // Only long user IDs are interned (event and click IDs point into the
    // input instead): about one user ID in eight, of ~1 per 30 events. The
    // table has about 3x that room; an input with far more long user IDs still works
    // through the table's locked overflow path.
    let total_bytes: usize = event_maps.iter().chain(&click_maps).chain(&link_maps).flatten().map(|m| m.len()).sum();
    // The arena is a virtual reservation; ARENA_DIV shrinks it (experiment).
    let arena_div: usize = env("ARENA_DIV", "1").parse().unwrap();
    keys::init_interner(1 << 12, (total_bytes / arena_div).max(64 << 20) >> 4);
    // Long user IDs are fingerprint keys; their names: ~1 distinct per 10 KB of input.
    keys::init_names(total_bytes / 8000 + (1 << 14));
    if let Ok(b) = std::env::var("FP_TEST_BITS") {
        keys::FP_TEST_MASK.store((1u64 << b.parse::<u32>().unwrap()) - 1, Ordering::Relaxed);
    }
    // One offset space over the event and click files, for keys that point into them.
    let (mut src_files, mut ev_base, mut ck_base, mut lk_base) =
        (Vec::new(), vec![0usize; event_maps.len()], vec![0usize; click_maps.len()], vec![0usize; link_maps.len()]);
    for (maps, bases) in [(&event_maps, &mut ev_base), (&click_maps, &mut ck_base), (&link_maps, &mut lk_base)] {
        for (i, m) in maps.iter().enumerate() {
            if let Some(m) = m {
                bases[i] = src_files.len();
                src_files.push((m.as_ptr(), m.len()));
            }
        }
    }
    let src_bases = keys::set_source(&src_files);
    for b in ev_base.iter_mut().chain(ck_base.iter_mut()).chain(lk_base.iter_mut()) {
        *b = src_bases[*b];
    }
    let event_bytes: usize = event_maps.iter().flatten().map(|m| m.len()).sum();
    let link_bytes: usize = link_maps.iter().flatten().map(|m| m.len()).sum();

    let est_events = event_bytes / 80 + 1;
    let tbits = (usize::BITS - (threads.max(2) - 1).leading_zeros()).max(1);
    let per_part: usize = env("PER_PART", "49152").parse().unwrap();
    crate::scatter2::NT.store(env("SCATTER_NT", "1") == "1", Ordering::Relaxed);
    crate::scatter2::DIRECT.store(env("SCAT", "") == "direct", Ordering::Relaxed);
    let per_part1: usize = env("PER_PART1", &per_part.to_string()).parse().unwrap();
    let per_part2: usize = env("PER_PART2", &per_part.to_string()).parse().unwrap();
    crate::tok::BATCH_RT.store(env("BATCH", "16384").parse::<usize>().unwrap().clamp(1024, 16384), Ordering::Relaxed);
    D1TMP.store(env("D1TMP", "0") == "1", Ordering::Relaxed);
    // At most 512 partitions of each kind: on the scorer more of them slow the
    // parse scatter by more than smaller partitions save later (60M events:
    // 1.86 s with 512/512 against 2.04 s with 2048/1024).
    let p2_bits = bits_for(est_events, per_part2, 9.min(29 - tbits));
    let ctx = Ctx {
        ws_s: ws / 1000,
        ws,
        we,
        base,
        day_lo,
        ndays,
        categories,
        unknown_cat,
        product_fast,
        product_slow,
        camp_short: {
            let cap = (campaign_ids.len() * 4).max(16).next_power_of_two();
            let mut t = vec![(u64::MAX, 0u32); cap];
            for (i, id) in campaign_ids.iter().enumerate() {
                if id.len() <= 7 {
                    let mut b = [0u8; 8];
                    b[..id.len()].copy_from_slice(id);
                    let k = u64::from_le_bytes(b) | (id.len() as u64) << 56;
                    let mut j = (k.wrapping_mul(crate::table::PHI) >> 40) as usize & (cap - 1);
                    while t[j].0 != u64::MAX {
                        j = (j + 1) & (cap - 1);
                    }
                    t[j] = (k, i as u32);
                }
            }
            t
        },
        camp_fast,
        camp_slow,
        campaign_ids,
        channels,
        p1_bits: bits_for(est_events, per_part1, 9),
        p2_bits,
        idx_bits: 32 - tbits - p2_bits,
        fast_ts: FastTs::new(ws - REACH - 10 * DAY, we + 10 * DAY),
    };
    let (p1, p2) = (1usize << ctx.p1_bits, 1usize << ctx.p2_bits);
    if std::env::var("CBENCH").is_ok() {
        conv_bench(&ctx, &event_maps);
        return true;
    }
    log!(t0, "setup: {} threads, {} MB events, p1={} p2={}", threads, event_bytes >> 20, p1, p2);

    // A: parse links (first, into the shared union-find), events and clicks, all in one pool.
    let links = Links::new(link_bytes / 20 + 64);
    let mut jobs: Vec<(u8, usize, usize, usize)> = Vec::new();
    for (kind, maps) in [(0u8, &link_maps), (1, &event_maps), (2, &click_maps)] {
        let mut js = Vec::new();
        chunks(maps, Job::Events, &mut js);
        jobs.extend(js.into_iter().filter_map(|j| match j {
            Job::Events(fi, s, e) => Some((kind, fi, s, e)),
            _ => None,
        }));
    }
    let per_pe = est_events / (threads * p2) * 5 / 4 + 64;
    let per_dd = est_events / (threads * p1) * 5 / 4 + 64;
    // Each thread allocates its own buffers, so the zeroing of their fresh
    // huge pages is spread over all cores (0.09 s faster on the scorer at 20M
    // events than allocating them all here). PALLOC=0 turns it off.
    let make = |t: usize| AState {
        tid: t as u32,
        pe: Scatter16::new(p2, per_pe),
        dd: Scatter16::new(p1, per_dd),
        pc: Scatter::new(p2, per_pe / 4),
        lu: Scatter16::new(p2, per_pe / 6),
        sc: Scanner::new(),
        buf: Vec::new(),
        bad: false,
    };
    let mut a_states: Vec<AState> = if env("PALLOC", "1") == "1" {
        std::thread::scope(|sc| {
            let hs: Vec<_> = (0..threads).map(|t| sc.spawn(move || make(t))).collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        })
    } else {
        (0..threads).map(make).collect()
    };
    let readio = env("READIO", "0") == "1";
    let open = |dir: &str| -> Vec<std::fs::File> {
        list_csv(&data.join(dir)).iter().map(|p| std::fs::File::open(p).expect("open")).collect()
    };
    let files: [Vec<std::fs::File>; 3] = if readio { [open("links"), open("events"), open("clicks")] } else { [vec![], vec![], vec![]] };
    let maps = [&link_maps, &event_maps, &click_maps];
    run_jobs(&mut a_states, jobs.len(), |st, j| {
        let (kind, fi, s, e) = jobs[j];
        let mut run = |st: &mut AState, data: &[u8], s: usize, e: usize| match kind {
            0 => parse_links(data, s, e, &links, &mut st.sc, lk_base[fi]),
            1 => parse_events(data, s, e, &ctx, st, ev_base[fi]),
            _ => parse_clicks(data, s, e, &ctx, st, ck_base[fi]),
        };
        if !readio {
            run(st, maps[kind as usize][fi].as_ref().unwrap(), s, e);
            return;
        }
        // Read the job's lines through a small reused buffer, a window at a time.
        let f = &files[kind as usize][fi];
        let size = maps[kind as usize][fi].as_ref().unwrap().len();
        let mut buf = std::mem::take(&mut st.buf);
        let mut pos = s;
        while pos < e {
            let mut want = (e - pos).min(256 << 10);
            loop {
                if buf.len() < want + 2 * tok::PAD + 128 {
                    buf.resize(want + 2 * tok::PAD + 128, 0);
                }
                std::os::unix::fs::FileExt::read_exact_at(f, &mut buf[tok::PAD..tok::PAD + want], pos as u64).expect("read");
                let mut used = want;
                if pos + want < e {
                    match memchr::memrchr(b'\n', &buf[tok::PAD..tok::PAD + want]) {
                        Some(x) => used = x + 1,
                        None => {
                            want = (want * 2).min(e - pos);
                            continue;
                        }
                    }
                }
                let mut end = tok::PAD + used;
                if pos + used == size && buf[end - 1] != b'\n' {
                    buf[end] = b'\n';
                    end += 1;
                }
                run(st, &buf, tok::PAD, end);
                pos += used;
                break;
            }
        }
        st.buf = buf;
    });
    a_states.iter_mut().for_each(|s| {
        s.pe.flush();
        s.dd.flush();
        s.pc.flush();
        s.lu.flush();
    });
    {
        let a = &a_states;
        if a.iter().any(|s| (0..p2).any(|j| s.lu.len(j) != 0)) {
            keys::note_nonpacked();
        }
        let mut tabs: Vec<Vec<u64>> = (0..threads).map(|_| Vec::new()).collect();
        run_jobs(&mut tabs, p2, |t, j| check_names(a, j, t));
    }
    if keys::NAMES_FAIL.load(Ordering::Relaxed) {
        eprintln!("an assumption of the fast pipeline does not hold (user id fingerprints)");
        return false;
    }
    // Every linked ID and its person (itself for the set's smallest ID).
    let mut rd: Vec<Scatter16<Rd>>;
    {
        let slots = links.slots.len();
        let per = slots.div_ceil(threads * 4);
        let p2b = ctx.p2_bits;
        rd = (0..threads).map(|_| Scatter16::new(p2, link_bytes / 20 / (threads * p2) + 64)).collect();
        run_jobs(&mut rd, slots.div_ceil(per), |r, j| {
            for i in j * per..((j + 1) * per).min(slots) {
                let k = links.slots[i].key.load(Ordering::Relaxed);
                if k != 0 {
                    let root = links.key_at(links.find(i as u32));
                    r.push(part(k, p2b), Rd { id: k, root });
                }
            }
        });
        rd.iter_mut().for_each(|r| r.flush());
    }
    if a_states.iter().any(|s| s.bad) {
        eprintln!("an assumption of the fast pipeline does not hold");
        return false;
    }
    log!(t0, "parse");
    if env("ISTAT", "0") == "1" {
        let (n, cap, extra) = keys::interner_stats();
        eprintln!("interner: {} of {} slots, {} in the locked overflow", n, cap, extra);
    }

    // B: dedup.
    let bases = Bases(a_states.iter_mut().map(|s| (0..p2).map(|pp| s.pe.base_ptr(pp)).collect()).collect());
    {
        let a = &a_states;
        let bases = &bases;
        let (p1b, p2b, ib) = (ctx.p1_bits, ctx.p2_bits, ctx.idx_bits);
        let mut b_states: Vec<BState> = (0..threads).map(|_| BState { bits: Vec::new(), cand: Vec::new(), keys: Vec::new(), best: Vec::new() }).collect();
        run_jobs(&mut b_states, p1, |st, j| dedup_partition(a, j, bases, p1b, p2b, ib, st));
    }
    log!(t0, "dedup");

    // D: sessions.
    let ncamp = ctx.campaign_ids.len();
    let mut c_states: Vec<CState> = (0..threads)
        .map(|_| CState {
            evs: Vec::new(),
            cks: Vec::new(),
            ev_tmp: Vec::new(),
            ck_tmp: Vec::new(),
            pmap: LocalMap::new(),
            roots: Vec::new(),
            persons: Vec::new(),
            local: Vec::new(),
            ev_start: Vec::new(),
            ck_start: Vec::new(),
            cursor: Vec::new(),
            cursor2: Vec::new(),
            runs: Vec::new(),
            rows: Vec::new(),
            acc: vec![[0u64; 4]; (ncamp + 1) * ndays],
            asst: vec![0u64; (ncamp + 1) * ndays],
            win: vec![0u32; ncamp + 1],
            active: Vec::new(),
            pos: vec![0u32; ncamp + 1],
            act: vec![[0u32; 2]; ncamp + 1],
            segs: Vec::new(),
            cats: Vec::new(),
        })
        .collect();
    let mut m_states: Vec<MState> = (0..threads)
        .map(|_| MState {
            ev: (0..p2).map(|_| Vec::with_capacity(per_pe * 3 / 4)).collect(),
            ck: (0..p2).map(|_| Vec::with_capacity(per_pe / 4)).collect(),
            nruns: vec![0; p2],
        })
        .collect();
    let reps: usize = env("DREP", "1").parse().unwrap();
    let overlap_out = env("OVERLAP_OUT", "0") == "1";
    SKIP.store(env("SKIP", "0").parse().unwrap(), Ordering::Relaxed);
    for rep in 0..reps {
        if rep > 0 {
            for c in c_states.iter_mut() {
                c.rows.clear();
                c.acc.iter_mut().for_each(|x| *x = [0; 4]);
                c.asst.iter_mut().for_each(|x| *x = 0);
            }
            for m in m_states.iter_mut() {
                m.ev.iter_mut().for_each(|v| v.clear());
                m.ck.iter_mut().for_each(|v| v.clear());
                m.nruns.iter_mut().for_each(|x| *x = 0);
            }
        }
        let td = Instant::now();
        let a = &a_states;
        let ctx = &ctx;
        let rd = &rd;
        // D1: own rows; persons without links are walked, linked IDs' rows move.
        let mut order: Vec<usize> = (0..p2).collect();
        let size = |j: usize| a.iter().map(|s| s.pe.len(j) + s.pc.parts[j].len()).sum::<usize>();
        order.sort_unstable_by_key(|&j| std::cmp::Reverse(size(j)));
        let mut pairs: Vec<(&mut CState, &mut MState)> = c_states.iter_mut().zip(m_states.iter_mut()).collect();
        run_jobs(&mut pairs, p2, |(st, mv), k| process_partition(st, mv, a, rd, order[k], ctx));
        drop(pairs);
        if std::env::var("FSTATS").is_ok() {
            let mv: usize = (0..p2).map(|j| m_states.iter().map(|m| m.ev[j].len()).sum::<usize>()).sum();
            let mvc: usize = (0..p2).map(|j| m_states.iter().map(|m| m.ck[j].len()).sum::<usize>()).sum();
            let ev: usize = (0..p2).map(|j| a.iter().map(|s| s.pe.len(j)).sum::<usize>()).sum();
            let ck: usize = (0..p2).map(|j| a.iter().map(|s| s.pc.parts[j].len()).sum::<usize>()).sum();
            let nrd: usize = (0..p2).map(|j| rd.iter().map(|r| r.len(j)).sum::<usize>()).sum();
            let roots: usize = (0..p2).map(|j| rd.iter().map(|r| r.records(j).iter().filter(|x| x.id == x.root).count()).sum::<usize>()).sum();
            let np: usize = c_states.iter().map(|c| c.rows.len()).sum();
            eprintln!("ev {} ck {} moved ev {} ck {} rd {} roots {} rows-so-far {}", ev, ck, mv, mvc, nrd, roots, np);
        }
        let td1 = td.elapsed().as_secs_f64();
        if reps == 1 {
            log!(t0, "sessions 1");
        }
        // D2: linked persons.
        let ms = &m_states;
        let size = |j: usize| ms.iter().map(|m| m.ev[j].len() + m.ck[j].len()).sum::<usize>();
        order.sort_unstable_by_key(|&j| std::cmp::Reverse(size(j)));
        if reps == 1 && overlap_out {
            // Each thread writes its user_daily part as soon as it runs out of jobs,
            // while the others finish theirs.
            let user_dir = out_dir.join("user_daily");
            std::fs::create_dir_all(&user_dir).expect("user_daily dir");
            let next = std::sync::atomic::AtomicUsize::new(0);
            std::thread::scope(|sc| {
                for (t, st) in c_states.iter_mut().enumerate() {
                    let (next, order, user_dir) = (&next, &order, &user_dir);
                    sc.spawn(move || {
                        loop {
                            let k = next.fetch_add(1, Ordering::Relaxed);
                            if k >= p2 {
                                break;
                            }
                            process_moved(st, ms, order[k], ctx);
                        }
                        write_user_file(user_dir, ctx, t, st);
                    });
                }
            });
        } else {
            run_jobs(&mut c_states, p2, |st, k| process_moved(st, ms, order[k], ctx));
        }
        if reps > 1 {
            eprintln!("rep {} d1 {:.4} d2 {:.4} total {:.4}", rep, td1, td.elapsed().as_secs_f64() - td1, td.elapsed().as_secs_f64());
        }
    }
    log!(t0, "sessions");

    write_outputs(&out_dir, &ctx, &c_states, !(reps == 1 && overlap_out));
    log!(t0, "done");
    {
        // The process exits right after: freeing these piecemeal only costs time.
        std::mem::forget((a_states, m_states, c_states, rd, links, bases));
        std::mem::forget((event_maps, click_maps, link_maps, ctx));
    }
    true
}

/// Byte length of the ID behind key `k`.
#[inline(always)]
fn key_len(k: u64) -> usize {
    if k & keys::INTERN_BIT != 0 {
        return keys::interned_len(k);
    }
    // Seven bits a byte, first byte highest, every byte non-zero, zero-padded.
    9 - (k.trailing_zeros() as usize) / 7
}

/// Appends the ID behind `k` as a PLAIN byte array (u32 length, bytes);
/// `out` must have 16 bytes of spare capacity beyond it.
#[inline(always)]
fn put_key(k: u64, out: &mut Vec<u8>) {
    if k & keys::INTERN_BIT != 0 {
        let at = out.len();
        out.extend_from_slice(&[0; 4]);
        key_bytes(k, out);
        let n = (out.len() - at - 4) as u32;
        out[at..at + 4].copy_from_slice(&n.to_le_bytes());
        return;
    }
    let n = key_len(k);
    assert!(out.capacity() - out.len() >= 13 + 4);
    unsafe {
        use std::arch::x86_64::_pdep_u64;
        let p = out.as_mut_ptr().add(out.len());
        (p as *mut u32).write_unaligned(n as u32);
        // Bytes 0..8 from the top 56 bits, byte 8 from the low 7.
        let hi = _pdep_u64(k >> 7, 0x7F7F_7F7F_7F7F_7F7F).swap_bytes();
        (p.add(4) as *mut u64).write_unaligned(hi);
        *p.add(12) = (k & 0x7F) as u8;
        out.set_len(out.len() + 4 + n);
    }
}

/// Writes thread `t`'s user_daily part.
fn write_user_file(user_dir: &Path, ctx: &Ctx, t: usize, s: &CState) {
    use crate::pq::{def_levels, def_levels_size, write_rows, Col, Column, Kind};
    let path = user_dir.join(format!("part-{:05}.parquet", t));
    {
                // Every column is encoded straight from the rows into the file buffer.
                let rows = &s.rows[..];
                let cats = &ctx.categories;
                let top = |r: &UserRow| (r.top != u32::MAX).then(|| &cats[r.top as usize][..]);
                let id_size = |a: usize, b: usize| rows[a..b].iter().map(|r| 4 + key_len(r.person)).sum::<usize>();
                let id_fill = |a: usize, b: usize, out: &mut Vec<u8>| {
                    out.reserve(id_size(a, b) + 16);
                    for r in &rows[a..b] {
                        put_key(r.person, out);
                    }
                };
                let i64_size = |a: usize, b: usize| (b - a) * 8;
                let i64_col = |f: fn(&UserRow) -> i64| {
                    move |a: usize, b: usize, out: &mut Vec<u8>| {
                        for r in &rows[a..b] {
                            out.extend_from_slice(&f(r).to_le_bytes());
                        }
                    }
                };
                let day_size = |a: usize, b: usize| (b - a) * 4;
                let day_fill = |a: usize, b: usize, out: &mut Vec<u8>| {
                    for r in &rows[a..b] {
                        out.extend_from_slice(&r.day.to_le_bytes());
                    }
                };
                let (ses, evs, rev) = (
                    i64_col(|r| r.sessions as i64),
                    i64_col(|r| r.events as i64),
                    i64_col(|r| r.revenue as i64),
                );
                let cat_size = |a: usize, b: usize| {
                    def_levels_size(b - a) + rows[a..b].iter().filter_map(top).map(|c| 4 + c.len()).sum::<usize>()
                };
                let cat_fill = |a: usize, b: usize, out: &mut Vec<u8>| {
                    let rs = &rows[a..b];
                    def_levels(rs.len(), |i| rs[i].top != u32::MAX, out);
                    for c in rs.iter().filter_map(top) {
                        out.extend_from_slice(&(c.len() as u32).to_le_bytes());
                        out.extend_from_slice(c);
                    }
                };
                write_rows(
                    &path,
                    rows.len(),
                    &[
                        Column { name: "user_id", data: Col::Gen { kind: Kind::Str, size: &id_size, fill: &id_fill } },
                        Column { name: "day", data: Col::Gen { kind: Kind::Date, size: &day_size, fill: &day_fill } },
                        Column { name: "sessions", data: Col::Gen { kind: Kind::I64, size: &i64_size, fill: &ses } },
                        Column { name: "events", data: Col::Gen { kind: Kind::I64, size: &i64_size, fill: &evs } },
                        Column { name: "revenue_cents", data: Col::Gen { kind: Kind::I64, size: &i64_size, fill: &rev } },
                        Column { name: "top_category", data: Col::Gen { kind: Kind::OptStr, size: &cat_size, fill: &cat_fill } },
                    ],
                )
                .expect("write user_daily");
    }
}

fn write_outputs(out_dir: &Path, ctx: &Ctx, c_states: &[CState], users: bool) {
    use crate::pq::{def_levels, def_levels_size, write, write_rows, Col, Column, Kind};
    let user_dir = out_dir.join("user_daily");
    let camp_dir = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&user_dir).expect("user_daily dir");
    std::fs::create_dir_all(&camp_dir).expect("campaign_daily dir");
    let (ncamp, ndays) = (ctx.campaign_ids.len(), ctx.ndays);
    // Merge per-thread counters (day-major: index day * (ncamp + 1) + camp).
    let mut acc = vec![[0u64; 4]; (ncamp + 1) * ndays];
    for s in c_states {
        for (t, x) in acc.iter_mut().zip(&s.acc) {
            for i in 0..3 {
                t[i] += x[i];
            }
        }
        for (t, x) in acc.iter_mut().zip(&s.asst) {
            t[3] += x;
        }
    }
    std::thread::scope(|sc| {
        if users {
            for (t, s) in c_states.iter().enumerate() {
                let user_dir = &user_dir;
                sc.spawn(move || write_user_file(user_dir, ctx, t, s));
            }
        }
        let acc = &acc;
        sc.spawn(move || {
            let (mut ids, mut id_ends, mut chans, mut chan_ends) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
            let (mut day, mut sessions, mut purchasing, mut revenue, mut assisted) =
                (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
            for camp in 0..=ncamp {
                for d in 0..ndays {
                    let a = acc[d * (ncamp + 1) + camp];
                    if a[0] == 0 && a[3] == 0 {
                        continue;
                    }
                    let (id, channel): (&[u8], &[u8]) = if camp == ncamp {
                        (b"organic", b"organic")
                    } else {
                        (&ctx.campaign_ids[camp], &ctx.channels[camp])
                    };
                    ids.extend_from_slice(id);
                    id_ends.push(ids.len() as u32);
                    chans.extend_from_slice(channel);
                    chan_ends.push(chans.len() as u32);
                    day.push((ctx.day_lo + d as i64) as i32);
                    sessions.push(a[0] as i64);
                    purchasing.push(a[1] as i64);
                    revenue.push(a[2] as i64);
                    assisted.push(if camp == ncamp { 0 } else { a[3] as i64 });
                }
            }
            write(
                &camp_dir.join("part-00000.parquet"),
                &[
                    Column { name: "campaign_id", data: Col::Str { bytes: &ids, ends: &id_ends } },
                    Column { name: "channel", data: Col::Str { bytes: &chans, ends: &chan_ends } },
                    Column { name: "day", data: Col::Date(&day) },
                    Column { name: "sessions", data: Col::I64(&sessions) },
                    Column { name: "purchasing_sessions", data: Col::I64(&purchasing) },
                    Column { name: "revenue_cents", data: Col::I64(&revenue) },
                    Column { name: "assisted_sessions", data: Col::I64(&assisted) },
                ],
            )
            .expect("write campaign_daily");
        });
    });
}

