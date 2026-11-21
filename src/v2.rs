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
    id: u64,
    ingest: i32,
    loc: u32,
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

    #[inline]
    fn campaign(&self, c: &[u8]) -> Option<u32> {
        if c.is_empty() {
            return None;
        }
        match fast::pack(c.as_ptr(), c.len()) {
            Some(k) => self.camp_fast.get(k).map(|v| v as u32),
            None => self.camp_slow.get(c).copied(),
        }
    }

    /// A ts value; `back16` says the 16 bytes before the value's end are readable.
    #[inline(always)]
    fn ts(&self, s: &[u8], back16: bool) -> Option<i64> {
        let n = s.len();
        if back16 && (1..=16).contains(&n) {
            // At most 16 bytes: F3 if every byte is a digit, else bad (F1 and F2 are longer).
            return fast::digits16(unsafe { s.as_ptr().add(n) }, n).map(|v| v as i64);
        }
        self.fast_ts.parse(s)
    }
}

#[inline(always)]
fn part(k: u64, bits: u32) -> usize {
    (mix(k) >> (64 - bits)) as usize
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

fn parse_links(file: &[u8], start: usize, end: usize, links: &Links, sc: &mut Scanner) {
    tok::scan_file::<2>(sc, file, start, end, |p, s, e| unsafe {
        let (na, nb) = ((e[0] - s[0]) as usize, (e[1] - s[1]) as usize);
        if na == 0 || nb == 0 {
            return;
        }
        let (ka, kb) = (key_at(p.add(s[0] as usize), na), key_at(p.add(s[1] as usize), nb));
        if ka != kb {
            let (a, b) = (links.insert(ka), links.insert(kb));
            links.union(a, b);
        }
    });
}

// ---------------------------------------------------------------- parse

struct AState {
    tid: u32,
    pe: Scatter16<PEv>,
    dd: Scatter16<Dd>,
    pc: Scatter<PCk, 8>,
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
    match fast::pack(p, n) {
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
    let ws_s = ctx.ws / 1000;
    if n == 20 {
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
    ingest: Option<i32>,
    ev: Option<PEv>,
}

/// Converts one well-formed event line (field ranges `s`/`e` from `p`, PAD
/// readable around each field). None: empty event_id, the line is dropped.
#[inline(always)]
unsafe fn convert_event(ctx: &Ctx, p: *const u8, s: &[u32; 7], e: &[u32; 7]) -> Option<EvOut> {
    let (ws, we, base) = (ctx.ws, ctx.we, ctx.base);
    let at = |i: usize| p.add(s[i] as usize);
    let ln = |i: usize| (e[i] - s[i]) as usize;
    if ln(0) == 0 {
        return None;
    }
    let id = key_at(at(0), ln(0));
    let ingest = ingest_at(ctx, at(3), ln(3));
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
        Some(PEv { person: key_at(at(1), ln(1)), ts: (ts - base) as u32, pc })
    };
    Some(EvOut { id, ingest, ev })
}

#[inline(never)]
fn parse_events(file: &[u8], start: usize, end: usize, ctx: &Ctx, st: &mut AState) {
    let (p1, p2, ib) = (ctx.p1_bits, ctx.p2_bits, ctx.idx_bits);
    let loc_hi = st.tid << (p2 + ib);
    let idx_max = (1usize << ib) - 2;
    let AState { pe, dd, sc, bad, .. } = st;
    tok::scan_file::<7>(sc, file, start, end, |p, s, e| unsafe {
        let Some(o) = convert_event(ctx, p, s, e) else { return };
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
        dd.push(part(o.id, p1), Dd { id: o.id, ingest, loc });
    });
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
            if let Some(o) = unsafe { convert_event(ctx, base, s, e) } {
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
fn parse_clicks(file: &[u8], start: usize, end: usize, ctx: &Ctx, st: &mut AState) {
    let (lo, hi, base, p2) = (ctx.ws - REACH, ctx.we, ctx.base, ctx.p2_bits);
    let AState { pc, sc, .. } = st;
    tok::scan_file::<4>(sc, file, start, end, |p, s, e| unsafe {
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
        let user = key_at(at(1), ln(1));
        pc.push(part(user, p2), PCk { person: user, id: key_at(at(0), ln(0)), ts: (ts - base) as u32, camp });
    });
}

// ---------------------------------------------------------------- dedup

struct BState {
    bits: Vec<u64>,
    cand: Vec<Dd>,
    /// Exact table over the candidates: id (0: empty) and its best (ingest, loc).
    keys: Vec<u64>,
    best: Vec<(i32, u32)>,
}

/// Raw base pointers of every (thread, person partition) event buffer.
struct Bases(Vec<Vec<*mut PEv>>);
unsafe impl Sync for Bases {}

/// A multiplicative hash's top bits: independent of `part`, which fixes the
/// top bits of mix(id) within a dedup partition.
#[inline(always)]
fn dd_hash(id: u64, shift: u32) -> usize {
    (id.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> shift) as usize
}

#[inline(never)]
fn dedup_partition(a: &[AState], j: usize, bases: &Bases, p2: u32, ib: u32, st: &mut BState) {
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
            let h = dd_hash(r.id, shift);
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
            let h = dd_hash(r.id, shift);
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
        st.best.resize(cap, (0, 0));
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
        // Keys are never 0 (packed IDs are non-empty, interned ones have the top bit).
        // mix's low bits: unrelated to both the partition and the filter slot.
        let mut i = mix(r.id) as usize & (cap - 1);
        loop {
            let k = keys[i];
            if k == r.id {
                let b = &mut best[i];
                if r.ingest < b.0 {
                    tomb(b.1);
                    *b = (r.ingest, r.loc);
                } else {
                    tomb(r.loc);
                }
                break;
            }
            if k == 0 {
                keys[i] = r.id;
                best[i] = (r.ingest, r.loc);
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
    rmap: LocalMap,
    roots: Vec<u64>,
    persons: Vec<u64>,
    local: Vec<u32>,
    ev_start: Vec<u32>,
    ck_start: Vec<u32>,
    cursor: Vec<u32>,
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

/// A linked ID whose person is another ID: where its rows must go.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Rd {
    id: u64,
    root: u64,
}

/// Phase C state: rows of linked IDs, re-keyed by person and re-partitioned.
struct MState {
    map: LocalMap,
    roots: Vec<u64>,
    mv: Scatter16<PEv>,
    mvc: Scatter<PCk, 8>,
}

/// Loads partition `u`'s redirects into `map` (ID -> index into `roots`).
fn load_redirects(rd: &[Scatter16<Rd>], u: usize, map: &mut LocalMap, roots: &mut Vec<u64>) -> bool {
    let n: usize = rd.iter().map(|r| r.len(u)).sum();
    roots.clear();
    map.reset(n.max(1));
    for r in rd {
        for x in r.records(u) {
            map.get_or_insert(x.id, roots.len() as u32);
            roots.push(x.root);
        }
    }
    n != 0
}

/// C: moves partition `u`'s rows of redirected IDs to their person's partition.
#[inline(never)]
fn redirect_partition(ms: &mut MState, a: &[AState], rd: &[Scatter16<Rd>], u: usize, p2: u32) {
    let MState { map, roots, mv, mvc } = ms;
    if !load_redirects(rd, u, map, roots) {
        return;
    }
    for s in a {
        for e in s.pe.records(u) {
            if e.ts == TOMB {
                continue;
            }
            if let Some(i) = map.get(e.person) {
                let root = roots[i as usize];
                mv.push(part(root, p2), PEv { person: root, ..*e });
            }
        }
        for c in &s.pc.parts[u] {
            if let Some(i) = map.get(c.person) {
                let root = roots[i as usize];
                mvc.push(part(root, p2), PCk { person: root, ..*c });
            }
        }
    }
}

/// Every event row of partition `j`, in a fixed order: own rows not
/// tombstoned (rows of redirected IDs included), then rows redirected here.
#[inline(always)]
fn each_ev(srcs: &[AState], ms: &[MState], j: usize, mut f: impl FnMut(&PEv)) {
    for s in srcs {
        for e in s.pe.records(j) {
            if e.ts != TOMB {
                f(e);
            }
        }
    }
    for m in ms {
        for e in m.mv.records(j) {
            f(e);
        }
    }
}

#[inline(always)]
fn each_ck(srcs: &[AState], ms: &[MState], j: usize, mut f: impl FnMut(&PCk)) {
    for s in srcs {
        for c in &s.pc.parts[j] {
            f(c);
        }
    }
    for m in ms {
        for c in &m.mvc.parts[j] {
            f(c);
        }
    }
}

/// Group marker: an ID whose rows phase C moved to its person.
const DEAD: u32 = u32::MAX;

/// Turns per-group counts into group starts (`starts`, one extra at the end)
/// and leaves each live group's write cursor at its start.
fn starts_of(cursor: &mut [u32], starts: &mut Vec<u32>) -> usize {
    starts.clear();
    let mut total = 0u32;
    for c in cursor.iter_mut() {
        starts.push(total);
        if *c != DEAD {
            let n = *c;
            *c = total;
            total += n;
        }
    }
    starts.push(total);
    total as usize
}

/// D: one person partition. Rows are grouped by ID with a counting sort on a
/// local index; an ID whose rows phase C moved to its person (a redirected
/// ID) is looked up once per ID, not per row, and its group stays empty.
#[inline(never)]
fn process_partition(st: &mut CState, srcs: &[AState], ms: &[MState], rd: &[Scatter16<Rd>], j: usize, ctx: &Ctx) {
    let CState {
        evs,
        cks,
        ev_tmp,
        ck_tmp,
        pmap,
        rmap,
        roots,
        persons,
        local,
        ev_start,
        ck_start,
        cursor,
        rows,
        acc,
        asst,
        win,
        active,
        pos,
        act,
        segs,
        cats,
    } = st;
    let any_rd = load_redirects(rd, j, rmap, roots);
    let n: usize = srcs.iter().map(|s| s.pe.len(j)).sum::<usize>() + ms.iter().map(|m| m.mv.len(j)).sum::<usize>();
    pmap.reset(n / 16 + 64);
    persons.clear();
    local.clear();
    local.reserve(n);
    cursor.clear();
    each_ev(srcs, ms, j, |e| {
        let (idx, inserted) = pmap.get_or_insert(e.person, persons.len() as u32);
        if inserted {
            persons.push(e.person);
            cursor.push(0);
        }
        unsafe {
            *cursor.get_unchecked_mut(idx as usize) += 1;
            let l = local.len();
            *local.as_mut_ptr().add(l) = idx;
            local.set_len(l + 1);
        }
    });
    let np = persons.len();
    if any_rd {
        for p in 0..np {
            if rmap.get(persons[p]).is_some() {
                cursor[p] = DEAD;
            }
        }
    }
    let total = starts_of(cursor, ev_start);
    evs.clear();
    evs.reserve(total);
    unsafe { evs.set_len(total) };
    let mut k = 0usize;
    each_ev(srcs, ms, j, |e| unsafe {
        let c = cursor.get_unchecked_mut(*local.get_unchecked(k) as usize);
        k += 1;
        if *c != DEAD {
            *evs.get_unchecked_mut(*c as usize) = (e.ts as u64) << 32 | e.pc as u64;
            *c += 1;
        }
    });
    // Clicks: only those of live IDs with events here.
    local.clear();
    for c in cursor.iter_mut() {
        if *c != DEAD {
            *c = 0;
        }
    }
    each_ck(srcs, ms, j, |c| {
        let idx = match pmap.get(c.person) {
            Some(i) if cursor[i as usize] != DEAD => {
                cursor[i as usize] += 1;
                i
            }
            _ => NONE,
        };
        local.push(idx);
    });
    let total = starts_of(cursor, ck_start);
    cks.clear();
    cks.resize(total, Ck::default());
    let mut k = 0usize;
    each_ck(srcs, ms, j, |c| unsafe {
        let idx = *local.get_unchecked(k);
        k += 1;
        if idx != NONE {
            let p = cursor.get_unchecked_mut(idx as usize);
            *cks.get_unchecked_mut(*p as usize) = Ck { id: c.id, ts: c.ts, camp: c.camp };
            *p += 1;
        }
    });
    let mut w = Walk { rows, acc, asst, win, active, pos, act, segs, cats };
    for p in 0..np {
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
        process_person(persons[p], pe, pc, ctx, &mut w);
    }
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
    let event_bytes: usize = event_maps.iter().flatten().map(|m| m.len()).sum();
    let link_bytes: usize = link_maps.iter().flatten().map(|m| m.len()).sum();

    let est_events = event_bytes / 80 + 1;
    let tbits = (usize::BITS - (threads.max(2) - 1).leading_zeros()).max(1);
    let per_part: usize = env("PER_PART", "49152").parse().unwrap();
    crate::scatter2::NT.store(env("SCATTER_NT", "1") == "1", Ordering::Relaxed);
    let p2_bits = bits_for(est_events, per_part, 10.min(29 - tbits));
    let ctx = Ctx {
        ws,
        we,
        base,
        day_lo,
        ndays,
        categories,
        unknown_cat,
        product_fast,
        product_slow,
        camp_fast,
        camp_slow,
        campaign_ids,
        channels,
        p1_bits: bits_for(est_events, per_part, 12),
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
    let mut a_states: Vec<AState> = (0..threads)
        .map(|t| AState {
            tid: t as u32,
            pe: Scatter16::new(p2, per_pe),
            dd: Scatter16::new(p1, per_dd),
            pc: Scatter::new(p2, per_pe / 4),
            sc: Scanner::new(),
            buf: Vec::new(),
            bad: false,
        })
        .collect();
    let readio = env("READIO", "0") == "1";
    let open = |dir: &str| -> Vec<std::fs::File> {
        list_csv(&data.join(dir)).iter().map(|p| std::fs::File::open(p).expect("open")).collect()
    };
    let files: [Vec<std::fs::File>; 3] = if readio { [open("links"), open("events"), open("clicks")] } else { [vec![], vec![], vec![]] };
    let maps = [&link_maps, &event_maps, &click_maps];
    run_jobs(&mut a_states, jobs.len(), |st, j| {
        let (kind, fi, s, e) = jobs[j];
        let mut run = |st: &mut AState, data: &[u8], s: usize, e: usize| match kind {
            0 => parse_links(data, s, e, &links, &mut st.sc),
            1 => parse_events(data, s, e, &ctx, st),
            _ => parse_clicks(data, s, e, &ctx, st),
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
    });
    // The redirects of every linked ID that is not its person's name.
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
                    if root != k {
                        r.push(part(k, p2b), Rd { id: k, root });
                    }
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

    // B: dedup.
    let bases = Bases(a_states.iter_mut().map(|s| (0..p2).map(|pp| s.pe.base_ptr(pp)).collect()).collect());
    {
        let a = &a_states;
        let bases = &bases;
        let (p2b, ib) = (ctx.p2_bits, ctx.idx_bits);
        let mut b_states: Vec<BState> = (0..threads).map(|_| BState { bits: Vec::new(), cand: Vec::new(), keys: Vec::new(), best: Vec::new() }).collect();
        run_jobs(&mut b_states, p1, |st, j| dedup_partition(a, j, bases, p2b, ib, st));
    }
    log!(t0, "dedup");

    // C: rows of linked IDs move to their person's partition.
    let mut m_states: Vec<MState> = (0..threads)
        .map(|_| MState {
            map: LocalMap::new(),
            roots: Vec::new(),
            mv: Scatter16::new(p2, per_pe / 2),
            mvc: Scatter::new(p2, per_pe / 8),
        })
        .collect();
    {
        let (a, rd, p2b) = (&a_states, &rd, ctx.p2_bits);
        run_jobs(&mut m_states, p2, |ms, u| redirect_partition(ms, a, rd, u, p2b));
        m_states.iter_mut().for_each(|m| {
            m.mv.flush();
            m.mvc.flush();
        });
    }
    log!(t0, "redirect");

    // C: sessions.
    let ncamp = ctx.campaign_ids.len();
    let mut c_states: Vec<CState> = (0..threads)
        .map(|_| CState {
            evs: Vec::new(),
            cks: Vec::new(),
            ev_tmp: Vec::new(),
            ck_tmp: Vec::new(),
            pmap: LocalMap::new(),
            rmap: LocalMap::new(),
            roots: Vec::new(),
            persons: Vec::new(),
            local: Vec::new(),
            ev_start: Vec::new(),
            ck_start: Vec::new(),
            cursor: Vec::new(),
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
    {
        let a = &a_states;
        let ctx = &ctx;
        let mut order: Vec<usize> = (0..p2).collect();
        let (ms, rd) = (&m_states, &rd);
        let size = |j: usize| {
            a.iter().map(|s| s.pe.len(j) + s.pc.parts[j].len()).sum::<usize>()
                + ms.iter().map(|m| m.mv.len(j) + m.mvc.parts[j].len()).sum::<usize>()
        };
        order.sort_unstable_by_key(|&j| std::cmp::Reverse(size(j)));
        run_jobs(&mut c_states, p2, |st, k| process_partition(st, a, ms, rd, order[k], ctx));
    }
    log!(t0, "sessions");

    write_outputs(&out_dir, &ctx, &c_states);
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
        let mut v = Vec::new();
        key_bytes(k, &mut v);
        return v.len();
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

fn write_outputs(out_dir: &Path, ctx: &Ctx, c_states: &[CState]) {
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
        for (t, s) in c_states.iter().enumerate() {
            let path = user_dir.join(format!("part-{:05}.parquet", t));
            sc.spawn(move || {
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
            });
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

