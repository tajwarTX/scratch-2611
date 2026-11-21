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
use parquet::data_type::ByteArray;

use crate::csv::{for_each_record, split};
use crate::fast;
use crate::keys::{self, any_interned, cmp_key, key_bytes};
use crate::legacy::{add_cat, chunks, env, list_csv, manifest_value, map_file, prefix, read_pairs, run_jobs, top_category, Job};
use crate::scatter::Scatter;
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
        eprintln!("[{:>7.3}s] {}", $t0.elapsed().as_secs_f64(), format!($($arg)*))
    };
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
        let found = match keys::pack(product) {
            Some(k) => self.product_fast.get(k).map(|v| v as u32),
            None => self.product_slow.get(product).copied(),
        };
        found.unwrap_or(self.unknown_cat)
    }

    #[inline]
    fn campaign(&self, c: &[u8]) -> Option<u32> {
        match keys::pack(c) {
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

#[inline(always)]
fn key(s: &[u8], simd: bool) -> u64 {
    if simd {
        if let Some(k) = fast::pack(s.as_ptr(), s.len()) {
            return k;
        }
    }
    keys::key_of(s)
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

fn parse_links(file: &[u8], start: usize, end: usize, links: &Links) {
    for_each_record::<2, 2>(file, start, end, |f| {
        if f[0].is_empty() || f[1].is_empty() {
            return;
        }
        let (ka, kb) = (keys::key_of(f[0]), keys::key_of(f[1]));
        if ka != kb {
            let (a, b) = (links.insert(ka), links.insert(kb));
            links.union(a, b);
        }
    });
}

// ---------------------------------------------------------------- parse

struct AState {
    tid: u32,
    pe: Scatter<PEv, 8>,
    dd: Scatter<Dd, 8>,
    pc: Scatter<PCk, 8>,
    bad: bool,
}

#[derive(Clone, Copy, Default)]
struct Pend {
    user: u64,
    id: u64,
    ingest: i32,
    ts: u32,
    pc: u32,
}

/// Fields of each line in [start, end): the SIMD tokenizer where the file
/// has lookahead room, the scalar splitter near its end. The bool says the
/// SIMD readers (16 bytes from a field start, 16 before a field end) are safe.
#[inline(always)]
fn for_each_line_fields<'a, const N: usize>(
    file: &'a [u8],
    start: usize,
    end: usize,
    mut each: impl FnMut(&[&'a [u8]; N], bool),
) {
    let mut p = start;
    let mut ranges = [(0u32, 0u32); N];
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let fast_limit = file.len().saturating_sub(fast::LOOKAHEAD);
    while p < end {
        if p <= fast_limit && p >= 16 {
            if let Some((next, ok)) = fast::tokenize::<N>(file, p, &mut ranges) {
                if ok {
                    for i in 0..N {
                        fields[i] = unsafe { file.get_unchecked(p + ranges[i].0 as usize..p + ranges[i].1 as usize) };
                    }
                    each(&fields, true);
                }
                p = next;
                continue;
            }
        }
        let nl = match memchr::memchr(b'\n', &file[p..end]) {
            Some(x) => p + x,
            None => end,
        };
        if split(&file[p..nl], &mut fields) {
            each(&fields, false);
        }
        p = nl + 1;
    }
}

#[inline(never)]
fn parse_events(file: &[u8], start: usize, end: usize, ctx: &Ctx, links: &Links, st: &mut AState) {
    let (ws, we, base) = (ctx.ws, ctx.we, ctx.base);
    let (p1, p2, ib) = (ctx.p1_bits, ctx.p2_bits, ctx.idx_bits);
    let loc_hi = st.tid << (p2 + ib);
    let idx_max = (1usize << ib) - 2;
    let mut ring = [Pend::default(); RING];
    let mut head = 0usize;
    let mut count = 0usize;
    let finish = |st: &mut AState, d: &Pend| {
        let person = links.person(d.user);
        let pp = part(person, p2);
        let idx = st.pe.push_idx(pp, PEv { person, ts: d.ts, pc: d.pc });
        if idx > idx_max {
            st.bad = true;
        }
        let loc = loc_hi | ((pp as u32) << ib) | (idx as u32 & ((1 << ib) - 1));
        st.dd.push(part(d.id, p1), Dd { id: d.id, ingest: d.ingest, loc });
    };
    for_each_line_fields::<7>(file, start, end, |f, simd| {
        if f[0].is_empty() {
            return;
        }
        let id = key(f[0], simd);
        let ingest_ms = ctx.ts(f[3], simd).unwrap_or(i64::MAX);
        let rel = ingest_ms.saturating_sub(ws);
        let ingest = (rel / 1000) as i32;
        if rel % 1000 != 0 || rel / 1000 != ingest as i64 {
            st.bad = true;
        }
        'valid: {
            if f[1].is_empty() || f[5].is_empty() {
                break 'valid;
            }
            let ts = match ctx.ts(f[2], simd) {
                Some(t) if t >= ws && t < we => t,
                _ => break 'valid,
            };
            let purchase = match f[4] {
                b"view" | b"cart" => false,
                b"purchase" => true,
                _ => break 'valid,
            };
            let mut pc = 0u32;
            if purchase {
                match parse_price(f[6]) {
                    Some(p) => pc = p,
                    None => break 'valid,
                }
            }
            if ingest_ms.saturating_sub(ts) > LATE {
                break 'valid;
            }
            if purchase {
                pc |= ctx.category(f[5]) << 24;
            }
            let user = key(f[1], simd);
            links.prefetch(user);
            if count == RING {
                let d = ring[head];
                finish(st, &d);
                count -= 1;
                head = (head + 1) % RING;
            }
            ring[(head + count) % RING] = Pend { user, id, ingest, ts: (ts - base) as u32, pc };
            count += 1;
            return;
        }
        st.dd.push(part(id, p1), Dd { id, ingest, loc: NONE });
    });
    while count > 0 {
        let d = ring[head];
        finish(st, &d);
        count -= 1;
        head = (head + 1) % RING;
    }
}

#[inline(never)]
fn parse_clicks(file: &[u8], start: usize, end: usize, ctx: &Ctx, links: &Links, st: &mut AState) {
    let (lo, hi, base, p2) = (ctx.ws - REACH, ctx.we, ctx.base, ctx.p2_bits);
    let mut ring = [PCk::default(); RING];
    let mut head = 0usize;
    let mut count = 0usize;
    for_each_line_fields::<4>(file, start, end, |f, simd| {
        if f[0].is_empty() || f[1].is_empty() {
            return;
        }
        let ts = match ctx.ts(f[3], simd) {
            Some(t) if t >= lo && t < hi => t,
            _ => return,
        };
        let Some(camp) = ctx.campaign(f[2]) else { return };
        let user = key(f[1], simd);
        links.prefetch(user);
        if count == RING {
            let mut c = ring[head];
            c.person = links.person(c.person);
            st.pc.push(part(c.person, p2), c);
            count -= 1;
            head = (head + 1) % RING;
        }
        ring[(head + count) % RING] = PCk { person: user, id: key(f[0], simd), ts: (ts - base) as u32, camp };
        count += 1;
    });
    while count > 0 {
        let mut c = ring[head];
        c.person = links.person(c.person);
        st.pc.push(part(c.person, p2), c);
        count -= 1;
        head = (head + 1) % RING;
    }
}

// ---------------------------------------------------------------- dedup

struct BState {
    counts: Vec<u8>,
    cand: Vec<Dd>,
}

/// Raw base pointers of every (thread, person partition) event buffer.
struct Bases(Vec<Vec<*mut PEv>>);
unsafe impl Sync for Bases {}

#[inline(never)]
fn dedup_partition(a: &[AState], j: usize, bases: &Bases, p2: u32, ib: u32, st: &mut BState) {
    let n: usize = a.iter().map(|s| s.dd.parts[j].len()).sum();
    let cap = (n * 8).max(64).next_power_of_two();
    let mask = cap - 1;
    st.counts.clear();
    st.counts.resize(cap, 0);
    for s in a {
        for r in &s.dd.parts[j] {
            let c = unsafe { st.counts.get_unchecked_mut(mix(r.id) as usize & mask) };
            *c = c.saturating_add(1);
        }
    }
    st.cand.clear();
    for s in a {
        for r in &s.dd.parts[j] {
            if unsafe { *st.counts.get_unchecked(mix(r.id) as usize & mask) } > 1 {
                st.cand.push(*r);
            }
        }
    }
    st.cand.sort_unstable_by(|x, y| x.id.cmp(&y.id).then(x.ingest.cmp(&y.ingest)));
    let c = &st.cand;
    let mut i = 0;
    while i < c.len() {
        let mut k = i + 1;
        while k < c.len() && c[k].id == c[i].id {
            let loc = c[k].loc;
            if loc != NONE {
                let t = (loc >> (p2 + ib)) as usize;
                let pp = ((loc >> ib) & ((1 << p2) - 1)) as usize;
                let idx = (loc & ((1 << ib) - 1)) as usize;
                unsafe { (*bases.0[t][pp].add(idx)).ts = TOMB };
            }
            k += 1;
        }
        i = k;
    }
}

// ---------------------------------------------------------------- sessions

struct CState {
    evs: Vec<PEv>,
    cks: Vec<PCk>,
    pmap: LocalMap,
    persons: Vec<u64>,
    local: Vec<u32>,
    ev_start: Vec<u32>,
    ck_start: Vec<u32>,
    cursor: Vec<u32>,
    rows: Vec<UserRow>,
    acc: Vec<[u64; 4]>,
    stamp: Vec<u32>,
    serial: u32,
    cats: Vec<(u32, u64)>,
}

struct Walk<'a> {
    rows: &'a mut Vec<UserRow>,
    acc: &'a mut Vec<[u64; 4]>,
    stamp: &'a mut Vec<u32>,
    serial: &'a mut u32,
    cats: &'a mut Vec<(u32, u64)>,
}

/// One person's events (sorted by ts) and clicks (sorted by ts, then
/// click_id descending): sessions, credit, assists, daily rows.
fn process_person(person: u64, evs: &[PEv], cks: &[PCk], ctx: &Ctx, w: &mut Walk) {
    let ndays = ctx.ndays;
    let organic = ctx.campaign_ids.len();
    let reach = REACH as u32;
    let (mut lo, mut hi) = (0usize, 0usize);
    let n = evs.len();
    let mut k = 0usize;
    let mut cur_day = i64::MIN;
    let (mut d_sessions, mut d_events, mut d_revenue) = (0u32, 0u32, 0u64);
    w.cats.clear();
    while k < n {
        let start = evs[k].ts;
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
            d_sessions = 0;
            d_events = 0;
            d_revenue = 0;
            w.cats.clear();
        }
        let mut m = k;
        let mut last = start;
        let mut s_revenue = 0u64;
        let mut s_purchase = false;
        while m < n && evs[m].ts - last <= GAP {
            let e = &evs[m];
            last = e.ts;
            if e.pc != 0 {
                let price = e.pc & 0x00FF_FFFF;
                s_revenue += price as u64;
                s_purchase = true;
                add_cat(w.cats, e.pc >> 24, price);
            }
            m += 1;
        }
        d_sessions += 1;
        d_events += (m - k) as u32;
        d_revenue += s_revenue;

        while hi < cks.len() && cks[hi].ts <= start {
            hi += 1;
        }
        while lo < hi && cks[lo].ts < start - reach {
            lo += 1;
        }
        let di = (day - ctx.day_lo) as usize;
        let camp = if lo < hi { cks[hi - 1].camp as usize } else { organic };
        let a = &mut w.acc[camp * ndays + di];
        a[0] += 1;
        a[1] += s_purchase as u64;
        a[2] += s_revenue;
        if lo < hi {
            *w.serial = w.serial.wrapping_add(1);
            if *w.serial == 0 {
                w.stamp.iter_mut().for_each(|s| *s = 0);
                *w.serial = 1;
            }
            let serial = *w.serial;
            for c in &cks[lo..hi] {
                let ci = c.camp as usize;
                if w.stamp[ci] != serial {
                    w.stamp[ci] = serial;
                    w.acc[ci * ndays + di][3] += 1;
                }
            }
        }
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
}

#[inline(never)]
fn process_partition(st: &mut CState, srcs: &[AState], j: usize, ctx: &Ctx) {
    let interned = any_interned();
    let CState { evs, cks, pmap, persons, local, ev_start, ck_start, cursor, rows, acc, stamp, serial, cats } = st;
    let n: usize = srcs.iter().map(|s| s.pe.parts[j].len()).sum();
    pmap.reset(n / 8 + 16);
    persons.clear();
    local.clear();
    cursor.clear();
    for s in srcs {
        for e in &s.pe.parts[j] {
            if e.ts == TOMB {
                continue;
            }
            let (idx, inserted) = pmap.get_or_insert(e.person, persons.len() as u32);
            if inserted {
                persons.push(e.person);
                cursor.push(0);
            }
            cursor[idx as usize] += 1;
            local.push(idx);
        }
    }
    let np = persons.len();
    prefix(cursor, ev_start);
    cursor.copy_from_slice(&ev_start[..np]);
    evs.clear();
    evs.resize(ev_start[np] as usize, PEv::default());
    let mut k = 0usize;
    for s in srcs {
        for e in &s.pe.parts[j] {
            if e.ts == TOMB {
                continue;
            }
            let idx = local[k] as usize;
            k += 1;
            let pos = cursor[idx] as usize;
            cursor[idx] += 1;
            evs[pos] = *e;
        }
    }
    local.clear();
    cursor.iter_mut().for_each(|c| *c = 0);
    for s in srcs {
        for c in &s.pc.parts[j] {
            let idx = pmap.get(c.person).unwrap_or(NONE);
            if idx != NONE {
                cursor[idx as usize] += 1;
            }
            local.push(idx);
        }
    }
    prefix(cursor, ck_start);
    cursor.copy_from_slice(&ck_start[..np]);
    cks.clear();
    cks.resize(ck_start[np] as usize, PCk::default());
    let mut k = 0usize;
    for s in srcs {
        for c in &s.pc.parts[j] {
            let idx = local[k];
            k += 1;
            if idx != NONE {
                let pos = cursor[idx as usize] as usize;
                cursor[idx as usize] += 1;
                cks[pos] = *c;
            }
        }
    }
    let mut w = Walk { rows, acc, stamp, serial, cats };
    for p in 0..np {
        let pe = &mut evs[ev_start[p] as usize..ev_start[p + 1] as usize];
        pe.sort_unstable_by_key(|e| e.ts);
        let pc = &mut cks[ck_start[p] as usize..ck_start[p + 1] as usize];
        if interned {
            pc.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then(cmp_key(b.id, a.id)));
        } else {
            pc.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then(b.id.cmp(&a.id)));
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
    if we <= ws || we - base >= u32::MAX as i64 - 1 {
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
    let p2_bits = bits_for(est_events, 24 << 10, 10.min(29 - tbits));
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
        p1_bits: bits_for(est_events, 24 << 10, 12),
        p2_bits,
        idx_bits: 32 - tbits - p2_bits,
        fast_ts: FastTs::new(ws - REACH - 10 * DAY, we + 10 * DAY),
    };
    let (p1, p2) = (1usize << ctx.p1_bits, 1usize << ctx.p2_bits);
    log!(t0, "setup: {} threads, {} MB events, p1={} p2={}", threads, event_bytes >> 20, p1, p2);

    // L: links.
    let links = Links::new(link_bytes / 20 * 2 + 64);
    {
        let mut jobs = Vec::new();
        chunks(&link_maps, Job::Events, &mut jobs);
        let mut st: Vec<()> = vec![(); threads];
        run_jobs(&mut st, jobs.len(), |_, j| {
            if let Job::Events(fi, s, e) = jobs[j] {
                parse_links(link_maps[fi].as_ref().unwrap(), s, e, &links);
            }
        });
        let slots = links.slots.len();
        let per = slots.div_ceil(threads * 4);
        run_jobs(&mut st, slots.div_ceil(per), |_, j| {
            for i in j * per..((j + 1) * per).min(slots) {
                let k = links.slots[i].key.load(Ordering::Relaxed);
                if k != 0 {
                    let root = links.find(i as u32);
                    links.slots[i].val.store(links.key_at(root), Ordering::Relaxed);
                }
            }
        });
    }
    log!(t0, "links");

    // A: parse.
    let mut jobs = Vec::new();
    chunks(&event_maps, Job::Events, &mut jobs);
    chunks(&click_maps, Job::Clicks, &mut jobs);
    let per_pe = est_events / (threads * p2) * 5 / 4 + 64;
    let per_dd = est_events / (threads * p1) * 5 / 4 + 64;
    let mut a_states: Vec<AState> = (0..threads)
        .map(|t| AState {
            tid: t as u32,
            pe: Scatter::new(p2, per_pe),
            dd: Scatter::new(p1, per_dd),
            pc: Scatter::new(p2, per_pe / 4),
            bad: false,
        })
        .collect();
    run_jobs(&mut a_states, jobs.len(), |st, j| match jobs[j] {
        Job::Events(fi, s, e) => parse_events(event_maps[fi].as_ref().unwrap(), s, e, &ctx, &links, st),
        Job::Clicks(fi, s, e) => parse_clicks(click_maps[fi].as_ref().unwrap(), s, e, &ctx, &links, st),
        Job::Links => {}
    });
    a_states.iter_mut().for_each(|s| {
        s.pe.flush();
        s.dd.flush();
        s.pc.flush();
    });
    if a_states.iter().any(|s| s.bad) {
        eprintln!("an assumption of the fast pipeline does not hold");
        return false;
    }
    log!(t0, "parse");

    // B: dedup.
    let bases = Bases(a_states.iter_mut().map(|s| s.pe.parts.iter_mut().map(|v| v.as_mut_ptr()).collect()).collect());
    {
        let a = &a_states;
        let bases = &bases;
        let (p2b, ib) = (ctx.p2_bits, ctx.idx_bits);
        let mut b_states: Vec<BState> = (0..threads).map(|_| BState { counts: Vec::new(), cand: Vec::new() }).collect();
        run_jobs(&mut b_states, p1, |st, j| dedup_partition(a, j, bases, p2b, ib, st));
    }
    log!(t0, "dedup");

    // C: sessions.
    let ncamp = ctx.campaign_ids.len();
    let mut c_states: Vec<CState> = (0..threads)
        .map(|_| CState {
            evs: Vec::new(),
            cks: Vec::new(),
            pmap: LocalMap::new(),
            persons: Vec::new(),
            local: Vec::new(),
            ev_start: Vec::new(),
            ck_start: Vec::new(),
            cursor: Vec::new(),
            rows: Vec::new(),
            acc: vec![[0u64; 4]; (ncamp + 1) * ndays],
            stamp: vec![0u32; ncamp + 1],
            serial: 0,
            cats: Vec::new(),
        })
        .collect();
    {
        let a = &a_states;
        let ctx = &ctx;
        run_jobs(&mut c_states, p2, |st, j| process_partition(st, a, j, ctx));
    }
    log!(t0, "sessions");

    write_outputs(&out_dir, &ctx, &c_states);
    log!(t0, "done");
    true
}

fn write_outputs(out_dir: &Path, ctx: &Ctx, c_states: &[CState]) {
    let user_dir = out_dir.join("user_daily");
    let camp_dir = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&user_dir).expect("user_daily dir");
    std::fs::create_dir_all(&camp_dir).expect("campaign_daily dir");
    let (ncamp, ndays) = (ctx.campaign_ids.len(), ctx.ndays);
    let mut acc = vec![[0u64; 4]; (ncamp + 1) * ndays];
    for s in c_states {
        for (t, x) in acc.iter_mut().zip(&s.acc) {
            for i in 0..4 {
                t[i] += x[i];
            }
        }
    }
    std::thread::scope(|sc| {
        for (t, s) in c_states.iter().enumerate() {
            let path = user_dir.join(format!("part-{:05}.parquet", t));
            sc.spawn(move || {
                let rows = &s.rows;
                let mut buf = Vec::with_capacity(16);
                let user_id: Vec<ByteArray> = rows
                    .iter()
                    .map(|r| {
                        buf.clear();
                        key_bytes(r.person, &mut buf);
                        ByteArray::from(buf.clone())
                    })
                    .collect();
                let cat_names: Vec<ByteArray> = ctx.categories.iter().map(|c| ByteArray::from(c.clone())).collect();
                let cols = crate::out::UserOut {
                    user_id: &user_id,
                    day: &rows.iter().map(|r| r.day).collect::<Vec<_>>(),
                    sessions: &rows.iter().map(|r| r.sessions as i64).collect::<Vec<_>>(),
                    events: &rows.iter().map(|r| r.events as i64).collect::<Vec<_>>(),
                    revenue: &rows.iter().map(|r| r.revenue as i64).collect::<Vec<_>>(),
                    top_def: &rows.iter().map(|r| (r.top != u32::MAX) as i16).collect::<Vec<_>>(),
                    top: &rows
                        .iter()
                        .filter(|r| r.top != u32::MAX)
                        .map(|r| cat_names[r.top as usize].clone())
                        .collect::<Vec<_>>(),
                };
                crate::out::write_user_daily(&path, &cols).expect("write user_daily");
            });
        }
        let acc = &acc;
        sc.spawn(move || {
            let mut c = crate::out::CampaignOut {
                campaign_id: Vec::new(),
                channel: Vec::new(),
                day: Vec::new(),
                sessions: Vec::new(),
                purchasing: Vec::new(),
                revenue: Vec::new(),
                assisted: Vec::new(),
            };
            for camp in 0..=ncamp {
                for d in 0..ndays {
                    let a = acc[camp * ndays + d];
                    if a[0] == 0 && a[3] == 0 {
                        continue;
                    }
                    let (id, channel): (&[u8], &[u8]) = if camp == ncamp {
                        (b"organic", b"organic")
                    } else {
                        (&ctx.campaign_ids[camp], &ctx.channels[camp])
                    };
                    c.campaign_id.push(ByteArray::from(id.to_vec()));
                    c.channel.push(ByteArray::from(channel.to_vec()));
                    c.day.push((ctx.day_lo + d as i64) as i32);
                    c.sessions.push(a[0] as i64);
                    c.purchasing.push(a[1] as i64);
                    c.revenue.push(a[2] as i64);
                    c.assisted.push(if camp == ncamp { 0 } else { a[3] as i64 });
                }
            }
            crate::out::write_campaign_daily(&camp_dir.join("part-00000.parquet"), &c).expect("write campaign_daily");
        });
    });
}
