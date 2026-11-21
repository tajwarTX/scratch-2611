//! The general pipeline (no compact-record assumptions). It is the fallback
//! when a dataset breaks an assumption of the fast pipeline in v2.rs.
//!
//! Original design notes:
//!
//! Phase A (parallel): parse every CSV line. Events are scattered into
//!   partitions by a hash of event_id; clicks are validated and kept per
//!   thread. One job meanwhile reads the links and builds the ID -> person map.
//! Phase B (parallel): per event_id partition, keep the copy with the smallest
//!   ingest_ts; surviving events and valid clicks go to partitions by person.
//! Phase C (parallel): per person partition, sort, sessionize, credit, assist,
//!   and aggregate the daily rows.
//! Then both tables are written as Parquet.


use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use memmap2::Mmap;

use crate::scatter::Scatter;
use parquet::data_type::ByteArray;

use crate::csv::{after_header, for_each_line, for_each_record, split};
use crate::keys::{any_interned, cmp_key, key_bytes, key_of, pack};
use crate::table::{mix, IndexTable, LocalMap, U64Map};
use crate::ts::{parse_price, parse_ts, FastTs};

const GAP: i64 = 1_800_000;
const REACH: i64 = 604_800_000;
const DAY: i64 = 86_400_000;
const LATE: i64 = 86_400_000;
const INVALID: i64 = i64::MIN;
const CHUNK: usize = 4 << 20;

macro_rules! log {
    ($t0:expr, $($arg:tt)*) => {
        eprintln!("[{:>7.3}s] {}", $t0.elapsed().as_secs_f64(), format!($($arg)*))
    };
}

/// One parsed event line: the dedup key and ingest_ts, plus the validated
/// fields (`ts == INVALID` when this copy fails validation).
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct RawEvent {
    id: u64,
    ingest: i64,
    user: u64,
    ts: i64,
    price: u32,
    cat: u32,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct RawClick {
    user: u64,
    ts: i64,
    id: u64,
    camp: u32,
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct PEvent {
    person: u64,
    ts: i64,
    price: u32,
    cat: u32,
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct PClick {
    person: u64,
    ts: i64,
    id: u64,
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
    fast_ts: FastTs,
}

impl Ctx {
    #[inline]
    fn category(&self, product: &[u8]) -> u32 {
        let found = match pack(product) {
            Some(k) => self.product_fast.get(k).map(|v| v as u32),
            None => self.product_slow.get(product).copied(),
        };
        found.unwrap_or(self.unknown_cat)
    }

    #[inline]
    fn campaign(&self, c: &[u8]) -> Option<u32> {
        match pack(c) {
            Some(k) => self.camp_fast.get(k).map(|v| v as u32),
            None => self.camp_slow.get(c).copied(),
        }
    }
}

#[inline(always)]
fn part(k: u64, bits: u32) -> usize {
    (mix(k) >> (64 - bits)) as usize
}

pub(crate) fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

pub(crate) fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                name.ends_with(".csv") && !name.starts_with('.') && p.is_file()
            })
            .collect(),
        Err(_) => Vec::new(),
    };
    files.sort();
    files
}

pub(crate) fn map_file(p: &Path) -> Option<Mmap> {
    let f = File::open(p).ok()?;
    if f.metadata().ok()?.len() == 0 {
        return None;
    }
    let m = unsafe { Mmap::map(&f).ok()? };
    #[cfg(unix)]
    {
        let _ = m.advise(memmap2::Advice::WillNeed);
    }
    Some(m)
}

pub(crate) fn manifest_value(text: &str, key: &str) -> Option<String> {
    let at = text.find(&format!("\"{}\"", key))? + key.len() + 2;
    let rest = &text[at..];
    let rest = &rest[rest.find(':')? + 1..];
    let rest = &rest[rest.find('"')? + 1..];
    Some(rest[..rest.find('"')?].to_string())
}

/// Reads a small two-column lookup file into (first, second) pairs.
pub(crate) fn read_pairs(path: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let data = std::fs::read(path).unwrap_or_default();
    let mut pairs = Vec::new();
    let mut f = [&b""[..]; 2];
    for_each_line(&data[after_header(&data)..], |line| {
        if split(line, &mut f) && !f[0].is_empty() {
            pairs.push((f[0].to_vec(), f[1].to_vec()));
        }
    });
    pairs
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        let up = parent[parent[x as usize] as usize];
        parent[x as usize] = up;
        x = up;
    }
    x
}

/// The links: union-find over every ID in a valid line, each set named by
/// its byte-wise smallest ID. Returns ID key -> person key.
#[inline(never)]
fn build_links(maps: &[Mmap]) -> U64Map {
    let total: usize = maps.iter().map(|m| m.len()).sum();
    let mut index = U64Map::with_capacity(total / 12 + 16);
    let mut keys: Vec<u64> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    let mut f = [&b""[..]; 2];
    for m in maps {
        let data = &m[..];
        for_each_line(&data[after_header(data)..], |line| {
            if !split(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
                return;
            }
            let (ka, kb) = (key_of(f[0]), key_of(f[1]));
            if ka == kb {
                return;
            }
            let mut id = |k: u64| -> u32 {
                index.get_or_insert(k, || {
                    keys.push(k);
                    parent.push(parent.len() as u32);
                    (keys.len() - 1) as u64
                }) as u32
            };
            let (ia, ib) = (id(ka), id(kb));
            let (ra, rb) = (find(&mut parent, ia), find(&mut parent, ib));
            if ra != rb {
                if ra < rb {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
                }
            }
        });
    }
    let n = keys.len();
    let mut root = vec![0u32; n];
    for i in 0..n {
        root[i] = find(&mut parent, i as u32);
    }
    let mut best = keys.clone();
    for i in 0..n {
        let r = root[i] as usize;
        if cmp_key(keys[i], best[r]) == std::cmp::Ordering::Less {
            best[r] = keys[i];
        }
    }
    index.map_values(|_, v| best[root[v as usize] as usize]);
    index
}

/// Runs jobs 0..njobs on one thread per state, each pulling the next job.
pub(crate) fn run_jobs<S: Send>(states: &mut [S], njobs: usize, f: impl Fn(&mut S, usize) + Sync) {
    let next = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        for st in states.iter_mut() {
            let (next, f) = (&next, &f);
            sc.spawn(move || loop {
                let j = next.fetch_add(1, Ordering::Relaxed);
                if j >= njobs {
                    break;
                }
                f(st, j);
            });
        }
    });
}

#[derive(Clone, Copy)]
pub(crate) enum Job {
    Links,
    Events(usize, usize, usize),
    Clicks(usize, usize, usize),
}

pub(crate) fn chunks(maps: &[Option<Mmap>], make: impl Fn(usize, usize, usize) -> Job, jobs: &mut Vec<Job>) {
    for (fi, m) in maps.iter().enumerate() {
        let Some(m) = m else { continue };
        let len = m.len();
        let mut start = after_header(m);
        while start < len {
            let mut end = (start + CHUNK).min(len);
            if end < len {
                end = match memchr::memchr(b'\n', &m[end..]) {
                    Some(p) => end + p + 1,
                    None => len,
                };
            }
            jobs.push(make(fi, start, end));
            start = end;
        }
    }
}

struct AState {
    ev: Scatter<RawEvent, 8>,
    ck: Vec<RawClick>,
    links: Option<U64Map>,
}

struct BState {
    pe: Scatter<PEvent, 8>,
    pc: Scatter<PClick, 8>,
    table: IndexTable,
    winners: Vec<RawEvent>,
}

struct CState {
    evs: Vec<PEvent>,
    cks: Vec<PClick>,
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

#[inline(never)]
fn parse_events(file: &[u8], start: usize, end: usize, ctx: &Ctx, st: &mut AState) {
    let (ws, we, bits) = (ctx.ws, ctx.we, ctx.p1_bits);
    let fast = &ctx.fast_ts;
    for_each_record::<7, 4>(file, start, end, |f| {
        if f[0].is_empty() {
            return;
        }
        let id = key_of(f[0]);
        let ingest = fast.parse(f[3]).unwrap_or(i64::MAX);
        let mut rec = RawEvent { id, ingest, user: 0, ts: INVALID, price: 0, cat: 0 };
        'valid: {
            if f[1].is_empty() || f[5].is_empty() {
                break 'valid;
            }
            let ts = match fast.parse(f[2]) {
                Some(t) if t >= ws && t < we => t,
                _ => break 'valid,
            };
            let purchase = match f[4] {
                b"view" | b"cart" => false,
                b"purchase" => true,
                _ => break 'valid,
            };
            if purchase {
                match parse_price(f[6]) {
                    Some(p) => rec.price = p,
                    None => break 'valid,
                }
            }
            if ingest.saturating_sub(ts) > LATE {
                break 'valid;
            }
            if purchase {
                rec.cat = ctx.category(f[5]);
            }
            rec.user = key_of(f[1]);
            rec.ts = ts;
        }
        st.ev.push(part(id, bits), rec);
    });
}

#[inline(never)]
fn parse_clicks(file: &[u8], start: usize, end: usize, ctx: &Ctx, st: &mut AState) {
    let (lo, hi) = (ctx.ws - REACH, ctx.we);
    let fast = &ctx.fast_ts;
    for_each_record::<4, 2>(file, start, end, |f| {
        if f[0].is_empty() || f[1].is_empty() {
            return;
        }
        let ts = match fast.parse(f[3]) {
            Some(t) if t >= lo && t < hi => t,
            _ => return,
        };
        let Some(camp) = ctx.campaign(f[2]) else { return };
        st.ck.push(RawClick { user: key_of(f[1]), ts, id: key_of(f[0]), camp });
    });
}

#[inline]
pub(crate) fn add_cat(cats: &mut Vec<(u32, u64)>, cat: u32, price: u32) {
    for c in cats.iter_mut() {
        if c.0 == cat {
            c.1 += price as u64;
            return;
        }
    }
    cats.push((cat, price as u64));
}

pub(crate) fn top_category(cats: &[(u32, u64)]) -> u32 {
    let mut best = u32::MAX;
    let mut best_sum = 0u64;
    for &(c, s) in cats {
        if best == u32::MAX || s > best_sum || (s == best_sum && c < best) {
            best = c;
            best_sum = s;
        }
    }
    best
}

/// One person's events (sorted by ts) and clicks (sorted by ts, then
/// click_id descending): sessions, credit, assists, and daily rows.
fn process_person(person: u64, evs: &[PEvent], cks: &[PClick], ctx: &Ctx, st: &mut CStateParts) {
    let ndays = ctx.ndays;
    let organic = ctx.campaign_ids.len();
    let (mut lo, mut hi) = (0usize, 0usize);
    let mut k = 0usize;
    let n = evs.len();
    let mut cur_day = i64::MIN;
    let (mut d_sessions, mut d_events, mut d_revenue) = (0u32, 0u32, 0u64);
    st.cats.clear();
    while k < n {
        let start = evs[k].ts;
        let day = start.div_euclid(DAY);
        if day != cur_day {
            if cur_day != i64::MIN {
                st.rows.push(UserRow {
                    person,
                    day: cur_day as i32,
                    sessions: d_sessions,
                    events: d_events,
                    revenue: d_revenue,
                    top: top_category(st.cats),
                });
            }
            cur_day = day;
            d_sessions = 0;
            d_events = 0;
            d_revenue = 0;
            st.cats.clear();
        }
        let mut m = k;
        let mut last = start;
        let mut s_revenue = 0u64;
        let mut s_purchase = false;
        while m < n && evs[m].ts - last <= GAP {
            let e = &evs[m];
            last = e.ts;
            if e.price > 0 {
                s_revenue += e.price as u64;
                s_purchase = true;
                add_cat(st.cats, e.cat, e.price);
            }
            m += 1;
        }
        d_sessions += 1;
        d_events += (m - k) as u32;
        d_revenue += s_revenue;

        while hi < cks.len() && cks[hi].ts <= start {
            hi += 1;
        }
        while lo < hi && cks[lo].ts < start - REACH {
            lo += 1;
        }
        let di = (day - ctx.day_lo) as usize;
        let camp = if lo < hi { cks[hi - 1].camp as usize } else { organic };
        let a = &mut st.acc[camp * ndays + di];
        a[0] += 1;
        a[1] += s_purchase as u64;
        a[2] += s_revenue;
        if lo < hi {
            *st.serial = st.serial.wrapping_add(1);
            if *st.serial == 0 {
                st.stamp.iter_mut().for_each(|s| *s = 0);
                *st.serial = 1;
            }
            let serial = *st.serial;
            for c in &cks[lo..hi] {
                let ci = c.camp as usize;
                if st.stamp[ci] != serial {
                    st.stamp[ci] = serial;
                    st.acc[ci * ndays + di][3] += 1;
                }
            }
        }
        k = m;
    }
    if cur_day != i64::MIN {
        st.rows.push(UserRow {
            person,
            day: cur_day as i32,
            sessions: d_sessions,
            events: d_events,
            revenue: d_revenue,
            top: top_category(st.cats),
        });
    }
}

struct CStateParts<'a> {
    rows: &'a mut Vec<UserRow>,
    acc: &'a mut Vec<[u64; 4]>,
    stamp: &'a mut Vec<u32>,
    serial: &'a mut u32,
    cats: &'a mut Vec<(u32, u64)>,
}

/// Exclusive prefix sums of `counts` into `starts` (len + 1 entries).
pub(crate) fn prefix(counts: &[u32], starts: &mut Vec<u32>) {
    starts.clear();
    let mut total = 0u32;
    for &c in counts {
        starts.push(total);
        total += c;
    }
    starts.push(total);
}

/// One person partition: group events and clicks by person with a counting
/// sort on a local person index, sort each person's few rows by time, walk.
#[inline(never)]
fn process_partition(st: &mut CState, srcs: &[BState], j: usize, ctx: &Ctx) {
    let interned = any_interned();
    let CState { evs, cks, pmap, persons, local, ev_start, ck_start, cursor, rows, acc, stamp, serial, cats } = st;
    let n: usize = srcs.iter().map(|s| s.pe.parts[j].len()).sum();
    pmap.reset(n / 8 + 16);
    persons.clear();
    local.clear();
    cursor.clear();
    for s in srcs {
        for e in &s.pe.parts[j] {
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
    evs.resize(n, PEvent::default());
    let mut k = 0usize;
    for s in srcs {
        for e in &s.pe.parts[j] {
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
            let idx = pmap.get(c.person).unwrap_or(u32::MAX);
            if idx != u32::MAX {
                cursor[idx as usize] += 1;
            }
            local.push(idx);
        }
    }
    prefix(cursor, ck_start);
    cursor.copy_from_slice(&ck_start[..np]);
    cks.clear();
    cks.resize(ck_start[np] as usize, PClick::default());
    let mut k = 0usize;
    for s in srcs {
        for c in &s.pc.parts[j] {
            let idx = local[k];
            k += 1;
            if idx != u32::MAX {
                let pos = cursor[idx as usize] as usize;
                cursor[idx as usize] += 1;
                cks[pos] = *c;
            }
        }
    }
    let mut parts = CStateParts { rows, acc, stamp, serial, cats };
    for p in 0..np {
        let pe = &mut evs[ev_start[p] as usize..ev_start[p + 1] as usize];
        pe.sort_unstable_by_key(|e| e.ts);
        let pc = &mut cks[ck_start[p] as usize..ck_start[p + 1] as usize];
        if interned {
            pc.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then(cmp_key(b.id, a.id)));
        } else {
            pc.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then(b.id.cmp(&a.id)));
        }
        process_person(persons[p], pe, pc, ctx, &mut parts);
    }
}

#[inline(never)]
fn dedup_partition(a: &[AState], j: usize, links: &U64Map, bits2: u32, st: &mut BState) {
    let n: usize = a.iter().map(|s| s.ev.parts[j].len()).sum();
    st.table.reset(n);
    st.winners.clear();
    for s in a {
        for r in &s.ev.parts[j] {
            match st.table.find_or_insert(r.id, mix(r.id), st.winners.len() as u32) {
                None => st.winners.push(*r),
                Some(w) => {
                    let w = &mut st.winners[w as usize];
                    if r.ingest < w.ingest {
                        *w = *r;
                    }
                }
            }
        }
    }
    const AHEAD: usize = 16;
    let winners = &st.winners;
    for (i, w) in winners.iter().enumerate() {
        if let Some(next) = winners.get(i + AHEAD) {
            links.prefetch(next.user);
        }
        if w.ts != INVALID {
            let person = links.get(w.user).unwrap_or(w.user);
            st.pe.push(part(person, bits2), PEvent { person, ts: w.ts, price: w.price, cat: w.cat });
        }
    }
}

#[inline(never)]
fn route_clicks(cks: &[RawClick], links: &U64Map, bits2: u32, st: &mut BState) {
    for (i, c) in cks.iter().enumerate() {
        if let Some(next) = cks.get(i + 16) {
            links.prefetch(next.user);
        }
        let person = links.get(c.user).unwrap_or(c.user);
        st.pc.push(part(person, bits2), PClick { person, ts: c.ts, id: c.id, camp: c.camp });
    }
}

pub(crate) fn bits_for(items: usize, per_part: usize) -> u32 {
    let parts = (items / per_part).clamp(2, 1 << 14).next_power_of_two();
    parts.trailing_zeros()
}

pub fn run() {
    let t0 = Instant::now();
    eprintln!("using the general pipeline");
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
    let day_lo = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day_lo + 1).max(1) as usize;

    // Lookups. Category indexes follow byte order, so ties go to the smaller index.
    let products = read_pairs(&data.join("products.csv"));
    let mut categories: Vec<Vec<u8>> = products.iter().map(|p| p.1.clone()).collect();
    categories.push(b"unknown".to_vec());
    categories.sort();
    categories.dedup();
    let cat_index: HashMap<Vec<u8>, u32> =
        categories.iter().enumerate().map(|(i, c)| (c.clone(), i as u32)).collect();
    let unknown_cat = cat_index[&b"unknown"[..]];
    let mut product_fast = U64Map::with_capacity(products.len());
    let mut product_slow = HashMap::new();
    for (id, cat) in &products {
        let c = cat_index[cat];
        match pack(id) {
            Some(k) => product_fast.insert(k, c as u64),
            None => {
                product_slow.insert(id.clone(), c);
            }
        }
    }
    let campaigns = read_pairs(&data.join("campaigns.csv"));
    let mut camp_fast = U64Map::with_capacity(campaigns.len());
    let mut camp_slow = HashMap::new();
    let mut campaign_ids = Vec::new();
    let mut channels = Vec::new();
    for (id, channel) in campaigns {
        let i = campaign_ids.len() as u32;
        let known = match pack(&id) {
            Some(k) => camp_fast.get(k).is_some(),
            None => camp_slow.contains_key(&id),
        };
        if known {
            continue;
        }
        match pack(&id) {
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
    let link_maps: Vec<Mmap> = list_csv(&data.join("links")).iter().filter_map(|p| map_file(p)).collect();
    let event_bytes: usize = event_maps.iter().flatten().map(|m| m.len()).sum();
    let click_bytes: usize = click_maps.iter().flatten().map(|m| m.len()).sum();

    let est_events = event_bytes / 80 + 1;
    let ctx = Ctx {
        ws,
        we,
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
        p1_bits: bits_for(est_events, env("P1_PER_PART", "16384").parse().unwrap()),
        p2_bits: bits_for(est_events, env("P2_PER_PART", "65536").parse().unwrap()),
        fast_ts: FastTs::new(ws - REACH - 10 * DAY, we + 10 * DAY),
    };
    let (p1, p2) = (1usize << ctx.p1_bits, 1usize << ctx.p2_bits);
    log!(t0, "setup: {} threads, {} MB events, {} MB clicks, p1={} p2={}", threads, event_bytes >> 20, click_bytes >> 20, p1, p2);

    // Phase A: parse.
    let mut jobs = vec![Job::Links];
    chunks(&click_maps, Job::Clicks, &mut jobs);
    chunks(&event_maps, Job::Events, &mut jobs);
    let per_part = est_events / (threads * p1) * 5 / 4 + 16;
    let mut a_states: Vec<AState> = (0..threads)
        .map(|_| AState {
            ev: Scatter::new(p1, per_part),
            ck: Vec::with_capacity(click_bytes / 45 / threads + 16),
            links: None,
        })
        .collect();
    run_jobs(&mut a_states, jobs.len(), |st, j| match jobs[j] {
        Job::Links => st.links = Some(build_links(&link_maps)),
        Job::Events(fi, s, e) => parse_events(event_maps[fi].as_ref().unwrap(), s, e, &ctx, st),
        Job::Clicks(fi, s, e) => parse_clicks(click_maps[fi].as_ref().unwrap(), s, e, &ctx, st),
    });
    let links = a_states.iter_mut().find_map(|s| s.links.take()).expect("links");
    a_states.iter_mut().for_each(|s| s.ev.flush());
    let raw_events: usize = a_states.iter().map(|s| s.ev.parts.iter().map(|v| v.len()).sum::<usize>()).sum();
    let raw_clicks: usize = a_states.iter().map(|s| s.ck.len()).sum();
    log!(t0, "phase A: {} event lines, {} valid clicks, {} linked ids", raw_events, raw_clicks, links.len());

    // Phase B: dedup events, resolve persons, repartition.
    let per_part2 = est_events / (threads * p2) * 5 / 4 + 16;
    let mut b_states: Vec<BState> = (0..threads)
        .map(|_| BState {
            pe: Scatter::new(p2, per_part2),
            pc: Scatter::new(p2, 0),
            table: IndexTable::new(),
            winners: Vec::new(),
        })
        .collect();
    {
        let a = &a_states;
        let links = &links;
        let bits2 = ctx.p2_bits;
        run_jobs(&mut b_states, p1 + threads, |st, j| {
            if j < p1 {
                dedup_partition(a, j, links, bits2, st);
            } else {
                route_clicks(&a[j - p1].ck, links, bits2, st);
            }
        });
    }
    drop(a_states);
    b_states.iter_mut().for_each(|s| {
        s.pe.flush();
        s.pc.flush();
    });
    let kept: usize = b_states.iter().map(|s| s.pe.parts.iter().map(|v| v.len()).sum::<usize>()).sum();
    log!(t0, "phase B: {} valid events", kept);

    // Phase C: sessions per person partition.
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
        let b = &b_states;
        let ctx = &ctx;
        run_jobs(&mut c_states, p2, |st, j| process_partition(st, b, j, ctx));
    }
    drop(b_states);
    let user_rows: usize = c_states.iter().map(|s| s.rows.len()).sum();
    log!(t0, "phase C: {} user_daily rows", user_rows);

    // Output.
    let user_dir = out_dir.join("user_daily");
    let camp_dir = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&user_dir).expect("user_daily dir");
    std::fs::create_dir_all(&camp_dir).expect("campaign_daily dir");

    let mut acc = vec![[0u64; 4]; (ncamp + 1) * ndays];
    for s in &c_states {
        for (t, x) in acc.iter_mut().zip(&s.acc) {
            for i in 0..4 {
                t[i] += x[i];
            }
        }
    }
    let ctx_ref = &ctx;
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
                let cat_names: Vec<ByteArray> =
                    ctx_ref.categories.iter().map(|c| ByteArray::from(c.clone())).collect();
                let cols = crate::out::UserOut {
                    user_id: &user_id,
                    day: &rows.iter().map(|r| r.day).collect::<Vec<_>>(),
                    sessions: &rows.iter().map(|r| r.sessions as i64).collect::<Vec<_>>(),
                    events: &rows.iter().map(|r| r.events as i64).collect::<Vec<_>>(),
                    revenue: &rows.iter().map(|r| r.revenue as i64).collect::<Vec<_>>(),
                    top_def: &rows.iter().map(|r| (r.top != u32::MAX) as i16).collect::<Vec<_>>(),
                    top: &rows.iter().filter(|r| r.top != u32::MAX).map(|r| cat_names[r.top as usize].clone()).collect::<Vec<_>>(),
                };
                crate::out::write_user_daily(&path, &cols).expect("write user_daily");
            });
        }
        sc.spawn(|| {
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
                        (&ctx_ref.campaign_ids[camp], &ctx_ref.channels[camp])
                    };
                    c.campaign_id.push(ByteArray::from(id.to_vec()));
                    c.channel.push(ByteArray::from(channel.to_vec()));
                    c.day.push((day_lo + d as i64) as i32);
                    c.sessions.push(a[0] as i64);
                    c.purchasing.push(a[1] as i64);
                    c.revenue.push(a[2] as i64);
                    c.assisted.push(if camp == ncamp { 0 } else { a[3] as i64 });
                }
            }
            crate::out::write_campaign_daily(&camp_dir.join("part-00000.parquet"), &c).expect("write campaign_daily");
        });
    });
    log!(t0, "done");
}
