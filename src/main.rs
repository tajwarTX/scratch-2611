//! The pipeline: rules.md, implemented directly. `v2` is the fast path;
//! `legacy` is the general one it falls back to when a dataset breaks one of
//! v2's compact-record assumptions.

mod bench;
mod csv;
mod fast;
mod keys;
mod legacy;
mod out;
mod pq;
mod scatter;
mod scatter2;
mod table;
mod ts;
mod v2;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    if std::env::var("BENCH").is_ok() {
        bench::run();
        return;
    }
    let fast = std::env::var("PIPELINE").map_or(true, |v| v != "legacy");
    if !(fast && v2::run()) {
        legacy::run();
    }
    std::process::exit(0);
}
