//! Probe helper: writes each stdin line as an extra user_daily row (user_id = the line),
//! so the diagnostics appear as differing rows on the dev check.
#[path = "../pq.rs"]
mod pq;
use std::io::BufRead;
fn main() {
    let out = std::env::args().nth(1).unwrap();
    let (mut bytes, mut ends, mut n) = (Vec::new(), Vec::new(), 0usize);
    for l in std::io::stdin().lock().lines().map_while(Result::ok) {
        let l: String = l.chars().take(400).collect();
        bytes.extend_from_slice(format!("{:03} {}", n, l).as_bytes());
        ends.push(bytes.len() as u32);
        n += 1;
    }
    let day = vec![0i32; n];
    let z = vec![0i64; n];
    let present = vec![false; n];
    pq::write(std::path::Path::new(&out), &[
        pq::Column { name: "user_id", data: pq::Col::Str { bytes: &bytes, ends: &ends } },
        pq::Column { name: "day", data: pq::Col::Date(&day) },
        pq::Column { name: "sessions", data: pq::Col::I64(&z) },
        pq::Column { name: "events", data: pq::Col::I64(&z) },
        pq::Column { name: "revenue_cents", data: pq::Col::I64(&z) },
        pq::Column { name: "top_category", data: pq::Col::OptStr { present: &present, bytes: &[], ends: &[] } },
    ]).unwrap();
}
