//! Probe helper: runs a command, prints "MAXRSS <MB> EXIT <code>" (peak RSS of the child) to stderr.
fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let st = std::process::Command::new(&args[0]).args(&args[1..]).status().expect("spawn");
    #[repr(C)]
    struct Ru {
        t: [i64; 4],
        maxrss: i64,
        rest: [i64; 13],
    }
    extern "C" {
        fn getrusage(who: i32, ru: *mut Ru) -> i32;
    }
    let mut ru = Ru { t: [0; 4], maxrss: 0, rest: [0; 13] };
    unsafe { getrusage(-1, &mut ru) };
    let code = st.code().unwrap_or(128 + std::os::unix::process::ExitStatusExt::signal(&st).unwrap_or(0));
    eprintln!("MAXRSS {} EXIT {}", ru.maxrss / 1024, code);
}
