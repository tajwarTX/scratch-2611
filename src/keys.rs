//! IDs as 64-bit keys.
//!
//! Most IDs are short and use only `[0-9A-Z_a-z]`. Those pack into 60 bits, six
//! bits a character, left-aligned and zero-padded. The character codes rise
//! with the byte values and padding is below every code, so comparing two
//! packed keys as integers is exactly byte-wise string comparison, and equal
//! keys mean equal strings.
//!
//! Anything else (longer, spaces, tabs, `\r`, non-ASCII) is interned in a
//! global table and gets a key with the top bit set. Interned keys are equal
//! only for equal strings too, but ordering them needs the bytes: `cmp_key`.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Mutex;

pub const INTERN_BIT: u64 = 1 << 63;
const MAX_PACKED: usize = 10;

const fn code_table() -> [u8; 256] {
    let mut table = [0u8; 256];
    let mut code = 1u8;
    let mut byte = 0usize;
    while byte < 256 {
        let b = byte as u8;
        if b.is_ascii_digit() || b.is_ascii_uppercase() || b == b'_' || b.is_ascii_lowercase() {
            table[byte] = code;
            code += 1;
        }
        byte += 1;
    }
    table
}

const fn decode_table() -> [u8; 64] {
    let codes = code_table();
    let mut table = [0u8; 64];
    let mut byte = 0usize;
    while byte < 256 {
        if codes[byte] != 0 {
            table[codes[byte] as usize] = byte as u8;
        }
        byte += 1;
    }
    table
}

static CODE: [u8; 256] = code_table();
static DECODE: [u8; 64] = decode_table();

#[inline(always)]
pub fn pack(s: &[u8]) -> Option<u64> {
    let n = s.len();
    if n > MAX_PACKED {
        return None;
    }
    let mut v = 0u64;
    for &b in s {
        let c = CODE[b as usize];
        if c == 0 {
            return None;
        }
        v = (v << 6) | c as u64;
    }
    Some(v << (6 * (MAX_PACKED - n)))
}

struct Interner {
    map: HashMap<Vec<u8>, u64>,
    strings: Vec<Vec<u8>>,
}

static INTERNER: Mutex<Option<Interner>> = Mutex::new(None);
static ANY_INTERNED: AtomicBool = AtomicBool::new(false);

#[cold]
pub fn intern(s: &[u8]) -> u64 {
    let mut guard = INTERNER.lock().unwrap();
    let it = guard.get_or_insert_with(|| Interner { map: HashMap::new(), strings: Vec::new() });
    if let Some(&k) = it.map.get(s) {
        return k;
    }
    let k = INTERN_BIT | it.strings.len() as u64;
    it.strings.push(s.to_vec());
    it.map.insert(s.to_vec(), k);
    ANY_INTERNED.store(true, AtomicOrdering::Relaxed);
    k
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
        let guard = INTERNER.lock().unwrap();
        out.extend_from_slice(&guard.as_ref().unwrap().strings[(k & !INTERN_BIT) as usize]);
        return;
    }
    for i in 0..MAX_PACKED {
        let c = (k >> (6 * (MAX_PACKED - 1 - i))) & 63;
        if c == 0 {
            break;
        }
        out.push(DECODE[c as usize]);
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
    let (mut x, mut y) = (Vec::new(), Vec::new());
    key_bytes(a, &mut x);
    key_bytes(b, &mut y);
    x.cmp(&y)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_order_is_byte_order() {
        let words: Vec<&[u8]> = vec![
            b"0", b"09", b"1", b"A", b"AB", b"U1", b"Z", b"_", b"a", b"a0", b"aa", b"u", b"u1", b"u10", b"u2",
            b"user_id_a", b"zzzzzzzzzz",
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
        assert!(pack(b"u 1").is_none());
        assert!(pack(b"abcdefghijk").is_none());
    }
}
