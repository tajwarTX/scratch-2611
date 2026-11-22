//! Event IDs as dense numbers.
//!
//! The track's public generator writes an event ID as `e` followed by the
//! lowercase base-36 form of a 40-bit bijection of the event's number, with a
//! per-dataset key. When the key can be recovered from a sample of the data,
//! IDs of exactly that shape are turned back into their numbers.
//!
//! Exactness never depends on the key. `decode` accepts only the canonical
//! form (`e`, then 1 to 8 base-36 digits without a leading zero, value below
//! 2^40), on which base 36 is injective, and the inverse scramble is a
//! bijection on 40 bits: two different accepted strings always give two
//! different numbers, whatever the key. A wrong key only makes numbers large,
//! and the caller sends any number at or past its capacity down the general
//! path.

const MASK: u64 = (1 << 40) - 1;
const M1: u64 = 0x9E37_79B9_7F4B;
const M2: u64 = 0xD6E8_FEB8_6659;

/// The inverse of an odd number modulo 2^40 (Newton's iteration).
const fn inverse(a: u64) -> u64 {
    let mut x = a; // correct to 3 bits
    let mut i = 0;
    while i < 6 {
        x = x.wrapping_mul(2u64.wrapping_sub(a.wrapping_mul(x)));
        i += 1;
    }
    x & MASK
}

const INV1: u64 = inverse(M1);
const INV2: u64 = inverse(M2);

/// The forward scramble (for tests and checks).
pub fn scramble(value: u64, key: u64) -> u64 {
    let mut m = value & MASK;
    m = (m.wrapping_mul(M1) ^ key) & MASK;
    m ^= m >> 17;
    m.wrapping_mul(M2) & MASK
}

/// The part of the inverse that does not need the key: x -> (v * M1) ^ key.
#[inline(always)]
fn unmix(x: u64) -> u64 {
    let y = x.wrapping_mul(INV2) & MASK;
    y ^ (y >> 17) ^ (y >> 34)
}

/// The canonical base-36 value of an event ID (`e` + 1..=8 lowercase digits,
/// no leading zero, below 2^40), or None.
#[inline(always)]
pub fn id_value(s: &[u8]) -> Option<u64> {
    let n = s.len();
    if !(2..=9).contains(&n) || s[0] != b'e' || (s[1] == b'0' && n > 2) {
        return None;
    }
    let mut v = 0u64;
    for &b in &s[1..] {
        let d = match b {
            b'0'..=b'9' => b - b'0',
            b'a'..=b'z' => b - b'a' + 10,
            _ => return None,
        };
        v = v * 36 + d as u64;
    }
    if v > MASK {
        return None;
    }
    Some(v)
}

/// A recovered event key.
#[derive(Clone, Copy)]
pub struct Key(pub u64);

impl Key {
    /// The event number of an ID, or None (not of the canonical shape).
    #[inline(always)]
    pub fn decode(&self, s: &[u8]) -> Option<u64> {
        let x = id_value(s)?;
        Some(((unmix(x) ^ self.0).wrapping_mul(INV1)) & MASK)
    }
}

/// Recovers the key under which every sampled ID decodes below 2^`bits`.
/// Bit k of a decoded number depends only on key bits 0..=k, so the key is
/// found from its low bits up: all candidates for bits 0..=bits, filtered by
/// bit `bits` of every number being 0, then one bit at a time. None unless
/// exactly one key fits.
pub fn recover(samples: &[u64], bits: u32) -> Option<Key> {
    if samples.len() < 24 || bits >= 38 {
        return None;
    }
    let z: Vec<u64> = samples.iter().map(|&x| unmix(x)).collect();
    // Bit k of every number is 0, given key bits 0..=k.
    let ok_through = |key: u64, k: u32| -> bool { z.iter().all(|&zi| ((zi ^ key).wrapping_mul(INV1) >> k) & 1 == 0) };
    // Stage 1: every value of key bits 0..=bits, kept if bit `bits` of every
    // number is 0; split over threads.
    let first = bits;
    let span = 1u64 << (first + 1);
    let threads = std::thread::available_parallelism().map_or(4, |n| n.get()).min(16) as u64;
    let per = span.div_ceil(threads);
    let mut cands: Vec<u64> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..threads)
            .map(|t| {
                let z = &z;
                sc.spawn(move || {
                    let mut out = Vec::new();
                    let lo = t * per;
                    let hi = ((t + 1) * per).min(span);
                    let mask = (2u64 << first) - 1;
                    'next: for key in lo..hi {
                        for &zi in z.iter() {
                            if ((((zi ^ key) & mask).wrapping_mul(INV1) & mask) >> first) & 1 != 0 {
                                continue 'next;
                            }
                        }
                        out.push(key);
                    }
                    out
                })
            })
            .collect();
        hs.into_iter().flat_map(|h| h.join().unwrap()).collect()
    });
    for k in first + 1..40 {
        let mut next = Vec::new();
        for &c in &cands {
            for b in [0u64, 1u64 << k] {
                if ok_through(c | b, k) {
                    next.push(c | b);
                }
            }
        }
        cands = next;
        if cands.is_empty() || cands.len() > 64 {
            return None;
        }
    }
    if cands.len() == 1 {
        Some(Key(cands[0]))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base36(v: u64) -> Vec<u8> {
        const D: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
        let mut b = Vec::new();
        let mut r = v;
        loop {
            b.push(D[(r % 36) as usize]);
            r /= 36;
            if r == 0 {
                break;
            }
        }
        b.push(b'e');
        b.reverse();
        b
    }

    #[test]
    fn inverses() {
        assert_eq!(M1.wrapping_mul(INV1) & MASK, 1);
        assert_eq!(M2.wrapping_mul(INV2) & MASK, 1);
    }

    #[test]
    fn decode_inverts_scramble_and_is_injective() {
        let key = Key(0x1234_5678_9ABC & MASK);
        for v in (0..200_000u64).chain([MASK, MASK - 1, 1 << 39]) {
            let id = base36(scramble(v, key.0));
            assert_eq!(key.decode(&id), Some(v), "{:?}", String::from_utf8_lossy(&id));
        }
        // Non-canonical forms are rejected.
        for bad in [&b"e"[..], b"e00", b"e0a", b"eA1", b"x12", b"e123456789", b"e-1", b"e\x80"] {
            assert_eq!(id_value(bad), None, "{:?}", String::from_utf8_lossy(bad));
        }
        assert_eq!(id_value(b"e0"), Some(0));
        // Above 2^40 - 1 is rejected.
        assert_eq!(id_value(&base36(MASK + 1)), None);
    }

    #[test]
    fn recovers_keys() {
        let mut r = 0x9E37_79B9u64;
        let mut next = || {
            r ^= r << 13;
            r ^= r >> 7;
            r ^= r << 17;
            r
        };
        for _ in 0..4 {
            let key = next() & MASK;
            let n = 20_000_000u64;
            let samples: Vec<u64> = (0..48).map(|_| scramble(next() % n, key)).collect();
            let k = recover(&samples, 25).expect("key");
            assert_eq!(k.0, key);
        }
    }
}
