//! The three timestamp forms of rules.md, to epoch milliseconds.

#[inline(always)]
fn digit(b: u8) -> u32 {
    b.wrapping_sub(b'0') as u32
}

pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year - 719_468
}

#[inline]
fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        2 => {
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// F1 `YYYY-MM-DDTHH:MM:SSZ` (20 bytes) or F2 `YYYY-MM-DDTHH:MM:SS+HH:MM` (25 bytes).
#[inline]
fn parse_civil(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let two = |i: usize| -> Option<u32> {
        let (a, b) = (digit(s[i]), digit(s[i + 1]));
        if a < 10 && b < 10 {
            Some(a * 10 + b)
        } else {
            None
        }
    };
    let year = two(0)? * 100 + two(2)?;
    let month = two(5)?;
    let day = two(8)?;
    let hour = two(11)?;
    let minute = two(14)?;
    let second = two(17)?;
    if month < 1 || month > 12 || day < 1 || day > days_in_month(year, month) || hour > 23 || minute > 59 || second > 59
    {
        return None;
    }
    let offset_minutes: i64 = if s.len() == 20 {
        if s[19] != b'Z' {
            return None;
        }
        0
    } else {
        let sign = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if s[22] != b':' {
            return None;
        }
        let (oh, om) = (two(20)?, two(23)?);
        if oh > 23 || om > 59 {
            return None;
        }
        sign * (oh as i64 * 60 + om as i64)
    };
    let days = days_from_civil(year as i64, month as i64, day as i64);
    Some((days * 86_400 + hour as i64 * 3_600 + minute as i64 * 60 + second as i64 - offset_minutes * 60) * 1_000)
}

/// A `ts` value in epoch milliseconds, or None for a bad timestamp. An F3
/// number too large for i64 is returned as i64::MAX: outside every window.
#[inline]
pub fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if (n == 20 || n == 25) && s[4] == b'-' {
        return parse_civil(s);
    }
    if n == 0 {
        return None;
    }
    let mut v: i64 = 0;
    let mut overflow = false;
    for &b in s {
        let d = digit(b);
        if d > 9 {
            return None;
        }
        if !overflow {
            match v.checked_mul(10).and_then(|t| t.checked_add(d as i64)) {
                Some(t) => v = t,
                None => overflow = true,
            }
        }
    }
    Some(if overflow { i64::MAX } else { v })
}

/// A purchase price: ASCII digits only, value 1 to 10,000,000.
#[inline]
pub fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &b in s {
        let d = digit(b);
        if d > 9 {
            return None;
        }
        v = (v * 10 + d as u64).min(100_000_000);
    }
    if (1..=10_000_000).contains(&v) {
        Some(v as u32)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forms() {
        let f1 = parse_ts(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(parse_ts(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse_ts(b"1791071890000"), Some(1_791_071_890_000));
        assert_eq!(parse_ts(b"0001791071890000"), Some(1_791_071_890_000));
        assert_eq!(f1, 1_791_071_890_000);
        assert_eq!(parse_ts(b"2026-10-04T01:30:00+02:00"), parse_ts(b"2026-10-03T23:30:00Z"));
        assert_eq!(parse_ts(b"2026-10-03T10:00:00-00:00"), parse_ts(b"2026-10-03T10:00:00Z"));
        for bad in [
            &b""[..], b"2026-10-03T23:58:10.000Z", b"2026-10-03T23:58:10z", b"2026-10-03t23:58:10Z",
            b"2026-10-03 23:58:10Z", b"2026-10-03T23:58:10", b"1791071890000.0", b"1.79e12", b"-5",
            b"2026-13-03T23:58:10Z", b"2026-02-30T23:58:10Z", b"2027-02-29T23:58:10Z", b"2026-10-03T24:00:00Z",
            b"2026-10-03T23:58:60Z", b"2026-10-03T23:58:10+24:00", b"2026-10-03T23:58:10+05:60",
            b"2026-10-03T23:58:10+0200",
        ] {
            assert_eq!(parse_ts(bad), None, "{:?}", std::str::from_utf8(bad));
        }
        assert!(parse_ts(b"2028-02-29T00:00:00Z").is_some());
        assert_eq!(parse_ts(b"999999999999999999999"), Some(i64::MAX));
        assert_eq!(parse_price(b"0250"), Some(250));
        assert_eq!(parse_price(b"10000000"), Some(10_000_000));
        assert_eq!(parse_price(b"10000001"), None);
        assert_eq!(parse_price(b"0"), None);
        assert_eq!(parse_price(b"00000000000000000000001"), Some(1));
    }
}

/// Fast parsing for timestamps whose date is near the window. A table maps
/// each `YYYY-MM-` prefix in range to the day number of its first day and the
/// day-of-month span covered; anything else goes to `parse_ts`, so results
/// are always exactly `parse_ts`'s.
pub struct FastTs {
    prefixes: [(u64, i64, u32, u32); 6],
    count: usize,
    /// The prefixes again, indexed by (last year digit, month) for `dt_secs`.
    by_ym: Vec<(u64, i64, u32, u32)>,
}

/// `dt_secs`'s table slot for a `YYYY-MM-` word.
#[inline(always)]
fn ym_slot(key: u64) -> usize {
    let y4 = ((key >> 24) & 0xF) as usize;
    let m = (((key >> 40) & 0xF) * 10 + ((key >> 48) & 0xF)) as usize & 15;
    y4 * 16 + m
}

impl FastTs {
    /// Covers every date from `lo_ms`'s day to `hi_ms`'s day.
    pub fn new(lo_ms: i64, hi_ms: i64) -> Self {
        let mut prefixes = [(0u64, 0i64, 0u32, 0u32); 6];
        let mut count = 0;
        let (lo, hi) = (lo_ms.div_euclid(86_400_000), hi_ms.div_euclid(86_400_000));
        let mut day = lo;
        while day <= hi && count < prefixes.len() {
            let (y, m, d) = civil_from_days(day);
            if !(1000..=9999).contains(&y) {
                day += 1;
                continue;
            }
            let text = format!("{:04}-{:02}-", y, m);
            let key = u64::from_le_bytes(text.as_bytes()[..8].try_into().unwrap());
            let dim = days_in_month(y as u32, m as u32);
            let last = (d as i64 + (hi - day)).min(dim as i64) as u32;
            prefixes[count] = (key, days_from_civil(y, m, 1), d as u32, last);
            count += 1;
            day += (last - d as u32) as i64 + 1;
        }
        let mut by_ym = vec![(0u64, 0i64, 1u32, 0u32); 256];
        for &e in &prefixes[..count] {
            by_ym[ym_slot(e.0)] = e;
        }
        FastTs { prefixes, count, by_ym }
    }

    /// F1 with word-at-a-time checks; None means "not handled here" (the
    /// caller then uses `parse`, which is exact for everything).
    #[inline(always)]
    pub fn f1_swar(&self, s: &[u8]) -> Option<i64> {
        if s.len() != 20 {
            return None;
        }
        let key = u64::from_le_bytes(s[..8].try_into().unwrap());
        let b = u64::from_le_bytes(s[8..16].try_into().unwrap());
        let c = u32::from_le_bytes(s[16..20].try_into().unwrap());
        if (b & 0x0000_FF00_00FF_0000) != 0x0000_3A00_0054_0000 || (c & 0xFF00_00FF) != 0x5A00_003A {
            return None;
        }
        let d = b ^ 0x3030_0030_3000_3030;
        if ((d.wrapping_add(0x7676_0076_7600_7676)) | d) & 0x8080_0080_8000_8080 != 0 {
            return None;
        }
        let e = c ^ 0x0030_3000;
        if ((e.wrapping_add(0x0076_7600)) | e) & 0x0080_8000 != 0 {
            return None;
        }
        let byte = |x: u64, i: u32| ((x >> (8 * i)) & 0xFF) as u32;
        let dd = byte(d, 0) * 10 + byte(d, 1);
        let hh = byte(d, 3) * 10 + byte(d, 4);
        let mm = byte(d, 6) * 10 + byte(d, 7);
        let ss = ((e >> 8) & 0xFF) * 10 + ((e >> 16) & 0xFF);
        if hh > 23 || mm > 59 || ss > 59 {
            return None;
        }
        for &(k, base, first, last) in &self.prefixes[..self.count] {
            if k == key {
                if dd < first || dd > last {
                    return None;
                }
                let days = base + dd as i64 - 1;
                return Some((days * 86_400 + hh as i64 * 3_600 + mm as i64 * 60 + ss as i64) * 1_000);
            }
        }
        None
    }

    /// Epoch seconds of an F1 (`n == 20`) or F2 (`n == 25`) value at `p`
    /// (24 bytes readable), branch-free on the common path; None means "not
    /// handled here" (the caller then uses `parse`, exact for everything).
    #[inline(always)]
    pub unsafe fn dt_secs(&self, p: *const u8, n: usize) -> Option<i64> {
        let key = (p as *const u64).read_unaligned();
        let b = (p.add(8) as *const u64).read_unaligned();
        let c = (p.add(16) as *const u64).read_unaligned();
        // b = "DDTHH:MM", c = ":SS" then Z, or "+HH:MM".
        let d = b ^ 0x3030_0030_3000_3030;
        let e = c ^ 0x3000_3030_0030_3000;
        let shape_b = (b & 0x0000_FF00_00FF_0000) == 0x0000_3A00_0054_0000;
        let dig_b = ((d.wrapping_add(0x7676_0076_7600_7676)) | d) & 0x8080_0080_8000_8080 == 0;
        // Seconds digits are bytes 1, 2 of c; offset digits bytes 4, 5, 7 and (byte 8 =) p[24].
        let dig_s = ((e.wrapping_add(0x0000_0000_0076_7600)) | e) & 0x0000_0000_0080_8000 == 0;
        let colon = (c & 0xFF) == 0x3A;
        let byte = |x: u64, i: u32| ((x >> (8 * i)) & 0xFF) as u32;
        let dd = byte(d, 0) * 10 + byte(d, 1);
        let hh = byte(d, 3) * 10 + byte(d, 4);
        let mm = byte(d, 6) * 10 + byte(d, 7);
        let ss = byte(e, 1) * 10 + byte(e, 2);
        let t = *self.by_ym.get_unchecked(ym_slot(key));
        let hit = t.0 == key;
        let (base, first, last) = (t.1, if hit { t.2 } else { 99 }, t.3);
        let ok = shape_b & dig_b & dig_s & colon & (hh <= 23) & (mm <= 59) & (ss <= 59) & (dd >= first) & (dd <= last);
        let mut secs = (base + dd as i64 - 1) * 86_400 + (hh * 3_600 + mm * 60 + ss) as i64;
        let zone = byte(c, 3);
        if n == 20 {
            if !(ok & (zone == b'Z' as u32)) {
                return None;
            }
            return Some(secs);
        }
        // F2: sign, HH, ':', MM.
        let o = [byte(e, 4), byte(e, 5), byte(e, 7), (*p.add(24) as u32).wrapping_sub(b'0' as u32)];
        let odig = o.iter().all(|&x| x <= 9);
        let (oh, om) = (o[0] * 10 + o[1], o[2] * 10 + o[3]);
        let sign = zone == b'+' as u32;
        if !(ok & odig & (sign | (zone == b'-' as u32)) & (byte(c, 6) == b':' as u32) & (oh <= 23) & (om <= 59)) {
            return None;
        }
        let off = (oh * 3_600 + om * 60) as i64;
        secs -= if sign { off } else { -off };
        Some(secs)
    }

    #[inline(always)]
    pub fn parse(&self, s: &[u8]) -> Option<i64> {
        if let Some(v) = self.f1_swar(s) {
            return Some(v);
        }
        let n = s.len();
        if n == 20 || n == 25 {
            let key = u64::from_le_bytes(s[..8].try_into().unwrap());
            for &(k, base, first, last) in &self.prefixes[..self.count] {
                if k != key {
                    continue;
                }
                let (d1, d2) = (digit(s[8]), digit(s[9]));
                let dd = d1 * 10 + d2;
                if d1 > 9 || d2 > 9 || dd < first || dd > last || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
                    break;
                }
                let t = [digit(s[11]), digit(s[12]), digit(s[14]), digit(s[15]), digit(s[17]), digit(s[18])];
                if t.iter().any(|&x| x > 9) {
                    break;
                }
                let (h, mi, se) = (t[0] * 10 + t[1], t[2] * 10 + t[3], t[4] * 10 + t[5]);
                if h > 23 || mi > 59 || se > 59 {
                    return None;
                }
                let offset: i64 = if n == 20 {
                    if s[19] != b'Z' {
                        return None;
                    }
                    0
                } else {
                    let sign = match s[19] {
                        b'+' => 1,
                        b'-' => -1,
                        _ => return None,
                    };
                    let o = [digit(s[20]), digit(s[21]), digit(s[23]), digit(s[24])];
                    if s[22] != b':' || o.iter().any(|&x| x > 9) {
                        return None;
                    }
                    let (oh, om) = (o[0] * 10 + o[1], o[2] * 10 + o[3]);
                    if oh > 23 || om > 59 {
                        return None;
                    }
                    sign * (oh as i64 * 60 + om as i64)
                };
                let days = base + dd as i64 - 1;
                return Some((days * 86_400 + h as i64 * 3_600 + mi as i64 * 60 + se as i64 - offset * 60) * 1_000);
            }
        }
        parse_ts(s)
    }
}

pub fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era = (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let month = if month_index < 10 { month_index + 3 } else { month_index - 9 };
    (year_of_era + era * 400 + i64::from(month <= 2), month, day_of_year - (153 * month_index + 2) / 5 + 1)
}

#[cfg(test)]
mod fast_tests {
    use super::*;

    #[test]
    fn fast_equals_general() {
        // A window around Feb 29 2028 and one across a year end.
        for (lo, hi) in [("2028-02-10T00:00:00Z", "2028-03-10T00:00:00Z"), ("2026-12-20T00:00:00Z", "2027-01-20T00:00:00Z")] {
            let fast = FastTs::new(parse_ts(lo.as_bytes()).unwrap(), parse_ts(hi.as_bytes()).unwrap());
            let mut cases: Vec<String> = Vec::new();
            for y in [2026, 2027, 2028] {
                for m in 1..=12 {
                    for d in [0, 1, 9, 10, 19, 20, 28, 29, 30, 31, 32] {
                        for t in ["T00:00:00Z", "T23:59:59Z", "T24:00:00Z", "T12:60:00Z", "T12:00:60Z", "T12:34:56+05:30",
                                  "T01:00:00-10:00", "T12:00:00+24:00", "T12:00:00+05:60", "T12:00:00-00:00", "t12:00:00Z",
                                  "T12:00:00z", " 12:00:00Z", "T1a:00:00Z", "T12:00:00+0a:00", "T12:00:00*05:00"] {
                            cases.push(format!("{:04}-{:02}-{:02}{}", y, m, d, t));
                        }
                    }
                }
            }
            cases.push("1791071890000".into());
            cases.push("2028-02-2a".into());
            let dt = |t: &[u8]| -> Option<i64> {
                if t.len() != 20 && t.len() != 25 {
                    return None;
                }
                let mut buf = t.to_vec();
                buf.extend_from_slice(&[0xAA; 32]);
                unsafe { fast.dt_secs(buf.as_ptr(), t.len()) }.map(|s| s * 1000)
            };
            for c in &cases {
                if let Some(v) = dt(c.as_bytes()) {
                    assert_eq!(Some(v), parse_ts(c.as_bytes()), "dt {}", c);
                }
                assert_eq!(fast.parse(c.as_bytes()), parse_ts(c.as_bytes()), "{}", c);
                if let Some(v) = fast.f1_swar(c.as_bytes()) {
                    assert_eq!(Some(v), parse_ts(c.as_bytes()), "swar {}", c);
                }
            }
            assert!(!lo.starts_with("2028") || (dt(b"2028-02-29T12:34:56Z").is_some() && dt(b"2028-02-29T12:34:56+05:30").is_some()));
            let good2 = b"2028-02-29T12:34:56-09:45";
            for i in 0..25 {
                for v in 0..=255u8 {
                    let mut t = *good2;
                    t[i] = v;
                    if let Some(x) = dt(&t) {
                        assert_eq!(Some(x), parse_ts(&t), "dt2 {:?}", String::from_utf8_lossy(&t));
                    }
                }
            }
            // Every byte position of a valid F1 replaced by every byte value.
            let good = b"2028-02-29T12:34:56Z";
            for i in 0..20 {
                for v in 0..=255u8 {
                    let mut t = *good;
                    t[i] = v;
                    if let Some(x) = fast.f1_swar(&t) {
                        assert_eq!(Some(x), parse_ts(&t), "swar {:?}", String::from_utf8_lossy(&t));
                    }
                    if let Some(x) = dt(&t) {
                        assert_eq!(Some(x), parse_ts(&t), "dt {:?}", String::from_utf8_lossy(&t));
                    }
                    assert_eq!(fast.parse(&t), parse_ts(&t));
                }
            }
        }
    }
}
