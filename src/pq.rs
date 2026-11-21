//! A minimal Parquet writer: flat schemas, PLAIN encoding, uncompressed,
//! one data page (v1) per column chunk, row groups of at most ROW_GROUP rows.
//! Columns are written straight from the caller's arrays into one buffer.

use std::io::Write;

const ROW_GROUP: usize = 1 << 20;

/// Physical types (parquet.thrift `Type`).
const INT32: i32 = 1;
const INT64: i32 = 2;
const BYTE_ARRAY: i32 = 6;
/// Converted types.
const UTF8: i32 = 0;
const DATE: i32 = 6;

pub enum Col<'a> {
    /// Strings given as one byte buffer and end offsets, all present.
    Str { bytes: &'a [u8], ends: &'a [u32] },
    /// Strings with nulls: `present[i]` says row i has a value; the values of
    /// the present rows come in order from `bytes`/`ends`.
    OptStr { present: &'a [bool], bytes: &'a [u8], ends: &'a [u32] },
    Date(&'a [i32]),
    I64(&'a [i64]),
}

pub struct Column<'a> {
    pub name: &'a str,
    pub data: Col<'a>,
}

// ---- thrift compact protocol

struct T {
    out: Vec<u8>,
    last: Vec<i16>,
}

impl T {
    fn new() -> Self {
        T { out: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.out.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.out.push(v as u8);
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let delta = id - *self.last.last().unwrap();
        if (1..=15).contains(&delta) {
            self.out.push(((delta as u8) << 4) | ty);
        } else {
            self.out.push(ty);
            self.zigzag(id as i64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, 5);
        self.zigzag(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, 6);
        self.zigzag(v);
    }
    fn bin(&mut self, id: i16, v: &[u8]) {
        self.field(id, 8);
        self.varint(v.len() as u64);
        self.out.extend_from_slice(v);
    }
    fn begin(&mut self, id: i16) {
        self.field(id, 12);
        self.last.push(0);
    }
    fn end(&mut self) {
        self.out.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, 9);
        self.list_header(elem, n);
    }
    fn list_header(&mut self, elem: u8, n: usize) {
        if n < 15 {
            self.out.push(((n as u8) << 4) | elem);
        } else {
            self.out.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    /// A struct element of a list.
    fn elem_begin(&mut self) {
        self.last.push(0);
    }
}

struct ChunkMeta {
    ty: i32,
    name: String,
    num_values: i64,
    offset: i64,
    size: i64,
}

fn page_header(num_values: usize, body: usize, optional: bool) -> Vec<u8> {
    let mut t = T::new();
    t.i32(1, 0); // DATA_PAGE
    t.i32(2, body as i32);
    t.i32(3, body as i32);
    t.begin(5);
    t.i32(1, num_values as i32);
    t.i32(2, 0); // PLAIN
    t.i32(3, 3); // RLE definition levels
    t.i32(4, 3); // RLE repetition levels
    t.end();
    let _ = optional;
    t.out.push(0);
    t.out
}

fn put_strings(body: &mut Vec<u8>, bytes: &[u8], ends: &[u32], from: usize, to: usize) {
    let mut start = if from == 0 { 0 } else { ends[from - 1] as usize };
    for &e in &ends[from..to] {
        let e = e as usize;
        body.extend_from_slice(&((e - start) as u32).to_le_bytes());
        body.extend_from_slice(&bytes[start..e]);
        start = e;
    }
}

/// Writes one Parquet file with the given columns (all of the same length).
pub fn write(path: &std::path::Path, cols: &[Column]) -> std::io::Result<()> {
    let rows = match &cols[0].data {
        Col::Str { ends, .. } => ends.len(),
        Col::OptStr { present, .. } => present.len(),
        Col::Date(v) => v.len(),
        Col::I64(v) => v.len(),
    };
    let mut buf: Vec<u8> = Vec::with_capacity(rows * 64 + 1024);
    buf.extend_from_slice(b"PAR1");
    let mut groups: Vec<(usize, Vec<ChunkMeta>)> = Vec::new();
    // Per optional column: how many present values precede each row group.
    let mut opt_done: Vec<usize> = vec![0; cols.len()];
    let mut start = 0;
    while start < rows {
        let end = (start + ROW_GROUP).min(rows);
        let n = end - start;
        let mut chunks = Vec::new();
        for (ci, c) in cols.iter().enumerate() {
            let mut body = Vec::new();
            let (ty, optional) = match &c.data {
                Col::Str { bytes, ends } => {
                    put_strings(&mut body, bytes, ends, start, end);
                    (BYTE_ARRAY, false)
                }
                Col::OptStr { present, bytes, ends } => {
                    // Definition levels: RLE/bit-packed hybrid, bit width 1, one
                    // bit-packed run of ceil(n/8) groups, after a 4-byte length.
                    let groups8 = n.div_ceil(8);
                    let mut levels = Vec::with_capacity(groups8 + 5);
                    let mut header = T::new();
                    header.varint(((groups8 as u64) << 1) | 1);
                    levels.extend_from_slice(&header.out);
                    let mut count = 0;
                    for g in 0..groups8 {
                        let mut byte = 0u8;
                        for b in 0..8 {
                            let r = start + g * 8 + b;
                            if r < end && present[r] {
                                byte |= 1 << b;
                                count += 1;
                            }
                        }
                        levels.push(byte);
                    }
                    if n == 0 {
                        levels.clear();
                    }
                    body.extend_from_slice(&(levels.len() as u32).to_le_bytes());
                    body.extend_from_slice(&levels);
                    let from = opt_done[ci];
                    put_strings(&mut body, bytes, ends, from, from + count);
                    opt_done[ci] = from + count;
                    (BYTE_ARRAY, true)
                }
                Col::Date(v) => {
                    for x in &v[start..end] {
                        body.extend_from_slice(&x.to_le_bytes());
                    }
                    (INT32, false)
                }
                Col::I64(v) => {
                    for x in &v[start..end] {
                        body.extend_from_slice(&x.to_le_bytes());
                    }
                    (INT64, false)
                }
            };
            let offset = buf.len();
            buf.extend_from_slice(&page_header(n, body.len(), optional));
            buf.extend_from_slice(&body);
            chunks.push(ChunkMeta {
                ty,
                name: c.name.to_string(),
                num_values: n as i64,
                offset: offset as i64,
                size: (buf.len() - offset) as i64,
            });
        }
        groups.push((n, chunks));
        start = end;
    }

    // FileMetaData.
    let mut t = T::new();
    t.i32(1, 1);
    t.list(2, 12, cols.len() + 1);
    t.elem_begin();
    t.bin(4, b"schema");
    t.i32(5, cols.len() as i32);
    t.end();
    for c in cols {
        t.elem_begin();
        let (ty, rep, conv) = match c.data {
            Col::Str { .. } => (BYTE_ARRAY, 0, Some(UTF8)),
            Col::OptStr { .. } => (BYTE_ARRAY, 1, Some(UTF8)),
            Col::Date(_) => (INT32, 0, Some(DATE)),
            Col::I64(_) => (INT64, 0, None),
        };
        t.i32(1, ty);
        t.i32(3, rep);
        t.bin(4, c.name.as_bytes());
        if let Some(conv) = conv {
            t.i32(6, conv);
            // LogicalType union: STRING is field 1, DATE is field 6 (empty structs).
            t.begin(10);
            t.begin(if conv == UTF8 { 1 } else { 6 });
            t.end();
            t.end();
        }
        t.end();
    }
    t.i64(3, rows as i64);
    t.list(4, 12, groups.len());
    for (n, chunks) in &groups {
        t.elem_begin();
        t.list(1, 12, chunks.len());
        let mut total = 0i64;
        for ch in chunks {
            t.elem_begin();
            t.i64(2, ch.offset);
            t.begin(3);
            t.i32(1, ch.ty);
            t.list(2, 5, 2);
            t.zigzag(0); // PLAIN
            t.zigzag(3); // RLE
            t.list(3, 8, 1);
            t.varint(ch.name.len() as u64);
            t.out.extend_from_slice(ch.name.as_bytes());
            t.i32(4, 0); // UNCOMPRESSED
            t.i64(5, ch.num_values);
            t.i64(6, ch.size);
            t.i64(7, ch.size);
            t.i64(9, ch.offset);
            t.end();
            t.end();
            total += ch.size;
        }
        t.i64(2, total);
        t.i64(3, *n as i64);
        t.end();
    }
    t.bin(6, b"pipeline");
    t.out.push(0);
    buf.extend_from_slice(&t.out);
    buf.extend_from_slice(&(t.out.len() as u32).to_le_bytes());
    buf.extend_from_slice(b"PAR1");
    std::fs::File::create(path)?.write_all(&buf)
}
