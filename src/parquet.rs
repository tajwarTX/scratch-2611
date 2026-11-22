//! Minimal Parquet writer: one row group, one PLAIN data page per column,
//! no compression. Enough for the two output tables.

use std::io::Write;

pub enum Values {
    /// BYTE_ARRAY (UTF8), PLAIN-encoded already: 4-byte LE length + bytes per non-null value.
    Str { plain: Vec<u8>, defs: Option<Vec<bool>> },
    /// INT32 with DATE logical type (days since epoch).
    Date(Vec<i32>),
    /// INT64.
    I64(Vec<i64>),
}

pub struct Column {
    pub name: &'static str,
    pub values: Values,
}

// ---- thrift compact protocol ----
struct T {
    b: Vec<u8>,
    last: Vec<i16>,
}

impl T {
    fn new() -> Self {
        T { b: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        loop {
            if v < 0x80 {
                self.b.push(v as u8);
                return;
            }
            self.b.push((v as u8 & 0x7f) | 0x80);
            v >>= 7;
        }
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn fh(&mut self, id: i16, ty: u8) {
        let last = *self.last.last().unwrap();
        let d = id - last;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            let z = ((id as i64) << 1) ^ ((id as i64) >> 63);
            self.varint(z as u64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32f(&mut self, id: i16, v: i32) {
        self.fh(id, 5);
        self.zz(v as i64);
    }
    fn i64f(&mut self, id: i16, v: i64) {
        self.fh(id, 6);
        self.zz(v);
    }
    fn strf(&mut self, id: i16, s: &[u8]) {
        self.fh(id, 8);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
    fn begin_struct(&mut self, id: i16) {
        self.fh(id, 12);
        self.last.push(0);
    }
    fn begin_struct_elem(&mut self) {
        self.last.push(0);
    }
    fn end_struct(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, elem_ty: u8, n: usize) {
        self.fh(id, 9);
        self.list_header(elem_ty, n);
    }
    fn list_header(&mut self, elem_ty: u8, n: usize) {
        if n < 15 {
            self.b.push(((n as u8) << 4) | elem_ty);
        } else {
            self.b.push(0xf0 | elem_ty);
            self.varint(n as u64);
        }
    }
}

const T_BYTE_ARRAY: i32 = 6;
const T_INT32: i32 = 1;
const T_INT64: i32 = 2;

fn encode_defs(defs: &[bool]) -> Vec<u8> {
    // RLE/bit-packed hybrid, bit width 1, one bit-packed run, with 4-byte length prefix.
    let groups = (defs.len() + 7) / 8;
    let mut body = Vec::with_capacity(groups + 8);
    let mut h = ((groups as u64) << 1) | 1;
    loop {
        if h < 0x80 {
            body.push(h as u8);
            break;
        }
        body.push((h as u8 & 0x7f) | 0x80);
        h >>= 7;
    }
    for g in 0..groups {
        let mut byte = 0u8;
        for k in 0..8 {
            let i = g * 8 + k;
            if i < defs.len() && defs[i] {
                byte |= 1 << k;
            }
        }
        body.push(byte);
    }
    let mut out = Vec::with_capacity(body.len() + 4);
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

pub fn write(path: &std::path::Path, columns: &[Column], nrows: usize) -> std::io::Result<()> {
    let mut file: Vec<u8> = Vec::new();
    file.extend_from_slice(b"PAR1");
    struct Meta {
        ty: i32,
        name: &'static str,
        offset: i64,
        size: i64,
    }
    let mut metas = Vec::new();
    for col in columns {
        let (ty, data): (i32, Vec<u8>) = match &col.values {
            Values::Str { plain, defs } => {
                let mut d = Vec::new();
                if let Some(defs) = defs {
                    d.extend_from_slice(&encode_defs(defs));
                }
                d.extend_from_slice(plain);
                (T_BYTE_ARRAY, d)
            }
            Values::Date(v) => {
                let mut d = Vec::with_capacity(v.len() * 4);
                for x in v {
                    d.extend_from_slice(&x.to_le_bytes());
                }
                (T_INT32, d)
            }
            Values::I64(v) => {
                let mut d = Vec::with_capacity(v.len() * 8);
                for x in v {
                    d.extend_from_slice(&x.to_le_bytes());
                }
                (T_INT64, d)
            }
        };
        let mut h = T::new();
        h.i32f(1, 0); // DATA_PAGE
        h.i32f(2, data.len() as i32);
        h.i32f(3, data.len() as i32);
        h.begin_struct(5);
        h.i32f(1, nrows as i32);
        h.i32f(2, 0); // PLAIN
        h.i32f(3, 3); // RLE
        h.i32f(4, 3); // RLE
        h.end_struct();
        h.b.push(0);
        let offset = file.len() as i64;
        file.extend_from_slice(&h.b);
        file.extend_from_slice(&data);
        let size = file.len() as i64 - offset;
        metas.push(Meta { ty, name: col.name, offset, size });
    }
    // FileMetaData
    let mut m = T::new();
    m.i32f(1, 1);
    m.list(2, 12, columns.len() + 1);
    // root
    m.begin_struct_elem();
    m.strf(4, b"schema");
    m.i32f(5, columns.len() as i32);
    m.end_struct();
    for col in columns {
        m.begin_struct_elem();
        let (ty, rep, conv, logical): (i32, i32, Option<i32>, Option<i16>) = match &col.values {
            Values::Str { defs, .. } => (T_BYTE_ARRAY, if defs.is_some() { 1 } else { 0 }, Some(0), Some(1)),
            Values::Date(_) => (T_INT32, 0, Some(6), Some(6)),
            Values::I64(_) => (T_INT64, 0, None, None),
        };
        m.i32f(1, ty);
        m.i32f(3, rep);
        m.strf(4, col.name.as_bytes());
        if let Some(c) = conv {
            m.i32f(6, c);
        }
        if let Some(l) = logical {
            // LogicalType union: 1 = STRING (StringType{}), 6 = DATE (DateType{})
            m.begin_struct(10);
            m.begin_struct(l);
            m.end_struct();
            m.end_struct();
        }
        m.end_struct();
    }
    m.i64f(3, nrows as i64);
    m.list(4, 12, 1);
    m.begin_struct_elem();
    m.list(1, 12, columns.len());
    let mut total = 0i64;
    for meta in &metas {
        total += meta.size;
        m.begin_struct_elem();
        m.i64f(2, meta.offset);
        m.begin_struct(3);
        m.i32f(1, meta.ty);
        m.list(2, 5, 2);
        m.zz(0); // PLAIN
        m.zz(3); // RLE
        m.list(3, 8, 1);
        m.varint(meta.name.len() as u64);
        m.b.extend_from_slice(meta.name.as_bytes());
        m.i32f(4, 0); // UNCOMPRESSED
        m.i64f(5, nrows as i64);
        m.i64f(6, meta.size);
        m.i64f(7, meta.size);
        m.i64f(9, meta.offset);
        m.end_struct();
        m.end_struct();
    }
    m.i64f(2, total);
    m.i64f(3, nrows as i64);
    m.end_struct();
    m.b.push(0);
    file.extend_from_slice(&m.b);
    file.extend_from_slice(&(m.b.len() as u32).to_le_bytes());
    file.extend_from_slice(b"PAR1");
    let mut f = std::fs::File::create(path)?;
    f.write_all(&file)?;
    Ok(())
}

/// Physical/logical type of a `ColRef`.
#[derive(Clone, Copy, PartialEq)]
pub enum Ty {
    Str,
    Date,
    I32,
    I64,
}

/// A column whose page payload is already PLAIN-encoded: `data` is the values
/// (little-endian integers, or 4-byte length + bytes per non-null string);
/// `defs` (nullable columns) is the bit-packed definition levels, one bit per row,
/// exactly (rows + 7) / 8 bytes.
pub struct ColRef<'a> {
    pub name: &'static str,
    pub ty: Ty,
    pub defs: Option<&'a [u8]>,
    pub data: &'a [u8],
}

fn varint_bytes(mut v: u64, out: &mut Vec<u8>) {
    loop {
        if v < 0x80 {
            out.push(v as u8);
            return;
        }
        out.push((v as u8 & 0x7f) | 0x80);
        v >>= 7;
    }
}

/// Writes one row group with one uncompressed PLAIN data page per column, without
/// copying the column data (one `write` per piece).
pub fn write_cols(path: &std::path::Path, cols: &[ColRef], nrows: usize) -> std::io::Result<()> {
    let mut f = std::fs::File::create(path)?;
    f.write_all(b"PAR1")?;
    let mut pos = 4i64;
    let mut metas: Vec<(i32, &'static str, i64, i64)> = Vec::new();
    for c in cols {
        // definition levels: 4-byte length, then one bit-packed run of (rows + 7) / 8 groups
        let mut dl: Vec<u8> = Vec::new();
        if let Some(d) = c.defs {
            let groups = (nrows + 7) / 8;
            assert_eq!(d.len(), groups);
            let mut hdr = Vec::new();
            varint_bytes(((groups as u64) << 1) | 1, &mut hdr);
            dl.extend_from_slice(&((hdr.len() + groups) as u32).to_le_bytes());
            dl.extend_from_slice(&hdr);
        }
        let page = dl.len() + c.defs.map_or(0, |d| d.len()) + c.data.len();
        let mut h = T::new();
        h.i32f(1, 0); // DATA_PAGE
        h.i32f(2, page as i32);
        h.i32f(3, page as i32);
        h.begin_struct(5);
        h.i32f(1, nrows as i32);
        h.i32f(2, 0); // PLAIN
        h.i32f(3, 3); // RLE
        h.i32f(4, 3); // RLE
        h.end_struct();
        h.b.push(0);
        f.write_all(&h.b)?;
        if let Some(d) = c.defs {
            f.write_all(&dl)?;
            f.write_all(d)?;
        }
        f.write_all(c.data)?;
        let size = (h.b.len() + page) as i64;
        let ty = match c.ty {
            Ty::Str => T_BYTE_ARRAY,
            Ty::Date | Ty::I32 => T_INT32,
            Ty::I64 => T_INT64,
        };
        metas.push((ty, c.name, pos, size));
        pos += size;
    }
    let mut m = T::new();
    m.i32f(1, 1);
    m.list(2, 12, cols.len() + 1);
    m.begin_struct_elem();
    m.strf(4, b"schema");
    m.i32f(5, cols.len() as i32);
    m.end_struct();
    for (c, meta) in cols.iter().zip(&metas) {
        m.begin_struct_elem();
        m.i32f(1, meta.0);
        m.i32f(3, if c.defs.is_some() { 1 } else { 0 });
        m.strf(4, c.name.as_bytes());
        let (conv, logical): (Option<i32>, Option<i16>) = match c.ty {
            Ty::Str => (Some(0), Some(1)),
            Ty::Date => (Some(6), Some(6)),
            Ty::I32 | Ty::I64 => (None, None),
        };
        if let Some(cv) = conv {
            m.i32f(6, cv);
        }
        if let Some(l) = logical {
            m.begin_struct(10);
            m.begin_struct(l);
            m.end_struct();
            m.end_struct();
        }
        m.end_struct();
    }
    m.i64f(3, nrows as i64);
    m.list(4, 12, 1);
    m.begin_struct_elem();
    m.list(1, 12, cols.len());
    let mut total = 0i64;
    for meta in &metas {
        total += meta.3;
        m.begin_struct_elem();
        m.i64f(2, meta.2);
        m.begin_struct(3);
        m.i32f(1, meta.0);
        m.list(2, 5, 2);
        m.zz(0); // PLAIN
        m.zz(3); // RLE
        m.list(3, 8, 1);
        m.varint(meta.1.len() as u64);
        m.b.extend_from_slice(meta.1.as_bytes());
        m.i32f(4, 0); // UNCOMPRESSED
        m.i64f(5, nrows as i64);
        m.i64f(6, meta.3);
        m.i64f(7, meta.3);
        m.i64f(9, meta.2);
        m.end_struct();
        m.end_struct();
    }
    m.i64f(2, total);
    m.i64f(3, nrows as i64);
    m.end_struct();
    m.b.push(0);
    f.write_all(&m.b)?;
    f.write_all(&(m.b.len() as u32).to_le_bytes())?;
    f.write_all(b"PAR1")?;
    Ok(())
}

/// The bytes of a slice of plain integers (little-endian targets only).
pub fn bytes_of<T: Copy>(v: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, std::mem::size_of_val(v)) }
}
