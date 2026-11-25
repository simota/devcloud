//! Arrow IPC encoding for Storage API reads, written by hand (no Arrow
//! dependency): an encapsulated `Schema` message for the session and one
//! encapsulated `RecordBatch` message (metadata + body) per `ReadRows`
//! response, as `pyarrow.ipc.read_schema` / `read_record_batch` and Arrow
//! stream readers expect.
//!
//! Type mapping: `STRING` → Utf8, `BYTES` → Binary, `INT64` → Int64,
//! `FLOAT64` → Float64, `BOOL` → Bool, `RECORD` → Struct, `REPEATED` → List
//! (non-null, child `item`). Buffers are uncompressed.

use serde_json::Value;
use tonic::Status;

use crate::model::StoredRow;

use super::codec::{
    bool_value, bytes_value, float_value, int_value, invalid_row, record_field, repeated_items,
    row_values, Column, Kind, Mode, Scalar,
};

pub(super) struct ArrowEncoder(Vec<Column>);

impl ArrowEncoder {
    pub(super) fn new(columns: Vec<Column>) -> Self {
        Self(columns)
    }

    pub(super) fn schema(&self) -> Vec<u8> {
        let fields = self.0.iter().map(field_table).collect();
        let schema = Table(vec![(0, Val::I16(0)), (1, Val::Ref(Obj::Tables(fields)))]);
        encapsulate(message(1, schema, 0), &[])
    }

    pub(super) fn record_batch(&self, rows: &[StoredRow]) -> Result<Vec<u8>, Status> {
        let mut builders: Vec<Builder> = self.0.iter().map(Builder::for_column).collect();
        for row in rows {
            for ((column, value), builder) in
                row_values(row, &self.0).into_iter().zip(&mut builders)
            {
                builder.append_column(column, &value)?;
            }
        }
        let mut layout = Layout::default();
        for builder in &builders {
            builder.write(&mut layout);
        }
        let mut nodes = Vec::new();
        for (length, nulls) in &layout.nodes {
            nodes.extend((*length as i64).to_le_bytes());
            nodes.extend((*nulls as i64).to_le_bytes());
        }
        let mut buffers = Vec::new();
        for (offset, length) in &layout.buffers {
            buffers.extend((*offset as i64).to_le_bytes());
            buffers.extend((*length as i64).to_le_bytes());
        }
        let batch = Table(vec![
            (0, Val::I64(rows.len() as i64)),
            (1, Val::Ref(Obj::Structs(nodes, layout.nodes.len()))),
            (2, Val::Ref(Obj::Structs(buffers, layout.buffers.len()))),
        ]);
        let body = layout.body;
        Ok(encapsulate(message(3, batch, body.len()), &body))
    }
}

// --- schema -----------------------------------------------------------------

fn field_table(column: &Column) -> Table {
    let element = |kind: &Kind| -> (u8, Table, Vec<Table>) {
        match kind {
            Kind::Scalar(Scalar::Integer) => (
                2,
                Table(vec![(0, Val::I32(64)), (1, Val::Bool(true))]),
                vec![],
            ),
            Kind::Scalar(Scalar::Float) => (3, Table(vec![(0, Val::I16(2))]), vec![]),
            Kind::Scalar(Scalar::Bytes) => (4, Table(vec![]), vec![]),
            Kind::Scalar(Scalar::String) => (5, Table(vec![]), vec![]),
            Kind::Scalar(Scalar::Boolean) => (6, Table(vec![]), vec![]),
            Kind::Record(children) => (
                13,
                Table(vec![]),
                children.iter().map(field_table).collect(),
            ),
        }
    };
    let (type_id, type_table, children, nullable) = if column.mode == Mode::Repeated {
        let (id, table, grandchildren) = element(&column.kind);
        let item = Table(vec![
            (0, Val::Ref(Obj::Str("item".into()))),
            (1, Val::Bool(false)),
            (2, Val::U8(id)),
            (3, Val::Ref(Obj::Table(table))),
            (5, Val::Ref(Obj::Tables(grandchildren))),
        ]);
        (12, Table(vec![]), vec![item], false)
    } else {
        let (id, table, children) = element(&column.kind);
        (id, table, children, column.mode == Mode::Nullable)
    };
    Table(vec![
        (0, Val::Ref(Obj::Str(column.name.clone()))),
        (1, Val::Bool(nullable)),
        (2, Val::U8(type_id)),
        (3, Val::Ref(Obj::Table(type_table))),
        (5, Val::Ref(Obj::Tables(children))),
    ])
}

/// `Message { version: V5, header_type, header, bodyLength }`.
fn message(header_type: u8, header: Table, body_length: usize) -> Vec<u8> {
    finish(Table(vec![
        (0, Val::I16(4)),
        (1, Val::U8(header_type)),
        (2, Val::Ref(Obj::Table(header))),
        (3, Val::I64(body_length as i64)),
    ]))
}

/// IPC encapsulation: continuation marker, metadata length, 8-byte aligned
/// flatbuffer, then the body.
fn encapsulate(mut metadata: Vec<u8>, body: &[u8]) -> Vec<u8> {
    while !metadata.len().is_multiple_of(8) {
        metadata.push(0);
    }
    let mut out = Vec::with_capacity(8 + metadata.len() + body.len());
    out.extend(0xFFFF_FFFFu32.to_le_bytes());
    out.extend((metadata.len() as i32).to_le_bytes());
    out.extend(metadata);
    out.extend(body);
    out
}

// --- columnar builders ------------------------------------------------------

#[derive(Default)]
struct Bitmap {
    bytes: Vec<u8>,
    len: usize,
}

impl Bitmap {
    fn push(&mut self, bit: bool) {
        if self.len.is_multiple_of(8) {
            self.bytes.push(0);
        }
        if bit {
            self.bytes[self.len / 8] |= 1 << (self.len % 8);
        }
        self.len += 1;
    }
}

#[derive(Default)]
struct Validity {
    bits: Bitmap,
    nulls: usize,
}

impl Validity {
    fn push(&mut self, valid: bool) {
        self.bits.push(valid);
        self.nulls += usize::from(!valid);
    }
}

enum Builder {
    Fixed {
        scalar: Scalar,
        validity: Validity,
        data: Vec<u8>,
        bits: Bitmap,
    },
    Variable {
        scalar: Scalar,
        validity: Validity,
        offsets: Vec<i32>,
        data: Vec<u8>,
    },
    Struct {
        validity: Validity,
        children: Vec<Builder>,
    },
    List {
        validity: Validity,
        offsets: Vec<i32>,
        child: Box<Builder>,
    },
}

impl Builder {
    fn for_column(column: &Column) -> Self {
        let element = Self::for_kind(&column.kind);
        if column.mode == Mode::Repeated {
            Self::List {
                validity: Validity::default(),
                offsets: vec![0],
                child: Box::new(element),
            }
        } else {
            element
        }
    }

    fn for_kind(kind: &Kind) -> Self {
        match kind {
            Kind::Scalar(scalar @ (Scalar::String | Scalar::Bytes)) => Self::Variable {
                scalar: *scalar,
                validity: Validity::default(),
                offsets: vec![0],
                data: Vec::new(),
            },
            Kind::Scalar(scalar) => Self::Fixed {
                scalar: *scalar,
                validity: Validity::default(),
                data: Vec::new(),
                bits: Bitmap::default(),
            },
            Kind::Record(children) => Self::Struct {
                validity: Validity::default(),
                children: children.iter().map(Self::for_column).collect(),
            },
        }
    }

    fn append_column(&mut self, column: &Column, value: &Value) -> Result<(), Status> {
        match (self, column.mode) {
            (
                Self::List {
                    validity,
                    offsets,
                    child,
                },
                Mode::Repeated,
            ) => {
                let items = repeated_items(value)?;
                for item in items {
                    if item.is_null() {
                        return Err(invalid_row());
                    }
                    child.append_value(&column.kind, item)?;
                }
                validity.push(true);
                let end = offsets.last().copied().unwrap_or_default() + items.len() as i32;
                offsets.push(end);
                Ok(())
            }
            (builder, mode) => {
                if value.is_null() && mode == Mode::Required {
                    return Err(invalid_row());
                }
                builder.append_value(&column.kind, value)
            }
        }
    }

    /// Appends one element (`null` appends a null slot).
    fn append_value(&mut self, kind: &Kind, value: &Value) -> Result<(), Status> {
        let valid = !value.is_null();
        match self {
            Self::Fixed {
                scalar,
                validity,
                data,
                bits,
            } => {
                match scalar {
                    Scalar::Integer => {
                        let v = if valid { int_value(value)? } else { 0 };
                        data.extend(v.to_le_bytes());
                    }
                    Scalar::Float => {
                        let v = if valid { float_value(value)? } else { 0.0 };
                        data.extend(v.to_le_bytes());
                    }
                    _ => bits.push(valid && bool_value(value)?),
                }
                validity.push(valid);
            }
            Self::Variable {
                scalar,
                validity,
                offsets,
                data,
            } => {
                if valid {
                    data.extend(bytes_value(*scalar, value)?);
                }
                let end = i32::try_from(data.len())
                    .map_err(|_| Status::resource_exhausted("Arrow column exceeds size limit"))?;
                offsets.push(end);
                validity.push(valid);
            }
            Self::Struct { validity, children } => {
                let Kind::Record(columns) = kind else {
                    unreachable!("struct builders come from RECORD columns")
                };
                for (child, column) in children.iter_mut().zip(columns) {
                    let field = if valid {
                        record_field(value, &column.name)
                    } else {
                        &Value::Null
                    };
                    if !valid && column.mode != Mode::Repeated {
                        // A null struct still needs a (null) slot in each child.
                        child.append_value(&column.kind, &Value::Null)?;
                    } else {
                        child.append_column(column, field)?;
                    }
                }
                validity.push(valid);
            }
            Self::List { .. } => unreachable!("lists only appear for REPEATED columns"),
        }
        Ok(())
    }

    fn len(&self) -> usize {
        match self {
            Self::Fixed { validity, .. }
            | Self::Variable { validity, .. }
            | Self::Struct { validity, .. }
            | Self::List { validity, .. } => validity.bits.len,
        }
    }

    /// Emits field nodes and buffers in pre-order, as the IPC format requires.
    fn write(&self, layout: &mut Layout) {
        match self {
            Self::Fixed {
                scalar,
                validity,
                data,
                bits,
            } => {
                layout.node(self.len(), validity);
                if *scalar == Scalar::Boolean {
                    layout.buffer(&bits.bytes);
                } else {
                    layout.buffer(data);
                }
            }
            Self::Variable {
                validity,
                offsets,
                data,
                ..
            } => {
                layout.node(self.len(), validity);
                layout.buffer(&i32_bytes(offsets));
                layout.buffer(data);
            }
            Self::Struct { validity, children } => {
                layout.node(self.len(), validity);
                for child in children {
                    child.write(layout);
                }
            }
            Self::List {
                validity,
                offsets,
                child,
            } => {
                layout.node(self.len(), validity);
                layout.buffer(&i32_bytes(offsets));
                child.write(layout);
            }
        }
    }
}

fn i32_bytes(values: &[i32]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}

#[derive(Default)]
struct Layout {
    nodes: Vec<(usize, usize)>,
    buffers: Vec<(usize, usize)>,
    body: Vec<u8>,
}

impl Layout {
    /// Field node plus its validity buffer (empty when nothing is null).
    fn node(&mut self, length: usize, validity: &Validity) {
        self.nodes.push((length, validity.nulls));
        if validity.nulls == 0 {
            self.buffer(&[]);
        } else {
            self.buffer(&validity.bits.bytes);
        }
    }

    fn buffer(&mut self, data: &[u8]) {
        self.buffers.push((self.body.len(), data.len()));
        self.body.extend(data);
        while !self.body.len().is_multiple_of(8) {
            self.body.push(0);
        }
    }
}

// --- minimal flatbuffers writer ----------------------------------------------

/// A flatbuffer table: `(field slot, value)` pairs.
struct Table(Vec<(u16, Val)>);

enum Val {
    U8(u8),
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    Ref(Obj),
}

enum Obj {
    Table(Table),
    Str(String),
    Tables(Vec<Table>),
    /// Vector of 8-byte aligned structs: raw bytes and element count.
    Structs(Vec<u8>, usize),
}

impl Val {
    fn size(&self) -> usize {
        match self {
            Val::U8(_) | Val::Bool(_) => 1,
            Val::I16(_) => 2,
            Val::I32(_) | Val::Ref(_) => 4,
            Val::I64(_) => 8,
        }
    }
}

/// Serializes a root table front to back: every child object is written
/// after the offset that points at it, so all `uoffset`s are forward.
fn finish(root: Table) -> Vec<u8> {
    let mut buf = vec![0; 4];
    let pos = write_table(&mut buf, root);
    patch(&mut buf, 0, pos);
    buf
}

fn align(buf: &mut Vec<u8>, to: usize) {
    while !buf.len().is_multiple_of(to) {
        buf.push(0);
    }
}

fn patch(buf: &mut [u8], at: usize, target: usize) {
    buf[at..at + 4].copy_from_slice(&((target - at) as u32).to_le_bytes());
}

fn write_table(buf: &mut Vec<u8>, table: Table) -> usize {
    let mut fields = table.0;
    fields.sort_by_key(|(_, value)| std::cmp::Reverse(value.size()));
    let mut offsets = Vec::with_capacity(fields.len());
    let mut size: usize = 4; // soffset to the vtable
    for (_, value) in &fields {
        let width = value.size();
        size = size.div_ceil(width) * width;
        offsets.push(size);
        size += width;
    }
    let size = size.div_ceil(4) * 4;
    let slots = fields
        .iter()
        .map(|(slot, _)| *slot as usize + 1)
        .max()
        .unwrap_or(0);
    let mut vtable = vec![0u16; slots];
    for ((slot, _), offset) in fields.iter().zip(&offsets) {
        vtable[*slot as usize] = *offset as u16;
    }

    align(buf, 2);
    let vtable_pos = buf.len();
    buf.extend(((4 + 2 * slots) as u16).to_le_bytes());
    buf.extend((size as u16).to_le_bytes());
    for entry in vtable {
        buf.extend(entry.to_le_bytes());
    }
    align(buf, 8);
    let table_pos = buf.len();
    buf.extend(((table_pos - vtable_pos) as i32).to_le_bytes());
    buf.resize(table_pos + size, 0);

    let mut children = Vec::new();
    for ((_, value), offset) in fields.into_iter().zip(offsets) {
        let at = table_pos + offset;
        let bytes: Vec<u8> = match value {
            Val::U8(v) => vec![v],
            Val::Bool(v) => vec![u8::from(v)],
            Val::I16(v) => v.to_le_bytes().to_vec(),
            Val::I32(v) => v.to_le_bytes().to_vec(),
            Val::I64(v) => v.to_le_bytes().to_vec(),
            Val::Ref(obj) => {
                children.push((at, obj));
                continue;
            }
        };
        buf[at..at + bytes.len()].copy_from_slice(&bytes);
    }
    for (at, obj) in children {
        let pos = write_obj(buf, obj);
        patch(buf, at, pos);
    }
    table_pos
}

fn write_obj(buf: &mut Vec<u8>, obj: Obj) -> usize {
    match obj {
        Obj::Table(table) => write_table(buf, table),
        Obj::Str(text) => {
            align(buf, 4);
            let pos = buf.len();
            buf.extend((text.len() as u32).to_le_bytes());
            buf.extend(text.as_bytes());
            buf.push(0);
            pos
        }
        Obj::Tables(tables) => {
            align(buf, 4);
            let pos = buf.len();
            buf.extend((tables.len() as u32).to_le_bytes());
            let slots = buf.len();
            buf.resize(slots + 4 * tables.len(), 0);
            for (i, table) in tables.into_iter().enumerate() {
                let table_pos = write_table(buf, table);
                patch(buf, slots + 4 * i, table_pos);
            }
            pos
        }
        Obj::Structs(bytes, count) => {
            while buf.len() % 8 != 4 {
                buf.push(0);
            }
            let pos = buf.len();
            buf.extend((count as u32).to_le_bytes());
            buf.extend(bytes);
            pos
        }
    }
}
