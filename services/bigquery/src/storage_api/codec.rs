//! Storage API row codecs: the table column model shared by the Avro and
//! Arrow read encoders, and the Protobuf append decoder. Columns may be
//! scalar (`STRING`, `BYTES`, `INT64`, `FLOAT64`, `BOOL`) or `RECORD`/`STRUCT`,
//! in `NULLABLE`, `REQUIRED` or `REPEATED` mode.

use std::collections::{BTreeMap, HashMap, HashSet};

use base64::{engine::general_purpose::STANDARD, Engine};
use prost::encoding::{self, DecodeContext, WireType};
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::DescriptorProto;
use serde_json::{json, value::to_raw_value, Map, Value};
use tonic::Status;

use crate::model::{RawJson, StoredRow, TableFieldSchema};
use crate::sql_eval::{raw_float, raw_int};

use super::proto::AvroSchema;

/// BigQuery's nesting limit for `RECORD` columns.
const MAX_DEPTH: usize = 15;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Scalar {
    String,
    Bytes,
    Integer,
    Float,
    Boolean,
}

impl Scalar {
    fn from_table_type(field_type: &str) -> Result<Self, Status> {
        match field_type.to_ascii_uppercase().as_str() {
            "" | "STRING" => Ok(Self::String),
            "BYTES" => Ok(Self::Bytes),
            "INTEGER" | "INT64" => Ok(Self::Integer),
            "FLOAT" | "FLOAT64" => Ok(Self::Float),
            "BOOLEAN" | "BOOL" => Ok(Self::Boolean),
            _ => Err(Status::unimplemented("unsupported Storage API column type")),
        }
    }

    fn from_proto(field_type: Type) -> Result<Self, Status> {
        match field_type {
            Type::String => Ok(Self::String),
            Type::Bytes => Ok(Self::Bytes),
            Type::Int32
            | Type::Int64
            | Type::Sint32
            | Type::Sint64
            | Type::Sfixed32
            | Type::Sfixed64
            | Type::Uint32 => Ok(Self::Integer),
            Type::Float | Type::Double => Ok(Self::Float),
            Type::Bool => Ok(Self::Boolean),
            _ => Err(Status::unimplemented("unsupported Protobuf field type")),
        }
    }

    fn avro_name(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Bytes => "bytes",
            Self::Integer => "long",
            Self::Float => "double",
            Self::Boolean => "boolean",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Mode {
    Nullable,
    Required,
    Repeated,
}

#[derive(Debug)]
pub(super) enum Kind {
    Scalar(Scalar),
    Record(Vec<Column>),
}

#[derive(Debug)]
pub(super) struct Column {
    pub(super) name: String,
    pub(super) kind: Kind,
    pub(super) mode: Mode,
}

impl Column {
    fn build(field: &TableFieldSchema, depth: usize) -> Result<Self, Status> {
        if depth > MAX_DEPTH {
            return Err(Status::invalid_argument("columns are nested too deeply"));
        }
        validate_name(&field.name)?;
        let mode = match field.mode.to_ascii_uppercase().as_str() {
            "REQUIRED" => Mode::Required,
            "REPEATED" => Mode::Repeated,
            _ => Mode::Nullable,
        };
        let kind = if matches!(
            field.field_type.to_ascii_uppercase().as_str(),
            "RECORD" | "STRUCT"
        ) {
            if field.fields.is_empty() {
                return Err(Status::invalid_argument("RECORD column has no fields"));
            }
            Kind::Record(columns(field.fields.iter(), depth + 1)?)
        } else {
            Kind::Scalar(Scalar::from_table_type(&field.field_type)?)
        };
        Ok(Self {
            name: field.name.clone(),
            kind,
            mode,
        })
    }
}

/// Builds columns, rejecting duplicate names.
pub(super) fn table_columns<'a>(
    fields: impl Iterator<Item = &'a TableFieldSchema>,
) -> Result<Vec<Column>, Status> {
    columns(fields, 1)
}

fn columns<'a>(
    fields: impl Iterator<Item = &'a TableFieldSchema>,
    depth: usize,
) -> Result<Vec<Column>, Status> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for field in fields {
        if !seen.insert(field.name.to_ascii_lowercase()) {
            return Err(Status::invalid_argument("duplicate column name"));
        }
        out.push(Column::build(field, depth)?);
    }
    Ok(out)
}

fn validate_name(name: &str) -> Result<(), Status> {
    let mut bytes = name.bytes();
    let valid_first = bytes
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == b'_');
    if !valid_first || !bytes.all(|c| c.is_ascii_alphanumeric() || c == b'_') {
        return Err(Status::invalid_argument("invalid column name"));
    }
    Ok(())
}

pub(super) fn invalid_row() -> Status {
    Status::failed_precondition("stored row does not match the table schema")
}

/// Parses a stored row's top-level values for the given columns.
pub(super) fn row_values<'a>(row: &StoredRow, columns: &'a [Column]) -> Vec<(&'a Column, Value)> {
    columns
        .iter()
        .map(|column| {
            let value = row
                .json
                .get(&column.name)
                .and_then(|raw| serde_json::from_str(raw.get()).ok())
                .unwrap_or(Value::Null);
            (column, value)
        })
        .collect()
}

/// A scalar JSON value as stored by insertAll (numbers may be JSON strings).
pub(super) fn scalar_text(value: &Value) -> String {
    value.to_string()
}

pub(super) fn int_value(value: &Value) -> Result<i64, Status> {
    raw_int(&scalar_text(value)).ok_or_else(invalid_row)
}

pub(super) fn float_value(value: &Value) -> Result<f64, Status> {
    raw_float(&scalar_text(value)).ok_or_else(invalid_row)
}

pub(super) fn bool_value(value: &Value) -> Result<bool, Status> {
    value.as_bool().ok_or_else(invalid_row)
}

pub(super) fn bytes_value(scalar: Scalar, value: &Value) -> Result<Vec<u8>, Status> {
    let text = value.as_str().ok_or_else(invalid_row)?;
    if scalar == Scalar::Bytes {
        STANDARD.decode(text).map_err(|_| invalid_row())
    } else {
        Ok(text.as_bytes().to_vec())
    }
}

/// Elements of a `REPEATED` value (`null` reads as an empty array).
pub(super) fn repeated_items(value: &Value) -> Result<&[Value], Status> {
    match value {
        Value::Null => Ok(&[]),
        Value::Array(items) => Ok(items),
        _ => Err(invalid_row()),
    }
}

pub(super) fn record_field<'a>(value: &'a Value, name: &str) -> &'a Value {
    match value {
        Value::Object(map) => map.get(name).unwrap_or(&Value::Null),
        _ => &Value::Null,
    }
}

// --- Avro -------------------------------------------------------------------

pub(super) struct AvroEncoder(Vec<Column>);

impl AvroEncoder {
    pub(super) fn new(columns: Vec<Column>) -> Self {
        Self(columns)
    }

    pub(super) fn schema(&self) -> AvroSchema {
        AvroSchema {
            schema: json!({
                "type": "record",
                "name": "Row",
                "fields": avro_fields(&self.0, "Row"),
            })
            .to_string(),
        }
    }

    pub(super) fn encode(&self, row: &StoredRow) -> Result<Vec<u8>, Status> {
        let mut out = Vec::new();
        for (column, value) in row_values(row, &self.0) {
            avro_column(column, &value, &mut out)?;
        }
        Ok(out)
    }
}

fn avro_fields(columns: &[Column], namespace: &str) -> Vec<Value> {
    columns
        .iter()
        .map(|column| {
            let item = match &column.kind {
                Kind::Scalar(scalar) => json!(scalar.avro_name()),
                Kind::Record(children) => {
                    let nested = format!("{namespace}.{}", column.name);
                    json!({
                        "type": "record",
                        "name": column.name,
                        "namespace": namespace,
                        "fields": avro_fields(children, &nested),
                    })
                }
            };
            match column.mode {
                Mode::Required => json!({"name": column.name, "type": item}),
                Mode::Repeated => {
                    json!({"name": column.name, "type": {"type": "array", "items": item}})
                }
                Mode::Nullable => {
                    json!({"name": column.name, "type": ["null", item], "default": null})
                }
            }
        })
        .collect()
}

fn avro_column(column: &Column, value: &Value, out: &mut Vec<u8>) -> Result<(), Status> {
    match column.mode {
        Mode::Repeated => {
            let items = repeated_items(value)?;
            if !items.is_empty() {
                avro_long(items.len() as i64, out);
                for item in items {
                    avro_value(&column.kind, item, out)?;
                }
            }
            avro_long(0, out);
            Ok(())
        }
        Mode::Nullable => {
            avro_long(i64::from(!value.is_null()), out);
            if value.is_null() {
                return Ok(());
            }
            avro_value(&column.kind, value, out)
        }
        Mode::Required => avro_value(&column.kind, value, out),
    }
}

fn avro_value(kind: &Kind, value: &Value, out: &mut Vec<u8>) -> Result<(), Status> {
    if value.is_null() {
        return Err(invalid_row());
    }
    match kind {
        Kind::Record(children) => {
            for child in children {
                avro_column(child, record_field(value, &child.name), out)?;
            }
        }
        Kind::Scalar(scalar @ (Scalar::String | Scalar::Bytes)) => {
            let data = bytes_value(*scalar, value)?;
            avro_long(data.len() as i64, out);
            out.extend(data);
        }
        Kind::Scalar(Scalar::Integer) => avro_long(int_value(value)?, out),
        Kind::Scalar(Scalar::Float) => out.extend(float_value(value)?.to_le_bytes()),
        Kind::Scalar(Scalar::Boolean) => out.push(u8::from(bool_value(value)?)),
    }
    Ok(())
}

fn avro_long(value: i64, out: &mut Vec<u8>) {
    encoding::encode_varint(((value << 1) ^ (value >> 63)) as u64, out);
}

// --- Protobuf ---------------------------------------------------------------

enum ProtoKind {
    Scalar(Type),
    Message(ProtoMessage),
}

struct ProtoField {
    name: String,
    kind: ProtoKind,
    repeated: bool,
    required: bool,
}

struct ProtoMessage(HashMap<u32, ProtoField>);

pub(super) struct ProtoDecoder(ProtoMessage);

impl ProtoDecoder {
    /// Accepts a self-contained descriptor: message-typed fields must refer to
    /// (possibly nested) `nested_type` entries of the root descriptor.
    pub(super) fn new(descriptor: DescriptorProto) -> Result<Self, Status> {
        let mut types = HashMap::new();
        collect_types(&descriptor, &mut types);
        Ok(Self(build_message(&descriptor, &types, 1)?))
    }

    pub(super) fn check_table(&self, fields: &[TableFieldSchema]) -> Result<(), Status> {
        check_message(&self.0, fields)
    }

    pub(super) fn decode(&self, data: &[u8]) -> Result<BTreeMap<String, RawJson>, &'static str> {
        let Value::Object(map) = decode_message(&self.0, data)? else {
            unreachable!("messages decode to objects")
        };
        map.into_iter()
            .map(|(name, value)| Ok((name, to_raw_value(&value).map_err(|_| "invalid row value")?)))
            .collect()
    }
}

fn collect_types<'a>(
    descriptor: &'a DescriptorProto,
    types: &mut HashMap<&'a str, &'a DescriptorProto>,
) {
    for nested in &descriptor.nested_type {
        types.insert(nested.name(), nested);
        collect_types(nested, types);
    }
}

fn build_message(
    descriptor: &DescriptorProto,
    types: &HashMap<&str, &DescriptorProto>,
    depth: usize,
) -> Result<ProtoMessage, Status> {
    if depth > MAX_DEPTH {
        return Err(Status::invalid_argument(
            "Protobuf messages are nested too deeply",
        ));
    }
    let mut names = HashSet::new();
    let mut fields = HashMap::new();
    for field in &descriptor.field {
        if field.oneof_index.is_some() && !field.proto3_optional() {
            return Err(Status::unimplemented(
                "oneof Protobuf fields are unsupported",
            ));
        }
        let field_type = Type::try_from(field.r#type.unwrap_or_default())
            .map_err(|_| Status::invalid_argument("invalid Protobuf field type"))?;
        let kind = if field_type == Type::Message {
            let type_name = field.type_name();
            let short = type_name.rsplit('.').next().unwrap_or(type_name);
            let nested = types.get(short).ok_or_else(|| {
                Status::invalid_argument("Protobuf message field type is not a nested type")
            })?;
            ProtoKind::Message(build_message(nested, types, depth + 1)?)
        } else {
            Scalar::from_proto(field_type)?;
            ProtoKind::Scalar(field_type)
        };
        let number = field.number();
        if !(1..=536_870_911).contains(&number)
            || (19_000..=19_999).contains(&number)
            || field.name().is_empty()
            || !names.insert(field.name().to_owned())
            || fields.contains_key(&(number as u32))
        {
            return Err(Status::invalid_argument(
                "invalid or duplicate Protobuf field",
            ));
        }
        fields.insert(
            number as u32,
            ProtoField {
                name: field.name().to_owned(),
                kind,
                repeated: field.label() == Label::Repeated,
                required: field.label() == Label::Required,
            },
        );
    }
    Ok(ProtoMessage(fields))
}

fn check_message(message: &ProtoMessage, fields: &[TableFieldSchema]) -> Result<(), Status> {
    let columns: HashMap<_, _> = fields
        .iter()
        .map(|field| (field.name.as_str(), field))
        .collect();
    for field in message.0.values() {
        let column = columns.get(field.name.as_str()).ok_or_else(|| {
            Status::invalid_argument("Protobuf schema contains a column absent from the table")
        })?;
        if field.repeated != column.mode.eq_ignore_ascii_case("REPEATED") {
            return Err(Status::invalid_argument(
                "Protobuf field repetition does not match the table",
            ));
        }
        let is_record = matches!(
            column.field_type.to_ascii_uppercase().as_str(),
            "RECORD" | "STRUCT"
        );
        match &field.kind {
            ProtoKind::Message(nested) if is_record => check_message(nested, &column.fields)?,
            ProtoKind::Scalar(field_type)
                if !is_record
                    && Scalar::from_table_type(&column.field_type)?
                        == Scalar::from_proto(*field_type)? => {}
            _ => {
                return Err(Status::invalid_argument(
                    "Protobuf field type does not match the table",
                ))
            }
        }
    }
    Ok(())
}

fn decode_message(message: &ProtoMessage, mut data: &[u8]) -> Result<Value, &'static str> {
    let mut row = Map::new();
    while !data.is_empty() {
        let (tag, wire) = encoding::decode_key(&mut data).map_err(|_| "invalid Protobuf row")?;
        let field = message.0.get(&tag).ok_or("unknown Protobuf field number")?;
        let mut values = Vec::new();
        match &field.kind {
            ProtoKind::Message(nested) => {
                if wire != WireType::LengthDelimited {
                    return Err("invalid Protobuf field value");
                }
                let len = encoding::decode_varint(&mut data).map_err(|_| "invalid Protobuf row")?;
                let len = usize::try_from(len)
                    .ok()
                    .filter(|&len| len <= data.len())
                    .ok_or("invalid Protobuf row")?;
                let (body, rest) = data.split_at(len);
                data = rest;
                values.push(decode_message(nested, body)?);
            }
            ProtoKind::Scalar(field_type) => {
                decode_scalars(*field_type, wire, &mut data, field.repeated, &mut values)?
            }
        }
        if field.repeated {
            let entry = row
                .entry(field.name.clone())
                .or_insert_with(|| Value::Array(Vec::new()));
            if let Value::Array(items) = entry {
                items.extend(values);
            }
        } else if let Some(value) = values.pop() {
            // Protobuf semantics: the last occurrence of a singular field wins.
            row.insert(field.name.clone(), value);
        }
    }
    if message
        .0
        .values()
        .any(|field| field.required && !row.contains_key(&field.name))
    {
        return Err("required Protobuf field is missing");
    }
    Ok(Value::Object(row))
}

/// Decodes one (or, for packed repeated fields, several) scalar values.
fn decode_scalars(
    field_type: Type,
    wire: WireType,
    data: &mut &[u8],
    repeated: bool,
    out: &mut Vec<Value>,
) -> Result<(), &'static str> {
    macro_rules! decode {
        ($kind:ident, $map:expr) => {{
            if repeated {
                let mut values = Vec::new();
                encoding::$kind::merge_repeated(wire, &mut values, data, DecodeContext::default())
                    .map_err(|_| "invalid Protobuf field value")?;
                out.extend(values.into_iter().map($map));
            } else {
                let mut value = Default::default();
                encoding::$kind::merge(wire, &mut value, data, DecodeContext::default())
                    .map_err(|_| "invalid Protobuf field value")?;
                out.push($map(value));
            }
        }};
    }
    match field_type {
        Type::String => decode!(string, |v: String| json!(v)),
        Type::Bytes => decode!(bytes, |v: Vec<u8>| json!(STANDARD.encode(v))),
        Type::Int32 => decode!(int32, |v: i32| json!(v)),
        Type::Int64 => decode!(int64, |v: i64| json!(v)),
        Type::Sint32 => decode!(sint32, |v: i32| json!(v)),
        Type::Sint64 => decode!(sint64, |v: i64| json!(v)),
        Type::Sfixed32 => decode!(sfixed32, |v: i32| json!(v)),
        Type::Sfixed64 => decode!(sfixed64, |v: i64| json!(v)),
        Type::Uint32 => decode!(uint32, |v: u32| json!(v)),
        Type::Float => decode!(float, |v: f32| json!(f64::from(v))),
        Type::Double => decode!(double, |v: f64| json!(v)),
        Type::Bool => decode!(bool, |v: bool| json!(v)),
        _ => return Err("unsupported Protobuf field type"),
    }
    if out.iter().any(Value::is_null) {
        return Err("non-finite Protobuf floating point value");
    }
    Ok(())
}
