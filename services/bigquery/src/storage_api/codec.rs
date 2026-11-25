use std::collections::{BTreeMap, HashMap, HashSet};

use base64::{engine::general_purpose::STANDARD, Engine};
use prost::encoding::{self, WireType};
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::DescriptorProto;
use serde_json::{json, value::to_raw_value, Value};
use tonic::Status;

use crate::model::{RawJson, StoredRow, TableFieldSchema};
use crate::sql_eval::{raw_float, raw_int};

use super::proto::AvroSchema;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scalar {
    String,
    Bytes,
    Integer,
    Float,
    Boolean,
}

impl Scalar {
    fn from_table(field: &TableFieldSchema) -> Result<Self, Status> {
        if field.mode.eq_ignore_ascii_case("REPEATED") {
            return Err(Status::unimplemented("repeated columns are unsupported"));
        }
        match field.field_type.to_ascii_uppercase().as_str() {
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

struct AvroField {
    name: String,
    scalar: Scalar,
    nullable: bool,
}

pub(super) struct AvroEncoder(Vec<AvroField>);

impl AvroEncoder {
    pub(super) fn new(fields: Vec<&TableFieldSchema>) -> Result<Self, Status> {
        let mut seen = HashSet::new();
        let mut columns = Vec::with_capacity(fields.len());
        for field in fields {
            let mut name = field.name.bytes();
            let valid_first = name
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == b'_');
            if !valid_first || !name.all(|c| c.is_ascii_alphanumeric() || c == b'_') {
                return Err(Status::invalid_argument("invalid Avro column name"));
            }
            if !seen.insert(&field.name) {
                return Err(Status::invalid_argument("duplicate Avro column name"));
            }
            columns.push(AvroField {
                name: field.name.clone(),
                scalar: Scalar::from_table(field)?,
                nullable: !field.mode.eq_ignore_ascii_case("REQUIRED"),
            });
        }
        Ok(Self(columns))
    }

    pub(super) fn schema(&self) -> AvroSchema {
        let fields: Vec<_> = self
            .0
            .iter()
            .map(|field| {
                let scalar = field.scalar.avro_name();
                if field.nullable {
                    json!({"name": field.name, "type": ["null", scalar], "default": null})
                } else {
                    json!({"name": field.name, "type": scalar})
                }
            })
            .collect();
        AvroSchema {
            schema: json!({"type": "record", "name": "Row", "fields": fields}).to_string(),
        }
    }

    pub(super) fn encode(&self, row: &StoredRow) -> Result<Vec<u8>, Status> {
        let mut out = Vec::new();
        for field in &self.0 {
            let value = row.json.get(&field.name).map_or("null", |v| v.get());
            let is_null = value == "null";
            if field.nullable {
                avro_long(i64::from(!is_null), &mut out);
                if is_null {
                    continue;
                }
            }
            let invalid =
                || Status::failed_precondition("stored row does not match the Avro schema");
            match field.scalar {
                Scalar::String | Scalar::Bytes => {
                    let string: String = serde_json::from_str(value).map_err(|_| invalid())?;
                    let data = if field.scalar == Scalar::Bytes {
                        STANDARD.decode(string).map_err(|_| invalid())?
                    } else {
                        string.into_bytes()
                    };
                    avro_long(data.len() as i64, &mut out);
                    out.extend(data);
                }
                Scalar::Integer => avro_long(raw_int(value).ok_or_else(invalid)?, &mut out),
                Scalar::Float => {
                    out.extend(raw_float(value).ok_or_else(invalid)?.to_le_bytes());
                }
                Scalar::Boolean => {
                    let value: bool = serde_json::from_str(value).map_err(|_| invalid())?;
                    out.push(u8::from(value));
                }
            }
        }
        Ok(out)
    }
}

fn avro_long(value: i64, out: &mut Vec<u8>) {
    encoding::encode_varint(((value << 1) ^ (value >> 63)) as u64, out);
}

struct ProtoField {
    name: String,
    field_type: Type,
    required: bool,
}

pub(super) struct ProtoDecoder(HashMap<u32, ProtoField>);

impl ProtoDecoder {
    pub(super) fn new(descriptor: DescriptorProto) -> Result<Self, Status> {
        let mut names = HashSet::new();
        let mut fields = HashMap::new();
        for field in descriptor.field {
            if field.label() == Label::Repeated || field.oneof_index.is_some() {
                return Err(Status::unimplemented(
                    "repeated and oneof Protobuf fields are unsupported",
                ));
            }
            let field_type = Type::try_from(field.r#type.unwrap_or_default())
                .map_err(|_| Status::invalid_argument("invalid Protobuf field type"))?;
            Scalar::from_proto(field_type)?;
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
            let required = field.label() == Label::Required;
            fields.insert(
                number as u32,
                ProtoField {
                    name: field.name.unwrap_or_default(),
                    field_type,
                    required,
                },
            );
        }
        Ok(Self(fields))
    }

    pub(super) fn check_table(&self, fields: &[TableFieldSchema]) -> Result<(), Status> {
        let columns: HashMap<_, _> = fields
            .iter()
            .map(|field| (field.name.as_str(), field))
            .collect();
        for field in self.0.values() {
            let column = columns.get(field.name.as_str()).ok_or_else(|| {
                Status::invalid_argument("Protobuf schema contains a column absent from the table")
            })?;
            if Scalar::from_table(column)? != Scalar::from_proto(field.field_type)? {
                return Err(Status::invalid_argument(
                    "Protobuf field type does not match the table",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn decode(
        &self,
        mut data: &[u8],
    ) -> Result<BTreeMap<String, RawJson>, &'static str> {
        let mut row = BTreeMap::new();
        while !data.is_empty() {
            let (tag, wire) =
                encoding::decode_key(&mut data).map_err(|_| "invalid Protobuf row")?;
            let field = self.0.get(&tag).ok_or("unknown Protobuf field number")?;
            let value = decode_scalar(field.field_type, wire, &mut data)?;
            row.insert(
                field.name.clone(),
                to_raw_value(&value).map_err(|_| "invalid row value")?,
            );
        }
        if self
            .0
            .values()
            .any(|field| field.required && !row.contains_key(&field.name))
        {
            return Err("required Protobuf field is missing");
        }
        Ok(row)
    }
}

fn decode_scalar(
    field_type: Type,
    wire: WireType,
    data: &mut &[u8],
) -> Result<Value, &'static str> {
    macro_rules! decode {
        ($kind:ident) => {{
            let mut value = Default::default();
            encoding::$kind::merge(wire, &mut value, data, Default::default())
                .map_err(|_| "invalid Protobuf field value")?;
            value
        }};
    }
    let value = match field_type {
        Type::String => json!(decode!(string)),
        Type::Bytes => {
            let bytes: Vec<u8> = decode!(bytes);
            json!(STANDARD.encode(bytes))
        }
        Type::Int32 => json!(decode!(int32)),
        Type::Int64 => json!(decode!(int64)),
        Type::Sint32 => json!(decode!(sint32)),
        Type::Sint64 => json!(decode!(sint64)),
        Type::Sfixed32 => json!(decode!(sfixed32)),
        Type::Sfixed64 => json!(decode!(sfixed64)),
        Type::Uint32 => json!(decode!(uint32)),
        Type::Float => json!(f64::from(decode!(float))),
        Type::Double => json!(decode!(double)),
        Type::Bool => json!(decode!(bool)),
        _ => return Err("unsupported Protobuf field type"),
    };
    if value.is_null() {
        return Err("non-finite Protobuf floating point value");
    }
    Ok(value)
}
