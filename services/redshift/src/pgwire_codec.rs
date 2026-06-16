//! PostgreSQL wire protocol message encode/decode.
//!
//! Parity: `internal/services/redshift/pgwire.rs` (protocol constants) and
//! `internal/services/redshift/pgwire_codec.rs`. Encoders are pure functions
//! that frame a message body onto any `Write`; decoders are pure functions
//! over byte buffers (`PayloadReader` mirrors legacy `bytes.Reader` usage).

use std::collections::HashMap;
use std::io::{self, Read, Write};

use crate::pg_types::{PgField, PG_DEFAULT_BACKEND_PID};
use crate::server::default_str;

pub const PG_PROTOCOL_VERSION: i32 = 196608;
pub const PG_SSL_REQUEST_CODE: i32 = 80877103;
pub const PG_AUTH_OK: i32 = 0;
pub const PG_AUTH_CLEARTEXT: i32 = 3;
pub const PG_TRANSACTION_IDLE: u8 = b'I';
pub const PG_DEFAULT_SECRET_KEY: i32 = 0;

/// Mirrors `readMessagePayload`: 4-byte big-endian length (inclusive of
/// itself) followed by `length - 4` payload bytes.
pub fn read_message_payload(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut length_bytes = [0u8; 4];
    r.read_exact(&mut length_bytes)?;
    let length = u32::from_be_bytes(length_bytes) as usize;
    if length < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid PostgreSQL message length",
        ));
    }
    let mut payload = vec![0u8; length - 4];
    r.read_exact(&mut payload)?;
    Ok(payload)
}

/// Mirrors `readPasswordMessage`: a typed `p` message whose payload is a
/// NUL-terminated password.
pub fn read_password_message(r: &mut impl Read) -> io::Result<String> {
    let mut message_type = [0u8; 1];
    r.read_exact(&mut message_type)?;
    if message_type[0] != b'p' {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected password message",
        ));
    }
    let payload = read_message_payload(r)?;
    Ok(read_cstring(&payload))
}

/// Mirrors `parseStartupParameters`: alternating NUL-terminated key/value
/// pairs; an empty key terminates the list.
pub fn parse_startup_parameters(payload: &[u8]) -> HashMap<String, String> {
    let parts: Vec<&[u8]> = payload.split(|byte| *byte == 0).collect();
    let mut params = HashMap::new();
    let mut i = 0;
    while i + 1 < parts.len() {
        let key = String::from_utf8_lossy(parts[i]);
        if key.is_empty() {
            break;
        }
        params.insert(
            key.into_owned(),
            String::from_utf8_lossy(parts[i + 1]).into_owned(),
        );
        i += 2;
    }
    params
}

/// Mirrors `readCString`: everything up to the first NUL (or the whole buffer).
pub fn read_cstring(payload: &[u8]) -> String {
    match payload.iter().position(|byte| *byte == 0) {
        Some(idx) => String::from_utf8_lossy(&payload[..idx]).into_owned(),
        None => String::from_utf8_lossy(payload).into_owned(),
    }
}

/// Sequential reader over a message payload. Mirrors the legacy handlers' use of
/// `bytes.Reader` + `readCStringFromReader` / `readInt16FromReader` /
/// `readInt32FromReader`; every accessor returns `None` on truncation.
pub struct PayloadReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> PayloadReader<'a> {
    pub fn new(data: &'a [u8]) -> PayloadReader<'a> {
        PayloadReader { data, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    /// Mirrors `readCStringFromReader`: fails when EOF arrives before the NUL.
    pub fn cstring(&mut self) -> Option<String> {
        let rest = &self.data[self.pos..];
        let idx = rest.iter().position(|byte| *byte == 0)?;
        self.pos += idx + 1;
        Some(String::from_utf8_lossy(&rest[..idx]).into_owned())
    }

    pub fn i16(&mut self) -> Option<i16> {
        let bytes = self.bytes(2)?;
        Some(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    pub fn i32(&mut self) -> Option<i32> {
        let bytes = self.bytes(4)?;
        Some(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn bytes(&mut self, count: usize) -> Option<&'a [u8]> {
        if self.remaining() < count {
            return None;
        }
        let slice = &self.data[self.pos..self.pos + count];
        self.pos += count;
        Some(slice)
    }

    /// Mirrors `readInt16Values` (negative counts are protocol errors).
    pub fn i16_values(&mut self, count: i16) -> Option<Vec<i16>> {
        if count < 0 {
            return None;
        }
        let mut values = Vec::with_capacity(count as usize);
        for _ in 0..count {
            values.push(self.i16()?);
        }
        Some(values)
    }
}

/// Mirrors `parseDescribeOrClosePayload`: a target byte (`S` or `P`) followed
/// by a NUL-terminated name.
pub fn parse_describe_or_close_payload(payload: &[u8]) -> Option<(u8, String)> {
    let (&target, rest) = payload.split_first()?;
    let name = PayloadReader::new(rest).cstring()?;
    if target != b'S' && target != b'P' {
        return None;
    }
    Some((target, name))
}

/// Mirrors `writeMessage`: optional tag byte (0 = untagged startup-style
/// framing), then big-endian length (body + 4), then the body.
pub fn write_message(w: &mut impl Write, message_type: u8, body: &[u8]) -> io::Result<()> {
    if message_type != 0 {
        w.write_all(&[message_type])?;
    }
    w.write_all(&((body.len() as u32 + 4).to_be_bytes()))?;
    w.write_all(body)
}

/// Mirrors `writeCString` (body-building flavor).
pub fn put_cstring(buf: &mut Vec<u8>, value: &str) {
    buf.extend_from_slice(value.as_bytes());
    buf.push(0);
}

pub fn put_i16(buf: &mut Vec<u8>, value: i16) {
    buf.extend_from_slice(&value.to_be_bytes());
}

pub fn put_i32(buf: &mut Vec<u8>, value: i32) {
    buf.extend_from_slice(&value.to_be_bytes());
}

/// Mirrors `writeAuthCleartextPassword`.
pub fn write_auth_cleartext_password(w: &mut impl Write) -> io::Result<()> {
    write_message(w, b'R', &PG_AUTH_CLEARTEXT.to_be_bytes())
}

/// Mirrors `writeAuthenticationOK`.
pub fn write_authentication_ok(w: &mut impl Write) -> io::Result<()> {
    write_message(w, b'R', &PG_AUTH_OK.to_be_bytes())
}

/// Mirrors `writeParameterStatuses`. legacy iterates a map (random order); the
/// order is not part of the protocol contract, so a fixed order is used here.
/// Empty values are skipped exactly like legacy.
pub fn write_parameter_statuses(
    w: &mut impl Write,
    startup_params: &HashMap<String, String>,
) -> io::Result<()> {
    let param = |key: &str| startup_params.get(key).cloned().unwrap_or_default();
    let client_encoding = default_str(&param("client_encoding"), "UTF8");
    let session_authorization = default_str(&param("user"), "dev");
    let statuses = [
        ("server_version", "8.0.2".to_string()),
        ("server_encoding", "UTF8".to_string()),
        ("client_encoding", client_encoding),
        ("DateStyle", "ISO, MDY".to_string()),
        ("integer_datetimes", "on".to_string()),
        ("standard_conforming_strings", "on".to_string()),
        ("application_name", param("application_name")),
        ("is_superuser", "on".to_string()),
        ("session_authorization", session_authorization),
    ];
    for (key, value) in statuses {
        if value.is_empty() {
            continue;
        }
        let mut body = Vec::new();
        put_cstring(&mut body, key);
        put_cstring(&mut body, &value);
        write_message(w, b'S', &body)?;
    }
    Ok(())
}

/// Mirrors `writeBackendKeyData`.
pub fn write_backend_key_data(w: &mut impl Write) -> io::Result<()> {
    let mut body = Vec::new();
    put_i32(&mut body, PG_DEFAULT_BACKEND_PID);
    put_i32(&mut body, PG_DEFAULT_SECRET_KEY);
    write_message(w, b'K', &body)
}

/// Mirrors `writeReadyForQuery`.
pub fn write_ready_for_query(w: &mut impl Write) -> io::Result<()> {
    write_message(w, b'Z', &[PG_TRANSACTION_IDLE])
}

/// Mirrors `writeRowDescription`.
pub fn write_row_description(w: &mut impl Write, fields: &[PgField]) -> io::Result<()> {
    let mut body = Vec::new();
    put_i16(&mut body, fields.len() as i16);
    for field in fields {
        put_cstring(&mut body, &field.name);
        put_i32(&mut body, 0); // table OID
        put_i16(&mut body, 0); // attribute number
        put_i32(&mut body, field.type_oid);
        put_i16(&mut body, field.type_size);
        put_i32(&mut body, -1); // type modifier
        put_i16(&mut body, 0); // text format
    }
    write_message(w, b'T', &body)
}

/// Mirrors `writeParameterDescription`.
pub fn write_parameter_description(w: &mut impl Write, parameter_oids: &[i32]) -> io::Result<()> {
    let mut body = Vec::new();
    put_i16(&mut body, parameter_oids.len() as i16);
    for oid in parameter_oids {
        put_i32(&mut body, *oid);
    }
    write_message(w, b't', &body)
}

/// Mirrors `writeDataRow`.
pub fn write_data_row(w: &mut impl Write, values: &[String]) -> io::Result<()> {
    let mut body = Vec::new();
    put_i16(&mut body, values.len() as i16);
    for value in values {
        put_i32(&mut body, value.len() as i32);
        body.extend_from_slice(value.as_bytes());
    }
    write_message(w, b'D', &body)
}

/// Mirrors `writeCommandComplete`.
pub fn write_command_complete(w: &mut impl Write, tag: &str) -> io::Result<()> {
    let mut body = Vec::new();
    put_cstring(&mut body, tag);
    write_message(w, b'C', &body)
}

/// Mirrors `writeErrorResponse`: ERROR severity + SQLSTATE + message fields.
pub fn write_error_response(w: &mut impl Write, sql_state: &str, message: &str) -> io::Result<()> {
    let mut body = Vec::new();
    body.push(b'S');
    put_cstring(&mut body, "ERROR");
    body.push(b'C');
    put_cstring(&mut body, sql_state);
    body.push(b'M');
    put_cstring(&mut body, message);
    body.push(0);
    write_message(w, b'E', &body)
}
