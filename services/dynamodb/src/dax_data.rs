//! Local single-node implementation of the binary DAX data plane.

mod attribute;

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Cursor, ErrorKind};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ciborium::value::Value as Cbor;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::attribute::item_key;
use crate::errors::ApiError;
use crate::model::{AttributeDefinition, Item};
use crate::requests::{DeleteItemRequest, GetItemRequest, PutItemRequest};
use crate::server::Server;

// Method IDs and encoding are defined by the AWS DAX client's generated
// Stubs.js, Tube.js and Assembler.js (amazon-dax-client 1.2.10).
const AUTHORIZE: i64 = 1_489_122_155;
const ENDPOINTS: i64 = 455_855_874;
const KEY_SCHEMA: i64 = -742_646_399;
const ATTRIBUTE_LIST: i64 = 670_678_385;
const ATTRIBUTE_LIST_ID: i64 = -1_230_579_644;
const GET: i64 = 263_244_906;
const PUT: i64 = -2_106_490_455;
const DELETE: i64 = 1_013_539_361;

const MAX_VALUE_BYTES: usize = 1024 * 1024;
const MAX_CONNECTIONS: usize = 64;
const IO_TIMEOUT: Duration = Duration::from_secs(30);
const CACHE_TTL_MS: i64 = 300_000;
const CACHE_CAPACITY: usize = 256;
const MAX_ATTRIBUTE_LISTS: usize = 4096;
const MAX_ATTRIBUTE_NAME_BYTES: usize = 4 * 1024 * 1024;
const EMPTY_ATTRIBUTE_LIST_ID: i64 = 1;

pub fn supports_auth_mode(mode: &str) -> bool {
    mode.is_empty() || mode.eq_ignore_ascii_case("relaxed")
}

pub async fn serve(listener: TcpListener, server: Arc<Mutex<Server>>) -> io::Result<()> {
    if !supports_auth_mode(server.lock().map_err(|_| invalid())?.auth_mode()) {
        return Err(io::Error::new(
            ErrorKind::PermissionDenied,
            "DAX requires relaxed auth mode",
        ));
    }
    let addr = listener.local_addr()?;
    let std::net::IpAddr::V4(ip) = addr.ip() else {
        return Err(invalid());
    };
    if !ip.is_loopback() {
        return Err(invalid());
    }
    let endpoint = Cbor::Array(vec![Cbor::Array(vec![
        Cbor::Text("local-dax".into()),
        Cbor::Text(ip.to_string()),
        Cbor::Bytes(ip.octets().to_vec()),
        Cbor::from(addr.port()),
        Cbor::from(1),
        Cbor::Text("local".into()),
        Cbor::Bytes(vec![0; 16]),
    ])]);
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = connections.join_next(), if !connections.is_empty() => {}
            accepted = listener.accept(), if connections.len() < MAX_CONNECTIONS => {
                let (socket, _) = accepted?;
                connections.spawn(connection(socket, server.clone(), endpoint.clone()));
            }
        }
    }
}

async fn connection(
    socket: TcpStream,
    server: Arc<Mutex<Server>>,
    endpoint: Cbor,
) -> io::Result<()> {
    let mut tube = Tube {
        socket,
        buffer: Vec::new(),
        offset: 0,
    };
    timeout(IO_TIMEOUT, tube.greeting()).await??;
    loop {
        let (method, args) = timeout(IO_TIMEOUT, tube.request()).await??;
        if method == AUTHORIZE {
            // The SDK pipelines this one-way message before the next RPC.
            continue;
        }
        let response = {
            let mut server = server.lock().map_err(|_| invalid())?;
            dispatch(&mut server, &endpoint, method, &args)
        };
        let mut bytes = Vec::new();
        match response {
            Ok(value) => {
                bytes.push(0x80); // Empty CBOR array is the success marker.
                write_cbor(&value, &mut bytes)?;
            }
            Err(error) => {
                write_cbor(&Cbor::Array(vec![4.into()]), &mut bytes)?;
                write_cbor(&Cbor::Text(error.message), &mut bytes)?;
                write_cbor(
                    &Cbor::Array(vec![
                        Cbor::Text("local-dax".into()),
                        Cbor::Text(error.name),
                        error.status.into(),
                    ]),
                    &mut bytes,
                )?;
            }
        }
        timeout(IO_TIMEOUT, tube.socket.write_all(&bytes)).await??;
        if arity(method).is_none() {
            // Without a known signature there is no next message boundary.
            return Ok(());
        }
    }
}

fn arity(method: i64) -> Option<usize> {
    match method {
        AUTHORIZE => Some(5),
        ENDPOINTS => Some(0),
        KEY_SCHEMA | ATTRIBUTE_LIST | ATTRIBUTE_LIST_ID => Some(1),
        GET | DELETE => Some(3),
        PUT => Some(4),
        _ => None,
    }
}

type CborResult<T> = io::Result<T>;

fn invalid() -> io::Error {
    io::Error::new(ErrorKind::InvalidData, "invalid DAX message")
}

fn read_cbor<T: std::io::Read>(reader: T) -> Result<Cbor, ciborium::de::Error<io::Error>> {
    ciborium::de::from_reader_with_recursion_limit(reader, 48)
}

fn write_cbor(value: &Cbor, output: &mut Vec<u8>) -> io::Result<()> {
    ciborium::ser::into_writer(value, output).map_err(|_| invalid())
}

struct Tube {
    socket: TcpStream,
    buffer: Vec<u8>,
    offset: usize,
}

impl Tube {
    async fn value(&mut self) -> CborResult<Cbor> {
        loop {
            let mut input = Cursor::new(&self.buffer[self.offset..]);
            match read_cbor(&mut input) {
                Ok(value) if input.position() <= MAX_VALUE_BYTES as u64 => {
                    self.offset += input.position() as usize;
                    return Ok(value);
                }
                Err(ciborium::de::Error::Io(err))
                    if err.kind() == ErrorKind::UnexpectedEof
                        && self.buffer.len() - self.offset < MAX_VALUE_BYTES => {}
                _ => return Err(invalid()),
            }
            if self.offset > 0 {
                self.buffer.drain(..self.offset);
                self.offset = 0;
            }
            let mut chunk = [0; 8192];
            let read = self.socket.read(&mut chunk).await?;
            if read == 0 {
                return Err(ErrorKind::UnexpectedEof.into());
            }
            self.buffer.extend_from_slice(&chunk[..read]);
        }
    }

    async fn greeting(&mut self) -> CborResult<()> {
        if self.value().await?.as_text() != Some("J7yne5G") || integer(&self.value().await?)? != 0 {
            return Err(invalid());
        }
        let session = self.value().await?;
        if !(session.is_null() || session.is_bytes())
            || !self.value().await?.is_map()
            || integer(&self.value().await?)? != 0
        {
            return Err(invalid());
        }
        Ok(())
    }

    async fn request(&mut self) -> CborResult<(i64, Vec<Cbor>)> {
        if integer(&self.value().await?)? != 1 {
            return Err(invalid());
        }
        let method = integer(&self.value().await?)?;
        let mut args = Vec::new();
        for _ in 0..arity(method).unwrap_or(0) {
            args.push(self.value().await?);
        }
        Ok((method, args))
    }
}

fn integer(value: &Cbor) -> CborResult<i64> {
    value
        .as_integer()
        .and_then(|n| i64::try_from(n).ok())
        .ok_or_else(invalid)
}

fn bytes(value: &Cbor) -> CborResult<&[u8]> {
    value.as_bytes().map(Vec::as_slice).ok_or_else(invalid)
}

fn validation(_: io::Error) -> ApiError {
    ApiError::validation("invalid DAX argument")
}

fn dispatch(
    server: &mut Server,
    endpoint: &Cbor,
    method: i64,
    args: &[Cbor],
) -> Result<Cbor, ApiError> {
    match (method, args) {
        (ENDPOINTS, []) => Ok(endpoint.clone()),
        (KEY_SCHEMA, [table]) => {
            let name = table_name(table)?;
            let schema = key_schema(server, name)?;
            Ok(Cbor::Map(
                schema
                    .iter()
                    .map(|definition| {
                        (
                            Cbor::Text(definition.attribute_name.clone()),
                            Cbor::Text(definition.attribute_type.clone()),
                        )
                    })
                    .collect(),
            ))
        }
        (ATTRIBUTE_LIST, [id]) => {
            let names = server
                .dax_items
                .attribute_list(integer(id).map_err(validation)?)?;
            Ok(Cbor::Array(names.iter().cloned().map(Cbor::Text).collect()))
        }
        (ATTRIBUTE_LIST_ID, [names]) => {
            let names = names
                .as_array()
                .ok_or_else(|| validation(invalid()))?
                .iter()
                .map(|name| name.as_text().map(str::to_owned).ok_or_else(invalid))
                .collect::<CborResult<Vec<_>>>()
                .map_err(validation)?;
            server
                .dax_items
                .register_attribute_list(names)
                .map(Cbor::from)
        }
        (GET | DELETE, [table, key, options]) => {
            let options = Options::decode(method, options)?;
            item_operation(server, method, table_name(table)?, key, None, options)
        }
        (PUT, [table, key, values, options]) => {
            let options = Options::decode(method, options)?;
            item_operation(
                server,
                method,
                table_name(table)?,
                key,
                Some(values),
                options,
            )
        }
        _ => Err(ApiError::new(
            400,
            "UnknownOperationException",
            "DAX method is not implemented",
        )),
    }
}

fn table_name(value: &Cbor) -> Result<&str, ApiError> {
    std::str::from_utf8(bytes(value).map_err(validation)?).map_err(|_| validation(invalid()))
}

fn key_schema<'a>(
    server: &'a Server,
    table: &str,
) -> Result<Vec<&'a AttributeDefinition>, ApiError> {
    let description = &server
        .tables
        .get(table)
        .ok_or_else(|| ApiError::not_found("table not found"))?
        .description;
    ["HASH", "RANGE"]
        .into_iter()
        .filter_map(|kind| {
            description
                .key_schema
                .iter()
                .find(|element| element.key_type == kind)
        })
        .map(|element| {
            description
                .attribute_definitions
                .iter()
                .find(|def| def.attribute_name == element.attribute_name)
                .ok_or_else(|| ApiError::internal("invalid table key schema"))
        })
        .collect()
}

#[derive(Default)]
struct Options {
    consistent: bool,
    capacity: &'static str,
    return_values: &'static str,
}

impl Options {
    fn decode(method: i64, value: &Cbor) -> Result<Self, ApiError> {
        let mut options = Self::default();
        if value.is_null() {
            return Ok(options);
        }
        let mut seen = BTreeSet::new();
        for (param, value) in value.as_map().ok_or_else(|| validation(invalid()))? {
            let param = integer(param).map_err(validation)?;
            if !seen.insert(param) {
                return Err(validation(invalid()));
            }
            match (param, method) {
                (2, GET) => {
                    options.consistent = value.as_bool().ok_or_else(|| validation(invalid()))?
                }
                (3, _) => {
                    options.capacity = match integer(value).map_err(validation)? {
                        0 => "NONE",
                        1 => "TOTAL",
                        2 => "INDEXES",
                        _ => return Err(validation(invalid())),
                    }
                }
                (7, PUT | DELETE) => {
                    options.return_values = match integer(value).map_err(validation)? {
                        1 => "NONE",
                        2 => "ALL_OLD",
                        _ => return Err(validation(invalid())),
                    }
                }
                _ => return Err(ApiError::validation("unsupported DAX request option")),
            }
        }
        Ok(options)
    }
}

fn item_operation(
    server: &mut Server,
    method: i64,
    table: &str,
    key: &Cbor,
    values: Option<&Cbor>,
    options: Options,
) -> Result<Cbor, ApiError> {
    let schema = key_schema(server, table)?;
    let mut encoded_key = bytes(key).map_err(validation)?;
    let mut key = Item::new();
    for (index, definition) in schema.iter().enumerate() {
        let kind = definition.attribute_type.as_str();
        let raw = index + 1 == schema.len();
        let value = if kind == "N" && raw && index == 1 {
            attribute::decode_sort_number(std::mem::take(&mut encoded_key))
        } else {
            attribute::decode_key(kind, &mut encoded_key, raw)
        }
        .map_err(validation)?;
        let max = if index == 0 { 2048 } else { 1024 };
        attribute::validate_key_size(kind, &value, max).map_err(validation)?;
        key.insert(definition.attribute_name.clone(), value);
    }
    if !encoded_key.is_empty() || key.is_empty() {
        return Err(validation(invalid()));
    }
    let mut item = key.clone();
    if let Some(values) = values {
        let mut input = bytes(values).map_err(validation)?;
        let id = integer(&read_cbor(&mut input).map_err(|_| validation(invalid()))?)
            .map_err(validation)?;
        for name in server.dax_items.attribute_list(id)? {
            let value = read_cbor(&mut input).map_err(|_| validation(invalid()))?;
            if item
                .insert(name.clone(), attribute::decode(&value).map_err(validation)?)
                .is_some()
            {
                return Err(validation(invalid()));
            }
        }
        if !input.is_empty() {
            return Err(validation(invalid()));
        }
    }

    let state = &server.tables[table];
    let canonical_key = item_key(&state.description, &key).map_err(ApiError::validation)?;
    let cache_key = (table.to_owned(), canonical_key.clone());
    let now = server.now_millis();
    if method == GET && !options.consistent {
        if let Some(cached) = server.dax_items.get(&cache_key, now) {
            return item_response(&mut server.dax_items, 0, cached.as_ref(), &key, None);
        }
    }

    server.expire_ttl_items(server.now_unix())?;
    let state = &server.tables[table];
    // The legacy JSON store preserves the spelling of numeric keys. Recover
    // that spelling when a CBOR number refers to an item written via HTTP.
    if !state.items.contains_key(&canonical_key) && key.values().any(|attr| attr.get("N").is_some())
    {
        if let Some(existing) = state.items.values().find(|item| {
            key.iter().all(|(name, value)| {
                item.get(name)
                    .is_some_and(|stored| attribute::key_values_equal(stored, value))
            })
        }) {
            for (name, value) in &mut key {
                *value = existing[name].clone();
                item.insert(name.clone(), value.clone());
            }
        }
    }
    let capacity = match options.capacity {
        "TOTAL" | "INDEXES" => Some(table),
        _ => None,
    };
    match method {
        GET => {
            let response = server.get_item(&GetItemRequest {
                table_name: table.to_owned(),
                key: key.clone(),
                consistent_read: options.consistent,
                ..Default::default()
            })?;
            let response: Value = serde_json::from_slice(&response)
                .map_err(|_| ApiError::internal("invalid DynamoDB response"))?;
            let item: Option<Item> = response
                .get("Item")
                .cloned()
                .map(serde_json::from_value)
                .transpose()
                .map_err(|_| ApiError::internal("invalid DynamoDB item"))?;
            let payload = item_response(&mut server.dax_items, 0, item.as_ref(), &key, capacity)?;
            if !options.consistent {
                server.dax_items.insert(cache_key, item, now);
            }
            Ok(payload)
        }
        PUT | DELETE => {
            let old = if options.return_values == "ALL_OLD" {
                let storage_key =
                    item_key(&state.description, &key).map_err(ApiError::validation)?;
                state.items.get(&storage_key).cloned()
            } else {
                None
            };
            let payload = item_response(&mut server.dax_items, 2, old.as_ref(), &key, capacity)?;
            if method == PUT {
                server.put_item(&PutItemRequest {
                    table_name: table.to_owned(),
                    item: item.clone(),
                    ..Default::default()
                })?;
            } else {
                server.delete_item(&DeleteItemRequest {
                    table_name: table.to_owned(),
                    key,
                    ..Default::default()
                })?;
            }
            server
                .dax_items
                .insert(cache_key, (method == PUT).then_some(item), now);
            Ok(payload)
        }
        _ => unreachable!(),
    }
}

fn item_response(
    cache: &mut ItemCache,
    field: i32,
    item: Option<&Item>,
    key: &Item,
    capacity: Option<&str>,
) -> Result<Cbor, ApiError> {
    let mut fields = Vec::new();
    if let Some(item) = item {
        let (names, values): (Vec<_>, Vec<_>) = item
            .iter()
            .filter(|(name, _)| !key.contains_key(*name))
            .unzip();
        let values = values
            .into_iter()
            .map(attribute::encode)
            .collect::<CborResult<Vec<_>>>()
            .map_err(validation)?;
        let id = cache.register_attribute_list(names.into_iter().cloned().collect())?;
        let mut data = Vec::new();
        write_cbor(&Cbor::from(id), &mut data).map_err(validation)?;
        for value in values {
            write_cbor(&value, &mut data).map_err(validation)?;
        }
        fields.push((Cbor::from(field), Cbor::Bytes(data)));
    }
    if let Some(table) = capacity {
        let mut data = Vec::new();
        for value in [
            Cbor::Text(table.to_owned()),
            1.into(),
            Cbor::Null,
            Cbor::Null,
            Cbor::Null,
        ] {
            write_cbor(&value, &mut data).map_err(validation)?;
        }
        fields.push((1.into(), Cbor::Bytes(data)));
    }
    Ok(Cbor::Map(fields))
}

type CacheKey = (String, String);

struct CacheEntry {
    item: Option<Item>,
    expires_ms: i64,
    last_used: u64,
}

#[derive(Default)]
pub(crate) struct ItemCache {
    entries: BTreeMap<CacheKey, CacheEntry>,
    access: u64,
    attribute_lists: Vec<Vec<String>>,
    attribute_ids: BTreeMap<Vec<String>, i64>,
    attribute_name_bytes: usize,
}

impl ItemCache {
    fn get(&mut self, key: &CacheKey, now_ms: i64) -> Option<Option<Item>> {
        let entry = self.entries.get_mut(key)?;
        if now_ms >= entry.expires_ms {
            self.entries.remove(key);
            return None;
        }
        self.access += 1;
        entry.last_used = self.access;
        Some(entry.item.clone())
    }

    fn insert(&mut self, key: CacheKey, item: Option<Item>, now_ms: i64) {
        self.access += 1;
        self.entries.insert(
            key,
            CacheEntry {
                item,
                expires_ms: now_ms.saturating_add(CACHE_TTL_MS),
                last_used: self.access,
            },
        );
        if self.entries.len() > CACHE_CAPACITY {
            self.entries.retain(|_, entry| now_ms < entry.expires_ms);
            if self.entries.len() > CACHE_CAPACITY {
                let oldest = self
                    .entries
                    .iter()
                    .min_by_key(|(_, entry)| entry.last_used)
                    .map(|(key, _)| key.clone())
                    .unwrap();
                self.entries.remove(&oldest);
            }
        }
    }

    pub(crate) fn remove_table(&mut self, table: &str) {
        self.entries.retain(|(name, _), _| name != table);
    }

    fn attribute_list(&self, id: i64) -> Result<&[String], ApiError> {
        if id == EMPTY_ATTRIBUTE_LIST_ID {
            return Ok(&[]);
        }
        id.checked_sub(EMPTY_ATTRIBUTE_LIST_ID + 1)
            .and_then(|index| usize::try_from(index).ok())
            .and_then(|index| self.attribute_lists.get(index))
            .map(Vec::as_slice)
            .ok_or_else(|| ApiError::validation("unknown DAX attribute list"))
    }

    fn register_attribute_list(&mut self, names: Vec<String>) -> Result<i64, ApiError> {
        if names.is_empty() {
            return Ok(EMPTY_ATTRIBUTE_LIST_ID);
        }
        if let Some(&id) = self.attribute_ids.get(&names) {
            return Ok(id);
        }
        let mut unique = BTreeSet::new();
        if names
            .iter()
            .any(|name| name.is_empty() || !unique.insert(name))
        {
            return Err(ApiError::validation("invalid DAX attribute list"));
        }
        let size: usize = names.iter().map(String::len).sum();
        if self.attribute_lists.len() == MAX_ATTRIBUTE_LISTS
            || self.attribute_name_bytes + size > MAX_ATTRIBUTE_NAME_BYTES
        {
            return Err(ApiError::new(
                400,
                "LimitExceededException",
                "local DAX attribute dictionary is full",
            ));
        }
        self.attribute_name_bytes += size;
        self.attribute_lists.push(names.clone());
        let id = EMPTY_ATTRIBUTE_LIST_ID + self.attribute_lists.len() as i64;
        self.attribute_ids.insert(names, id);
        Ok(id)
    }
}
