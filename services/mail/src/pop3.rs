//! Local POP3 server (RFC 1939 + RFC 2449 `CAPA` + RFC 2595 `STLS` + implicit
//! TLS `POP3S`) backed by the shared mail `Service`.
//!
//! Never logs credentials, authorization tokens, or message bodies.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, ReadHalf, WriteHalf,
};
use tokio::net::TcpListener;

use crate::service::Service;
use crate::smtp::{SmtpLimits, SMTP_AUTH_STRICT};
use crate::tls::{accept_tls, constant_time_eq, is_transient_accept_error, MailTls, MaybeTls};

/// Configuration for the local POP3 server.
#[derive(Clone, Debug, Default)]
pub struct Pop3Config {
    pub addr: String,
    pub auth_mode: String,
    pub username: String,
    pub password: String,
}

impl Pop3Config {
    fn is_strict(&self) -> bool {
        self.auth_mode.trim().eq_ignore_ascii_case(SMTP_AUTH_STRICT)
    }

    fn verify_credentials(&self, user: &str, pass: &str) -> bool {
        if !self.is_strict() {
            return true;
        }
        constant_time_eq(user.as_bytes(), self.username.as_bytes())
            & constant_time_eq(pass.as_bytes(), self.password.as_bytes())
    }
}

/// Local POP3 server sharing the same `Service` as SMTP, IMAP, and HTTP.
#[derive(Clone)]
pub struct Pop3Server {
    config: Pop3Config,
    service: Arc<Service>,
    limits: SmtpLimits,
    tls: Option<MailTls>,
}

impl Pop3Server {
    pub fn new(config: Pop3Config, service: Arc<Service>) -> Self {
        Self {
            config,
            service,
            limits: SmtpLimits::standard(),
            tls: None,
        }
    }

    pub fn with_limits(mut self, limits: SmtpLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn with_tls(mut self, tls: MailTls) -> Self {
        self.tls = Some(tls);
        self
    }

    pub fn with_optional_tls(mut self, tls: Option<MailTls>) -> Self {
        self.tls = tls;
        self
    }

    fn clone_for_conn(&self) -> Self {
        Self {
            config: self.config.clone(),
            service: Arc::clone(&self.service),
            limits: self.limits.clone(),
            tls: self.tls.clone(),
        }
    }

    pub async fn run(&self) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.config.addr).await?;
        self.serve(listener, std::future::pending()).await
    }

    pub async fn serve(
        &self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()>,
    ) -> std::io::Result<()> {
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                accepted = listener.accept() => {
                    let sock = match accepted {
                        Ok((sock, _)) => sock,
                        Err(e) => {
                            if !is_transient_accept_error(&e) {
                                return Err(e);
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    };
                    let server = self.clone_for_conn();
                    tokio::spawn(async move {
                        server.handle_conn(sock).await;
                    });
                }
            }
        }
    }

    pub async fn serve_tls(
        &self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()>,
    ) -> std::io::Result<()> {
        if self.tls.is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "POP3S listener requires TLS configuration",
            ));
        }
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                accepted = listener.accept() => {
                    let sock = match accepted {
                        Ok((sock, _)) => sock,
                        Err(e) => {
                            if !is_transient_accept_error(&e) {
                                return Err(e);
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    };
                    let server = self.clone_for_conn();
                    tokio::spawn(async move {
                        server.handle_tls_conn(sock).await;
                    });
                }
            }
        }
    }

    pub async fn handle_conn<S>(self, io: S)
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        self.run_session(MaybeTls::Plain(io), false).await;
    }

    pub async fn handle_tls_conn<S>(self, io: S)
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let Some(tls) = self.tls.as_ref() else {
            return;
        };
        let Ok(stream) = accept_tls(io, &tls.acceptor, self.limits.idle_timeout).await else {
            return;
        };
        self.run_session(stream, true).await;
    }

    async fn run_session<S>(self, io: MaybeTls<S>, tls_active: bool)
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut session = Pop3Session {
            config: self.config,
            service: self.service,
            conn: Some(Conn::new(io)),
            limits: self.limits,
            tls: self.tls,
            tls_active,
            state: Pop3State::Authorization { pending_user: None },
        };

        if !session.ok("devcloud POP3 ready").await {
            return;
        }
        loop {
            match session.read_line().await {
                None => return,
                Some(line) => {
                    if !session.handle_line(&line).await {
                        return;
                    }
                }
            }
        }
    }
}

struct Conn<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    reader: BufReader<ReadHalf<MaybeTls<S>>>,
    writer: WriteHalf<MaybeTls<S>>,
}

impl<S> Conn<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn new(io: MaybeTls<S>) -> Self {
        let (read_half, write_half) = tokio::io::split(io);
        Self {
            reader: BufReader::new(read_half),
            writer: write_half,
        }
    }
}

struct Pop3Entry {
    id: String,
    raw: Vec<u8>,
    octets: usize,
    deleted: bool,
}

enum Pop3State {
    Authorization { pending_user: Option<String> },
    Transaction { entries: Vec<Pop3Entry> },
}

struct Pop3Session<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    config: Pop3Config,
    service: Arc<Service>,
    conn: Option<Conn<S>>,
    limits: SmtpLimits,
    tls: Option<MailTls>,
    tls_active: bool,
    state: Pop3State,
}

impl<S> Pop3Session<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn tls_required(&self) -> bool {
        !self.tls_active && self.tls.as_ref().is_some_and(|t| t.require)
    }

    async fn read_line(&mut self) -> Option<String> {
        let mut buf = Vec::new();
        let limit = self.limits.max_line_bytes;
        let deadline = self
            .limits
            .idle_timeout
            .map(|t| tokio::time::Instant::now() + t);
        loop {
            let reader = &mut self.conn.as_mut()?.reader;
            let available = if let Some(deadline) = deadline {
                match tokio::time::timeout_at(deadline, reader.fill_buf()).await {
                    Ok(Ok(bytes)) => bytes,
                    Ok(Err(_)) => return None,
                    Err(_) => {
                        self.err("idle timeout").await;
                        return None;
                    }
                }
            } else {
                reader.fill_buf().await.ok()?
            };
            if available.is_empty() {
                break;
            }
            let take = available
                .iter()
                .position(|&b| b == b'\n')
                .map_or(available.len(), |i| i + 1);
            if limit > 0 && buf.len().saturating_add(take) > limit {
                self.err("line exceeds limit").await;
                return None;
            }
            let finished = available[take - 1] == b'\n';
            buf.extend_from_slice(&available[..take]);
            reader.consume(take);
            if finished {
                break;
            }
        }
        if buf.is_empty() {
            return None;
        }
        if buf.last() == Some(&b'\n') {
            buf.pop();
            if buf.last() == Some(&b'\r') {
                buf.pop();
            }
        }
        Some(String::from_utf8_lossy(&buf).into_owned())
    }

    async fn write_raw(&mut self, bytes: &[u8]) -> bool {
        let Some(conn) = self.conn.as_mut() else {
            return false;
        };
        if conn.writer.write_all(bytes).await.is_err() {
            return false;
        }
        conn.writer.flush().await.is_ok()
    }

    async fn ok(&mut self, msg: &str) -> bool {
        let line = if msg.is_empty() {
            "+OK\r\n".to_string()
        } else {
            format!("+OK {msg}\r\n")
        };
        self.write_raw(line.as_bytes()).await
    }

    async fn err(&mut self, msg: &str) -> bool {
        let line = format!("-ERR {msg}\r\n");
        self.write_raw(line.as_bytes()).await
    }

    async fn handle_line(&mut self, line: &str) -> bool {
        let (cmd, arg) = split_command(line);
        match cmd.as_str() {
            "CAPA" => {
                if !arg.is_empty() {
                    return self.err("syntax error").await;
                }
                self.handle_capa().await
            }
            "STLS" => self.handle_stls(&arg).await,
            "QUIT" => self.handle_quit().await,
            _ => match self.state {
                Pop3State::Authorization { .. } => self.handle_auth_state(&cmd, &arg).await,
                Pop3State::Transaction { .. } => self.handle_transaction_state(&cmd, &arg).await,
            },
        }
    }

    async fn handle_capa(&mut self) -> bool {
        let mut out = String::from("+OK Capability list follows\r\n");
        let in_auth = matches!(self.state, Pop3State::Authorization { .. });
        if !self.tls_required() || !in_auth {
            out.push_str("USER\r\n");
            if in_auth {
                out.push_str("SASL PLAIN\r\n");
            }
        }
        out.push_str("UIDL\r\n");
        out.push_str("TOP\r\n");
        if in_auth && self.tls.is_some() && !self.tls_active {
            out.push_str("STLS\r\n");
        }
        out.push_str(".\r\n");
        self.write_raw(out.as_bytes()).await
    }

    async fn handle_stls(&mut self, arg: &str) -> bool {
        if !arg.is_empty() {
            return self.err("syntax error").await;
        }
        if !matches!(self.state, Pop3State::Authorization { .. }) {
            return self.err("STLS only valid in authorization state").await;
        }
        let Some(tls) = self.tls.clone() else {
            return self.err("STLS not supported").await;
        };
        if self.tls_active {
            return self.err("TLS already active").await;
        }
        if self
            .conn
            .as_ref()
            .is_some_and(|c| !c.reader.buffer().is_empty())
        {
            self.err("pipelining not allowed with STLS").await;
            return false;
        }
        if !self.ok("Begin TLS negotiation").await {
            return false;
        }
        let Some(conn) = self.conn.take() else {
            return false;
        };
        let maybe = conn.reader.into_inner().unsplit(conn.writer);
        let Some(plain) = maybe.into_plain() else {
            return false;
        };
        let Ok(tls_stream) = accept_tls(plain, &tls.acceptor, self.limits.idle_timeout).await
        else {
            return false;
        };
        self.conn = Some(Conn::new(tls_stream));
        self.tls_active = true;
        // RFC 2595 §4: discard prior auth state after STLS.
        self.state = Pop3State::Authorization { pending_user: None };
        true
    }

    async fn handle_auth_state(&mut self, cmd: &str, arg: &str) -> bool {
        match cmd {
            "USER" => {
                if self.tls_required() {
                    return self.err("Must issue a STLS command first").await;
                }
                if arg.is_empty() {
                    return self.err("missing username").await;
                }
                self.state = Pop3State::Authorization {
                    pending_user: Some(arg.to_string()),
                };
                self.ok("user accepted").await
            }
            "PASS" => {
                if self.tls_required() {
                    return self.err("Must issue a STLS command first").await;
                }
                let user = match &mut self.state {
                    Pop3State::Authorization { pending_user } => pending_user.take(),
                    _ => None,
                };
                let Some(user) = user else {
                    return self.err("send USER first").await;
                };
                self.complete_login(&user, arg).await
            }
            "AUTH" => {
                if self.tls_required() {
                    return self.err("Must issue a STLS command first").await;
                }
                self.handle_sasl_auth(arg).await
            }
            "NOOP" | "STAT" | "LIST" | "UIDL" | "RETR" | "TOP" | "DELE" | "RSET" => {
                self.err("not authenticated").await
            }
            "" => self.err("empty command").await,
            _ => self.err("unknown command").await,
        }
    }

    async fn handle_sasl_auth(&mut self, arg: &str) -> bool {
        let (mech, initial) = split_command(arg);
        if mech.is_empty() {
            let out = "+OK\r\nPLAIN\r\n.\r\n";
            return self.write_raw(out.as_bytes()).await;
        }
        if mech != "PLAIN" {
            return self.err("unsupported authentication mechanism").await;
        }
        let encoded = if initial.is_empty() {
            if !self.write_raw(b"+ \r\n").await {
                return false;
            }
            match self.read_line().await {
                Some(line) => line,
                None => return false,
            }
        } else {
            initial
        };
        if encoded == "*" {
            return self.err("authentication cancelled").await;
        }
        let Ok(decoded) = BASE64.decode(encoded.as_bytes()) else {
            return self.err("invalid base64").await;
        };
        let text = String::from_utf8_lossy(&decoded);
        let parts: Vec<&str> = text.splitn(3, '\u{0}').collect();
        if parts.len() != 3 {
            return self.err("malformed PLAIN credentials").await;
        }
        self.complete_login(parts[1], parts[2]).await
    }

    async fn complete_login(&mut self, user: &str, pass: &str) -> bool {
        if !self.config.verify_credentials(user, pass) {
            return self.err("authentication failed").await;
        }
        let snapshot = match self.service.list_mailbox() {
            Ok(s) => s,
            Err(_) => return self.err("failed to open mailbox").await,
        };
        let mut entries = Vec::with_capacity(snapshot.messages.len());
        for item in snapshot.messages {
            let raw = match self.service.get_raw(&item.message.id) {
                Ok(Some(bytes)) => bytes,
                Ok(None) => continue,
                Err(_) => return self.err("failed to read mailbox").await,
            };
            let octets = crlf_octet_len(&raw);
            entries.push(Pop3Entry {
                id: item.message.id,
                raw,
                octets,
                deleted: false,
            });
        }
        let count = entries.len();
        self.state = Pop3State::Transaction { entries };
        self.ok(&format!("mailbox ready ({count} messages)")).await
    }

    async fn handle_transaction_state(&mut self, cmd: &str, arg: &str) -> bool {
        match cmd {
            "NOOP" => {
                if !arg.is_empty() {
                    return self.err("syntax error").await;
                }
                self.ok("").await
            }
            "STAT" => {
                if !arg.is_empty() {
                    return self.err("syntax error").await;
                }
                let (count, octets) = self.active_totals();
                self.ok(&format!("{count} {octets}")).await
            }
            "LIST" => self.handle_list(arg).await,
            "UIDL" => self.handle_uidl(arg).await,
            "RETR" => self.handle_retr(arg).await,
            "TOP" => self.handle_top(arg).await,
            "DELE" => self.handle_dele(arg).await,
            "RSET" => {
                if !arg.is_empty() {
                    return self.err("syntax error").await;
                }
                if let Pop3State::Transaction { entries } = &mut self.state {
                    for entry in entries.iter_mut() {
                        entry.deleted = false;
                    }
                }
                let (count, octets) = self.active_totals();
                self.ok(&format!("maildrop has {count} messages ({octets} octets)"))
                    .await
            }
            "" => self.err("empty command").await,
            _ => self.err("unknown command").await,
        }
    }

    fn active_totals(&self) -> (usize, usize) {
        let Pop3State::Transaction { entries } = &self.state else {
            return (0, 0);
        };
        let mut count = 0usize;
        let mut octets = 0usize;
        for entry in entries {
            if !entry.deleted {
                count += 1;
                octets += entry.octets;
            }
        }
        (count, octets)
    }

    fn get_active_entry(&self, num: usize) -> Option<&Pop3Entry> {
        if num == 0 {
            return None;
        }
        let Pop3State::Transaction { entries } = &self.state else {
            return None;
        };
        entries.get(num - 1).filter(|e| !e.deleted)
    }

    async fn handle_list(&mut self, arg: &str) -> bool {
        if arg.is_empty() {
            let (count, total) = self.active_totals();
            let mut out = format!("+OK {count} messages ({total} octets)\r\n");
            if let Pop3State::Transaction { entries } = &self.state {
                for (idx, entry) in entries.iter().enumerate() {
                    if !entry.deleted {
                        out.push_str(&format!("{} {}\r\n", idx + 1, entry.octets));
                    }
                }
            }
            out.push_str(".\r\n");
            self.write_raw(out.as_bytes()).await
        } else {
            let Ok(num) = arg.parse::<usize>() else {
                return self.err("invalid message number").await;
            };
            match self.get_active_entry(num) {
                Some(entry) => {
                    let octets = entry.octets;
                    self.ok(&format!("{num} {octets}")).await
                }
                None => self.err("no such message").await,
            }
        }
    }

    async fn handle_uidl(&mut self, arg: &str) -> bool {
        if arg.is_empty() {
            let mut out = String::from("+OK\r\n");
            if let Pop3State::Transaction { entries } = &self.state {
                for (idx, entry) in entries.iter().enumerate() {
                    if !entry.deleted {
                        out.push_str(&format!("{} {}\r\n", idx + 1, entry.id));
                    }
                }
            }
            out.push_str(".\r\n");
            self.write_raw(out.as_bytes()).await
        } else {
            let Ok(num) = arg.parse::<usize>() else {
                return self.err("invalid message number").await;
            };
            match self.get_active_entry(num) {
                Some(entry) => {
                    let id = entry.id.clone();
                    self.ok(&format!("{num} {id}")).await
                }
                None => self.err("no such message").await,
            }
        }
    }

    async fn handle_retr(&mut self, arg: &str) -> bool {
        let Ok(num) = arg.parse::<usize>() else {
            return self.err("invalid message number").await;
        };
        let Some(entry) = self.get_active_entry(num) else {
            return self.err("no such message").await;
        };
        let octets = entry.octets;
        let payload = dot_stuff_message(&entry.raw);
        let mut out = format!("+OK {octets} octets\r\n").into_bytes();
        out.extend_from_slice(&payload);
        out.extend_from_slice(b".\r\n");
        self.write_raw(&out).await
    }

    async fn handle_top(&mut self, arg: &str) -> bool {
        let mut parts = arg.split_whitespace();
        let (Some(msg_str), Some(lines_str), None) = (parts.next(), parts.next(), parts.next())
        else {
            return self.err("syntax error").await;
        };
        let (Ok(num), Ok(max_lines)) = (msg_str.parse::<usize>(), lines_str.parse::<usize>())
        else {
            return self.err("invalid TOP arguments").await;
        };
        let Some(entry) = self.get_active_entry(num) else {
            return self.err("no such message").await;
        };
        let payload = format_top(&entry.raw, max_lines);
        let mut out = b"+OK top of message follows\r\n".to_vec();
        out.extend_from_slice(&payload);
        out.extend_from_slice(b".\r\n");
        self.write_raw(&out).await
    }

    async fn handle_dele(&mut self, arg: &str) -> bool {
        let Ok(num) = arg.parse::<usize>() else {
            return self.err("invalid message number").await;
        };
        if num == 0 {
            return self.err("no such message").await;
        }
        if let Pop3State::Transaction { entries } = &mut self.state {
            if let Some(entry) = entries.get_mut(num - 1) {
                if !entry.deleted {
                    entry.deleted = true;
                    return self.ok(&format!("message {num} deleted")).await;
                }
            }
        }
        self.err("no such message").await
    }

    async fn handle_quit(&mut self) -> bool {
        // RFC 1939 §6: UPDATE state commits deletions only on QUIT.
        let mut had_error = false;
        if let Pop3State::Transaction { entries } = &self.state {
            for entry in entries {
                if entry.deleted && self.service.delete(&entry.id).is_err() {
                    had_error = true;
                }
            }
        }
        if had_error {
            self.err("some deleted messages not removed").await;
        } else {
            self.ok("bye").await;
        }
        false
    }
}

fn split_command(line: &str) -> (String, String) {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return (String::new(), String::new());
    }
    match trimmed.split_once(|c: char| c == ' ' || c == '\t') {
        None => (trimmed.to_ascii_uppercase(), String::new()),
        Some((cmd, rest)) => (cmd.to_ascii_uppercase(), rest.trim().to_string()),
    }
}

/// Computes the byte length of `raw` with canonical `\r\n` line endings.
fn crlf_octet_len(raw: &[u8]) -> usize {
    let mut total = 0usize;
    for line in split_lines(raw) {
        total += line.len() + 2;
    }
    total
}

/// Splits `raw` into lines without their `\r\n` or `\n` endings.
fn split_lines(raw: &[u8]) -> Vec<&[u8]> {
    if raw.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    while i < raw.len() {
        if raw[i] == b'\n' {
            let end = if i > start && raw[i - 1] == b'\r' {
                i - 1
            } else {
                i
            };
            out.push(&raw[start..end]);
            start = i + 1;
        }
        i += 1;
    }
    if start < raw.len() {
        let end = if raw.ends_with(b"\r") {
            raw.len() - 1
        } else {
            raw.len()
        };
        out.push(&raw[start..end]);
    }
    out
}

/// Byte-stuffs leading dots per RFC 1939 §3 and normalizes lines to `\r\n`.
fn dot_stuff_message(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(raw.len() + 16);
    for line in split_lines(raw) {
        if line.starts_with(b".") {
            out.push(b'.');
        }
        out.extend_from_slice(line);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Formats headers, blank line, and up to `max_body_lines` body lines (dot-stuffed).
fn format_top(raw: &[u8], max_body_lines: usize) -> Vec<u8> {
    let lines = split_lines(raw);
    let mut out = Vec::new();
    let mut in_body = false;
    let mut body_count = 0usize;
    for line in lines {
        if !in_body {
            if line.is_empty() {
                in_body = true;
                out.extend_from_slice(b"\r\n");
                continue;
            }
            if line.starts_with(b".") {
                out.push(b'.');
            }
            out.extend_from_slice(line);
            out.extend_from_slice(b"\r\n");
        } else {
            if body_count >= max_body_lines {
                break;
            }
            if line.starts_with(b".") {
                out.push(b'.');
            }
            out.extend_from_slice(line);
            out.extend_from_slice(b"\r\n");
            body_count += 1;
        }
    }
    if !in_body {
        out.extend_from_slice(b"\r\n");
    }
    out
}
