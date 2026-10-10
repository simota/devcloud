//! Mirrors `internal/services/mail/smtp.rs`.
//!
//! `goroutine + net.Conn` → `tokio::spawn` + an `AsyncRead + AsyncWrite` stream.
//! The session is a state machine driven line-by-line, identical in behavior to
//! the legacy server: same reply codes, same sequence checks, same DATA framing
//! (dot-unstuffing + cumulative size limit), same AUTH PLAIN/LOGIN flows.

use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, ReadHalf, WriteHalf,
};
use tokio::net::TcpListener;

use crate::model::Envelope;
use crate::service::Service;

pub const SMTP_AUTH_OFF: &str = "off";
pub const SMTP_AUTH_RELAXED: &str = "relaxed";
pub const SMTP_AUTH_STRICT: &str = "strict";

/// Mirrors legacy `SMTPConfig`.
#[derive(Clone, Debug, Default)]
pub struct SmtpConfig {
    pub addr: String,
    pub max_message_bytes: i64,
    pub auth_mode: String,
    pub username: String,
    pub password: String,
}

impl SmtpConfig {
    fn auth_mode_normalized(&self) -> String {
        let mode = self.auth_mode.trim().to_ascii_lowercase();
        if mode.is_empty() {
            SMTP_AUTH_OFF.to_string()
        } else {
            mode
        }
    }
}

/// Mirrors legacy `SMTPServer`.
pub struct SmtpServer {
    config: SmtpConfig,
    service: Arc<Service>,
    envelope_capture: bool,
    limits: SmtpLimits,
}

/// Zero/None limits retain the existing unlimited session behavior.
#[derive(Clone, Debug, Default)]
pub struct SmtpLimits {
    pub max_line_bytes: usize,
    pub idle_timeout: Option<Duration>,
    pub max_recipients: usize,
}

impl SmtpLimits {
    /// What a server gets unless `with_limits` says otherwise: a line (a
    /// command, or one line of DATA) can no longer grow memory without bound,
    /// nor can an endless RCPT loop, nor can a stalled client hold a session
    /// forever. All are far above what real mail uses (RFC 5321 caps a text
    /// line at 1000 octets, requires only 100 recipients, and suggests a
    /// 5-minute command timeout).
    pub fn standard() -> Self {
        SmtpLimits {
            max_line_bytes: 1024 * 1024,
            idle_timeout: Some(Duration::from_secs(300)),
            max_recipients: 1000,
        }
    }
}

impl SmtpServer {
    pub fn new(config: SmtpConfig, service: Arc<Service>) -> Self {
        Self {
            config,
            service,
            envelope_capture: false,
            limits: SmtpLimits::standard(),
        }
    }

    pub fn with_envelope_capture(mut self) -> Self {
        self.envelope_capture = true;
        self
    }

    pub fn with_limits(mut self, limits: SmtpLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Mirrors `SMTPServer.Run`: accept loop on the configured address. A fresh
    /// per-connection `SmtpServer` (cheap: config clone + `Arc` clone) is moved
    /// into each spawned task so the session future is `'static`.
    pub async fn run(&self) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.config.addr).await?;
        loop {
            let sock = match listener.accept().await {
                Ok((sock, _)) => sock,
                // Out of descriptors, or a peer that went away before the
                // accept: wait and keep serving rather than stop the service.
                Err(e) => {
                    // Only transient errors; a broken listener still stops.
                    let transient = matches!(
                        e.kind(),
                        std::io::ErrorKind::ConnectionAborted
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::Interrupted
                            | std::io::ErrorKind::WouldBlock
                            | std::io::ErrorKind::TimedOut
                    ) || matches!(e.raw_os_error(), Some(12 | 23 | 24)); // ENOMEM, ENFILE, EMFILE
                    if !transient {
                        return Err(e);
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let server = SmtpServer {
                config: self.config.clone(),
                service: Arc::clone(&self.service),
                envelope_capture: self.envelope_capture,
                limits: self.limits.clone(),
            };
            tokio::spawn(async move {
                server.handle_conn(sock).await;
            });
        }
    }

    /// Mirrors `SMTPServer.handleConn`. Generic over the transport so tests can
    /// drive it over an in-memory duplex (the parity of legacy `net.Pipe`).
    /// Consumes `self` so the returned future owns its state and is `'static`.
    pub async fn handle_conn<S>(self, io: S)
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let (read_half, write_half) = tokio::io::split(io);
        let mut session = Session {
            config: self.config,
            service: self.service,
            reader: BufReader::new(read_half),
            writer: write_half,
            greeted: false,
            has_mail_from: false,
            authenticated: false,
            crlf_seen: false,
            envelope: Envelope::default(),
            helo: String::new(),
            envelope_capture: self.envelope_capture,
            limits: self.limits,
        };

        if !session.reply(220, "devcloud ESMTP ready").await {
            return;
        }
        loop {
            match session.read_line_string().await {
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

struct Session<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    config: SmtpConfig,
    service: Arc<Service>,
    reader: BufReader<ReadHalf<S>>,
    writer: WriteHalf<S>,
    greeted: bool,
    has_mail_from: bool,
    authenticated: bool,
    /// Whether the client has ended a line with CRLF. Once it has, a bare-LF
    /// `.` line inside DATA is content, not the end of the message: treating
    /// `\n.\n` as the terminator lets one message smuggle in another.
    crlf_seen: bool,
    envelope: Envelope,
    helo: String,
    envelope_capture: bool,
    limits: SmtpLimits,
}

impl<S> Session<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// One line with its terminator kept, bounded by `max_line_bytes`; with
    /// an idle timeout the whole line must arrive in time (a client trickling
    /// a byte at a time cannot hold the session forever). `None` on EOF/error.
    async fn read_line_raw(&mut self) -> Option<Vec<u8>> {
        let mut buf = Vec::new();
        let limit = self.limits.max_line_bytes;
        let deadline = self
            .limits
            .idle_timeout
            .map(|t| tokio::time::Instant::now() + t);
        loop {
            let available = if let Some(deadline) = deadline {
                match tokio::time::timeout_at(deadline, self.reader.fill_buf()).await {
                    Ok(Ok(bytes)) => bytes,
                    Ok(Err(_)) => return None,
                    Err(_) => {
                        self.reply(421, "idle timeout").await;
                        return None;
                    }
                }
            } else {
                self.reader.fill_buf().await.ok()?
            };
            if available.is_empty() {
                break;
            }
            let take = available
                .iter()
                .position(|&b| b == b'\n')
                .map_or(available.len(), |i| i + 1);
            if limit > 0 && buf.len().saturating_add(take) > limit {
                self.reply(500, "line exceeds limit").await;
                return None;
            }
            let finished = available[take - 1] == b'\n';
            buf.extend_from_slice(&available[..take]);
            self.reader.consume(take);
            if finished {
                break;
            }
        }
        if buf.is_empty() {
            return None;
        }
        Some(buf)
    }

    /// Mirrors `textproto.Reader.ReadLine`: read up to '\n', strip a trailing
    /// '\r\n' or '\n'. `None` on EOF/error.
    async fn read_line_bytes(&mut self) -> Option<Vec<u8>> {
        let mut buf = self.read_line_raw().await?;
        if strip_line_end(&mut buf) {
            self.crlf_seen = true;
        }
        Some(buf)
    }

    async fn read_line_string(&mut self) -> Option<String> {
        self.read_line_bytes()
            .await
            .map(|b| String::from_utf8_lossy(&b).into_owned())
    }

    async fn reply(&mut self, code: u16, message: &str) -> bool {
        let line = format!("{} {}\r\n", code, message);
        if self.writer.write_all(line.as_bytes()).await.is_err() {
            return false;
        }
        self.writer.flush().await.is_ok()
    }

    /// Mirrors `smtpSession.handleLine`.
    async fn handle_line(&mut self, line: &str) -> bool {
        let (command, arg) = split_smtp_command(line);
        match command.as_str() {
            "HELO" | "EHLO" => {
                if arg.trim().is_empty() {
                    return self.reply(500, "syntax error").await;
                }
                self.greeted = true;
                self.helo = crate::service::sanitize_helo(&arg);
                self.reset_envelope();
                if command == "EHLO" {
                    self.reply_ehlo().await
                } else {
                    self.reply(250, "OK").await
                }
            }
            "MAIL" => {
                if !self.greeted {
                    return self.reply(503, "bad sequence of commands").await;
                }
                match parse_mail_from_arg(&arg) {
                    None => self.reply(500, "syntax error").await,
                    Some((from, size, has_size)) => {
                        let max = self.config.max_message_bytes;
                        if has_size && max > 0 && size > max {
                            self.reset_envelope();
                            return self.reply(552, "message size exceeds limit").await;
                        }
                        self.envelope = Envelope {
                            from,
                            to: Vec::new(),
                        };
                        self.has_mail_from = true;
                        self.reply(250, "OK").await
                    }
                }
            }
            "RCPT" => {
                if !self.has_mail_from {
                    return self.reply(503, "bad sequence of commands").await;
                }
                match parse_address_arg(&arg, "TO:") {
                    None => self.reply(500, "syntax error").await,
                    Some(to) => {
                        if self.limits.max_recipients > 0
                            && self.envelope.to.len() >= self.limits.max_recipients
                        {
                            return self.reply(452, "too many recipients").await;
                        }
                        self.envelope.to.push(to);
                        self.reply(250, "OK").await
                    }
                }
            }
            "DATA" => {
                if !self.has_mail_from || self.envelope.to.is_empty() {
                    return self.reply(503, "bad sequence of commands").await;
                }
                if !arg.trim().is_empty() {
                    return self.reply(500, "syntax error").await;
                }
                self.handle_data().await
            }
            "AUTH" => {
                if !self.greeted {
                    return self.reply(503, "bad sequence of commands").await;
                }
                if self.config.auth_mode_normalized() == SMTP_AUTH_OFF {
                    return self.reply(502, "command not implemented").await;
                }
                // RFC 4954: no AUTH once authenticated or during a mail
                // transaction.
                if self.authenticated || self.has_mail_from {
                    return self.reply(503, "bad sequence of commands").await;
                }
                self.handle_auth(&arg).await
            }
            "RSET" => {
                if !arg.trim().is_empty() {
                    return self.reply(500, "syntax error").await;
                }
                self.reset_envelope();
                self.reply(250, "OK").await
            }
            "NOOP" => self.reply(250, "OK").await,
            "QUIT" => {
                self.reply(221, "bye").await;
                false
            }
            "" => self.reply(500, "syntax error").await,
            _ => self.reply(502, "command not implemented").await,
        }
    }

    /// Mirrors `smtpSession.handleData`.
    async fn handle_data(&mut self) -> bool {
        if !self.reply(354, "End data with <CR><LF>.<CR><LF>").await {
            return false;
        }

        let mut raw: Vec<u8> = Vec::new();
        let mut oversized = false;
        let max = self.config.max_message_bytes;
        loop {
            let mut line = match self.read_line_raw().await {
                None => return false,
                Some(l) => l,
            };
            let crlf = strip_line_end(&mut line);
            self.crlf_seen |= crlf;
            // Only `<CRLF>.<CRLF>` ends a CRLF client's message; a client that
            // has only ever sent bare LF may end it with `.<LF>`.
            if line == b"." && (crlf || !self.crlf_seen) {
                break;
            }
            if line.starts_with(b"..") {
                line.remove(0);
            }
            if !oversized {
                let next_len = (raw.len() + line.len() + 2) as i64;
                if max > 0 && next_len > max {
                    oversized = true;
                } else {
                    raw.extend_from_slice(&line);
                    raw.extend_from_slice(b"\r\n");
                }
            }
        }
        if oversized {
            self.reset_envelope();
            return self.reply(552, "message size exceeds limit").await;
        }

        let envelope = self.envelope.clone();
        let received = if self.envelope_capture {
            self.service
                .receive_from_session(envelope, &self.helo, &raw)
        } else {
            self.service.receive(envelope, &raw)
        };
        match received {
            Err(_) => {
                // The transaction is over either way: a following RCPT or
                // DATA must not reuse this envelope.
                self.reset_envelope();
                self.reply(451, "requested action aborted: local error in processing")
                    .await
            }
            Ok(_) => {
                self.reset_envelope();
                self.reply(250, "OK").await
            }
        }
    }

    fn reset_envelope(&mut self) {
        self.envelope = Envelope::default();
        self.has_mail_from = false;
    }

    /// Mirrors `smtpSession.replyEHLO`.
    async fn reply_ehlo(&mut self) -> bool {
        let max = self.config.max_message_bytes;
        let auth_enabled = self.config.auth_mode_normalized() != SMTP_AUTH_OFF;

        let mut lines = vec!["devcloud".to_string()];
        if max > 0 {
            lines.push(format!("SIZE {}", max));
        }
        if auth_enabled {
            lines.push("AUTH PLAIN LOGIN".to_string());
        }

        let last = lines.len() - 1;
        for (i, payload) in lines.iter().enumerate() {
            let separator = if i == last { ' ' } else { '-' };
            let line = format!("250{}{}\r\n", separator, payload);
            if self.writer.write_all(line.as_bytes()).await.is_err() {
                return false;
            }
        }
        self.writer.flush().await.is_ok()
    }

    /// Mirrors `smtpSession.handleAuth`.
    async fn handle_auth(&mut self, arg: &str) -> bool {
        let (mechanism, initial) = split_auth_arg(arg);
        match mechanism.to_ascii_uppercase().as_str() {
            "PLAIN" => self.handle_auth_plain(&initial).await,
            "LOGIN" => self.handle_auth_login(&initial).await,
            "" => self.reply(501, "syntax error in AUTH").await,
            _ => self.reply(504, "unrecognized authentication type").await,
        }
    }

    async fn handle_auth_plain(&mut self, initial: &str) -> bool {
        let mut encoded = initial.to_string();
        if encoded.is_empty() {
            if !self.reply(334, "").await {
                return false;
            }
            match self.read_line_string().await {
                None => return false,
                Some(l) => encoded = l,
            }
        }
        if encoded == "*" {
            return self.reply(501, "authentication cancelled").await;
        }
        let decoded = match BASE64.decode(encoded.as_bytes()) {
            Ok(d) => d,
            Err(_) => return self.reply(501, "invalid base64").await,
        };
        let s = String::from_utf8_lossy(&decoded);
        let parts: Vec<&str> = s.splitn(3, '\u{0}').collect();
        if parts.len() != 3 {
            return self.reply(501, "malformed PLAIN credentials").await;
        }
        let username = parts[1].to_string();
        let password = parts[2].to_string();
        self.complete_auth(&username, &password).await
    }

    async fn handle_auth_login(&mut self, initial: &str) -> bool {
        let username = match self.read_auth_login_field(initial, "VXNlcm5hbWU6").await {
            Some(u) => u,
            None => return false,
        };
        let password = match self.read_auth_login_field("", "UGFzc3dvcmQ6").await {
            Some(p) => p,
            None => return false,
        };
        self.complete_auth(&username, &password).await
    }

    /// Returns `None` when the flow must abort (already replied to the client).
    async fn read_auth_login_field(&mut self, initial: &str, prompt: &str) -> Option<String> {
        let mut encoded = initial.to_string();
        if encoded.is_empty() {
            if !self.reply(334, prompt).await {
                return None;
            }
            match self.read_line_string().await {
                None => return None,
                Some(l) => encoded = l,
            }
        }
        if encoded == "*" {
            self.reply(501, "authentication cancelled").await;
            return None;
        }
        match BASE64.decode(encoded.as_bytes()) {
            Ok(d) => Some(String::from_utf8_lossy(&d).into_owned()),
            Err(_) => {
                self.reply(501, "invalid base64").await;
                None
            }
        }
    }

    async fn complete_auth(&mut self, username: &str, password: &str) -> bool {
        // Both compared in full, in constant time, so timing reveals neither.
        let user_ok = constant_time_eq(username.as_bytes(), self.config.username.as_bytes());
        let pass_ok = constant_time_eq(password.as_bytes(), self.config.password.as_bytes());
        if self.config.auth_mode_normalized() == SMTP_AUTH_STRICT && !(user_ok & pass_ok) {
            return self.reply(535, "authentication failed").await;
        }
        self.authenticated = true;
        self.reply(235, "authentication succeeded").await
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        diff |= usize::from(a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0));
    }
    diff == 0
}

/// Strips a trailing `\r\n` or `\n`; `true` when it was `\r\n`.
fn strip_line_end(buf: &mut Vec<u8>) -> bool {
    if buf.last() != Some(&b'\n') {
        return false;
    }
    buf.pop();
    if buf.last() == Some(&b'\r') {
        buf.pop();
        return true;
    }
    false
}

/// Mirrors `splitSMTPCommand`.
fn split_smtp_command(line: &str) -> (String, String) {
    let line = line.trim_end_matches([' ', '\t']);
    if line.is_empty() {
        return (String::new(), String::new());
    }
    match line.split_once(' ') {
        None => (line.to_ascii_uppercase(), String::new()),
        Some((command, arg)) => (
            command.to_ascii_uppercase(),
            arg.trim_start_matches([' ', '\t']).to_string(),
        ),
    }
}

/// Mirrors `splitAuthArg`.
fn split_auth_arg(arg: &str) -> (String, String) {
    let arg = arg.trim();
    if arg.is_empty() {
        return (String::new(), String::new());
    }
    match arg.split_once(' ') {
        None => (arg.to_string(), String::new()),
        Some((mechanism, rest)) => (mechanism.to_string(), rest.trim().to_string()),
    }
}

/// Mirrors `parseAddressArg`: path with no trailing content allowed.
fn parse_address_arg(arg: &str, prefix: &str) -> Option<String> {
    let (address, rest) = parse_path_arg(arg, prefix, false)?;
    if !rest.trim().is_empty() {
        return None;
    }
    Some(address)
}

/// Mirrors `parsePathArg`. Returns `(address, rest)`.
fn parse_path_arg(arg: &str, prefix: &str, allow_empty: bool) -> Option<(String, String)> {
    let arg = arg.trim();
    if !arg
        .get(..prefix.len())
        .is_some_and(|p| p.eq_ignore_ascii_case(prefix))
    {
        return None;
    }
    let rest = arg[prefix.len()..].trim();
    if rest.is_empty() {
        return None;
    }
    if rest.starts_with('<') {
        let end = rest.find('>')?;
        if !allow_empty && end == 1 {
            return None;
        }
        let after = &rest[end + 1..];
        if let Some(c) = after.chars().next() {
            if c != ' ' && c != '\t' {
                return None;
            }
        }
        let address = rest[1..end].to_string();
        let remaining = after.trim().to_string();
        return Some((address, remaining));
    }
    match rest.find([' ', '\t']) {
        None => Some((rest.to_string(), String::new())),
        Some(i) => {
            let address = &rest[..i];
            let remaining = &rest[i + 1..];
            if address.is_empty() {
                return None;
            }
            Some((address.to_string(), remaining.trim().to_string()))
        }
    }
}

/// Mirrors `parseMailFromArg`. Returns `(address, size, has_size)`.
fn parse_mail_from_arg(arg: &str) -> Option<(String, i64, bool)> {
    let (address, rest) = parse_path_arg(arg, "FROM:", true)?;
    for field in rest.split_whitespace() {
        // Keyword-only parameters (`SMTPUTF8`, `BODY` forms) carry no value.
        let Some((name, value)) = field.split_once('=') else {
            continue;
        };
        if !name.eq_ignore_ascii_case("SIZE") {
            continue;
        }
        if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        // A declared size too large for i64 is still a (very) large size.
        let parsed: i64 = value.parse().unwrap_or(i64::MAX);
        return Some((address, parsed, true));
    }
    Some((address, 0, false))
}
