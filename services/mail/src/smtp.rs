//! Mirrors `internal/services/mail/smtp.rs`.
//!
//! `goroutine + net.Conn` → `tokio::spawn` + an `AsyncRead + AsyncWrite` stream.
//! The session is a state machine driven line-by-line, identical in behavior to
//! the legacy server: same reply codes, same sequence checks, same DATA framing
//! (dot-unstuffing + cumulative size limit), same AUTH PLAIN/LOGIN flows.

use std::sync::Arc;

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
}

impl SmtpServer {
    pub fn new(config: SmtpConfig, service: Arc<Service>) -> Self {
        Self { config, service }
    }

    /// Mirrors `SMTPServer.Run`: accept loop on the configured address. A fresh
    /// per-connection `SmtpServer` (cheap: config clone + `Arc` clone) is moved
    /// into each spawned task so the session future is `'static`.
    pub async fn run(&self) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.config.addr).await?;
        loop {
            let (sock, _) = listener.accept().await?;
            let server = SmtpServer {
                config: self.config.clone(),
                service: Arc::clone(&self.service),
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
            envelope: Envelope::default(),
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
    envelope: Envelope,
}

impl<S> Session<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Mirrors `textproto.Reader.ReadLine`: read up to '\n', strip a trailing
    /// '\r\n' or '\n'. `None` on EOF/error.
    async fn read_line_bytes(&mut self) -> Option<Vec<u8>> {
        let mut buf = Vec::new();
        let n = self.reader.read_until(b'\n', &mut buf).await.ok()?;
        if n == 0 {
            return None;
        }
        if buf.last() == Some(&b'\n') {
            buf.pop();
            if buf.last() == Some(&b'\r') {
                buf.pop();
            }
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
                if self.authenticated {
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
            let mut line = match self.read_line_bytes().await {
                None => return false,
                Some(l) => l,
            };
            if line == b"." {
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
        match self.service.receive(envelope, &raw) {
            Err(_) => {
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
        if self.config.auth_mode_normalized() == SMTP_AUTH_STRICT
            && (username != self.config.username || password != self.config.password)
        {
            return self.reply(535, "authentication failed").await;
        }
        self.authenticated = true;
        self.reply(235, "authentication succeeded").await
    }
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
    if arg.len() < prefix.len() || !arg[..prefix.len()].eq_ignore_ascii_case(prefix) {
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
        let (name, value) = field.split_once('=')?;
        if !name.eq_ignore_ascii_case("SIZE") {
            continue;
        }
        let parsed: i64 = value.parse().ok().filter(|&v| v >= 0)?;
        return Some((address, parsed, true));
    }
    Some((address, 0, false))
}
