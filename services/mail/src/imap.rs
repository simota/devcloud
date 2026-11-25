//! Local IMAP4rev1 server (RFC 3501 + RFC 2595 `STARTTLS` + implicit TLS
//! `IMAPS`) backed by the shared mail `Service`.
//!
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadHalf,
    WriteHalf,
};
use tokio::net::TcpListener;

use crate::model::MailboxMessage;
use crate::service::Service;
use crate::smtp::{SmtpLimits, SMTP_AUTH_STRICT};
use crate::store::INBOX;
use crate::time_fmt::{imap_internaldate_from_rfc3339, rfc3339_from_imap_internaldate};
use crate::tls::{accept_tls, constant_time_eq, is_transient_accept_error, MailTls, MaybeTls};

/// Configuration for the local IMAP server.
#[derive(Clone, Debug, Default)]
pub struct ImapConfig {
    pub addr: String,
    pub auth_mode: String,
    pub username: String,
    pub password: String,
}

impl ImapConfig {
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

/// Local IMAP4rev1 server sharing the same `Service` as SMTP, POP3, and HTTP.
#[derive(Clone)]
pub struct ImapServer {
    config: ImapConfig,
    service: Arc<Service>,
    limits: SmtpLimits,
    tls: Option<MailTls>,
}

impl ImapServer {
    pub fn new(config: ImapConfig, service: Arc<Service>) -> Self {
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
                    let server = self.clone();
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
                "IMAPS listener requires TLS configuration",
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
                    let server = self.clone();
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
        let mut session = ImapSession {
            config: self.config,
            service: self.service,
            conn: Some(Conn::new(io)),
            limits: self.limits,
            tls: self.tls,
            tls_active,
            state: ImapState::NotAuthenticated,
        };

        let caps = session.capabilities_string();
        if !session
            .send_line(&format!(
                "* OK [CAPABILITY {caps}] devcloud IMAP4rev1 ready"
            ))
            .await
        {
            return;
        }
        while session.read_and_handle_command().await {}
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

    // Cancellation-safe: only consume bytes after fill_buf completes, and
    // retain partial lines while IDLE sends updates.
    async fn read_line_chunk(&mut self, buf: &mut Vec<u8>, limit: usize) -> Option<ReadLine> {
        let available = self.reader.fill_buf().await.ok()?;
        if available.is_empty() {
            return None;
        }
        let take = available
            .iter()
            .position(|&b| b == b'\n')
            .map_or(available.len(), |i| i + 1);
        if limit > 0 && buf.len().saturating_add(take) > limit {
            return Some(ReadLine::TooLong);
        }
        let finished = available[take - 1] == b'\n';
        buf.extend_from_slice(&available[..take]);
        self.reader.consume(take);
        Some(if finished {
            ReadLine::Complete
        } else {
            ReadLine::Partial
        })
    }
}

enum ReadLine {
    Partial,
    Complete,
    TooLong,
}

enum ImapState {
    NotAuthenticated,
    Authenticated,
    Selected {
        name: String,
        read_only: bool,
        entries: Vec<MailboxMessage>,
    },
}

struct ImapSession<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    config: ImapConfig,
    service: Arc<Service>,
    conn: Option<Conn<S>>,
    limits: SmtpLimits,
    tls: Option<MailTls>,
    tls_active: bool,
    state: ImapState,
}

impl<S> ImapSession<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn tls_required(&self) -> bool {
        !self.tls_active && self.tls.as_ref().is_some_and(|t| t.require)
    }

    fn capabilities_string(&self) -> String {
        let mut caps = vec!["IMAP4rev1", "IDLE", "LITERAL+"];
        if self.tls.is_some() && !self.tls_active {
            caps.push("STARTTLS");
        }
        if self.tls_required() {
            caps.push("LOGINDISABLED");
        } else {
            caps.push("AUTH=PLAIN");
        }
        caps.join(" ")
    }

    async fn read_raw_line(&mut self) -> Option<String> {
        let mut buf = Vec::new();
        let limit = self.limits.max_line_bytes;
        let deadline = self
            .limits
            .idle_timeout
            .map(|t| tokio::time::Instant::now() + t);
        loop {
            let read = self.conn.as_mut()?.read_line_chunk(&mut buf, limit);
            let result = if let Some(deadline) = deadline {
                match tokio::time::timeout_at(deadline, read).await {
                    Ok(result) => result,
                    Err(_) => {
                        self.send_line("* BYE idle timeout").await;
                        return None;
                    }
                }
            } else {
                read.await
            };
            match result {
                Some(ReadLine::Complete) => break,
                Some(ReadLine::Partial) => continue,
                Some(ReadLine::TooLong) => {
                    self.send_line("* BAD line exceeds limit").await;
                    return None;
                }
                None => return None,
            }
        }
        if buf.last() == Some(&b'\n') {
            buf.pop();
            if buf.last() == Some(&b'\r') {
                buf.pop();
            }
        }
        Some(String::from_utf8_lossy(&buf).into_owned())
    }

    async fn read_exact_bytes(&mut self, len: usize) -> Option<Vec<u8>> {
        let mut buf = vec![0u8; len];
        let deadline = self
            .limits
            .idle_timeout
            .map(|t| tokio::time::Instant::now() + t);
        let reader = &mut self.conn.as_mut()?.reader;
        if let Some(deadline) = deadline {
            match tokio::time::timeout_at(deadline, reader.read_exact(&mut buf)).await {
                Ok(Ok(_)) => Some(buf),
                Ok(Err(_)) => None,
                Err(_) => {
                    self.send_line("* BYE idle timeout").await;
                    None
                }
            }
        } else {
            reader.read_exact(&mut buf).await.ok()?;
            Some(buf)
        }
    }

    async fn read_and_handle_command(&mut self) -> bool {
        let Some(mut assembled) = self.read_raw_line().await else {
            return false;
        };
        let mut literal_hops = 0usize;
        while let Some((prefix_len, lit_len, sync)) = trailing_literal(&assembled) {
            let prefix = &assembled[..prefix_len];
            if let Some((tag, cmd, arg)) = command_parts(prefix) {
                if cmd.eq_ignore_ascii_case("APPEND") && !arg.is_empty() {
                    return self.handle_append(tag, arg, lit_len, sync).await;
                }
            }
            literal_hops += 1;
            if literal_hops > 16 {
                self.send_line("* BAD too many literals").await;
                return false;
            }
            let limit = self.limits.max_line_bytes;
            if limit > 0 && lit_len > limit {
                self.send_line("* BAD literal exceeds limit").await;
                return false;
            }
            if sync && !self.send_line("+ Ready for literal data").await {
                return false;
            }
            let Some(literal_bytes) = self.read_exact_bytes(lit_len).await else {
                return false;
            };
            let literal_str = String::from_utf8_lossy(&literal_bytes);
            assembled.truncate(prefix_len);
            assembled.push_str(&imap_quote(&literal_str));
            if let Some(rest) = self.read_raw_line().await {
                assembled.push_str(&rest);
            } else {
                return false;
            }
        }
        self.handle_line(&assembled).await
    }

    async fn send_bytes(&mut self, bytes: &[u8]) -> bool {
        let Some(conn) = self.conn.as_mut() else {
            return false;
        };
        if conn.writer.write_all(bytes).await.is_err() {
            return false;
        }
        conn.writer.flush().await.is_ok()
    }

    async fn send_line(&mut self, line: &str) -> bool {
        let mut out = Vec::with_capacity(line.len() + 2);
        out.extend_from_slice(line.as_bytes());
        out.extend_from_slice(b"\r\n");
        self.send_bytes(&out).await
    }

    async fn tagged(&mut self, tag: &str, status: &str, msg: &str) -> bool {
        self.send_line(&format!("{tag} {status} {msg}")).await
    }

    async fn handle_line(&mut self, line: &str) -> bool {
        if line.trim().is_empty() {
            return self.send_line("* BAD empty command").await;
        }
        let Some((tag, cmd, arg)) = command_parts(line) else {
            return self.send_line("* BAD missing command").await;
        };
        let cmd = cmd.to_ascii_uppercase();

        match cmd.as_str() {
            "CAPABILITY" => {
                let caps = self.capabilities_string();
                if !self.send_line(&format!("* CAPABILITY {caps}")).await {
                    return false;
                }
                self.tagged(tag, "OK", "CAPABILITY completed").await
            }
            "NOOP" => self.handle_noop(tag).await,
            "IDLE" => self.handle_idle(tag, arg).await,
            "LOGOUT" => {
                let _ = self
                    .send_line("* BYE devcloud IMAP4rev1 server logging out")
                    .await;
                let _ = self.tagged(tag, "OK", "LOGOUT completed").await;
                false
            }
            "STARTTLS" => self.handle_starttls(tag, arg).await,
            "LOGIN" => self.handle_login(tag, arg).await,
            "AUTHENTICATE" => self.handle_authenticate(tag, arg).await,
            "LIST" | "LSUB" => self.handle_list(tag, &cmd, arg).await,
            "STATUS" => self.handle_status(tag, arg).await,
            "SELECT" => self.handle_select(tag, arg, false).await,
            "EXAMINE" => self.handle_select(tag, arg, true).await,
            "CHECK" => {
                if !matches!(self.state, ImapState::Selected { .. }) {
                    return self.tagged(tag, "BAD", "no mailbox selected").await;
                }
                self.tagged(tag, "OK", "CHECK completed").await
            }
            "CLOSE" => self.handle_close(tag).await,
            "UNSELECT" => {
                if !matches!(self.state, ImapState::Selected { .. }) {
                    return self.tagged(tag, "BAD", "no mailbox selected").await;
                }
                self.state = ImapState::Authenticated;
                self.tagged(tag, "OK", "UNSELECT completed").await
            }
            "EXPUNGE" => self.handle_expunge(tag).await,
            "FETCH" => self.handle_fetch(tag, arg, false).await,
            "STORE" => self.handle_store(tag, arg, false).await,
            "SEARCH" => self.handle_search(tag, arg, false).await,
            "UID" => self.handle_uid(tag, arg).await,
            "CREATE" => self.handle_create(tag, arg).await,
            "APPEND" => {
                self.tagged(tag, "BAD", "APPEND requires a message literal")
                    .await
            }
            _ => {
                self.tagged(tag, "BAD", "unknown or unsupported command")
                    .await
            }
        }
    }

    async fn handle_noop(&mut self, tag: &str) -> bool {
        match self.refresh_selected().await {
            Ok(true) => self.tagged(tag, "OK", "NOOP completed").await,
            Ok(false) => false,
            Err(()) => self.tagged(tag, "NO", "failed to refresh mailbox").await,
        }
    }

    async fn refresh_selected(&mut self) -> Result<bool, ()> {
        let ImapState::Selected { name, entries, .. } = &mut self.state else {
            return Ok(true);
        };
        let snapshot = self.service.list_folder(name).map_err(|_| ())?.ok_or(())?;
        let mut updates = Vec::new();
        let mut remaining = 0;
        for entry in entries.iter() {
            if snapshot
                .messages
                .binary_search_by_key(&entry.uid, |m| m.uid)
                .is_err()
            {
                updates.push(format!("* {} EXPUNGE", remaining + 1));
            } else {
                remaining += 1;
            }
        }
        if snapshot.messages.len() > remaining {
            updates.push(format!("* {} EXISTS", snapshot.messages.len()));
        }
        for (idx, entry) in snapshot.messages.iter().enumerate() {
            if let Ok(old) = entries.binary_search_by_key(&entry.uid, |m| m.uid) {
                if entries[old].flags != entry.flags {
                    updates.push(format!(
                        "* {} FETCH (UID {} FLAGS ({}))",
                        idx + 1,
                        entry.uid,
                        format_flags(&entry.flags),
                    ));
                }
            }
        }
        *entries = snapshot.messages;
        for update in updates {
            if !self.send_line(&update).await {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn handle_idle(&mut self, tag: &str, arg: &str) -> bool {
        if !arg.is_empty() || matches!(self.state, ImapState::NotAuthenticated) {
            return self
                .tagged(tag, "BAD", "IDLE requires authentication and no arguments")
                .await;
        }
        let mut changes = self.service.mailbox_changes();
        if !self.send_line("+ idling").await {
            return false;
        }
        let idle_timeout = self.limits.idle_timeout;
        let timeout = async {
            match idle_timeout {
                Some(duration) => tokio::time::sleep(duration).await,
                None => std::future::pending().await,
            }
        };
        tokio::pin!(timeout);
        let mut buf = Vec::new();
        loop {
            match self.refresh_selected().await {
                Ok(true) => {}
                Ok(false) => return false,
                Err(()) => {
                    self.send_line("* BYE failed to refresh mailbox").await;
                    return false;
                }
            }
            loop {
                let conn = self.conn.as_mut().unwrap();
                tokio::select! {
                    _ = &mut timeout => {
                        self.send_line("* BYE idle timeout").await;
                        return false;
                    }
                    _ = changes.changed() => break,
                    result = conn.read_line_chunk(&mut buf, self.limits.max_line_bytes) => match result {
                        Some(ReadLine::Partial) => continue,
                        Some(ReadLine::Complete) if buf.eq_ignore_ascii_case(b"DONE\r\n")
                            || buf.eq_ignore_ascii_case(b"DONE\n") => {
                            return match self.refresh_selected().await {
                                Ok(true) => self.tagged(tag, "OK", "IDLE completed").await,
                                Ok(false) => false,
                                Err(()) => self.tagged(tag, "NO", "failed to refresh mailbox").await,
                            };
                        }
                        Some(ReadLine::Complete) => {
                            return self.tagged(tag, "BAD", "expected DONE").await;
                        }
                        Some(ReadLine::TooLong) => {
                            self.send_line("* BAD line exceeds limit").await;
                            return false;
                        }
                        None => return false,
                    }
                }
            }
        }
    }

    async fn handle_starttls(&mut self, tag: &str, arg: &str) -> bool {
        if !arg.is_empty() {
            return self.tagged(tag, "BAD", "STARTTLS takes no arguments").await;
        }
        if !matches!(self.state, ImapState::NotAuthenticated) {
            return self
                .tagged(tag, "BAD", "STARTTLS only valid before authentication")
                .await;
        }
        let Some(tls) = self.tls.clone() else {
            return self.tagged(tag, "BAD", "STARTTLS not available").await;
        };
        if self.tls_active {
            return self.tagged(tag, "BAD", "TLS already active").await;
        }
        if self
            .conn
            .as_ref()
            .is_some_and(|c| !c.reader.buffer().is_empty())
        {
            let _ = self
                .tagged(tag, "BAD", "pipelining not allowed with STARTTLS")
                .await;
            return false;
        }
        if !self.tagged(tag, "OK", "Begin TLS negotiation now").await {
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
        true
    }

    async fn handle_login(&mut self, tag: &str, arg: &str) -> bool {
        if !matches!(self.state, ImapState::NotAuthenticated) {
            return self.tagged(tag, "BAD", "already authenticated").await;
        }
        if self.tls_required() {
            return self
                .tagged(tag, "NO", "[PRIVACYREQUIRED] STARTTLS required")
                .await;
        }
        let tokens = parse_imap_args(arg);
        if tokens.len() != 2 {
            return self
                .tagged(tag, "BAD", "LOGIN requires username and password")
                .await;
        }
        if !self.authenticate("", &tokens[0], &tokens[1]) {
            return self
                .tagged(tag, "NO", "[AUTHENTICATIONFAILED] LOGIN failed")
                .await;
        }
        self.state = ImapState::Authenticated;
        self.tagged(tag, "OK", "LOGIN completed").await
    }

    async fn handle_authenticate(&mut self, tag: &str, arg: &str) -> bool {
        if !matches!(self.state, ImapState::NotAuthenticated) {
            return self.tagged(tag, "BAD", "already authenticated").await;
        }
        if self.tls_required() {
            return self
                .tagged(tag, "NO", "[PRIVACYREQUIRED] STARTTLS required")
                .await;
        }
        let mut parts = arg.splitn(2, |c: char| c == ' ' || c == '\t');
        let mech = parts.next().unwrap_or("").to_ascii_uppercase();
        let initial = parts.next().map(str::trim).unwrap_or("");
        if mech != "PLAIN" {
            return self
                .tagged(tag, "NO", "unsupported authentication mechanism")
                .await;
        }
        let encoded = if initial.is_empty() {
            if !self.send_line("+ ").await {
                return false;
            }
            match self.read_raw_line().await {
                Some(line) => line,
                None => return false,
            }
        } else {
            initial.to_string()
        };
        if encoded == "*" {
            return self.tagged(tag, "BAD", "authentication cancelled").await;
        }
        let Ok(decoded) = BASE64.decode(encoded.as_bytes()) else {
            return self.tagged(tag, "BAD", "invalid base64").await;
        };
        let text = String::from_utf8_lossy(&decoded);
        let fields: Vec<&str> = text.splitn(3, '\u{0}').collect();
        if fields.len() != 3 {
            return self.tagged(tag, "BAD", "malformed PLAIN credentials").await;
        }
        if !self.authenticate(fields[0], fields[1], fields[2]) {
            return self
                .tagged(tag, "NO", "[AUTHENTICATIONFAILED] AUTHENTICATE failed")
                .await;
        }
        self.state = ImapState::Authenticated;
        self.tagged(tag, "OK", "AUTHENTICATE completed").await
    }

    fn authenticate(&mut self, authzid: &str, user: &str, pass: &str) -> bool {
        if let Some(accounts) = self.service.accounts(&self.config.auth_mode) {
            let Some(account) = accounts.authenticate(authzid, user, pass) else {
                return false;
            };
            self.service = Arc::clone(&account.mailbox);
            true
        } else {
            self.config.verify_credentials(user, pass)
        }
    }

    async fn handle_list(&mut self, tag: &str, cmd: &str, arg: &str) -> bool {
        if matches!(self.state, ImapState::NotAuthenticated) {
            return self.tagged(tag, "BAD", "not authenticated").await;
        }
        let tokens = parse_imap_args(arg);
        if tokens.len() != 2 {
            return self
                .tagged(tag, "BAD", "LIST requires reference and pattern")
                .await;
        }
        let reference = &tokens[0];
        let pattern = &tokens[1];
        if pattern.is_empty() {
            if !self
                .send_line(&format!("* {cmd} (\\Noselect) \"/\" \"\""))
                .await
            {
                return false;
            }
        } else {
            let full_pattern = format!("{reference}{pattern}");
            let folders = match self.service.folders() {
                Ok(folders) => folders,
                Err(_) => return self.tagged(tag, "NO", "failed to list mailboxes").await,
            };
            for name in folders {
                if imap_wildcard_match(&full_pattern, &name)
                    && !self
                        .send_line(&format!(
                            "* {cmd} (\\HasNoChildren) \"/\" {}",
                            imap_quote(&name)
                        ))
                        .await
                {
                    return false;
                }
            }
        }
        self.tagged(tag, "OK", &format!("{cmd} completed")).await
    }

    async fn handle_create(&mut self, tag: &str, arg: &str) -> bool {
        if matches!(self.state, ImapState::NotAuthenticated) {
            return self.tagged(tag, "BAD", "not authenticated").await;
        }
        let Some((name, "")) = split_first_imap_arg(arg) else {
            return self.tagged(tag, "BAD", "expected mailbox name").await;
        };
        if name.is_empty()
            || name.len() > 255
            || !name
                .bytes()
                .all(|b| (b' '..=b'~').contains(&b) && !matches!(b, b'/' | b'*' | b'%'))
        {
            return self.tagged(tag, "NO", "unsupported mailbox name").await;
        }
        match self.service.create_folder(mailbox_name(&name)) {
            Ok(()) => self.tagged(tag, "OK", "CREATE completed").await,
            Err(_) => self.tagged(tag, "NO", "could not create mailbox").await,
        }
    }

    async fn handle_append(&mut self, tag: &str, arg: &str, len: usize, sync: bool) -> bool {
        let limit = self.limits.max_line_bytes;
        if limit > 0 && len > limit {
            let sent = self
                .tagged(tag, "NO", "message exceeds literal limit")
                .await;
            return sync && sent;
        }
        let prepare = || {
            if matches!(self.state, ImapState::NotAuthenticated) {
                return Err(("BAD", "not authenticated"));
            }
            let (name, flags, internal_date) =
                parse_append_args(arg).ok_or(("BAD", "invalid APPEND arguments"))?;
            let folders = self
                .service
                .folders()
                .map_err(|_| ("NO", "failed to inspect mailboxes"))?;
            if !folders.contains(mailbox_name(&name)) {
                return Err(("NO", "[TRYCREATE] mailbox does not exist"));
            }
            Ok((name, flags, internal_date))
        };
        let prepared = prepare();
        if sync {
            match &prepared {
                Err((status, message)) => return self.tagged(tag, status, message).await,
                Ok(_) if !self.send_line("+ Ready for literal data").await => return false,
                Ok(_) => {}
            }
        }
        let Some(raw) = self.read_exact_bytes(len).await else {
            return false;
        };
        let Some(trailer) = self.read_exact_bytes(2).await else {
            return false;
        };
        if trailer != b"\r\n" {
            self.tagged(tag, "BAD", "expected CRLF after message literal")
                .await;
            return false;
        }
        let (name, flags, internal_date) = match prepared {
            Ok(args) => args,
            Err((status, message)) => return self.tagged(tag, status, message).await,
        };
        let name = mailbox_name(&name);
        if self
            .service
            .append_to_folder(name, &raw, flags, internal_date)
            .is_err()
        {
            return self.tagged(tag, "NO", "failed to append message").await;
        }
        if matches!(&self.state, ImapState::Selected { name: selected, .. } if selected == name)
            && self.refresh_selected().await == Ok(false)
        {
            return false;
        }
        self.tagged(tag, "OK", "APPEND completed").await
    }

    async fn handle_status(&mut self, tag: &str, arg: &str) -> bool {
        if matches!(self.state, ImapState::NotAuthenticated) {
            return self.tagged(tag, "BAD", "not authenticated").await;
        }
        let Some((mailbox_raw, items_raw)) = split_first_imap_arg(arg) else {
            return self.tagged(tag, "BAD", "invalid STATUS arguments").await;
        };
        let items_inner = items_raw
            .trim()
            .strip_prefix('(')
            .and_then(|s| s.strip_suffix(')'))
            .unwrap_or(items_raw.trim());
        let name = mailbox_name(&mailbox_raw);
        let snap = match self.service.list_folder(name) {
            Ok(Some(s)) => s,
            Ok(None) => return self.tagged(tag, "NO", "mailbox does not exist").await,
            Err(_) => return self.tagged(tag, "NO", "failed to inspect mailbox").await,
        };
        let unseen = snap
            .messages
            .iter()
            .filter(|m| !m.flags.contains("\\Seen"))
            .count();
        let mut resp_items = Vec::new();
        for item in items_inner.split_whitespace() {
            match item.to_ascii_uppercase().as_str() {
                "MESSAGES" => resp_items.push(format!("MESSAGES {}", snap.messages.len())),
                "RECENT" => resp_items.push("RECENT 0".to_string()),
                "UIDNEXT" => resp_items.push(format!("UIDNEXT {}", snap.uid_next)),
                "UIDVALIDITY" => resp_items.push("UIDVALIDITY 1".to_string()),
                "UNSEEN" => resp_items.push(format!("UNSEEN {unseen}")),
                _ => return self.tagged(tag, "BAD", "unsupported STATUS item").await,
            }
        }
        if !self
            .send_line(&format!(
                "* STATUS {} ({})",
                imap_quote(name),
                resp_items.join(" ")
            ))
            .await
        {
            return false;
        }
        self.tagged(tag, "OK", "STATUS completed").await
    }

    async fn handle_select(&mut self, tag: &str, arg: &str, read_only: bool) -> bool {
        if matches!(self.state, ImapState::NotAuthenticated) {
            return self.tagged(tag, "BAD", "not authenticated").await;
        }
        let Some((name, "")) = split_first_imap_arg(arg) else {
            return self.tagged(tag, "BAD", "expected mailbox name").await;
        };
        let name = mailbox_name(&name);
        self.state = ImapState::Authenticated;
        let snap = match self.service.list_folder(name) {
            Ok(Some(s)) => s,
            Ok(None) => return self.tagged(tag, "NO", "mailbox does not exist").await,
            Err(_) => return self.tagged(tag, "NO", "failed to open mailbox").await,
        };
        let exists = snap.messages.len();
        let first_unseen = snap
            .messages
            .iter()
            .position(|m| !m.flags.contains("\\Seen"))
            .map(|i| i + 1);
        self.state = ImapState::Selected {
            name: name.to_string(),
            read_only,
            entries: snap.messages,
        };

        let mut lines = vec![
            format!("* {exists} EXISTS"),
            "* 0 RECENT".to_string(),
            "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)".to_string(),
        ];
        if read_only {
            lines.push("* OK [PERMANENTFLAGS ()] Read-only mailbox".to_string());
        } else {
            lines.push(
                "* OK [PERMANENTFLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft \\*)] Flags permitted"
                    .to_string(),
            );
        }
        lines.push("* OK [UIDVALIDITY 1] UIDs valid".to_string());
        lines.push(format!(
            "* OK [UIDNEXT {}] Predicted next UID",
            snap.uid_next
        ));
        if let Some(seq) = first_unseen {
            lines.push(format!("* OK [UNSEEN {seq}] First unseen message"));
        }
        for line in lines {
            if !self.send_line(&line).await {
                return false;
            }
        }
        if read_only {
            self.tagged(tag, "OK", "[READ-ONLY] EXAMINE completed")
                .await
        } else {
            self.tagged(tag, "OK", "[READ-WRITE] SELECT completed")
                .await
        }
    }

    async fn handle_close(&mut self, tag: &str) -> bool {
        let (name, read_only) = match &self.state {
            ImapState::Selected {
                name, read_only, ..
            } => (name, *read_only),
            _ => return self.tagged(tag, "BAD", "no mailbox selected").await,
        };
        if !read_only && self.expunge_folder(name).is_err() {
            return self.tagged(tag, "NO", "expunge failed").await;
        }
        self.state = ImapState::Authenticated;
        self.tagged(tag, "OK", "CLOSE completed").await
    }

    async fn handle_expunge(&mut self, tag: &str) -> bool {
        let (name, read_only) = match &self.state {
            ImapState::Selected {
                name, read_only, ..
            } => (name, *read_only),
            _ => return self.tagged(tag, "BAD", "no mailbox selected").await,
        };
        if read_only {
            return self.tagged(tag, "NO", "mailbox is read-only").await;
        }
        if self.expunge_folder(name).is_err() {
            return self.tagged(tag, "NO", "expunge failed").await;
        }
        match self.refresh_selected().await {
            Ok(true) => self.tagged(tag, "OK", "EXPUNGE completed").await,
            Ok(false) => false,
            Err(()) => self.tagged(tag, "NO", "failed to refresh mailbox").await,
        }
    }

    fn expunge_folder(&self, name: &str) -> Result<(), String> {
        let snapshot = self
            .service
            .list_folder(name)?
            .ok_or("mailbox does not exist")?;
        let ids: Vec<_> = snapshot
            .messages
            .iter()
            .map(|entry| entry.message.id.as_str())
            .collect();
        self.service.expunge_flagged(&ids)
    }

    async fn handle_uid(&mut self, tag: &str, arg: &str) -> bool {
        let (subcmd, rest) = match arg.split_once(|c: char| c == ' ' || c == '\t') {
            None => (arg.to_ascii_uppercase(), ""),
            Some((c, r)) => (c.to_ascii_uppercase(), r.trim()),
        };
        match subcmd.as_str() {
            "FETCH" => self.handle_fetch(tag, rest, true).await,
            "STORE" => self.handle_store(tag, rest, true).await,
            "SEARCH" => self.handle_search(tag, rest, true).await,
            _ => self.tagged(tag, "BAD", "unsupported UID command").await,
        }
    }

    async fn handle_fetch(&mut self, tag: &str, arg: &str, is_uid: bool) -> bool {
        let (read_only, entries) = match &self.state {
            ImapState::Selected {
                read_only, entries, ..
            } => (*read_only, entries.clone()),
            _ => return self.tagged(tag, "BAD", "no mailbox selected").await,
        };
        let Some((set_spec, items_raw)) = arg.split_once(|c: char| c == ' ' || c == '\t') else {
            return self.tagged(tag, "BAD", "FETCH syntax error").await;
        };
        let Some(indices) = resolve_set(set_spec, &entries, is_uid) else {
            return self.tagged(tag, "BAD", "invalid sequence set").await;
        };
        let Some(mut items) = parse_fetch_items(items_raw.trim()) else {
            return self.tagged(tag, "BAD", "invalid FETCH data items").await;
        };
        if is_uid && !items.iter().any(|i| matches!(i, FetchItem::Uid)) {
            items.insert(0, FetchItem::Uid);
        }

        let mark_seen = !read_only && items.iter().any(FetchItem::marks_seen);
        for idx in indices {
            let entry = &entries[idx];
            let seq = idx + 1;
            let raw = match self.service.get_raw(&entry.message.id) {
                Ok(Some(bytes)) => bytes,
                Ok(None) => continue,
                Err(_) => return self.tagged(tag, "NO", "failed to load message").await,
            };

            let id = entry.message.id.as_str();
            let Ok(previous) = self.service.message_flags(id) else {
                return self.tagged(tag, "NO", "failed to load flags").await;
            };
            let mut flags = previous;
            let saw_seen_mutation = mark_seen && !flags.contains("\\Seen");
            if saw_seen_mutation {
                let Ok(mut updated) = self.service.update_flags(&[id], |flags| {
                    flags.insert("\\Seen".to_string());
                }) else {
                    return self.tagged(tag, "NO", "failed to save flags").await;
                };
                let Some(current) = updated.remove(id) else {
                    continue;
                };
                flags = current;
                self.remember_flags(idx, &flags);
            } else if items.iter().any(|i| matches!(i, FetchItem::Flags)) {
                self.remember_flags(idx, &flags);
            }
            let include_flags =
                saw_seen_mutation && !items.iter().any(|i| matches!(i, FetchItem::Flags));

            let mut out = format!("* {seq} FETCH (").into_bytes();
            let mut first = true;
            for item in &items {
                if !first {
                    out.push(b' ');
                }
                first = false;
                write_fetch_item(&mut out, item, entry, &raw, &flags);
            }
            if include_flags {
                if !first {
                    out.push(b' ');
                }
                let f = format_flags(&flags);
                out.extend_from_slice(format!("FLAGS ({f})").as_bytes());
            }
            out.extend_from_slice(b")\r\n");
            if !self.send_bytes(&out).await {
                return false;
            }
        }
        let cmd_name = if is_uid { "UID FETCH" } else { "FETCH" };
        self.tagged(tag, "OK", &format!("{cmd_name} completed"))
            .await
    }

    async fn handle_store(&mut self, tag: &str, arg: &str, is_uid: bool) -> bool {
        let (read_only, entries) = match &self.state {
            ImapState::Selected {
                read_only, entries, ..
            } => (*read_only, entries.clone()),
            _ => return self.tagged(tag, "BAD", "no mailbox selected").await,
        };
        if read_only {
            return self.tagged(tag, "NO", "mailbox is read-only").await;
        }
        let Some((set_spec, rest)) = arg.split_once(|c: char| c == ' ' || c == '\t') else {
            return self.tagged(tag, "BAD", "STORE syntax error").await;
        };
        let rest = rest.trim_start();
        let Some((mode_raw, flags_raw)) = rest.split_once(|c: char| c == ' ' || c == '\t') else {
            return self.tagged(tag, "BAD", "STORE syntax error").await;
        };
        let Some(indices) = resolve_set(set_spec, &entries, is_uid) else {
            return self.tagged(tag, "BAD", "invalid sequence set").await;
        };
        let mode_upper = mode_raw.to_ascii_uppercase();
        let (op, silent) = match mode_upper.as_str() {
            "FLAGS" => (StoreOp::Replace, false),
            "FLAGS.SILENT" => (StoreOp::Replace, true),
            "+FLAGS" => (StoreOp::Add, false),
            "+FLAGS.SILENT" => (StoreOp::Add, true),
            "-FLAGS" => (StoreOp::Remove, false),
            "-FLAGS.SILENT" => (StoreOp::Remove, true),
            _ => return self.tagged(tag, "BAD", "invalid STORE mode").await,
        };
        let Some(new_flags) = parse_flag_list(flags_raw.trim()) else {
            return self.tagged(tag, "BAD", "invalid flags").await;
        };

        let ids: Vec<_> = indices
            .iter()
            .map(|&idx| entries[idx].message.id.as_str())
            .collect();
        let Ok(current) = self.service.update_flags(&ids, |flags| match op {
            StoreOp::Replace => *flags = new_flags.clone(),
            StoreOp::Add => flags.extend(new_flags.iter().cloned()),
            StoreOp::Remove => flags.retain(|flag| !new_flags.contains(flag)),
        }) else {
            return self.tagged(tag, "NO", "failed to save flags").await;
        };
        for idx in indices {
            let entry = &entries[idx];
            let seq = idx + 1;
            let Some(flags) = current.get(&entry.message.id) else {
                continue;
            };
            self.remember_flags(idx, flags);
            if !silent {
                let formatted = format_flags(flags);
                let line = if is_uid {
                    format!("* {seq} FETCH (UID {} FLAGS ({formatted}))", entry.uid)
                } else {
                    format!("* {seq} FETCH (FLAGS ({formatted}))")
                };
                if !self.send_line(&line).await {
                    return false;
                }
            }
        }
        let cmd_name = if is_uid { "UID STORE" } else { "STORE" };
        self.tagged(tag, "OK", &format!("{cmd_name} completed"))
            .await
    }

    fn remember_flags(&mut self, idx: usize, flags: &BTreeSet<String>) {
        if let ImapState::Selected { entries, .. } = &mut self.state {
            entries[idx].flags.clone_from(flags);
        }
    }

    async fn handle_search(&mut self, tag: &str, arg: &str, is_uid: bool) -> bool {
        let entries = match &self.state {
            ImapState::Selected { entries, .. } => entries.clone(),
            _ => return self.tagged(tag, "BAD", "no mailbox selected").await,
        };
        let tokens = tokenize_search(arg);
        let mut idx = 0usize;
        if tokens
            .get(idx)
            .is_some_and(|t| t.eq_ignore_ascii_case("CHARSET"))
        {
            idx += 2; // Skip CHARSET <name>
        }
        let Some(expr) = parse_search_expr(&tokens[idx.min(tokens.len())..]) else {
            return self.tagged(tag, "BAD", "invalid SEARCH criteria").await;
        };

        let mut matches = Vec::new();
        for (i, entry) in entries.iter().enumerate() {
            let seq = (i + 1) as u32;
            let raw = match self.service.get_raw(&entry.message.id) {
                Ok(Some(b)) => b,
                Ok(None) => continue,
                Err(_) => return self.tagged(tag, "NO", "search failed").await,
            };
            let Ok(flags) = self.service.message_flags(&entry.message.id) else {
                return self.tagged(tag, "NO", "search failed").await;
            };
            if eval_search(&expr, seq, entry, &entries, &raw, &flags) {
                matches.push(if is_uid { entry.uid } else { seq });
            }
        }
        let mut line = String::from("* SEARCH");
        for n in matches {
            line.push(' ');
            line.push_str(&n.to_string());
        }
        if !self.send_line(&line).await {
            return false;
        }
        let cmd_name = if is_uid { "UID SEARCH" } else { "SEARCH" };
        self.tagged(tag, "OK", &format!("{cmd_name} completed"))
            .await
    }
}

#[derive(Copy, Clone)]
enum StoreOp {
    Replace,
    Add,
    Remove,
}

// ---------------------------------------------------------------------------
// Parsing & formatting helpers
// ---------------------------------------------------------------------------

fn command_parts(line: &str) -> Option<(&str, &str, &str)> {
    let (tag, rest) = line.trim().split_once([' ', '\t'])?;
    let rest = rest.trim_start();
    let (cmd, arg) = rest.split_once([' ', '\t']).unwrap_or((rest, ""));
    Some((tag, cmd, arg.trim()))
}

fn mailbox_name(name: &str) -> &str {
    if name.eq_ignore_ascii_case(INBOX) {
        INBOX
    } else {
        name
    }
}

fn parse_append_args(arg: &str) -> Option<(String, BTreeSet<String>, Option<String>)> {
    let (name, mut rest) = split_first_imap_arg(arg)?;
    let mut flags = BTreeSet::new();
    if rest.starts_with('(') {
        let (list, tail) = rest.split_once(')')?;
        if !tail.is_empty() && !tail.starts_with([' ', '\t']) {
            return None;
        }
        flags = parse_flag_list(list.strip_prefix('(')?)?;
        rest = tail.trim_start();
    }
    let date = if rest.is_empty() {
        None
    } else {
        Some(rfc3339_from_imap_internaldate(
            rest.strip_prefix('"')?.strip_suffix('"')?,
        )?)
    };
    Some((name, flags, date))
}

fn trailing_literal(line: &str) -> Option<(usize, usize, bool)> {
    if !line.ends_with('}') {
        return None;
    }
    let open = line.rfind('{')?;
    if !line[..open].ends_with([' ', '\t']) {
        return None;
    }
    let inner = &line[open + 1..line.len() - 1];
    let (digits, sync) = match inner.strip_suffix('+') {
        Some(d) => (d, false),
        None => (inner, true),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let len = digits.parse::<usize>().ok()?;
    Some((open, len, sync))
}

fn imap_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        if c == '"' || c == '\\' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('"');
    out
}

fn parse_imap_args(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = input.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_ascii_whitespace() {
            chars.next();
            continue;
        }
        if c == '"' {
            chars.next();
            let mut s = String::new();
            while let Some(ch) = chars.next() {
                if ch == '\\' {
                    if let Some(esc) = chars.next() {
                        s.push(esc);
                    }
                } else if ch == '"' {
                    break;
                } else {
                    s.push(ch);
                }
            }
            out.push(s);
        } else {
            let mut s = String::new();
            while let Some(&ch) = chars.peek() {
                if ch.is_ascii_whitespace() {
                    break;
                }
                s.push(ch);
                chars.next();
            }
            out.push(s);
        }
    }
    out
}

fn split_first_imap_arg(input: &str) -> Option<(String, &str)> {
    let input = input.trim_start();
    if input.is_empty() {
        return None;
    }
    if let Some(rest) = input.strip_prefix('"') {
        let mut s = String::new();
        let mut escaped = false;
        for (i, ch) in rest.char_indices() {
            if escaped {
                s.push(ch);
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                let tail = &rest[i + 1..];
                if tail.is_empty() || tail.starts_with([' ', '\t']) {
                    return Some((s, tail.trim_start()));
                }
                return None;
            } else {
                s.push(ch);
            }
        }
        None
    } else {
        match input.split_once(|c: char| c.is_ascii_whitespace()) {
            Some((first, rest)) => Some((first.to_string(), rest.trim_start())),
            None => Some((input.to_string(), "")),
        }
    }
}

fn imap_wildcard_match(pattern: &str, mailbox: &str) -> bool {
    let pattern = pattern.as_bytes();
    let text = mailbox.as_bytes();
    let (mut pat, mut pos) = (0, 0);
    let mut wildcard = None;
    while pos < text.len() {
        match pattern.get(pat) {
            Some(b'*' | b'%') => {
                pat += 1;
                wildcard = Some((pat, pos));
            }
            Some(c)
                if *c == text[pos] || (mailbox == INBOX && c.eq_ignore_ascii_case(&text[pos])) =>
            {
                pat += 1;
                pos += 1;
            }
            _ => {
                let Some((after, consumed)) = wildcard else {
                    return false;
                };
                pat = after;
                pos = consumed + 1;
                wildcard = Some((after, pos));
            }
        }
    }
    pattern[pat..].iter().all(|b| matches!(b, b'*' | b'%'))
}

fn resolve_set(spec: &str, entries: &[MailboxMessage], is_uid: bool) -> Option<Vec<usize>> {
    if spec.is_empty() {
        return None;
    }
    let max_val: u32 = if is_uid {
        entries.last().map(|e| e.uid).unwrap_or(0)
    } else {
        entries.len() as u32
    };
    let mut ranges = Vec::new();
    for part in spec.split(',') {
        if part.is_empty() {
            return None;
        }
        if let Some((a, b)) = part.split_once(':') {
            let start = parse_seq_num(a, max_val)?;
            let end = parse_seq_num(b, max_val)?;
            ranges.push((start.min(end), start.max(end)));
        } else {
            let v = parse_seq_num(part, max_val)?;
            ranges.push((v, v));
        }
    }
    if entries.is_empty() {
        return Some(Vec::new());
    }
    let mut out = Vec::new();
    for (idx, entry) in entries.iter().enumerate() {
        let num = if is_uid { entry.uid } else { (idx + 1) as u32 };
        if ranges.iter().any(|&(lo, hi)| num >= lo && num <= hi) {
            out.push(idx);
        }
    }
    Some(out)
}

fn parse_seq_num(s: &str, max_val: u32) -> Option<u32> {
    if s == "*" {
        return Some(max_val);
    }
    let n = s.parse::<u32>().ok()?;
    if n == 0 {
        return None;
    }
    Some(n)
}

fn parse_flag_list(raw: &str) -> Option<BTreeSet<String>> {
    let inner = raw
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or(raw);
    inner.split_whitespace().map(canonical_flag).collect()
}

fn canonical_flag(f: &str) -> Option<String> {
    match f.to_ascii_lowercase().as_str() {
        "\\seen" => Some("\\Seen".to_string()),
        "\\answered" => Some("\\Answered".to_string()),
        "\\flagged" => Some("\\Flagged".to_string()),
        "\\deleted" => Some("\\Deleted".to_string()),
        "\\draft" => Some("\\Draft".to_string()),
        keyword
            if keyword.bytes().all(|b| {
                (b'!'..=b'~').contains(&b)
                    && !matches!(b, b'(' | b')' | b'{' | b'%' | b'*' | b'"' | b'\\' | b']')
            }) =>
        {
            Some(keyword.to_string())
        }
        _ => None,
    }
}

fn format_flags(flags: &BTreeSet<String>) -> String {
    flags.iter().cloned().collect::<Vec<_>>().join(" ")
}

// ---------------------------------------------------------------------------
// FETCH parsing & rendering
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum BodySection {
    All,
    Header,
    HeaderFields { fields: Vec<String>, negate: bool },
    Text,
}

#[derive(Clone, Debug)]
enum FetchItem {
    Uid,
    Flags,
    Rfc822Size,
    InternalDate,
    Envelope,
    Rfc822,
    Rfc822Header,
    Rfc822Text,
    BodyStructure {
        extended: bool,
    },
    Body {
        section: BodySection,
        label: String,
        peek: bool,
        partial: Option<(usize, usize)>,
    },
}

impl FetchItem {
    fn marks_seen(&self) -> bool {
        match self {
            FetchItem::Rfc822 | FetchItem::Rfc822Text => true,
            FetchItem::Body { peek, .. } => !*peek,
            _ => false,
        }
    }
}

fn parse_fetch_items(raw: &str) -> Option<Vec<FetchItem>> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("ALL") {
        return Some(vec![
            FetchItem::Flags,
            FetchItem::InternalDate,
            FetchItem::Rfc822Size,
            FetchItem::Envelope,
        ]);
    }
    if trimmed.eq_ignore_ascii_case("FAST") {
        return Some(vec![
            FetchItem::Flags,
            FetchItem::InternalDate,
            FetchItem::Rfc822Size,
        ]);
    }
    if trimmed.eq_ignore_ascii_case("FULL") {
        return Some(vec![
            FetchItem::Flags,
            FetchItem::InternalDate,
            FetchItem::Rfc822Size,
            FetchItem::Envelope,
            FetchItem::BodyStructure { extended: false },
        ]);
    }

    let inner = if trimmed.starts_with('(') {
        trimmed.strip_prefix('(')?.strip_suffix(')')?
    } else {
        trimmed
    };

    let mut items = Vec::new();
    let bytes = inner.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let start = i;
        let mut bracket_depth = 0usize;
        while i < bytes.len() {
            match bytes[i] {
                b'[' => bracket_depth += 1,
                b']' => bracket_depth = bracket_depth.saturating_sub(1),
                b' ' | b'\t' if bracket_depth == 0 => break,
                _ => {}
            }
            i += 1;
        }
        let token = &inner[start..i];
        items.push(parse_single_fetch_item(token)?);
    }
    if items.is_empty() {
        return None;
    }
    Some(items)
}

fn parse_single_fetch_item(token: &str) -> Option<FetchItem> {
    let upper = token.to_ascii_uppercase();
    match upper.as_str() {
        "UID" => return Some(FetchItem::Uid),
        "FLAGS" => return Some(FetchItem::Flags),
        "RFC822.SIZE" => return Some(FetchItem::Rfc822Size),
        "INTERNALDATE" => return Some(FetchItem::InternalDate),
        "ENVELOPE" => return Some(FetchItem::Envelope),
        "RFC822" => return Some(FetchItem::Rfc822),
        "RFC822.HEADER" => return Some(FetchItem::Rfc822Header),
        "RFC822.TEXT" => return Some(FetchItem::Rfc822Text),
        "BODY" => return Some(FetchItem::BodyStructure { extended: false }),
        "BODYSTRUCTURE" => return Some(FetchItem::BodyStructure { extended: true }),
        _ => {}
    }

    let (peek, rest) = if upper.starts_with("BODY.PEEK[") {
        (true, &token[10..])
    } else if upper.starts_with("BODY[") {
        (false, &token[5..])
    } else {
        return None;
    };

    let close = rest.rfind(']')?;
    let spec = rest[..close].trim();
    let after = rest[close + 1..].trim();
    let partial = if after.is_empty() {
        None
    } else {
        let inner = after.strip_prefix('<')?.strip_suffix('>')?;
        let (off, count) = inner.split_once('.')?;
        Some((off.parse::<usize>().ok()?, count.parse::<usize>().ok()?))
    };

    let spec_upper = spec.to_ascii_uppercase();
    let (section, label) = if spec_upper.is_empty() {
        (BodySection::All, "BODY[]".to_string())
    } else if spec_upper == "HEADER" {
        (BodySection::Header, "BODY[HEADER]".to_string())
    } else if spec_upper == "TEXT" || spec_upper == "1" {
        (BodySection::Text, format!("BODY[{spec_upper}]"))
    } else if let Some(list_raw) = spec_upper.strip_prefix("HEADER.FIELDS.NOT") {
        let fields = parse_header_field_list(list_raw.trim())?;
        let label = format!("BODY[HEADER.FIELDS.NOT ({})]", fields.join(" "));
        (
            BodySection::HeaderFields {
                fields,
                negate: true,
            },
            label,
        )
    } else if let Some(list_raw) = spec_upper.strip_prefix("HEADER.FIELDS") {
        let fields = parse_header_field_list(list_raw.trim())?;
        let label = format!("BODY[HEADER.FIELDS ({})]", fields.join(" "));
        (
            BodySection::HeaderFields {
                fields,
                negate: false,
            },
            label,
        )
    } else {
        return None;
    };

    Some(FetchItem::Body {
        section,
        label,
        peek,
        partial,
    })
}

fn parse_header_field_list(raw: &str) -> Option<Vec<String>> {
    let inner = raw.strip_prefix('(')?.strip_suffix(')')?;
    let fields: Vec<String> = parse_imap_args(inner);
    if fields.is_empty() {
        return None;
    }
    Some(fields)
}

fn write_fetch_item(
    out: &mut Vec<u8>,
    item: &FetchItem,
    entry: &MailboxMessage,
    raw: &[u8],
    flags: &BTreeSet<String>,
) {
    match item {
        FetchItem::Uid => out.extend_from_slice(format!("UID {}", entry.uid).as_bytes()),
        FetchItem::Flags => {
            out.extend_from_slice(format!("FLAGS ({})", format_flags(flags)).as_bytes());
        }
        FetchItem::Rfc822Size => {
            out.extend_from_slice(format!("RFC822.SIZE {}", raw.len()).as_bytes());
        }
        FetchItem::InternalDate => {
            let date = entry
                .message
                .received_at
                .as_deref()
                .map(imap_internaldate_from_rfc3339)
                .unwrap_or_else(|| "01-Jan-1970 00:00:00 +0000".to_string());
            out.extend_from_slice(format!("INTERNALDATE \"{date}\"").as_bytes());
        }
        FetchItem::Envelope => {
            let env = render_envelope(entry, raw);
            out.extend_from_slice(format!("ENVELOPE {env}").as_bytes());
        }
        FetchItem::Rfc822 => write_literal_item(out, "RFC822", raw),
        FetchItem::Rfc822Header => {
            let (hdrs, _) = split_header_body(raw);
            write_literal_item(out, "RFC822.HEADER", hdrs);
        }
        FetchItem::Rfc822Text => {
            let (_, body) = split_header_body(raw);
            write_literal_item(out, "RFC822.TEXT", body);
        }
        FetchItem::BodyStructure { extended } => {
            let (_, body) = split_header_body(raw);
            let lines = body.iter().filter(|&&b| b == b'\n').count();
            let name = if *extended { "BODYSTRUCTURE" } else { "BODY" };
            out.extend_from_slice(
                format!(
                    "{name} (\"TEXT\" \"PLAIN\" (\"CHARSET\" \"UTF-8\") NIL NIL \"7BIT\" {} {lines})",
                    body.len()
                )
                .as_bytes(),
            );
        }
        FetchItem::Body {
            section,
            label,
            partial,
            ..
        } => {
            let data = extract_body_section(raw, section);
            let (slice, full_label) = match *partial {
                Some((offset, count)) => {
                    let start = offset.min(data.len());
                    let end = start.saturating_add(count).min(data.len());
                    (&data[start..end], format!("{label}<{offset}>"))
                }
                None => (data.as_slice(), label.clone()),
            };
            write_literal_item(out, &full_label, slice);
        }
    }
}

fn write_literal_item(out: &mut Vec<u8>, label: &str, data: &[u8]) {
    out.extend_from_slice(format!("{label} {{{}}}\r\n", data.len()).as_bytes());
    out.extend_from_slice(data);
}

fn split_header_body(raw: &[u8]) -> (&[u8], &[u8]) {
    if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
        (&raw[..pos + 4], &raw[pos + 4..])
    } else if let Some(pos) = raw.windows(2).position(|w| w == b"\n\n") {
        (&raw[..pos + 2], &raw[pos + 2..])
    } else {
        (raw, b"")
    }
}

fn extract_body_section(raw: &[u8], section: &BodySection) -> Vec<u8> {
    match section {
        BodySection::All => raw.to_vec(),
        BodySection::Header => split_header_body(raw).0.to_vec(),
        BodySection::Text => split_header_body(raw).1.to_vec(),
        BodySection::HeaderFields { fields, negate } => {
            let (hdr_bytes, _) = split_header_body(raw);
            let hdr_text = String::from_utf8_lossy(hdr_bytes);
            let wanted: Vec<String> = fields.iter().map(|f| f.to_ascii_lowercase()).collect();
            let mut out = String::new();
            let mut keep_current = false;
            for raw_line in hdr_text.split('\n') {
                let line = raw_line.trim_end_matches('\r');
                if line.is_empty() {
                    break;
                }
                if line.starts_with(' ') || line.starts_with('\t') {
                    if keep_current {
                        out.push_str(line);
                        out.push_str("\r\n");
                    }
                    continue;
                }
                if let Some((name, _)) = line.split_once(':') {
                    let matched = wanted
                        .iter()
                        .any(|w| w == &name.trim().to_ascii_lowercase());
                    keep_current = if *negate { !matched } else { matched };
                    if keep_current {
                        out.push_str(line);
                        out.push_str("\r\n");
                    }
                } else {
                    keep_current = false;
                }
            }
            out.push_str("\r\n");
            out.into_bytes()
        }
    }
}

fn render_envelope(entry: &MailboxMessage, raw: &[u8]) -> String {
    let msg = &entry.message;
    let date = first_header(raw, "date")
        .or_else(|| msg.received_at.clone())
        .map(|s| imap_quote(&s))
        .unwrap_or_else(|| "NIL".to_string());
    let subject = if msg.subject.is_empty() {
        "NIL".to_string()
    } else {
        imap_quote(&msg.subject)
    };
    let from = if msg.from.is_empty() {
        "NIL".to_string()
    } else {
        format!("({})", format_imap_address(&msg.from))
    };
    let sender = from.clone();
    let reply_to = first_header(raw, "reply-to")
        .map(|s| format!("({})", format_imap_address(&s)))
        .unwrap_or_else(|| from.clone());
    let to = if msg.to.is_empty() {
        "NIL".to_string()
    } else {
        let addrs: Vec<String> = msg.to.iter().map(|a| format_imap_address(a)).collect();
        format!("({})", addrs.join(""))
    };
    let cc = first_header(raw, "cc")
        .map(|s| format!("({})", format_imap_address(&s)))
        .unwrap_or_else(|| "NIL".to_string());
    let bcc = "NIL".to_string();
    let in_reply_to = first_header(raw, "in-reply-to")
        .map(|s| imap_quote(&s))
        .unwrap_or_else(|| "NIL".to_string());
    let message_id = first_header(raw, "message-id")
        .map(|s| imap_quote(&s))
        .unwrap_or_else(|| "NIL".to_string());

    format!(
        "({date} {subject} {from} {sender} {reply_to} {to} {cc} {bcc} {in_reply_to} {message_id})"
    )
}

fn format_imap_address(raw: &str) -> String {
    let trimmed = raw.trim();
    let (name, addr) = if let (Some(lt), Some(gt)) = (trimmed.find('<'), trimmed.rfind('>')) {
        let display = trimmed[..lt].trim().trim_matches('"');
        let email = trimmed[lt + 1..gt].trim();
        (
            if display.is_empty() {
                None
            } else {
                Some(display)
            },
            email,
        )
    } else {
        (None, trimmed)
    };
    let (mailbox, host) = match addr.split_once('@') {
        Some((m, h)) => (m, h),
        None => (addr, ""),
    };
    let name_str = name.map(imap_quote).unwrap_or_else(|| "NIL".to_string());
    let mb_str = if mailbox.is_empty() {
        "NIL".to_string()
    } else {
        imap_quote(mailbox)
    };
    let host_str = if host.is_empty() {
        "NIL".to_string()
    } else {
        imap_quote(host)
    };
    format!("({name_str} NIL {mb_str} {host_str})")
}

fn first_header(raw: &[u8], name: &str) -> Option<String> {
    let (hdr_bytes, _) = split_header_body(raw);
    let text = String::from_utf8_lossy(hdr_bytes);
    for line in text.split('\n') {
        let line = line.trim_end_matches('\r');
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case(name) {
                let val = v.trim();
                if !val.is_empty() {
                    return Some(val.to_string());
                }
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// SEARCH parsing & evaluation
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum SearchExpr {
    All,
    And(Vec<SearchExpr>),
    Or(Box<SearchExpr>, Box<SearchExpr>),
    Not(Box<SearchExpr>),
    Flag(String, bool),
    Recent(bool),
    From(String),
    To(String),
    Subject(String),
    Header(String, String),
    Body(String),
    Text(String),
    Larger(usize),
    Smaller(usize),
    UidSet(String),
    SeqSet(String),
}

fn tokenize_search(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = input.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_ascii_whitespace() {
            chars.next();
        } else if c == '(' || c == ')' {
            out.push(c.to_string());
            chars.next();
        } else if c == '"' {
            chars.next();
            let mut s = String::new();
            while let Some(ch) = chars.next() {
                if ch == '\\' {
                    if let Some(esc) = chars.next() {
                        s.push(esc);
                    }
                } else if ch == '"' {
                    break;
                } else {
                    s.push(ch);
                }
            }
            out.push(s);
        } else {
            let mut s = String::new();
            while let Some(&ch) = chars.peek() {
                if ch.is_ascii_whitespace() || ch == '(' || ch == ')' {
                    break;
                }
                s.push(ch);
                chars.next();
            }
            out.push(s);
        }
    }
    out
}

fn parse_search_expr(tokens: &[String]) -> Option<SearchExpr> {
    if tokens.is_empty() {
        return None;
    }
    let mut pos = 0usize;
    let mut exprs = Vec::new();
    while pos < tokens.len() {
        exprs.push(parse_search_term(tokens, &mut pos)?);
    }
    if exprs.len() == 1 {
        Some(exprs.remove(0))
    } else {
        Some(SearchExpr::And(exprs))
    }
}

fn parse_search_term(tokens: &[String], pos: &mut usize) -> Option<SearchExpr> {
    let tok = tokens.get(*pos)?;
    *pos += 1;
    if tok == "(" {
        let mut inner = Vec::new();
        while tokens.get(*pos).is_some_and(|t| t != ")") {
            inner.push(parse_search_term(tokens, pos)?);
        }
        if tokens.get(*pos).map(String::as_str) != Some(")") {
            return None;
        }
        *pos += 1;
        return Some(SearchExpr::And(inner));
    }
    let upper = tok.to_ascii_uppercase();
    match upper.as_str() {
        "ALL" => Some(SearchExpr::All),
        "SEEN" => Some(SearchExpr::Flag("\\Seen".into(), true)),
        "UNSEEN" => Some(SearchExpr::Flag("\\Seen".into(), false)),
        "DELETED" => Some(SearchExpr::Flag("\\Deleted".into(), true)),
        "UNDELETED" => Some(SearchExpr::Flag("\\Deleted".into(), false)),
        "FLAGGED" => Some(SearchExpr::Flag("\\Flagged".into(), true)),
        "UNFLAGGED" => Some(SearchExpr::Flag("\\Flagged".into(), false)),
        "ANSWERED" => Some(SearchExpr::Flag("\\Answered".into(), true)),
        "UNANSWERED" => Some(SearchExpr::Flag("\\Answered".into(), false)),
        "DRAFT" => Some(SearchExpr::Flag("\\Draft".into(), true)),
        "UNDRAFT" => Some(SearchExpr::Flag("\\Draft".into(), false)),
        "RECENT" | "NEW" => Some(SearchExpr::Recent(true)),
        "OLD" => Some(SearchExpr::Recent(false)),
        "NOT" => Some(SearchExpr::Not(Box::new(parse_search_term(tokens, pos)?))),
        "OR" => {
            let a = parse_search_term(tokens, pos)?;
            let b = parse_search_term(tokens, pos)?;
            Some(SearchExpr::Or(Box::new(a), Box::new(b)))
        }
        "FROM" => {
            let val = tokens.get(*pos)?.clone();
            *pos += 1;
            Some(SearchExpr::From(val))
        }
        "TO" => {
            let val = tokens.get(*pos)?.clone();
            *pos += 1;
            Some(SearchExpr::To(val))
        }
        "SUBJECT" => {
            let val = tokens.get(*pos)?.clone();
            *pos += 1;
            Some(SearchExpr::Subject(val))
        }
        "HEADER" => {
            let field = tokens.get(*pos)?.clone();
            *pos += 1;
            let val = tokens.get(*pos)?.clone();
            *pos += 1;
            Some(SearchExpr::Header(field, val))
        }
        "BODY" => {
            let val = tokens.get(*pos)?.clone();
            *pos += 1;
            Some(SearchExpr::Body(val))
        }
        "TEXT" => {
            let val = tokens.get(*pos)?.clone();
            *pos += 1;
            Some(SearchExpr::Text(val))
        }
        "LARGER" => {
            let n = tokens.get(*pos)?.parse::<usize>().ok()?;
            *pos += 1;
            Some(SearchExpr::Larger(n))
        }
        "SMALLER" => {
            let n = tokens.get(*pos)?.parse::<usize>().ok()?;
            *pos += 1;
            Some(SearchExpr::Smaller(n))
        }
        "UID" => {
            let set = tokens.get(*pos)?.clone();
            *pos += 1;
            Some(SearchExpr::UidSet(set))
        }
        _ if tok
            .bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b':' | b',' | b'*')) =>
        {
            Some(SearchExpr::SeqSet(tok.clone()))
        }
        _ => None,
    }
}

fn eval_search(
    expr: &SearchExpr,
    seq: u32,
    entry: &MailboxMessage,
    all_entries: &[MailboxMessage],
    raw: &[u8],
    flags: &BTreeSet<String>,
) -> bool {
    match expr {
        SearchExpr::All => true,
        SearchExpr::And(list) => list
            .iter()
            .all(|e| eval_search(e, seq, entry, all_entries, raw, flags)),
        SearchExpr::Or(a, b) => {
            eval_search(a, seq, entry, all_entries, raw, flags)
                || eval_search(b, seq, entry, all_entries, raw, flags)
        }
        SearchExpr::Not(inner) => !eval_search(inner, seq, entry, all_entries, raw, flags),
        SearchExpr::Flag(flag, want) => flags.contains(flag) == *want,
        SearchExpr::Recent(want) => !*want,
        SearchExpr::From(needle) => {
            let n = needle.to_ascii_lowercase();
            entry.message.from.to_ascii_lowercase().contains(&n)
                || first_header(raw, "from").is_some_and(|h| h.to_ascii_lowercase().contains(&n))
        }
        SearchExpr::To(needle) => {
            let n = needle.to_ascii_lowercase();
            entry
                .message
                .to
                .iter()
                .any(|t| t.to_ascii_lowercase().contains(&n))
                || first_header(raw, "to").is_some_and(|h| h.to_ascii_lowercase().contains(&n))
        }
        SearchExpr::Subject(needle) => {
            let n = needle.to_ascii_lowercase();
            entry.message.subject.to_ascii_lowercase().contains(&n)
        }
        SearchExpr::Header(field, needle) => {
            let n = needle.to_ascii_lowercase();
            first_header(raw, field).is_some_and(|h| h.to_ascii_lowercase().contains(&n))
        }
        SearchExpr::Body(needle) => {
            let n = needle.to_ascii_lowercase();
            let (_, body) = split_header_body(raw);
            String::from_utf8_lossy(body)
                .to_ascii_lowercase()
                .contains(&n)
        }
        SearchExpr::Text(needle) => {
            let n = needle.to_ascii_lowercase();
            String::from_utf8_lossy(raw)
                .to_ascii_lowercase()
                .contains(&n)
        }
        SearchExpr::Larger(size) => raw.len() > *size,
        SearchExpr::Smaller(size) => raw.len() < *size,
        SearchExpr::UidSet(spec) => resolve_set(spec, all_entries, true)
            .is_some_and(|idxs| idxs.iter().any(|&i| all_entries[i].uid == entry.uid)),
        SearchExpr::SeqSet(spec) => resolve_set(spec, all_entries, false)
            .is_some_and(|idxs| idxs.iter().any(|&i| (i + 1) as u32 == seq)),
    }
}
