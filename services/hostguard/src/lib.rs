//! Browser guards shared by every devcloud HTTP service.
//!
//! devcloud listens on loopback, which a web page can still reach: directly
//! (cross-site requests, which CORS only partly blocks) or through DNS
//! rebinding (the page's own domain re-pointed at 127.0.0.1, which makes its
//! requests same-origin). [`trusted_host`] refuses the latter for every
//! request; [`loopback_origin`] is the Origin rule for endpoints that browsers
//! from other sites must not drive.
/// DNS-rebinding guard. A page on `http://evil.example` whose name is
/// re-pointed at 127.0.0.1 makes same-origin requests (no `Origin`, so the
/// CSRF guard cannot tell), but its `Host` still names the attacker's domain.
/// Trusted: no `Host` (HTTP/1.0, in-process callers), IP literals, single-label
/// names (`localhost`, docker-compose service names), and names under
/// suffixes no public DNS answers for (`.localhost`, `.internal` as in
/// `host.docker.internal`, `.local`, `.test`, `.example`, `.invalid`,
/// `.home.arpa`). `DEVCLOUD_ALLOWED_HOSTS` adds names (comma-separated; a
/// leading `.` allows a suffix, `*` allows any host).
pub fn trusted_host(host: &str) -> bool {
    let host = host.trim();
    if host.is_empty() {
        return true;
    }
    let name = if let Some(v6) = host.strip_prefix('[') {
        return v6
            .split_once(']')
            .is_some_and(|(ip, _)| ip.parse::<std::net::Ipv6Addr>().is_ok());
    } else {
        match host.rsplit_once(':') {
            Some((name, port)) if port.bytes().all(|b| b.is_ascii_digit()) => name,
            _ => host,
        }
    };
    let name = name.trim_end_matches('.').to_ascii_lowercase();
    if name.parse::<std::net::Ipv4Addr>().is_ok() || !name.contains('.') {
        return !name.is_empty();
    }
    const PRIVATE_SUFFIXES: [&str; 7] = [
        ".localhost",
        ".internal",
        ".local",
        ".test",
        ".example",
        ".invalid",
        ".home.arpa",
    ];
    if PRIVATE_SUFFIXES.iter().any(|s| name.ends_with(s)) {
        return true;
    }
    allowed_hosts(
        &std::env::var("DEVCLOUD_ALLOWED_HOSTS").unwrap_or_default(),
        &name,
    )
}

/// Whether `name` (lowercase, no port) is in a `DEVCLOUD_ALLOWED_HOSTS` list.
fn allowed_hosts(list: &str, name: &str) -> bool {
    list.split(',')
        .map(|entry| entry.trim().trim_end_matches('.').to_ascii_lowercase())
        .filter(|entry| !entry.is_empty())
        .any(|entry| {
            entry == "*"
                || entry == name
                || (entry.starts_with('.') && (name.ends_with(&entry) || name == &entry[1..]))
        })
}

/// No `Origin` (SDKs, CLIs and other non-browser clients send none) or a
/// loopback one (local front-ends such as the devcloud dashboard).
pub fn loopback_origin(origin: &str) -> bool {
    if origin.is_empty() {
        return true;
    }
    let Some((scheme, rest)) = origin.split_once("://") else {
        return false; // includes the opaque `null` origin
    };
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return false;
    }
    let host = if let Some(v6) = rest.strip_prefix('[') {
        match v6.split_once(']') {
            Some((h, _)) => h.to_string(),
            None => return false,
        }
    } else {
        rest.rsplit_once(':')
            .map(|(h, _)| h)
            .unwrap_or(rest)
            .to_ascii_lowercase()
    };
    host == "localhost" || host == "127.0.0.1" || host == "::1" || host.ends_with(".localhost")
}

/// Whether a request is a write from a browser page on another site (CSRF):
/// any method but GET/HEAD/OPTIONS, carrying a non-loopback `Origin` or, with
/// no `Origin`, `Sec-Fetch-Site: cross-site`. Browsers send such "simple"
/// requests (form or `text/plain` bodies) without any CORS preflight, and
/// none of the devcloud APIs serves CORS, so no legitimate browser client is
/// refused; SDKs and CLIs send no `Origin`.
pub fn cross_site_write(method: &str, origin: &str, sec_fetch_site: &str) -> bool {
    if matches!(method, "GET" | "HEAD" | "OPTIONS") {
        return false;
    }
    if origin.is_empty() {
        return sec_fetch_site.eq_ignore_ascii_case("cross-site");
    }
    !loopback_origin(origin)
}

/// A complete `403 Forbidden` HTTP/1.1 response for a request refused by
/// [`cross_site_write`].
pub fn cross_site_response() -> Vec<u8> {
    forbidden("cross-origin requests to devcloud APIs are not allowed\n")
}

/// A complete `403 Forbidden` HTTP/1.1 response for a request refused by
/// [`trusted_host`], for services to write as is.
pub fn untrusted_host_response() -> Vec<u8> {
    forbidden("untrusted Host header (DNS rebinding guard); add the name to DEVCLOUD_ALLOWED_HOSTS to allow it\n")
}

fn forbidden(body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 403 Forbidden\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_guard_refuses_rebinding_names() {
        for ok in [
            "",
            "127.0.0.1:18025",
            "localhost",
            "localhost:9000",
            "[::1]:8080",
            "192.168.1.20:18025",
            "devcloud:18025",
            "host.docker.internal:18025",
            "abc.lambda-url.us-east-1.localhost:19010",
            "mymac.local",
            "devcloud.test",
            "LOCALHOST.",
        ] {
            assert!(trusted_host(ok), "{ok}");
        }
        for bad in [
            "evil.example.com",
            "evil.example.com:18025",
            "127.0.0.1.nip.io",
            "localhost.evil.com",
            "[not-an-ip]",
            ":80",
        ] {
            assert!(!trusted_host(bad), "{bad}");
        }
        assert!(allowed_hosts(
            "devcloud.corp.dev, .lan.example.org",
            "devcloud.corp.dev"
        ));
        assert!(allowed_hosts(".lan.example.org", "box.lan.example.org"));
        assert!(allowed_hosts(".lan.example.org", "lan.example.org"));
        assert!(!allowed_hosts(".lan.example.org", "evillan.example.org"));
        assert!(allowed_hosts("*", "anything.com"));
        assert!(!allowed_hosts("", "anything.com"));
    }

    #[test]
    fn loopback_origins() {
        assert!(loopback_origin(""));
        assert!(loopback_origin("http://127.0.0.1:18025"));
        assert!(loopback_origin("http://localhost:5173"));
        assert!(loopback_origin("http://[::1]:8080"));
        assert!(loopback_origin("http://app.localhost"));
        assert!(!loopback_origin("https://evil.example.com"));
        assert!(!loopback_origin("http://127.0.0.1.evil.example"));
        assert!(!loopback_origin("null"));
        assert!(!loopback_origin("file://"));
    }

    #[test]
    fn cross_site_writes() {
        assert!(!cross_site_write(
            "GET",
            "https://evil.example",
            "cross-site"
        ));
        assert!(!cross_site_write("POST", "", ""));
        assert!(!cross_site_write(
            "POST",
            "http://localhost:3000",
            "cross-site"
        ));
        assert!(cross_site_write("POST", "https://evil.example", ""));
        assert!(cross_site_write("DELETE", "null", ""));
        assert!(cross_site_write("POST", "", "cross-site"));
    }

    #[test]
    fn refusal_is_a_complete_response() {
        let text = String::from_utf8(untrusted_host_response()).unwrap();
        let (head, body) = text.split_once("\r\n\r\n").unwrap();
        assert!(head.starts_with("HTTP/1.1 403 Forbidden"));
        assert!(head.contains(&format!("Content-Length: {}", body.len())));
    }
}
