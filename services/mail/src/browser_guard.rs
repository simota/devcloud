//! Browser boundary for local introspection/control APIs. SDK/CLI requests
//! without Origin remain usable; browser mutations and rebinding hosts follow
//! the same policy as the dashboard.

pub(crate) fn allowed(method: &str, host: &str, origin: &str, fetch_site: &str) -> bool {
    if !trusted_host(host) {
        return false;
    }
    if matches!(method, "GET" | "HEAD" | "OPTIONS") {
        return true;
    }
    if origin.is_empty() {
        return !fetch_site.eq_ignore_ascii_case("cross-site");
    }
    let Some((scheme, authority)) = origin.split_once("://") else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return false;
    }
    let Some(name) = hostname(authority) else {
        return false;
    };
    authority.eq_ignore_ascii_case(host.trim())
        || name == "localhost"
        || name == "127.0.0.1"
        || name == "::1"
        || name.ends_with(".localhost")
}

fn hostname(authority: &str) -> Option<String> {
    let authority = authority.trim();
    if authority.is_empty()
        || authority
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
        || authority.contains(['/', '\\', '@', '?', '#'])
    {
        return None;
    }
    let valid_port = |port: &str| {
        !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) && port.parse::<u16>().is_ok()
    };
    let name = if let Some(v6) = authority.strip_prefix('[') {
        let (ip, suffix) = v6.split_once(']')?;
        if ip.parse::<std::net::Ipv6Addr>().is_err()
            || (!suffix.is_empty() && !suffix.strip_prefix(':').is_some_and(valid_port))
        {
            return None;
        }
        ip
    } else if let Some((name, port)) = authority.rsplit_once(':') {
        if name.contains(':') || !valid_port(port) {
            return None;
        }
        name
    } else {
        authority
    };
    if name.is_empty()
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b".-_:".contains(&b))
    {
        return None;
    }
    Some(name.trim_end_matches('.').to_ascii_lowercase())
}

fn trusted_host(host: &str) -> bool {
    // In-process requests and HTTP/1.0 clients need not send Host.
    if host.trim().is_empty() {
        return true;
    }
    let Some(name) = hostname(host) else {
        return false;
    };
    if name.parse::<std::net::IpAddr>().is_ok() || !name.contains('.') {
        return !name.is_empty();
    }
    if [
        ".localhost",
        ".internal",
        ".local",
        ".test",
        ".example",
        ".invalid",
        ".home.arpa",
    ]
    .iter()
    .any(|suffix| name.ends_with(suffix))
    {
        return true;
    }
    allowed_hosts(
        &std::env::var("DEVCLOUD_ALLOWED_HOSTS").unwrap_or_default(),
        &name,
    )
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_existing_local_clients_and_same_origin_access() {
        for host in [
            "",
            "localhost:16380",
            "127.0.0.1:18026",
            "[::1]:18026",
            "redis:16380",
            "host.docker.internal:16380",
            "devcloud.local:18026",
            "192.168.1.2:18026",
        ] {
            assert!(allowed("POST", host, "", ""));
        }
        assert!(allowed(
            "POST",
            "127.0.0.1:16380",
            "http://localhost:18025",
            "same-site"
        ));
        assert!(allowed(
            "DELETE",
            "devcloud.local:18026",
            "http://devcloud.local:18026",
            "same-origin"
        ));
        assert!(allowed(
            "GET",
            "127.0.0.1:18026",
            "https://evil.com",
            "cross-site"
        ));
        assert!(allowed_hosts(
            "mail.acme.com,.dev.acme.com",
            "api.dev.acme.com"
        ));
        assert!(allowed_hosts("mail.acme.com", "mail.acme.com"));
        assert!(allowed_hosts("*", "any.acme.com"));
        assert!(!allowed_hosts(".dev.acme.com", "notdev.acme.com"));
    }

    #[test]
    fn rejects_cross_site_mutations_rebinding_and_malformed_authorities() {
        for method in ["POST", "DELETE", "PUT"] {
            for origin in [
                "https://evil.com",
                "null",
                "http://localhost:123@evil.com",
                "http://[::1]evil.com",
                "file://localhost",
            ] {
                assert!(!allowed(method, "127.0.0.1:16380", origin, "cross-site"));
            }
            assert!(!allowed(method, "127.0.0.1:16380", "", "cross-site"));
        }
        for host in [
            "evil.com",
            "[::1]evil.com",
            "local/evil.com",
            "local:abc",
            "local:999999",
        ] {
            assert!(!allowed("GET", host, "", "same-origin"));
        }
    }
}
