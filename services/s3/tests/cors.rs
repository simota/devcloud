use devcloud_s3::cors::default_rule;
use devcloud_s3::http::{route, route_with_auth, route_with_cors, AuthConfig, Request, Response};
use devcloud_s3::store::FileBucketStore;

const CONFIG: &str = r#"<CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <CORSRule>
    <ID>app</ID>
    <AllowedOrigin>https://app.example.com</AllowedOrigin>
    <AllowedOrigin>http://*.local.test</AllowedOrigin>
    <AllowedMethod>GET</AllowedMethod>
    <AllowedMethod>PUT</AllowedMethod>
    <AllowedHeader>x-amz-*</AllowedHeader>
    <AllowedHeader>Content-Type</AllowedHeader>
    <ExposeHeader>ETag</ExposeHeader>
    <MaxAgeSeconds>600</MaxAgeSeconds>
  </CORSRule>
  <CORSRule>
    <AllowedOrigin>*</AllowedOrigin>
    <AllowedMethod>HEAD</AllowedMethod>
  </CORSRule>
</CORSConfiguration>"#;

fn store() -> FileBucketStore {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "devcloud-s3-cors-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    FileBucketStore::new(dir)
}

fn request(method: &str, target: &str, headers: &[(&str, &str)], body: &[u8]) -> Request {
    let mut req = Request::new(method, target, body.to_vec());
    for (name, value) in headers {
        req.headers.insert(name.to_string(), value.to_string());
    }
    req
}

fn preflight(origin: &str, method: &str, headers: &str) -> Request {
    let mut fields = vec![
        ("origin", origin),
        ("access-control-request-method", method),
    ];
    if !headers.is_empty() {
        fields.push(("access-control-request-headers", headers));
    }
    request("OPTIONS", "/data/key.txt", &fields, b"")
}

fn configured_store() -> FileBucketStore {
    let store = store();
    assert_eq!(
        route(&store, &request("PUT", "/data", &[], b"")).status,
        200
    );
    let put = request("PUT", "/data?cors", &[], CONFIG.as_bytes());
    assert_eq!(route(&store, &put).status, 200);
    store
}

fn header<'a>(response: &'a Response, name: &str) -> Option<&'a str> {
    response.headers.get(name).map(String::as_str)
}

fn body(response: &Response) -> String {
    String::from_utf8_lossy(&response.body).into_owned()
}

fn strict() -> AuthConfig {
    AuthConfig {
        auth_mode: "strict".to_string(),
        access_key_id: "key".to_string(),
        secret_access_key: "secret".to_string(),
        region: "us-east-1".to_string(),
    }
}

#[test]
fn configuration_round_trips_and_deletes() {
    let store = store();
    assert_eq!(
        route(&store, &request("PUT", "/data", &[], b"")).status,
        200
    );

    let missing = route(&store, &request("GET", "/data?cors", &[], b""));
    assert_eq!(missing.status, 404);
    assert!(body(&missing).contains("NoSuchCORSConfiguration"));

    let put = request("PUT", "/data?cors", &[], CONFIG.as_bytes());
    assert_eq!(route(&store, &put).status, 200);
    let got = route(&store, &request("GET", "/data?cors", &[], b""));
    assert_eq!(got.status, 200);
    let xml = body(&got);
    assert!(xml.contains("<ID>app</ID>"));
    assert!(xml.contains("<AllowedOrigin>http://*.local.test</AllowedOrigin>"));
    assert!(xml.contains("<AllowedHeader>Content-Type</AllowedHeader>"));
    assert!(xml.contains("<MaxAgeSeconds>600</MaxAgeSeconds>"));

    assert_eq!(
        route(&store, &request("DELETE", "/data?cors", &[], b"")).status,
        204
    );
    assert_eq!(
        route(&store, &request("GET", "/data?cors", &[], b"")).status,
        404
    );
}

#[test]
fn invalid_configurations_are_rejected() {
    let store = store();
    assert_eq!(
        route(&store, &request("PUT", "/data", &[], b"")).status,
        200
    );
    let rule = |inner: &str| {
        format!("<CORSConfiguration><CORSRule>{inner}</CORSRule></CORSConfiguration>")
    };
    for xml in [
        "not xml".to_string(),
        "<CORSConfiguration></CORSConfiguration>".to_string(),
        rule("<AllowedMethod>GET</AllowedMethod>"),
        rule("<AllowedOrigin>*</AllowedOrigin>"),
        rule("<AllowedOrigin>*</AllowedOrigin><AllowedMethod>PATCH</AllowedMethod>"),
        rule("<AllowedOrigin>*.*</AllowedOrigin><AllowedMethod>GET</AllowedMethod>"),
        rule("<AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod><MaxAgeSeconds>x</MaxAgeSeconds>"),
        rule("<AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod><ExposeHeader>*</ExposeHeader>"),
        rule("<AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod><ExposeHeader>X\r\nSet-Cookie: a=b</ExposeHeader>"),
        rule("<AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod><ExposeHeader>A B</ExposeHeader>"),
    ] {
        let response = route(&store, &request("PUT", "/data?cors", &[], xml.as_bytes()));
        assert_eq!(response.status, 400, "{xml}");
        assert!(body(&response).contains("MalformedXML"), "{xml}");
    }
    let absent = request("PUT", "/absent?cors", &[], CONFIG.as_bytes());
    assert_eq!(route(&store, &absent).status, 404);
}

#[test]
fn preflight_matches_origin_method_and_headers() {
    let store = configured_store();

    let allowed = route(
        &store,
        &preflight("https://app.example.com", "PUT", "Content-Type, X-Amz-Date"),
    );
    assert_eq!(allowed.status, 200);
    assert_eq!(
        header(&allowed, "Access-Control-Allow-Origin"),
        Some("https://app.example.com")
    );
    assert_eq!(
        header(&allowed, "Access-Control-Allow-Methods"),
        Some("GET, PUT")
    );
    assert_eq!(
        header(&allowed, "Access-Control-Allow-Headers"),
        Some("content-type, x-amz-date")
    );
    assert_eq!(
        header(&allowed, "Access-Control-Allow-Credentials"),
        Some("true")
    );
    assert_eq!(header(&allowed, "Access-Control-Max-Age"), Some("600"));
    assert_eq!(
        header(&allowed, "Access-Control-Expose-Headers"),
        Some("ETag")
    );

    let wildcard = route(&store, &preflight("http://dev.local.test", "GET", ""));
    assert_eq!(wildcard.status, 200);
    assert_eq!(
        header(&wildcard, "Access-Control-Allow-Origin"),
        Some("http://dev.local.test")
    );

    let any = route(&store, &preflight("https://other.test", "HEAD", ""));
    assert_eq!(any.status, 200);
    assert_eq!(header(&any, "Access-Control-Allow-Origin"), Some("*"));
    assert_eq!(header(&any, "Access-Control-Allow-Credentials"), None);

    for denied in [
        preflight("https://evil.test", "PUT", ""),
        preflight("https://app.example.com", "DELETE", ""),
        preflight("https://app.example.com", "PUT", "authorization"),
        preflight("HTTPS://APP.EXAMPLE.COM", "PUT", ""),
    ] {
        let response = route(&store, &denied);
        assert_eq!(response.status, 403);
        assert!(body(&response).contains("CORSResponse"));
        assert_eq!(header(&response, "Access-Control-Allow-Origin"), None);
    }
}

#[test]
fn preflight_without_configuration_or_required_headers() {
    let store = store();
    let absent = route(&store, &preflight("https://a.test", "GET", ""));
    assert_eq!(absent.status, 404);

    assert_eq!(
        route(&store, &request("PUT", "/data", &[], b"")).status,
        200
    );
    let disabled = route(&store, &preflight("https://a.test", "GET", ""));
    assert_eq!(disabled.status, 403);
    assert!(body(&disabled).contains("CORS is not enabled"));

    let no_origin = request(
        "OPTIONS",
        "/data",
        &[("access-control-request-method", "GET")],
        b"",
    );
    assert_eq!(route(&store, &no_origin).status, 400);
    let no_method = request("OPTIONS", "/data", &[("origin", "https://a.test")], b"");
    assert_eq!(route(&store, &no_method).status, 400);
}

#[test]
fn actual_requests_carry_cors_headers_only_when_matched() {
    let store = configured_store();
    let origin = [("origin", "https://app.example.com")];
    let put = route(&store, &request("PUT", "/data/key.txt", &origin, b"hello"));
    assert_eq!(put.status, 200);
    assert_eq!(
        header(&put, "Access-Control-Allow-Origin"),
        Some("https://app.example.com")
    );
    assert!(header(&put, "Vary").unwrap().contains("Origin"));

    // Browser clients need the headers to read error responses too.
    let missing = route(&store, &request("GET", "/data/none.txt", &origin, b""));
    assert_eq!(missing.status, 404);
    assert_eq!(
        header(&missing, "Access-Control-Allow-Origin"),
        Some("https://app.example.com")
    );

    let other = [("origin", "https://evil.test")];
    let denied = route(&store, &request("GET", "/data/key.txt", &other, b""));
    assert_eq!(denied.status, 200);
    assert_eq!(header(&denied, "Access-Control-Allow-Origin"), None);
    assert!(header(&denied, "Vary").is_some());

    let plain = route(&store, &request("GET", "/data/key.txt", &[], b""));
    assert_eq!(header(&plain, "Access-Control-Allow-Origin"), None);
    assert_eq!(header(&plain, "Vary"), None);
}

#[test]
fn strict_mode_answers_preflight_without_a_signature_but_still_rejects_requests() {
    let store = configured_store();
    let auth = strict();
    let allowed = route_with_auth(
        &store,
        &preflight("https://app.example.com", "PUT", ""),
        &auth,
    );
    assert_eq!(allowed.status, 200);

    let origin = [("origin", "https://app.example.com")];
    let unsigned = route_with_auth(
        &store,
        &request("GET", "/data/key.txt", &origin, b""),
        &auth,
    );
    assert_eq!(unsigned.status, 403);
    assert!(body(&unsigned).contains("AccessDenied"));
    assert_eq!(
        header(&unsigned, "Access-Control-Allow-Origin"),
        Some("https://app.example.com")
    );
}

#[test]
fn server_default_applies_until_a_bucket_defines_its_own_rules() {
    let store = store();
    let auth = AuthConfig::default();
    let rule = default_rule(&["https://app.example.com".to_string()]).unwrap();
    let default = Some(&rule);

    // The default also covers ListBuckets and creating a bucket that does not exist yet.
    let root = request(
        "OPTIONS",
        "/",
        &[
            ("origin", "https://app.example.com"),
            ("access-control-request-method", "GET"),
        ],
        b"",
    );
    assert_eq!(route_with_cors(&store, &root, &auth, default).status, 200);
    assert_eq!(route_with_cors(&store, &root, &auth, None).status, 403);

    let origin = [("origin", "https://app.example.com")];
    let create = route_with_cors(
        &store,
        &request("PUT", "/data", &origin, b""),
        &auth,
        default,
    );
    assert_eq!(create.status, 200);
    assert_eq!(
        header(&create, "Access-Control-Allow-Origin"),
        Some("https://app.example.com")
    );
    assert!(header(&create, "Access-Control-Expose-Headers")
        .unwrap()
        .contains("ETag"));
    let with_any_header = preflight("https://app.example.com", "DELETE", "x-custom");
    assert_eq!(
        route_with_cors(&store, &with_any_header, &auth, default).status,
        200
    );

    let own = "<CORSConfiguration><CORSRule><AllowedOrigin>https://own.test</AllowedOrigin>\
               <AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>";
    let put = request("PUT", "/data?cors", &[], own.as_bytes());
    assert_eq!(route(&store, &put).status, 200);
    let replaced = preflight("https://app.example.com", "GET", "");
    assert_eq!(
        route_with_cors(&store, &replaced, &auth, default).status,
        403
    );
}

#[test]
fn bucket_with_only_a_cors_configuration_can_be_deleted() {
    let store = configured_store();
    let origin = [("origin", "https://app.example.com")];
    let deleted = route(&store, &request("DELETE", "/data", &origin, b""));
    assert_eq!(deleted.status, 204);
    assert_eq!(
        header(&deleted, "Access-Control-Allow-Origin"),
        None,
        "DELETE is not allowed by the configured rules"
    );
    assert_eq!(
        route(&store, &request("HEAD", "/data", &[], b"")).status,
        404
    );
}

#[test]
fn introspection_paths_are_not_cors_enabled() {
    let store = configured_store();
    let auth = AuthConfig::default();
    let rule = default_rule(&["*".to_string()]).unwrap();
    let origin = [("origin", "https://app.example.com")];
    let response = route_with_cors(
        &store,
        &request("GET", "/_introspect/buckets", &origin, b""),
        &auth,
        Some(&rule),
    );
    assert_eq!(response.status, 200);
    assert_eq!(header(&response, "Access-Control-Allow-Origin"), None);
}
