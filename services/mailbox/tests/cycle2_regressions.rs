mod common;

use common::{json, request, Fixture};
use devcloud_mailbox::mailhog;

#[test]
fn eml_disposition_matches_mailhog_exactly() {
    let f = Fixture::new();
    let message = f.receive(b"Subject: plain\r\n\r\nbody\r\n");
    let response = request(
        &f.app,
        "GET",
        &format!("/api/v1/messages/{}/download", message.id),
    );
    assert!(response
        .headers
        .iter()
        .any(|(k, v)| k == "Content-Disposition"
            && v == &format!("attachment; filename=\"{}.eml\"", message.id)));
}

#[test]
fn folded_headers_preserve_original_whitespace() {
    let projected = mailhog::project(
        &devcloud_mail::Message::default(),
        b"X-Folded: part one\r\n\tpart two\r\n  part three\r\n\r\nbody",
        "mailhog.example",
    );
    assert_eq!(
        projected.content.headers["X-Folded"][0],
        "part one\tpart two  part three"
    );
}

#[test]
fn multipart_without_preamble_keeps_sizes_bodies_and_download_indices() {
    let f = Fixture::new();
    let first = "Content-Type: text/plain\r\n\r\nplain";
    let second =
        "Content-Type: application/pdf\r\nContent-Transfer-Encoding: base64\r\n\r\nAAEC/w==";
    let raw = format!("Content-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\n{first}\r\n--x\r\n{second}\r\n--x--\r\n");
    let message = f.receive(raw.as_bytes());
    let base = format!("/api/v1/messages/{}", message.id);
    let value = json(&request(&f.app, "GET", &base));
    let parts = value["MIME"]["Parts"].as_array().unwrap();
    assert_eq!(parts.len(), 3);
    for (index, (body, size)) in [
        ("plain", first.len()),
        ("AAEC/w==", second.len()),
        ("--", 2),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(parts[index]["Body"], body);
        assert_eq!(parts[index]["Size"], size);
    }
    assert_eq!(
        request(&f.app, "GET", &format!("{base}/mime/part/0/download")).body,
        b"plain"
    );
    assert_eq!(
        request(&f.app, "GET", &format!("{base}/mime/part/1/download")).body,
        [0, 1, 2, 255]
    );
}

#[test]
fn synthesized_trace_headers_append_case_insensitively() {
    let projected = mailhog::project(
        &devcloud_mail::Message {
            envelope_from: Some("sender@test".into()),
            ..Default::default()
        },
        b"received: from upstream\r\nReturn-path: <old@test>\r\n\r\nbody",
        "mailhog.example",
    );
    assert_eq!(projected.content.headers["received"].len(), 2);
    assert_eq!(projected.content.headers["received"][0], "from upstream");
    assert_eq!(
        projected.content.headers["Return-path"],
        ["<old@test>", "<sender@test>"]
    );
    assert!(!projected.content.headers.contains_key("Received"));
    assert!(!projected.content.headers.contains_key("Return-Path"));
}

#[test]
fn part_download_echoes_only_mail_headers_needed_for_parity() {
    let f = Fixture::new();
    let message = f.receive(b"Content-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\nContent-Type: text/plain\r\nContent-Transfer-Encoding: 7bit\r\nContent-ID: <part>\r\nContent-Description: description\r\nMIME-Version: 1.0\r\nSet-Cookie: injected=1\r\nLocation: https://evil.test\r\nRefresh: 0\r\nLink: <https://evil.test>\r\nContent-Encoding: gzip\r\nX-Arbitrary: no\r\n\r\nbody\r\n--x--\r\n");
    let response = request(
        &f.app,
        "GET",
        &format!("/api/v1/messages/{}/mime/part/0/download", message.id),
    );
    for name in [
        "Content-Transfer-Encoding",
        "Content-ID",
        "Content-Description",
        "MIME-Version",
    ] {
        assert!(response.headers.iter().any(|(k, _)| k == name), "{name}");
    }
    for name in [
        "Set-Cookie",
        "Location",
        "Refresh",
        "Link",
        "Content-Encoding",
        "X-Arbitrary",
    ] {
        assert!(
            !response
                .headers
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case(name)),
            "{name}"
        );
    }
}

#[test]
fn html_base_is_inside_head_or_after_doctype_and_other_content_is_unchanged() {
    use devcloud_mailbox::{mime, ui_api};
    let base = "<base target=\"_blank\">";
    for (source, expected) in [
        ("<!doctype html><html><head><title>Mail</title></head><body><img src=\"cid:pixel\"></body></html>",
         format!("<!doctype html><html><head>{base}<title>Mail</title></head><body><img src=\"cid:pixel\"></body></html>")),
        (" \n<!DOCTYPE html><html><body>Body</body></html>",
         format!(" \n<!DOCTYPE html>{base}<html><body>Body</body></html>")),
        ("<HTML><HEAD data-x='>'><title>Mail</title></HEAD></HTML>",
         format!("<HTML><HEAD data-x='>'>{base}<title>Mail</title></HEAD></HTML>")),
        ("<p>fragment <img src=\"cid:pixel\"></p>",
         format!("{base}<p>fragment <img src=\"cid:pixel\"></p>")),
    ] {
        let mut parsed = mime::parse(format!("Content-Type: text/html\r\n\r\n{source}").as_bytes());
        parsed.attachments.push(mime::Attachment { filename: "pixel.png".into(), content_type: "image/png".into(), bytes: vec![0, 1, 2], content_id: "pixel".into() });
        assert_eq!(ui_api::html(&parsed).unwrap(), expected);
    }
}

#[test]
fn html_security_headers_and_unsafe_ui_attachment_sanitization_remain() {
    let f = Fixture::new();
    let message = f.receive(b"Content-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\nContent-Type: text/html\r\n\r\n<!doctype html><head></head><script>bad()</script>\r\n--x\r\nContent-Type: image/svg+xml\r\nContent-Disposition: attachment; filename*=UTF-8''evil%0D%0AX-Injected%3A%20yes.svg\r\n\r\n<svg/>\r\n--x--\r\n");
    let base = format!("/api/mailbox/messages/{}", message.id);
    let response = request(&f.app, "GET", &format!("{base}/html"));
    assert!(response
        .headers
        .iter()
        .any(|(k, v)| k == "Referrer-Policy" && v == "no-referrer"));
    assert!(response
        .headers
        .iter()
        .any(|(k, v)| k == "Content-Security-Policy" && v == devcloud_mailbox::routes::HTML_CSP));
    assert!(!devcloud_mailbox::routes::HTML_CSP.contains("allow-scripts"));
    assert!(!devcloud_mailbox::routes::HTML_CSP.contains("allow-same-origin"));
    let attachment = request(&f.app, "GET", &format!("{base}/attachments/0"));
    assert_eq!(attachment.headers[0].1, "application/octet-stream");
    assert_eq!(attachment.body, b"<svg/>");
    let wire = devcloud_mailbox::http::response_head(&attachment, false);
    assert!(!String::from_utf8_lossy(&wire).contains("\r\nX-Injected:"));
}

#[derive(Default)]
struct DisappearingStore {
    reads: std::sync::atomic::AtomicUsize,
}

impl devcloud_mail::Store for DisappearingStore {
    fn append(
        &self,
        _: devcloud_mail::Message,
        _: &[u8],
    ) -> Result<devcloud_mail::Message, String> {
        Err("unused".into())
    }
    fn list(
        &self,
        _: devcloud_mail::ListMessagesInput,
    ) -> Result<devcloud_mail::ListMessagesResult, String> {
        Ok(Default::default())
    }
    fn list_all(&self) -> Result<Vec<devcloud_mail::MessageEntry>, String> {
        Ok(["gone", "live"]
            .into_iter()
            .map(|id| devcloud_mail::MessageEntry {
                id: id.into(),
                received_at: None,
            })
            .collect())
    }
    fn get(&self, id: &str) -> Result<Option<devcloud_mail::Message>, String> {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok((id == "live").then(|| devcloud_mail::Message {
            id: id.into(),
            ..Default::default()
        }))
    }
    fn get_raw(&self, _: &str) -> Result<Option<Vec<u8>>, String> {
        Ok(Some(b"Subject: match\r\n\r\nbody\r\n".to_vec()))
    }
    fn delete(&self, _: &str) -> Result<(), String> {
        Ok(())
    }
    fn delete_all(&self) -> Result<(), String> {
        Ok(())
    }
}

fn disappearing_app() -> (
    devcloud_mailbox::routes::App,
    std::sync::Arc<DisappearingStore>,
) {
    let store = std::sync::Arc::new(DisappearingStore::default());
    let app = devcloud_mailbox::routes::App::new(
        std::sync::Arc::new(devcloud_mail::Service::new(store.clone())),
        devcloud_mail::http::HttpAuth {
            auth_mode: "off".into(),
            username: String::new(),
            password: String::new(),
        },
        "mailhog.example".into(),
        Vec::new(),
    );
    (app, store)
}

#[test]
fn disappearing_messages_are_skipped_and_search_reuses_the_loaded_projection() {
    for route in [
        "/api/v1/messages",
        "/api/v2/messages",
        "/api/v2/search?kind=containing&query=body",
    ] {
        let (app, store) = disappearing_app();
        let value = json(&request(&app, "GET", route));
        let items = if route == "/api/v1/messages" {
            value.as_array().unwrap()
        } else {
            assert_eq!(value["count"], 1);
            value["items"].as_array().unwrap()
        };
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["ID"], "live");
        assert_eq!(store.reads.load(std::sync::atomic::Ordering::Relaxed), 2);
    }
}

#[test]
fn publishing_without_subscribers_never_loads_a_message() {
    let (app, store) = disappearing_app();
    app.publish("live");
    assert_eq!(store.reads.load(std::sync::atomic::Ordering::Relaxed), 0);
    let mut subscriber = app.events.subscribe();
    app.publish("live");
    let event = subscriber.try_recv().unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&event).unwrap()["ID"],
        "live"
    );
}

#[test]
fn list_snapshot_contains_only_id_and_time_while_get_retains_bodies() {
    let f = Fixture::new();
    let message = f.receive(b"Subject: metadata\r\n\r\noriginal body\r\n");
    assert_eq!(
        f.app.service.list_all().unwrap(),
        [devcloud_mail::MessageEntry {
            id: message.id.clone(),
            received_at: message.received_at.clone(),
        }]
    );
    assert_eq!(
        f.app.service.get(&message.id).unwrap().unwrap().text_body,
        "original body\r\n"
    );
}
