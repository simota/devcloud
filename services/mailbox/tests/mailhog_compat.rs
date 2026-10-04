mod common;

use common::{json, request, Fixture};
use devcloud_mailbox::mailhog;

#[test]
fn observed_qp_shape_matches_envelope_headers_and_data() {
    let f = Fixture::new();
    let raw = b"From: Header <hdr@a.example>\r\nTo: r1@b.example\r\nSubject: =?ISO-2022-JP?B?GyRCJUYlOSVIGyhC?=\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\nlink=3Dhttps://x.example/?a=3D1\r\n";
    let message = f.receive(raw);
    let page = json(&request(&f.app, "GET", "/api/v2/messages"));
    assert_eq!(page["total"], 1);
    assert_eq!(page["count"], 1);
    let item = &page["items"][0];
    assert_eq!(
        item["From"],
        serde_json::json!({"Relays":null,"Mailbox":"env","Domain":"a.example","Params":""})
    );
    assert_eq!(item["To"].as_array().unwrap().len(), 2);
    assert_eq!(item["Raw"]["From"], "env@a.example");
    assert_eq!(item["Raw"]["Helo"], "client.example");
    assert_eq!(
        item["Raw"]["Data"],
        String::from_utf8_lossy(mailhog::raw_data(raw)).as_ref()
    );
    assert_eq!(item["Content"]["Body"], "link=3Dhttps://x.example/?a=3D1");
    assert_eq!(item["Content"]["Size"], 212);
    assert!(item["Content"]["MIME"].is_null());
    assert!(item["MIME"].is_null());
    assert_eq!(
        item["Content"]["Headers"]["Subject"][0],
        "=?ISO-2022-JP?B?GyRCJUYlOSVIGyhC?="
    );
    assert_eq!(item["Content"]["Headers"]["Message-ID"][0], message.id);
    let received = item["Content"]["Headers"]["Received"][0].as_str().unwrap();
    assert!(
        received.starts_with("from client.example by mailhog.example (MailHog)\r\n          id ")
    );
    assert!(received.ends_with("+0000"));
    assert_eq!(
        item,
        &json(&request(
            &f.app,
            "GET",
            &format!("/api/v1/messages/{}", message.id)
        ))
    );
}

#[test]
fn pagination_search_and_deletes_follow_oracle_rules() {
    let f = Fixture::new();
    assert_eq!(
        request(&f.app, "GET", "/api/v2/messages").body,
        b"{\"total\":0,\"count\":0,\"start\":0,\"items\":[]}"
    );
    assert_eq!(request(&f.app, "GET", "/api/v1/messages").body, b"[]");
    for n in 0..255 {
        f.receive(
            format!("From: Header <hdr@a>\r\nSubject: s{n}\r\n\r\n=3D body {n}\r\n").as_bytes(),
        );
    }
    for value in ["0", "-1", "abc", "1.5", "+3", "9223372036854775808"] {
        let page = json(&request(
            &f.app,
            "GET",
            &format!("/api/v2/messages?limit={value}&start=abc"),
        ));
        assert_eq!(page["count"], 50);
        assert_eq!(page["start"], 0);
    }
    let page = json(&request(&f.app, "GET", "/api/v2/messages?limit=999"));
    assert_eq!(page["count"], 250);
    assert_eq!(page["items"][0]["Content"]["Headers"]["Subject"][0], "s254");
    let page = json(&request(&f.app, "GET", "/api/v2/messages?start=255"));
    assert_eq!(page["count"], 0);
    assert_eq!(page["total"], 255);
    for (kind, query) in [
        ("from", "HDR@A"),
        ("from", "ENV@A"),
        ("to", "r2@b"),
        ("containing", "%3D3D"),
    ] {
        let response = request(
            &f.app,
            "GET",
            &format!("/api/v2/search?kind={kind}&query={query}"),
        );
        assert_eq!(response.headers[0].1, "application/json");
        assert_eq!(json(&response)["total"], 255);
    }
    assert_eq!(
        json(&request(
            &f.app,
            "GET",
            "/api/v2/search?kind=from&query=env&start=999"
        ))["total"],
        0
    );
    for query in ["kind=nope&query=x", "kind=from&query="] {
        let r = request(&f.app, "GET", &format!("/api/v2/search?{query}"));
        assert_eq!(r.status, 400);
        assert!(r.body.is_empty());
    }
    let id = page_id(&f);
    assert_eq!(
        request(&f.app, "POST", &format!("/api/v1/messages/{id}")).status,
        405
    );
    assert_eq!(
        request(&f.app, "DELETE", &format!("/api/v1/messages/{id}")).status,
        200
    );
    assert_eq!(
        request(&f.app, "GET", &format!("/api/v1/messages/{id}")).status,
        404
    );
    assert_eq!(
        request(&f.app, "DELETE", &format!("/api/v1/messages/{id}")).status,
        404
    );
    assert!(request(&f.app, "DELETE", "/api/v1/messages")
        .body
        .is_empty());
    assert_eq!(
        json(&request(&f.app, "GET", "/api/v2/messages"))["total"],
        0
    );
}

fn page_id(f: &Fixture) -> String {
    json(&request(&f.app, "GET", "/api/v2/messages"))["items"][0]["ID"]
        .as_str()
        .unwrap()
        .into()
}

#[test]
fn naive_parts_and_downloads_keep_mailhog_numbering_and_transfer_rules() {
    let f = Fixture::new();
    let raw = b"Content-Type: multipart/mixed; boundary=x\r\n\r\npreamble\r\n--x\r\nContent-Type: multipart/alternative; boundary=y\r\n\r\n--y\r\nContent-Type: text/plain\r\n\r\ninside\r\n--y--\r\n--x\r\nContent-Type: application/pdf\r\nContent-Disposition: attachment; filename=\"safe.pdf\"\r\nContent-Transfer-Encoding: base64\r\n\r\nAAEC/w==\r\n--x\r\nContent-Type: text/plain\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\na=3Db\r\n--x--\r\nepilogue\r\n";
    let message = f.receive(raw);
    let value = json(&request(
        &f.app,
        "GET",
        &format!("/api/v1/messages/{}", message.id),
    ));
    let parts = value["MIME"]["Parts"].as_array().unwrap();
    assert_eq!(parts.len(), 5);
    assert_eq!(parts[0]["Headers"], serde_json::json!({}));
    assert_eq!(parts[0]["Body"], "preamble");
    assert_eq!(parts[4]["Body"], "--\r\nepilogue");
    assert!(parts[1]["MIME"]["Parts"].is_array());
    assert_eq!(parts[2]["Size"], b"Content-Type: application/pdf\r\nContent-Disposition: attachment; filename=\"safe.pdf\"\r\nContent-Transfer-Encoding: base64\r\n\r\nAAEC/w==".len());
    let route = |index| format!("/api/v1/messages/{}/mime/part/{index}/download", message.id);
    let r = request(&f.app, "GET", &route(2));
    assert_eq!(r.body, [0, 1, 2, 255]);
    assert!(r
        .headers
        .iter()
        .any(|(k, v)| k == "Content-Disposition" && v.contains("safe.pdf")));
    assert!(r
        .headers
        .iter()
        .any(|(k, v)| k == "Content-Transfer-Encoding" && v == "base64"));
    assert_eq!(request(&f.app, "GET", &route(3)).body, b"a=3Db");
    assert_eq!(request(&f.app, "GET", &route(99)).status, 404);
    let r = request(
        &f.app,
        "GET",
        &format!("/api/v1/messages/{}/download", message.id),
    );
    assert_eq!(r.headers[0].1, "message/rfc822");
    assert!(String::from_utf8_lossy(&r.body).contains("Return-Path: <env@a.example>\r\n"));
    assert!(r
        .body
        .ends_with(value["Content"]["Body"].as_str().unwrap().as_bytes()));
    let missing = f.receive(b"Content-Type: multipart/mixed\r\n\r\nbody\r\n");
    assert_eq!(
        json(&request(
            &f.app,
            "GET",
            &format!("/api/v1/messages/{}", missing.id)
        ))["MIME"],
        serde_json::json!({"Parts":null})
    );
}

#[test]
fn captured_null_sender_and_old_records_are_distinct_after_restart() {
    let f = Fixture::new();
    let raw = b"From: Display <header@a>\r\n\r\nbody\r\n";
    let null = f
        .app
        .service
        .receive_from(devcloud_mail::Envelope::default(), "helo", raw)
        .unwrap();
    let old = f
        .app
        .service
        .receive(devcloud_mail::Envelope::default(), raw)
        .unwrap();
    let restarted =
        devcloud_mail::Service::new(std::sync::Arc::new(devcloud_mail::FileStore::new(
            f.root.join("mail"),
            std::sync::Arc::new(devcloud_mail::FileBlobStore::new(f.root.join("blobs"))),
        )));
    for (id, sender, helo) in [(null.id, "", "helo"), (old.id, "header@a", "")] {
        let message = restarted.get(&id).unwrap().unwrap();
        let raw = restarted.get_raw(&id).unwrap().unwrap();
        let projected = mailhog::project(&message, &raw, "mailhog.example");
        assert_eq!(projected.raw.from, sender);
        assert_eq!(projected.raw.helo, helo);
    }
}
#[test]
fn repeated_prefix_boundary_split_retains_mailhog_parts() {
    let boundary = format!("{}b", "a".repeat(4096));
    let raw = format!("Content-Type: multipart/mixed; boundary={boundary}\r\n\r\n{}\r\n--{boundary}\r\nContent-Type: text/plain\r\n\r\nbody\r\n--{boundary}--", "a".repeat(128*1024));
    let message = mailhog::project(
        &devcloud_mail::Message::default(),
        raw.as_bytes(),
        "mailhog.example",
    );
    let parts = message.mime.unwrap().parts.unwrap();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[1].body, "body");
    assert_eq!(parts[2].body, "--");
}
#[test]
fn v1_and_ui_lists_enforce_their_distinct_caps() {
    let f = Fixture::new();
    for n in 0..1005 {
        f.receive(format!("Subject: message-{n}\r\n\r\nbody\r\n").as_bytes());
    }
    let v1 = json(&request(&f.app, "GET", "/api/v1/messages"));
    assert_eq!(v1.as_array().unwrap().len(), 1000);
    assert_eq!(v1[0]["Content"]["Headers"]["Subject"][0], "message-1004");
    let ui = json(&request(&f.app, "GET", "/api/mailbox/messages?limit=1000"));
    assert_eq!(ui["total"], 1005);
    assert_eq!(ui["items"].as_array().unwrap().len(), 100);
    assert_eq!(ui["items"][0]["subject"], "message-1004");
}
