mod common;

use common::{json, request, Fixture};

#[test]
fn decoded_detail_summary_search_and_original_attachment_bytes() {
    let f = Fixture::new();
    let raw = b"From: =?UTF-8?Q?J=C3=BCrgen?= <hdr@a>\r\nTo: \"Last, First\" <r@b>, second@b\r\nCc: c@b\r\nSubject: =?ISO-2022-JP?B?GyRCJUYlOSVIGyhC?=\r\nContent-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\ncaf=C3=A9 a=3Db\r\n--x\r\nContent-Type: application/pdf; name=fallback.pdf\r\nContent-Disposition: attachment; filename*=UTF-8''%E8%AB%8B%E6%B1%82.pdf\r\nContent-Transfer-Encoding: base64\r\n\r\nAAEC/w==\r\n--x--\r\n";
    let message = f.receive(raw);
    let base = format!("/api/mailbox/messages/{}", message.id);
    let detail = json(&request(&f.app, "GET", &base));
    assert_eq!(detail["id"], message.id);
    assert_eq!(
        detail["from"],
        serde_json::json!({"name":"Jürgen","address":"hdr@a"})
    );
    assert_eq!(detail["to"][0]["name"], "Last, First");
    assert_eq!(detail["to"].as_array().unwrap().len(), 2);
    assert_eq!(detail["cc"][0]["address"], "c@b");
    assert_eq!(detail["subject"], "テスト");
    assert_eq!(detail["text"], "café a=b");
    assert_eq!(detail["warnings"], serde_json::json!([]));
    assert_eq!(
        detail["attachments"][0],
        serde_json::json!({"index":0,"filename":"請求.pdf","contentType":"application/pdf","size":4})
    );
    let download = request(&f.app, "GET", &format!("{base}/attachments/0"));
    assert_eq!(download.body, [0, 1, 2, 255]);
    assert!(download
        .headers
        .iter()
        .any(|(k, v)| k == "Content-Disposition"
            && v.contains("filename*=UTF-8''%E8%AB%8B%E6%B1%82.pdf")));
    assert_eq!(request(&f.app, "GET", &format!("{base}/raw")).body, raw);
    assert_eq!(
        request(&f.app, "GET", &format!("{base}/attachments/99")).status,
        404
    );
    let list = json(&request(
        &f.app,
        "GET",
        "/api/mailbox/messages?q=%E3%83%86%E3%82%B9%E3%83%88",
    ));
    assert_eq!(list["total"], 1);
    assert_eq!(list["items"][0]["id"], message.id);
    assert_eq!(list["items"][0]["snippet"], "café a=b");
    assert!(list["items"][0].get("text").is_none());
    assert_eq!(
        json(&request(&f.app, "GET", "/api/mailbox/messages?q=absent"))["total"],
        0
    );
}

#[test]
fn malformed_message_does_not_block_lists_detail_search_events_or_delete() {
    let f = Fixture::new();
    let message = f.receive(b"Subject: broken\r\nContent-Type: multipart/mixed\r\n\r\nbody\r\n");
    f.receive(b"Subject: healthy\r\n\r\nhealthy body\r\n");
    for route in [
        "/api/v1/messages",
        "/api/v2/messages",
        "/api/v2/search?kind=containing&query=body",
        "/api/mailbox/messages",
    ] {
        assert_eq!(request(&f.app, "GET", route).status, 200);
    }
    let detail = json(&request(
        &f.app,
        "GET",
        &format!("/api/mailbox/messages/{}", message.id),
    ));
    assert!(!detail["warnings"].as_array().unwrap().is_empty());
    let mut receiver = f.app.events.subscribe();
    f.app.publish(&message.id);
    let event = receiver.try_recv().unwrap();
    let detail = json(&request(
        &f.app,
        "GET",
        &format!("/api/v1/messages/{}", message.id),
    ));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&event).unwrap(),
        detail
    );
    assert_eq!(
        request(
            &f.app,
            "DELETE",
            &format!("/api/v1/messages/{}", message.id)
        )
        .status,
        200
    );
    f.app.publish(&message.id);
    assert!(receiver.try_recv().is_err());
}
