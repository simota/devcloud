//! 1:1 parity port of `internal/services/mail/parser_test.go`.

use devcloud_mail::{parse_message, Envelope};

#[test]
fn parse_message_extracts_headers_and_body() {
    let message = parse_message(
        b"From: header@example.com\r\nTo: user@example.com\r\nSubject: Hello\r\n\r\nbody\r\n",
        &Envelope::default(),
    );

    assert_eq!(message.from, "header@example.com");
    assert_eq!(message.to, vec!["user@example.com".to_string()]);
    assert_eq!(message.subject, "Hello");
    assert_eq!(message.text_body, "body\r\n");
    assert_eq!(message.parse_error, "");
}

#[test]
fn parse_message_keeps_envelope_on_parse_failure() {
    let envelope = Envelope {
        from: "sender@example.com".to_string(),
        to: vec!["user@example.com".to_string()],
    };

    let message = parse_message(b"Subject: broken\r\nnot-a-header\r\n\r\nbody", &envelope);

    assert!(!message.parse_error.is_empty(), "ParseError is empty");
    assert_eq!(message.from, envelope.from);
    assert_eq!(message.to, envelope.to);
    assert_eq!(
        message.subject, "",
        "Subject should be empty on parse failure"
    );
}

// --- Differential-parity edge cases, pinned to the Go golden oracle ---
// (captured from `internal/services/mail.ParseMessage` with Envelope{From:"ENV"}).

#[test]
fn parity_header_without_blank_terminator_no_newline() {
    // Go golden oracle: a header block with no blank-line terminator and no
    // trailing newline still parses with parseErr="" and headers populated —
    // `mail.ReadMessage` returns the header then EOF on the empty body, which
    // ParseMessage does not treat as an error.
    let env = Envelope {
        from: "ENV".to_string(),
        ..Default::default()
    };
    let m = parse_message(b"From: a@b.com", &env);
    assert_eq!(m.parse_error, "");
    // Header `From` overrides the envelope From.
    assert_eq!(m.from, "a@b.com");
    assert_eq!(m.subject, "");
    assert_eq!(m.text_body, "");
}

#[test]
fn parity_header_without_blank_terminator_trailing_crlf() {
    // Go golden oracle: same as above with a trailing CRLF — parseErr="".
    let env = Envelope {
        from: "ENV".to_string(),
        ..Default::default()
    };
    let m = parse_message(b"From: a@b.com\r\n", &env);
    assert_eq!(m.parse_error, "");
    assert_eq!(m.from, "a@b.com");
}

#[test]
fn parity_multipart_part_with_trailing_blank_line() {
    // Go golden oracle: the part body keeps its own trailing CRLF — only the
    // CRLF immediately preceding the boundary delimiter is consumed, so a part
    // ending in a blank line yields "line1\r\n", not "line1".
    let raw = b"From: s@x\r\nContent-Type: multipart/alternative; boundary=\"b\"\r\n\r\n\
--b\r\nContent-Type: text/plain\r\n\r\nline1\r\n\r\n--b--\r\n";
    let m = parse_message(raw, &Envelope::default());
    assert_eq!(m.parse_error, "");
    assert_eq!(m.text_body, "line1\r\n");
}

#[test]
fn parity_multipart_quoted_boundary_with_semicolon() {
    // Go golden oracle: mime.ParseMediaType is quote-aware, so `boundary="a;b"`
    // is kept intact and the body parses to "hi". (Regression fix — a naive
    // split on ';' would corrupt the boundary and drop the body.)
    let raw = b"From: s@x\r\nContent-Type: multipart/alternative; boundary=\"a;b\"\r\n\r\n\
--a;b\r\nContent-Type: text/plain\r\n\r\nhi\r\n--a;b--\r\n";
    let m = parse_message(raw, &Envelope::default());
    assert_eq!(m.parse_error, "");
    assert_eq!(m.text_body, "hi");
}

#[test]
fn parse_message_extracts_multipart_text_and_html_bodies() {
    let raw = [
        "From: sender@example.com",
        "To: user@example.com",
        "Subject: Multipart",
        r#"Content-Type: multipart/alternative; boundary="devcloud-boundary""#,
        "",
        "--devcloud-boundary",
        "Content-Type: text/plain; charset=utf-8",
        "",
        "plain body",
        "--devcloud-boundary",
        "Content-Type: text/html; charset=utf-8",
        "",
        "<p>html body</p>",
        "--devcloud-boundary--",
        "",
    ]
    .join("\r\n");

    let message = parse_message(raw.as_bytes(), &Envelope::default());

    assert_eq!(message.text_body, "plain body");
    assert_eq!(message.html_body, "<p>html body</p>");
}
