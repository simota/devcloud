use devcloud_mail::Message;
use devcloud_mailbox::{mailhog, mime};

#[test]
fn transfer_charsets_and_encoded_words_have_independent_expected_text() {
    let mut warnings = Vec::new();
    assert_eq!(
        mime::decode_qp(b"a=3Db caf=C3=A9 soft=\r\nbreak", &mut warnings),
        "a=b café softbreak".as_bytes()
    );
    assert_eq!(
        mime::decode_base64(b"SG Vs\r\nbG8", &mut warnings),
        b"Hello"
    );
    assert_eq!(
        mime::decode_words(
            "=?UTF-8?Q?Caf=C3=A9?= \t=?UTF-8?B?IOKYlQ==?=",
            &mut warnings
        ),
        "Café ☕"
    );
    assert_eq!(
        mime::decode_words("=?ISO-2022-JP?B?GyRCJUYlOSVIGyhC?=", &mut warnings),
        "テスト"
    );
    for (label, bytes, expected) in [
        ("us-ascii", &b"plain"[..], "plain"),
        (
            "iso-8859-1",
            &[0x80, 0x85, 0x8a, 0x9f, 0xe9][..],
            "\u{80}\u{85}\u{8a}\u{9f}é",
        ),
        (
            "shift_jis",
            &[0x83, 0x65, 0x83, 0x58, 0x83, 0x67][..],
            "テスト",
        ),
        (
            "euc-jp",
            &[0xa5, 0xc6, 0xa5, 0xb9, 0xa5, 0xc8][..],
            "テスト",
        ),
        ("utf-8", &b"\xef\xbb\xbftext"[..], "\u{feff}text"),
    ] {
        assert_eq!(mime::charset_decode(bytes, label, &mut warnings), expected);
    }
    assert!(warnings.is_empty(), "{warnings:?}");
    for label in ["x-unknown", "replacement", "iso-2022-cn"] {
        warnings.clear();
        assert_eq!(
            mime::charset_decode(b"fallback", label, &mut warnings),
            "fallback"
        );
        assert!(!warnings.is_empty());
    }
    warnings.clear();
    mime::charset_decode(&[0xff], "utf-8", &mut warnings);
    assert!(!warnings.is_empty());
}

#[test]
fn multipart_matches_lines_preserves_bytes_and_flushes_unterminated_part() {
    let raw = b"Content-Type: multipart/mixed; boundary=\"a;b\"\r\n\r\npreamble\r\n--a;b \t\r\nContent-Type: text/plain\r\n\r\n--a;bx\r\nbody\r\n--a;b\r\nContent-Type: application/pdf\r\nContent-Disposition: attachment; filename*0*=UTF-8''%E8%AB%8B; filename*1*=%E6%B1%82.pdf\r\nContent-Transfer-Encoding: base64\r\n\r\nAAEC/w==\r\n--a;b--\r\nepilogue";
    let parsed = mime::parse(raw);
    assert_eq!(parsed.text, "--a;bx\r\nbody");
    assert_eq!(parsed.attachments.len(), 1);
    assert_eq!(parsed.attachments[0].filename, "請求.pdf");
    assert_eq!(parsed.attachments[0].bytes, [0, 1, 2, 255]);
    assert!(parsed.warnings.is_empty());
    let missing = mime::parse(b"Content-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\nContent-Type: text/plain\r\n\r\nlast part");
    assert_eq!(missing.text, "last part");
    assert!(!missing.warnings.is_empty());
}

#[test]
fn hostile_mime_is_bounded_and_reports_warnings() {
    let mut nested = Vec::new();
    for n in 0..10_000 {
        nested.extend_from_slice(
            format!("Content-Type: multipart/mixed; boundary=d{n}\r\n\r\n--d{n}\r\n").as_bytes(),
        );
    }
    nested.extend_from_slice(b"Content-Type: text/plain\r\n\r\nleaf\r\n");
    for n in (0..10_000).rev() {
        nested.extend_from_slice(format!("--d{n}--\r\n").as_bytes());
    }
    let parsed = mime::parse(&nested);
    assert!(parsed.warnings.iter().any(|s| s.contains("nesting")));
    assert_eq!(parsed.attachments.len(), 1);
    let legacy = devcloud_mail::parse_message(&nested, &devcloud_mail::Envelope::default());
    assert!(!legacy.parse_error.is_empty());
    let compat = mailhog::project(&legacy, &nested, "mailhog.example");
    assert!(serde_json::to_vec(&compat).is_ok());

    let mut parts = b"Content-Type: multipart/mixed; boundary=x\r\n\r\n".to_vec();
    for _ in 0..100_000 {
        parts.extend_from_slice(b"--x\r\nContent-Type: text/plain\r\n\r\na\r\n");
    }
    parts.extend_from_slice(b"--x--");
    let parsed = mime::parse(&parts);
    assert!(parsed.warnings.iter().any(|s| s.contains("part limit")));

    let mut big = b"Subject: retained\r\nX-Huge: ".to_vec();
    big.extend(std::iter::repeat_n(b'x', 2 * 1024 * 1024));
    big.extend_from_slice(b"\r\n\r\nbody");
    let parsed = mime::parse(&big);
    assert_eq!(mime::header(&parsed.headers, "Subject"), "retained");
    assert_eq!(parsed.text, "body");
    assert!(parsed
        .warnings
        .iter()
        .any(|s| s.contains("headers truncated")));
    let mut fields = Vec::new();
    for _ in 0..10_001 {
        fields.extend_from_slice(b"X: v\r\n");
    }
    fields.extend_from_slice(b"\r\nbody");
    let parsed = mime::parse(&fields);
    assert_eq!(parsed.headers.len(), 10_000);
    assert!(!parsed.warnings.is_empty());

    for raw in [
        &b"Content-Type: multipart/mixed\r\n\r\nbody"[..],
        &b"Content-Transfer-Encoding: base64\r\n\r\nSGVsbG8@@@!!!#"[..],
        &b"Content-Transfer-Encoding: quoted-printable\r\n\r\nbad =ZZ =4"[..],
    ] {
        assert!(!mime::parse(raw).warnings.is_empty());
    }
    let mut warnings = Vec::new();
    assert_eq!(mime::decode_qp(b"bad =ZZ =4", &mut warnings), b"bad =ZZ =4");
}

#[test]
fn fixed_seed_randomized_thousand_inputs_never_panic() {
    let mut seed = 0x7e91_cafe_babe_0123u64;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for n in 0..1000 {
        let len = (next() % 4096) as usize;
        let mut raw = match n % 4 {
            0 => b"Content-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\n".to_vec(),
            1 => b"Content-Type: text/plain; charset=replacement\r\nContent-Transfer-Encoding: base64\r\n\r\n".to_vec(),
            2 => b"Subject: =?UTF-8?Q?broken\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\n".to_vec(),
            _ => Vec::new(),
        };
        raw.extend((0..len).map(|_| next() as u8));
        let result = std::panic::catch_unwind(|| {
            let parsed = mime::parse(&raw);
            let mut warnings = Vec::new();
            mime::decode_words(&String::from_utf8_lossy(&raw), &mut warnings);
            let projected = mailhog::project(&Message::default(), &raw, "mailhog.example");
            serde_json::to_vec(&projected).unwrap();
            assert!(parsed.attachments.len() <= mime::MAX_PARTS);
        });
        assert!(result.is_ok(), "seed case {n}");
    }
}
#[test]
fn rfc2231_mixed_continuations_decode_only_starred_segments() {
    let (_, params) = mime::media_type("attachment; filename*0*=UTF-8''caf%C3%A9; filename*1=\"%20-literal\"; filename*2*=%20file.txt");
    let mut warnings = Vec::new();
    assert_eq!(
        mime::parameter(&params, "filename", &mut warnings).unwrap(),
        "café%20-literal file.txt"
    );
    assert!(warnings.is_empty());
}

#[test]
fn soft_breaks_split_words_and_inline_text_parts_follow_rfc_practice() {
    let mut warnings = Vec::new();
    // RFC 2045: transport padding may sit between `=` and the line break.
    assert_eq!(
        mime::decode_qp(b"foo= \r\nbar a=\t\nb", &mut warnings),
        b"foobar ab"
    );
    // A multibyte character split across adjacent encoded words.
    assert_eq!(
        mime::decode_words(
            "=?UTF-8?B?44GC?= =?UTF-8?B?44E=?= =?UTF-8?B?gg==?=",
            &mut warnings
        ),
        "ああ"
    );
    assert!(warnings.is_empty(), "{warnings:?}");

    // text, image, text (Apple Mail): both text parts are body text...
    let mixed = b"Content-Type: multipart/mixed; boundary=m\r\n\r\n--m\r\nContent-Type: text/plain\r\n\r\npart one\r\n--m\r\nContent-Type: image/png\r\nContent-Transfer-Encoding: base64\r\n\r\nAAEC\r\n--m\r\nContent-Type: text/plain\r\n\r\npart two\r\n--m--\r\n";
    let parsed = mime::parse(mixed);
    assert!(
        parsed.text.contains("part one") && parsed.text.contains("part two"),
        "{:?}",
        parsed.text
    );
    // ...but alternatives are one body rendered several ways.
    let alt = b"Content-Type: multipart/alternative; boundary=a\r\n\r\n--a\r\nContent-Type: text/plain\r\n\r\nfirst\r\n--a\r\nContent-Type: text/plain\r\n\r\nsecond\r\n--a--\r\n";
    assert_eq!(mime::parse(alt).text.trim(), "first");
}

#[test]
fn mailhog_json_keeps_repeated_headers() {
    let raw = b"Received: from a\r\nReceived: from b\r\nTo: x@y.test\r\nTo: z@w.test\r\nSubject: s\r\n\r\nbody\r\n";
    let message = Message {
        id: "m1".into(),
        raw: "r".into(),
        ..Default::default()
    };
    let projected =
        serde_json::to_value(mailhog::project(&message, raw, "mailhog.example")).unwrap();
    let headers = projected["Content"]["Headers"].clone();
    assert_eq!(headers["To"], serde_json::json!(["x@y.test", "z@w.test"]));
    let received = headers["Received"].as_array().unwrap();
    assert!(
        received.iter().any(|v| v == "from a") && received.iter().any(|v| v == "from b"),
        "{received:?}"
    );
}
