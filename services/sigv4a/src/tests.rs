//! Vectors that do not come from this crate: NIST P-256 point multiples,
//! RFC 6979 appendix A.2.5, the aws-sigv4 crate's `aws-signing-test-suite/v4a`
//! (`get-vanilla`), and chunk signatures produced by Python `cryptography`
//! (OpenSSL) over the chunk string-to-sign layout.

use super::*;

fn u256(hex_str: &str) -> U256 {
    let bytes: [u8; 32] = hex::decode(hex_str).unwrap().try_into().unwrap();
    U256::from_be_bytes(&bytes)
}

fn xy(point: AffinePoint) -> (String, String) {
    (
        hex::encode(point.x.to_be_bytes()),
        hex::encode(point.y.to_be_bytes()),
    )
}

const GX: &str = "6b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296";

#[test]
fn nist_p256_point_multiples() {
    let g = xy(p256::scalar_mul_g(&p256::ONE));
    assert_eq!(g.0, GX);
    assert_eq!(
        g.1,
        "4fe342e2fe1a7f9b8ee7eb4a7c0f9e162bce33576b315ececbb6406837bf51f5"
    );
    assert_eq!(
        xy(p256::scalar_mul_g(&u256(
            "0000000000000000000000000000000000000000000000000000000000000002"
        ))),
        (
            "7cf27b188d034f7e8a52380304b51ac3c08969e277f21b35a60b48fc47669978".to_string(),
            "07775510db8ed040293d9ac69f7430dbba7dade63ce982299e04b79d227873d1".to_string()
        )
    );
    // (n-1)G = -G.
    assert_eq!(
        xy(p256::scalar_mul_g(&u256(
            "ffffffff00000000ffffffffffffffffbce6faada7179e84f3b9cac2fc632550"
        ))),
        (
            GX.to_string(),
            "b01cbd1c01e58065711814b583f061e9d431cca994cea1313449bf97c840ae0a".to_string()
        )
    );
    assert!(p256::scalar_mul_g(&p256::N).infinity);
}

const RFC6979_X: &str = "c9afa9d845ba75166b5c215767b1d6934e50c3db36e89b127b8a622b120f6721";

#[test]
fn rfc6979_a25_public_key_nonces_and_signatures() {
    let key = SigningKey::from_scalar(u256(RFC6979_X)).unwrap();
    let (ux, uy) = key.public_key();
    assert_eq!(
        hex::encode(ux),
        "60fed4ba255a9d31c961eb74c6356d68c049b8923b61fa6ce669622e60f29fb6"
    );
    assert_eq!(
        hex::encode(uy),
        "7903fe1008b8bc99a41ae9e95628bc64f2f1b20c2d7e9f5177a3c294d4462299"
    );
    let cases = [
        (
            &b"sample"[..],
            "a6e3c57dd01abe90086538398355dd4c3b17aa873382b0f24d6129493d8aad60",
            "efd48b2aacb6a8fd1140dd9cd45e81d69d2c877b56aaf991c34d0ea84eaf3716",
            "f7cb1c942d657c41d436c7a1b6e29f65f3e900dbb9aff4064dc4ab2f843acda8",
        ),
        (
            &b"test"[..],
            "d16b6ae827f17175e040871a1c7ec3500192c4c92677336ec2537acaee0008e0",
            "f1abb023518351cd71d881567b1ea663ed3efcf6c5132b354f28d3b0b7d38367",
            "019f4113742a2b14bd25926b49c649155f267e60d3814b4c0cc84250e46f0083",
        ),
    ];
    for (message, k, r, s) in cases {
        let digest: [u8; 32] = Sha256::digest(message).into();
        assert_eq!(rfc6979_nonce(&key.d, &digest), u256(k));
        let der = hex::decode(key.sign(message)).unwrap();
        assert_eq!(parse_der_signature(&der), Some((u256(r), u256(s))));
        let expected = hex::encode(p256::encode_der_sig(&u256(r), &u256(s)));
        assert!(key.verify(message, &expected));
        assert!(!key.verify(b"other", &expected));
    }
}

/// `aws-signing-test-suite/v4a/get-vanilla` from the aws-sigv4 crate.
const AWS_ACCESS_KEY: &str = "AKIDEXAMPLE";
const AWS_SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
const AWS_HEADER_SIGNATURE: &str = "3045022018b4e277d0281864beb51d3600e23f88510ea5031d68ddfbb68614b82a5eb7d2022100effb9c5f22ed9ef3ae0ab243d21f06bce82365bbb79529a07b6888c343ae5f8c";

#[test]
fn aws_signing_test_suite_get_vanilla() {
    let key = SigningKey::derive(AWS_ACCESS_KEY, AWS_SECRET).unwrap();
    let (x, y) = key.public_key();
    assert_eq!(
        hex::encode(x),
        "b6618f6a65740a99e650b33b6b4b5bd0d43b176d721a3edfea7e7d2d56d936b1"
    );
    assert_eq!(
        hex::encode(y),
        "865ed22a7eadc9c5cb9d2cbaca1b3699139fedc5043dc6661864218330c8e518"
    );

    let canonical_request = "GET\n/\n\nhost:example.amazonaws.com\nx-amz-date:20150830T123600Z\nx-amz-region-set:us-east-1\n\nhost;x-amz-date;x-amz-region-set\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    let scope = credential_scope("20150830", "service");
    let header_sts = string_to_sign("20150830T123600Z", &scope, canonical_request);
    assert_eq!(
        header_sts,
        "AWS4-ECDSA-P256-SHA256\n20150830T123600Z\n20150830/service/aws4_request\ncf59db423e841c8b7e3444158185aa261b724a5c27cbe762676f3eed19f4dc02"
    );
    // header-signature.txt and the signature in header-signed-request.txt.
    for signature in [
        AWS_HEADER_SIGNATURE,
        "3046022100fddaaf816a31e30d04973875b13bc27b98da10907a3a1872fd5819a221334abc022100de2ee81d83b54f31efc82834de07e6cd4d5c6f25bfa08e0f29a15bb0977c343e",
    ] {
        assert!(key.verify(header_sts.as_bytes(), signature));
    }
    // query-signature.txt over query-string-to-sign.txt.
    let query_sts = "AWS4-ECDSA-P256-SHA256\n20150830T123600Z\n20150830/service/aws4_request\n890c4ed28c1a1ac10b5862719b537afbe392e987dc1aab1efa16fe7de41d3c81";
    assert!(key.verify(
        query_sts.as_bytes(),
        "3045022100b3246f8e1442dd58e73292ae1294720c3256ee58f6cea50f3a3f83b4a4b0331a0220329693f7745c9008a5887c0529ceccb0fae4e16d707b712ea66acdbaf963769a"
    ));
    assert!(!key.verify(query_sts.as_bytes(), AWS_HEADER_SIGNATURE));
    let other = SigningKey::derive(AWS_ACCESS_KEY, "different-secret").unwrap();
    assert!(!other.verify(header_sts.as_bytes(), AWS_HEADER_SIGNATURE));
}

#[test]
fn chunk_chain_verifies_externally_produced_signatures() {
    let key = SigningKey::derive(AWS_ACCESS_KEY, AWS_SECRET).unwrap();
    let scope = credential_scope("20150830", "s3");
    let chunk_hello = "3046022100ff6e3623db7f77f75b931852daf3addaf1128f0dce6b1c0e2a3e2ad4cd8d1730022100da44720fff205bd2b544a302ee8e6cd068f08351b22ccb7ce7da3f3586454774";
    let chunk_final = "30440220295bd9b43fdb6d66e53d36c2e380bdd46085f50dfa0fee6e41a2be1bca03eaa202207e2075b5739c9d1ab61a82b250b461a26bfcd1948688b2fd10274d0a9de05761";
    let trailer = "3046022100e0bca91ddc68331586653f5f4a1b21939bd1f8d68ae93f9fa457d4a26508bb36022100f0a973c3d23652dc80e799b3646ed62d6dae1469bcedea75f6f3bd6929c55952";
    let trailers = canonical_trailers(&[("x-amz-checksum-crc32", "AAAA")]);

    let mut chain = ChunkChain::new(
        key.clone(),
        "20150830T123600Z",
        &scope,
        AWS_HEADER_SIGNATURE,
    );
    assert!(chain.verify_chunk(&format!("{chunk_hello:*<144}"), b"hello"));
    assert!(chain.verify_chunk(chunk_final, b""));
    assert!(chain.verify_trailer(trailer, trailers.as_bytes()));

    // Reordered chunks or altered data break the chain.
    let mut chain = ChunkChain::new(
        key.clone(),
        "20150830T123600Z",
        &scope,
        AWS_HEADER_SIGNATURE,
    );
    assert!(!chain.verify_chunk(chunk_hello, b"hellO"));
    let mut chain = ChunkChain::new(key, "20150830T123600Z", &scope, AWS_HEADER_SIGNATURE);
    assert!(!chain.verify_chunk(chunk_final, b""));
}

#[test]
fn encoded_chunks_round_trip_and_shape_checks() {
    let key = SigningKey::derive("dev", "dev").unwrap();
    let scope = credential_scope("20261125", "sqs");
    let mut signer = ChunkChain::new(key.clone(), "20261125T000000Z", &scope, "3006020101020101");
    let body = encode_signed_chunks(
        &mut signer,
        &[b"abc"],
        Some(&[("x-amz-checksum-crc32", "AAAA")]),
        true,
    );
    let text = String::from_utf8(body).unwrap();
    let lines: Vec<&str> = text.split("\r\n").collect();
    assert!(lines[0].starts_with("3;chunk-signature=") && lines[0].len() == 18 + 144);
    assert_eq!(lines[1], "abc");
    assert!(lines[2].starts_with("0;chunk-signature="));
    assert_eq!(lines[3], "x-amz-checksum-crc32:AAAA");
    assert!(lines[4].starts_with("x-amz-trailer-signature:"));
    assert_eq!(&lines[5..], ["", ""]);

    let sig = |line: &str| line.split_once('=').unwrap().1.to_string();
    let mut verifier = ChunkChain::new(key, "20261125T000000Z", &scope, "3006020101020101");
    assert!(verifier.verify_chunk(&sig(lines[0]), b"abc"));
    assert!(verifier.verify_chunk(&sig(lines[2]), b""));
    assert!(verifier.verify_trailer(
        lines[4].split_once(':').unwrap().1,
        b"x-amz-checksum-crc32:AAAA\n"
    ));

    assert!(is_signature_shape(&sig(lines[0])));
    assert!(is_signature_shape(AWS_HEADER_SIGNATURE));
    assert!(!is_signature_shape("abcd"));
    assert!(!is_signature_shape(&"a".repeat(64)));
    assert!(region_set_matches("eu-west-1, us-east-1", "us-east-1"));
    assert!(region_set_matches("*", "us-east-1"));
    assert!(!region_set_matches("eu-west-1", "us-east-1"));
    assert!(!region_set_matches("", "us-east-1"));
}

#[test]
fn authorization_header_parsing_and_chain_helper() {
    let header = format!(
        "AWS4-ECDSA-P256-SHA256 Credential=AKIDEXAMPLE/20150830/service/aws4_request, SignedHeaders=host;x-amz-date;x-amz-region-set, Signature={AWS_HEADER_SIGNATURE}"
    );
    assert!(is_authorization(&header));
    assert!(!is_authorization("AWS4-HMAC-SHA256 Credential=a"));
    assert!(!is_authorization("AWS4-ECDSA-P256-SHA256X Credential=a"));
    assert_eq!(
        parse_authorization(&header),
        Some(Authorization {
            access_key: AWS_ACCESS_KEY,
            date_stamp: "20150830",
            service: "service",
            signed_headers: "host;x-amz-date;x-amz-region-set",
            signature: AWS_HEADER_SIGNATURE,
        })
    );
    for bad in [
        "AWS4-ECDSA-P256-SHA256 Credential=a/20150830/us-east-1/s/aws4_request, SignedHeaders=host, Signature=30",
        "AWS4-ECDSA-P256-SHA256 Credential=a/20150830/s/aws4_request, SignedHeaders=host",
        "AWS4-ECDSA-P256-SHA256 Credential=a/20150830/s/aws4_request, SignedHeaders=, Signature=30",
        "AWS4-HMAC-SHA256 Credential=a/20150830/s/aws4_request, SignedHeaders=host, Signature=30",
    ] {
        assert_eq!(parse_authorization(bad), None, "{bad}");
    }

    let key = SigningKey::derive("dev", "dev").unwrap();
    let new_chain = || {
        ChunkChain::new(
            key.clone(),
            "20261125T000000Z",
            "20261125/ssm/aws4_request",
            "3006020101020101",
        )
    };
    let mut signer = new_chain();
    let s1 = signer.sign_chunk(b"{}");
    let s2 = signer.sign_chunk(b"");
    let t = signer.sign_trailer(b"x-amz-checksum-crc32:AAAA\n");
    let trailers = [("X-Amz-Checksum-Crc32", "AAAA")];
    assert!(verify_chunk_chain(
        new_chain(),
        [
            (Some(s1.as_str()), &b"{}"[..]),
            (Some(s2.as_str()), &b""[..])
        ],
        Some((Some(t.as_str()), &trailers[..])),
    ));
    assert!(!verify_chunk_chain(
        new_chain(),
        [(Some(s1.as_str()), &b"{}"[..]), (None, &b""[..])],
        None,
    ));
    assert!(!verify_chunk_chain(
        new_chain(),
        [
            (Some(s1.as_str()), &b"{}"[..]),
            (Some(s2.as_str()), &b""[..])
        ],
        Some((None, &trailers[..])),
    ));
}
