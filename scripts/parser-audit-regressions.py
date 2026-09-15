"""Temporary audit-branch helper; only the generated Rust tests are deliverables."""
from pathlib import Path
import re
import sys

root = Path(sys.argv[1] if len(sys.argv) > 1 else '.')
source = (root / 'orchestrator/src/config.rs').read_text()
ports = re.findall(r'parse_int\("(server\.[^"]+)", value\)\? as i32', source)
ints = []
longs = []
for match in re.finditer(r'let n = parse_int\("([^"]+)", value\)\?;([\s\S]*?)cfg\.[^=]+ = n( as i32)?;', source):
    (ints if match.group(3) else longs).append(match.group(1))
assert (len(ports), len(ints), len(longs)) == (16, 22, 10)
ports += ['server.mailHTTPPort', 'server.bigqueryPort', 'server.redshiftApiPort', 'server.redisHTTPPort']
ints += [p.replace('.dataApi.', '.dataAPI.') for p in ints if '.dataApi.' in p]
zero_allowed = ['services.sqs.defaultVisibilityTimeoutSeconds', 'services.sqs.defaultDelaySeconds', 'services.sqs.defaultReceiveWaitTimeSeconds', 'services.pubsub.pullWaitTimeoutSeconds']

def const(name, values):
    return f'    const {name}: &[&str] = &[\n' + ''.join(f'        "{v}",\n' for v in values) + '    ];\n\n'

def insert_tests(path, text):
    p = root / path
    s = p.read_text()
    marker = '#[cfg(test)]\nmod tests {\n    use super::*;\n'
    assert s.count(marker) == 1, path
    p.write_text(s.replace(marker, marker + '\n' + text, 1))

config_tests = const('PORT_FIELDS', ports) + const('I32_LIMIT_FIELDS', ints) + const('ZERO_ALLOWED_FIELDS', zero_allowed) + const('I64_LIMIT_FIELDS', longs) + r'''    fn apply_numeric(cfg: &mut Config, field: &str, value: &str) -> io::Result<()> {
        let path: Vec<String> = field.split('.').map(str::to_string).collect();
        apply_config_value(cfg, &path, value)
    }

    #[test]
    fn regression_integer_narrowing_is_rejected_without_mutation() {
        for field in PORT_FIELDS.iter().chain(I32_LIMIT_FIELDS) {
            for value in ["2147483648", "4294978321", "9223372036854775807", "-2147483649"] {
                let mut cfg = default_config();
                let before = cfg.clone();
                let err = apply_numeric(&mut cfg, field, value)
                    .expect_err("out-of-range integer must not wrap");
                assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{field}");
                // Do not print Config: it contains credentials.
                assert!(cfg == before, "rejected {field} must not change config");
            }
        }
    }

    #[test]
    fn regression_ports_reject_invalid_tcp_bounds() {
        for field in PORT_FIELDS {
            for value in ["-1", "0", "65536", "2147483647"] {
                let mut cfg = default_config();
                assert!(apply_numeric(&mut cfg, field, value).is_err(), "{field}");
            }
            for value in ["1", "11025", "65535"] {
                let mut cfg = default_config();
                apply_numeric(&mut cfg, field, value).expect("valid TCP port");
            }
        }
    }

    #[test]
    fn regression_service_limit_zero_rules_and_upper_bounds_are_preserved() {
        for field in I32_LIMIT_FIELDS {
            let mut cfg = default_config();
            apply_numeric(&mut cfg, field, "2147483647").expect("i32 upper bound");
            assert!(apply_numeric(&mut cfg, field, "-1").is_err(), "{field}");
            let zero = apply_numeric(&mut cfg, field, "0");
            assert_eq!(zero.is_ok(), ZERO_ALLOWED_FIELDS.contains(field), "{field}");
        }
    }

    #[test]
    fn regression_i64_limits_are_not_narrowed_to_i32() {
        for field in I64_LIMIT_FIELDS {
            for value in ["2147483648", "5368709120", "9223372036854775807"] {
                let mut cfg = default_config();
                apply_numeric(&mut cfg, field, value).expect("valid i64 limit");
            }
        }
        let mut cfg = default_config();
        apply_numeric(&mut cfg, "services.s3.maxObjectBytes", "5368709120").unwrap();
        assert_eq!(cfg.services.s3.max_object_bytes, 5_368_709_120);
    }

    #[test]
    fn regression_load_config_rejects_wrapping_port_in_crlf_yaml() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "devcloud-config-numeric-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&dir).unwrap();
        let path = dir.join("config.yaml");
        fs::write(&path, "server:\r\n  smtpPort: 4294978321\r\n").unwrap();
        let result = load_config(path.to_str().unwrap());
        fs::remove_file(&path).unwrap();
        fs::remove_dir(&dir).unwrap();
        assert!(matches!(result, Err(ref err) if err.kind() == io::ErrorKind::InvalidData));
    }

'''
insert_tests('orchestrator/src/config.rs', config_tests)

time_tests = r'''    #[test]
    fn regression_fraction_rejects_non_ascii_at_every_precision_boundary() {
        for prefix_len in 0..=12 {
            for character in ['é', 'あ', '🙂'] {
                let value = format!(
                    "1970-01-01T00:00:00.{}{character}Z",
                    "1".repeat(prefix_len)
                );
                assert_eq!(parse_rfc3339(&value), None);
            }
        }
    }

    #[test]
    fn regression_fraction_rejects_empty_and_non_decimal_suffixes() {
        for fraction in ["", "+1", "-1", "123456789x", "1234567890 ", "1234567890\n", "1234567890\0"] {
            let value = format!("1970-01-01T00:00:00.{fraction}Z");
            assert_eq!(parse_rfc3339(&value), None);
        }
    }

    #[test]
    fn regression_fraction_preserves_valid_nanosecond_precision() {
        assert_eq!(parse_rfc3339("1970-01-01T00:00:00Z"), Some((0, 0)));
        for (fraction, nanos) in [
            ("0", 0),
            ("1", 100_000_000),
            ("001", 1_000_000),
            ("12345678", 123_456_780),
            ("123456789", 123_456_789),
            ("123456789123456789", 123_456_789),
        ] {
            let value = format!("1970-01-01T00:00:00.{fraction}Z");
            assert_eq!(parse_rfc3339(&value), Some((0, nanos)));
        }
    }

'''
for svc in ('s3', 'pubsub'):
    insert_tests(f'services/{svc}/src/time_fmt.rs', time_tests)

s3_test = r'''
#[test]
fn regression_invalid_retention_fraction_does_not_change_persisted_retention() {
    let root = tempdir();
    let store = FileBucketStore::new(&root);
    assert_eq!(route(&store, &req("PUT", "/data")).status, 200);
    assert_eq!(route(&store, &req("PUT", "/data/locked.txt")).status, 200);
    let valid = br#"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2099-01-01T00:00:00Z</RetainUntilDate></Retention>"#;
    assert_eq!(
        route(&store, &Request::new("PUT", "/data/locked.txt?retention", valid.to_vec())).status,
        200
    );
    let before = route(&store, &req("GET", "/data/locked.txt?retention"));
    assert_eq!(before.status, 200);
    for time in ["2099-01-01T00:00:00.Z", "2099-01-01T00:00:00.123456789xZ"] {
        let body = format!(
            "<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>{time}</RetainUntilDate></Retention>"
        );
        let response = route(
            &store,
            &Request::new("PUT", "/data/locked.txt?retention", body.into_bytes()),
        );
        assert_eq!(response.status, 400);
        assert!(String::from_utf8(response.body).unwrap().contains("<Code>InvalidArgument</Code>"));
        let reopened = FileBucketStore::new(&root);
        let after = route(&reopened, &req("GET", "/data/locked.txt?retention"));
        assert_eq!(after.status, 200);
        assert_eq!(after.body, before.body);
    }
    std::fs::remove_dir_all(root).unwrap();
}
'''
p = root / 'services/s3/tests/http_parity.rs'
p.write_text(p.read_text() + s3_test)

pubsub_test = r'''
#[test]
fn regression_invalid_seek_fraction_preserves_inflight_delivery() {
    let (dir, md) = (tempdir(), tempdir());
    let mut s = server(&dir, &md);
    let messages = vec![serde_json::json!({"data": "aGk="})];
    s.publish("devcloud", "orders", &messages).unwrap();
    let pulled: serde_json::Value = serde_json::from_slice(
        &s.pull("devcloud", "sub1", 1).unwrap().body,
    ).unwrap();
    let ack_id = pulled["receivedMessages"][0]["ackId"].as_str().unwrap().to_string();
    for time in ["2026-05-30T11:00:00.Z", "2026-05-30T11:00:00.123456789xZ"] {
        let response = route(
            &mut s,
            &req(
                "POST",
                "/v1/projects/devcloud/subscriptions/sub1:seek",
                &serde_json::json!({"time": time}).to_string(),
            ),
        );
        assert_eq!(response.status, 400);
        let error: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(error["error"]["code"], 400);
        assert_eq!(error["error"]["status"], "INVALID_ARGUMENT");
        assert_eq!(error["error"]["message"], "invalid seek time");
        assert_eq!(s.pull("devcloud", "sub1", 1).unwrap().body, b"{}\n");
    }
    s.acknowledge("devcloud", "sub1", &[ack_id]).unwrap();
    std::fs::remove_dir_all(dir).unwrap();
    std::fs::remove_dir_all(md).unwrap();
}
'''
p = root / 'services/pubsub/tests/http_parity.rs'
p.write_text(p.read_text() + pubsub_test)
