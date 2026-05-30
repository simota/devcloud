//! A minimal CSV writer matching Go's `encoding/csv` (default comma, `\n`
//! terminator) byte-for-byte, used for the inventory report. A field is quoted
//! iff it is non-empty and either equals `\.`, contains `"`/`,`/`\n`/`\r`, or
//! begins with a whitespace rune; quotes inside a quoted field are doubled.

pub fn write_csv(records: &[Vec<String>]) -> Vec<u8> {
    let mut out = Vec::new();
    for record in records {
        for (i, field) in record.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            if field_needs_quotes(field) {
                out.push(b'"');
                for ch in field.chars() {
                    if ch == '"' {
                        out.extend_from_slice(b"\"\"");
                    } else {
                        let mut buf = [0u8; 4];
                        out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                    }
                }
                out.push(b'"');
            } else {
                out.extend_from_slice(field.as_bytes());
            }
        }
        out.push(b'\n');
    }
    out
}

fn field_needs_quotes(field: &str) -> bool {
    if field.is_empty() {
        return false;
    }
    if field == "\\." {
        return true;
    }
    if field
        .bytes()
        .any(|c| c == b'\n' || c == b'\r' || c == b'"' || c == b',')
    {
        return true;
    }
    field.chars().next().is_some_and(char::is_whitespace)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoting_matches_go_encoding_csv() {
        let records = vec![
            vec!["Bucket".into(), "Key".into(), "Size".into(), "ETag".into()],
            vec![
                "data".into(),
                "simple.txt".into(),
                "3".into(),
                "\"9a0364b9\"".into(),
            ],
            vec![
                "data".into(),
                "has,comma".into(),
                "0".into(),
                "\"d41d8c\"".into(),
            ],
            vec![
                "data".into(),
                " leading-space".into(),
                "5".into(),
                "plain".into(),
            ],
            vec![
                "data".into(),
                "line\nbreak".into(),
                "1".into(),
                String::new(),
            ],
        ];
        let got = String::from_utf8(write_csv(&records)).unwrap();
        let want = "Bucket,Key,Size,ETag\n\
                    data,simple.txt,3,\"\"\"9a0364b9\"\"\"\n\
                    data,\"has,comma\",0,\"\"\"d41d8c\"\"\"\n\
                    data,\" leading-space\",5,plain\n\
                    data,\"line\nbreak\",1,\n";
        assert_eq!(got, want);
    }
}
