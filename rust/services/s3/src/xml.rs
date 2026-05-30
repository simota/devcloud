//! A minimal XML serializer matching Go's `encoding/xml` `Encoder.Encode`
//! byte-for-byte for the S3 response shapes: compact (no indentation), **no**
//! XML declaration, empty elements rendered as `<Name></Name>` (never
//! self-closing), and Go's text/attr escaping (`&`→`&amp;`, `<`→`&lt;`,
//! `>`→`&gt;`, `"`→`&#34;`, `'`→`&#39;`, and `\t`/`\n`/`\r` as numeric refs).
//!
//! Responses build an [`Element`] tree (children in struct-field order, with
//! `omitempty` handled by the builder) and serialize it via [`encode`].

/// An XML element: a name, ordered attributes, and content.
pub struct Element {
    name: String,
    attrs: Vec<(String, String)>,
    content: Content,
}

enum Content {
    Empty,
    Text(String),
    Children(Vec<Element>),
}

impl Element {
    pub fn new(name: &str) -> Self {
        Element {
            name: name.to_string(),
            attrs: Vec::new(),
            content: Content::Empty,
        }
    }

    /// Adds an attribute (rendered in insertion order).
    pub fn attr(mut self, name: &str, value: &str) -> Self {
        self.attrs.push((name.to_string(), value.to_string()));
        self
    }

    /// Sets chardata text content.
    pub fn text(mut self, value: &str) -> Self {
        self.content = Content::Text(value.to_string());
        self
    }

    /// Appends a child element.
    pub fn child(mut self, child: Element) -> Self {
        match &mut self.content {
            Content::Children(children) => children.push(child),
            _ => self.content = Content::Children(vec![child]),
        }
        self
    }

    /// Appends a child element holding only text (`<name>text</name>`).
    pub fn text_child(self, name: &str, value: &str) -> Self {
        self.child(Element::new(name).text(value))
    }

    fn write(&self, out: &mut String) {
        out.push('<');
        out.push_str(&self.name);
        for (k, v) in &self.attrs {
            out.push(' ');
            out.push_str(k);
            out.push_str("=\"");
            escape_into(out, v);
            out.push('"');
        }
        out.push('>');
        match &self.content {
            Content::Empty => {}
            Content::Text(t) => escape_into(out, t),
            Content::Children(children) => {
                for child in children {
                    child.write(out);
                }
            }
        }
        out.push_str("</");
        out.push_str(&self.name);
        out.push('>');
    }
}

/// Serializes an element tree to bytes, matching `xml.NewEncoder(w).Encode`.
pub fn encode(root: &Element) -> Vec<u8> {
    let mut out = String::new();
    root.write(&mut out);
    out.into_bytes()
}

/// Go `xml` text/attribute escaping.
fn escape_into(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            '\t' => out.push_str("&#x9;"),
            '\n' => out.push_str("&#xA;"),
            '\r' => out.push_str("&#xD;"),
            _ => out.push(c),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_element_is_not_self_closing() {
        let el = Element::new("Buckets");
        assert_eq!(encode(&el), b"<Buckets></Buckets>");
    }

    #[test]
    fn attrs_and_escaping() {
        let el = Element::new("ETag").text("\"a&b\"");
        assert_eq!(
            String::from_utf8(encode(&el)).unwrap(),
            "<ETag>&#34;a&amp;b&#34;</ETag>"
        );
        let el = Element::new("Root")
            .attr("xmlns", "ns")
            .text_child("X", "v");
        assert_eq!(
            String::from_utf8(encode(&el)).unwrap(),
            "<Root xmlns=\"ns\"><X>v</X></Root>"
        );
    }
}
