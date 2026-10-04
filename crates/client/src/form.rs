//! The query string: `application/x-www-form-urlencoded`, serialized as the
//! WHATWG URL standard's form encoding does (the binding's encoding for
//! query parameters, `http_api.md`, Arguments).

/// `name=value` pairs joined with `&`, each name and value form-encoded.
pub(crate) fn encode(pairs: &[(&str, String)]) -> String {
    let mut out = String::new();
    for (index, (name, value)) in pairs.iter().enumerate() {
        if index > 0 {
            out.push('&');
        }
        push_encoded(&mut out, name);
        out.push('=');
        push_encoded(&mut out, value);
    }
    out
}

/// The form serializer: `*-._`, ASCII letters and digits as they are, a
/// space as `+`, every other byte of the UTF-8 as `%XX` (upper-case hex).
fn push_encoded(out: &mut String, text: &str) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for byte in text.bytes() {
        match byte {
            b'*' | b'-' | b'.' | b'_' | b'0'..=b'9' | b'A'..=b'Z' | b'a'..=b'z' => {
                out.push(char::from(byte));
            }
            b' ' => out.push('+'),
            _ => {
                out.push('%');
                out.push(char::from(HEX[usize::from(byte >> 4)]));
                out.push(char::from(HEX[usize::from(byte & 0x0f)]));
            }
        }
    }
}
