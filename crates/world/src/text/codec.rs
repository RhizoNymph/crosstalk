//! The encodings a reader's copy of a span may arrive in, so `Decoded`
//! matches show real encoded text.

use crosstalk_spec::derived::provenance::matching::Codec;

const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Applies `codecs` in order: the first codec is applied to the original
/// text, the next to its output, and so on.
pub fn encode_chain<'a>(text: &str, codecs: impl IntoIterator<Item = &'a Codec>) -> String {
    codecs
        .into_iter()
        .fold(text.to_owned(), |acc, codec| encode(&acc, *codec))
}

pub fn encode(text: &str, codec: Codec) -> String {
    match codec {
        Codec::Base64 => base64(text.as_bytes()),
        Codec::Hex => hex(text.as_bytes()),
        Codec::UrlEncoding => url_encode(text),
        Codec::UnicodeNormalization => obfuscate_unicode(text),
        Codec::JsonString => json_string(text),
        Codec::YamlString => yaml_string(text),
    }
}

/// The text as the body of a JSON string literal (escaped, without quotes).
pub fn json_string(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if u32::from(c) < 0x20 => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out
}

/// The text as a single-quoted YAML scalar's body (quotes doubled).
pub fn yaml_string(text: &str) -> String {
    text.replace('\'', "''")
}

pub fn base64(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk.first().copied().unwrap_or(0),
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                let index = ((n >> (18 - 6 * i)) & 0x3f) as usize;
                out.push(char::from(BASE64[index]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Percent-encodes everything but RFC 3986 unreserved characters.
pub fn url_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// What NFKC folding and zero-width removal undo: a zero-width space after
/// every space and a fullwidth colon for every colon.
fn obfuscate_unicode(text: &str) -> String {
    let mut out = String::with_capacity(text.len() * 2);
    for c in text.chars() {
        match c {
            ' ' => out.push_str(" \u{200b}"),
            ':' => out.push('\u{ff1a}'),
            other => out.push(other),
        }
    }
    out
}

/// What whitespace and case normalization undo.
pub fn denormalize(text: &str) -> String {
    text.to_lowercase().replacen(' ', "\n  ", 2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_rfc4648_vectors() {
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
        assert_eq!(base64(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn url_encoding_escapes_reserved_bytes() {
        assert_eq!(url_encode("a b/c=d"), "a%20b%2Fc%3Dd");
        assert_eq!(url_encode("Zm8="), "Zm8%3D");
    }

    #[test]
    fn chains_apply_in_order() {
        let chain = [Codec::Base64, Codec::UrlEncoding];
        assert_eq!(encode_chain("fo", chain.iter()), "Zm8%3D");
    }

    #[test]
    fn hex_is_lowercase_pairs() {
        assert_eq!(hex(b"\x01\xab"), "01ab");
    }
}
