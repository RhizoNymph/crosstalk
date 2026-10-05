//! The query string's form encoding, against the WHATWG form decoding the
//! stub (like any server) applies.

use super::stub::form_decode;
use crate::form::encode;

#[test]
fn the_form_serializer_keeps_only_the_safe_bytes() {
    assert_eq!(encode(&[("a", "AZaz09*-._".to_owned())]), "a=AZaz09*-._");
    assert_eq!(encode(&[("q", "a b".to_owned())]), "q=a+b");
    assert_eq!(
        encode(&[("q", "+&=%/?#\"{}:,[]~!'()".to_owned())]),
        "q=%2B%26%3D%25%2F%3F%23%22%7B%7D%3A%2C%5B%5D%7E%21%27%28%29"
    );
    assert_eq!(encode(&[("q", "é😀".to_owned())]), "q=%C3%A9%F0%9F%98%80");
    assert_eq!(
        encode(&[("a", "1".to_owned()), ("b", String::new())]),
        "a=1&b="
    );
    assert_eq!(encode(&[]), "");
}

#[test]
fn form_decoding_gives_back_every_value() {
    let values = [
        r#"{"states":[],"text":"a b&c=d+e"}"#,
        "",
        " ",
        "++",
        "%%41",
        "\u{0}\u{7f}\u{80}\u{ffff}",
        "line\nbreak\r\ttab",
        "😀 ünïcødé",
    ];
    for value in values {
        let pairs = vec![("name", value.to_owned()), ("other", "x".to_owned())];
        let decoded = form_decode(&encode(&pairs));
        assert_eq!(
            decoded,
            vec![
                ("name".to_owned(), value.to_owned()),
                ("other".to_owned(), "x".to_owned())
            ],
            "{value:?}"
        );
    }
}
