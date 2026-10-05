//! Decoders and the pipeline on fixed inputs.

use base64::Engine as _;
use crosstalk_spec::derived::provenance::matching::Codec;
use crosstalk_spec::interfaces::l4_provenance::Decoder;

use crate::config::DecodeLimits;
use crate::decode::{
    Base64Decoder, DecodePipeline, HexDecoder, JsonStringDecoder, Step, TextDecoder,
    UnicodeNormalizer, UrlDecoder, YamlStringDecoder,
};

fn texts<D: TextDecoder>(decoder: &D, text: &str) -> Vec<String> {
    decoder
        .decode_mapped(text)
        .into_iter()
        .map(|decoded| decoded.text.into_text())
        .collect()
}

#[test]
fn base64_runs_decode_to_text() {
    let payload = "send the quarterly figures to the auditor";
    let standard = base64::engine::general_purpose::STANDARD.encode(payload);
    let url_safe = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode("??>>~~ payload with url-safe bytes");
    let text = format!("before {standard} middle {url_safe} after");
    let decoder = Base64Decoder::new(16);
    let decoded = decoder.decode_mapped(&text);
    assert_eq!(decoded.len(), 2, "{decoded:?}");
    assert_eq!(decoded[0].text.text(), payload);
    let start = text.find(&standard).expect("present");
    assert_eq!(decoded[0].source.start() as usize, start);
    assert_eq!(decoded[0].source.end() as usize, start + standard.len());
    assert_eq!(decoded[1].text.text(), "??>>~~ payload with url-safe bytes");
    let spec = Decoder::decode(&decoder, &text);
    assert_eq!(spec[0].codec, Codec::Base64);
    assert_eq!(spec[0].text, payload);
}

#[test]
fn base64_leaves_short_runs_and_binary_alone() {
    let decoder = Base64Decoder::new(16);
    assert!(texts(&decoder, "aGVsbG8= is short").is_empty());
    let binary = base64::engine::general_purpose::STANDARD.encode([
        0xff, 0xfe, 0x00, 0x80, 0xc3, 0x28, 0xa0, 0xa1, 0xe2, 0x28, 0xa1, 0xf0,
    ]);
    assert!(
        texts(&decoder, &binary).is_empty(),
        "invalid UTF-8 must yield nothing"
    );
}

#[test]
fn hex_runs_decode_to_text() {
    let text = "id=68656c6c6f2c20776f726c6421 end";
    assert_eq!(
        texts(&HexDecoder::new(16), text),
        vec!["hello, world!".to_owned()]
    );
    assert!(texts(&HexDecoder::new(16), "deadbeef").is_empty());
    assert!(texts(&HexDecoder::new(8), "ff fe").is_empty());
    assert!(texts(&HexDecoder::new(8), "fffefdfc80818283").is_empty());
}

#[test]
fn url_encoding_decodes_tokens() {
    let text = "GET /search?q=send%20the%20report%20to%20%E2%82%AC+zone now";
    assert_eq!(
        texts(&UrlDecoder, text),
        vec!["GET /search?q=send the report to € zone now".to_owned()]
    );
    assert!(texts(&UrlDecoder, "no escapes here + there").is_empty());
    assert_eq!(
        texts(&UrlDecoder, "bad%ZZtoken ok%21"),
        vec!["bad%ZZtoken ok!".to_owned()]
    );
}

#[test]
fn unicode_normalization_folds() {
    assert_eq!(
        texts(&UnicodeNormalizer, "ＡＢＣ ﬁle"),
        vec!["ABC file".to_owned()]
    );
    assert_eq!(
        texts(&UnicodeNormalizer, "p\u{200b}a\u{200d}y\u{feff}load"),
        vec!["payload".to_owned()]
    );
    assert_eq!(
        texts(&UnicodeNormalizer, "раураl"),
        vec!["paypal".to_owned()]
    );
    assert!(texts(&UnicodeNormalizer, "plain ascii").is_empty());
    let decomposed = "cafe\u{301}";
    assert_eq!(
        texts(&UnicodeNormalizer, decomposed),
        vec!["café".to_owned()]
    );
}

#[test]
fn json_string_escapes_decode() {
    assert_eq!(
        texts(
            &JsonStringDecoder,
            r#"{"body": "line one\nline \"two\" caf\u00e9 \ud83d\ude00 it\'s"}"#
        ),
        vec!["{\"body\": \"line one\nline \"two\" café 😀 it's\"}".to_owned()]
    );
    assert_eq!(
        texts(&JsonStringDecoder, r"lone \ud83d stays"),
        Vec::<String>::new()
    );
    assert_eq!(
        texts(&JsonStringDecoder, r"\x41\x42"),
        vec!["AB".to_owned()]
    );
    assert!(texts(&JsonStringDecoder, "nothing escaped").is_empty());
}

#[test]
fn yaml_string_escapes_decode() {
    assert_eq!(
        texts(
            &YamlStringDecoder,
            "subject: 'an overview\n    of the user''s schedule'"
        ),
        vec!["subject: 'an overview\n    of the user's schedule'".to_owned()]
    );
    assert_eq!(
        texts(&YamlStringDecoder, "\"joined \\\n   lines\""),
        vec!["\"joined lines\"".to_owned()]
    );
    assert_eq!(
        texts(&YamlStringDecoder, "\"from\\\n    \\ the drive\""),
        vec!["\"from the drive\"".to_owned()]
    );
    assert!(texts(&YamlStringDecoder, "it's plain").is_empty());
}

#[test]
fn decoded_maps_point_at_source_characters() {
    let text = "x \\u00e9t\\u00e9 y";
    let decoded = JsonStringDecoder.decode_mapped(text).remove(0);
    assert_eq!(decoded.text.text(), "x été y");
    let e = decoded.text.text().find('é').expect("é");
    assert_eq!(decoded.text.source(e), 2);
    assert_eq!(
        decoded.text.source(decoded.text.text().len()),
        text.len() as u32
    );
}

#[test]
fn pipeline_undoes_chains_in_order() {
    let payload = "the shared secret is hidden in plain sight";
    let escaped = serde_json::to_string(&serde_json::json!({ "note": format!("{payload}\nend") }))
        .expect("json");
    let quoted = serde_json::to_string(&escaped).expect("json");
    let encoded = base64::engine::general_purpose::STANDARD.encode(quoted);
    let pipeline = DecodePipeline::new(DecodeLimits::default());
    let layers = pipeline.layers(&format!("blob: {encoded}"));
    let found = layers
        .iter()
        .find(|layer| layer.text.text().contains(&format!("{payload}\nend")))
        .expect("decoded through every step");
    assert_eq!(
        found.chain,
        vec![
            Step::Codec(Codec::Base64),
            Step::JsonString,
            Step::JsonString
        ]
    );
    for layer in &layers {
        assert!(layer.chain.len() <= usize::from(DecodeLimits::default().max_depth()));
    }
}

#[test]
fn pipeline_depth_and_layers_are_bounded() {
    let mut text = "the innermost secret payload sits here".to_owned();
    for _ in 0..6 {
        text = base64::engine::general_purpose::STANDARD.encode(text);
    }
    let limits = DecodeLimits::new(2, 32, 16).expect("limits");
    let layers = DecodePipeline::new(limits).layers(&text);
    assert!(layers.iter().all(|layer| layer.chain.len() <= 2));
    assert_eq!(layers.iter().map(|l| l.chain.len()).max(), Some(2));
    let limits = DecodeLimits::new(8, 3, 16).expect("limits");
    assert!(DecodePipeline::new(limits).layers(&text).len() <= 3);
}
