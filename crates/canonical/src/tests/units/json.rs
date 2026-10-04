//! Canonical JSON on fixed inputs.

use crosstalk_spec::observed::message::{AssistantPart, ToolArguments};

use crate::json::{Json, JsonError, canonicalize};
use crate::tests::support::{START, normalize, response_parts, sse, stop, streamed};

fn canonical(text: &str) -> String {
    canonicalize(text)
        .unwrap_or_else(|error| panic!("{text:?} is JSON: {error}"))
        .0
}

/// RFC 8785's structure vectors (3.2.2 without its numbers, 3.2.3's
/// sorting by UTF-16 code units), plus whitespace, nesting and escapes.
pub fn rfc8785_structure_vectors() {
    let sorting = "{\n  \"\\u20ac\": \"Euro Sign\",\n  \"\\r\": \"Carriage Return\",\n  \
                   \"\\ufb33\": \"Hebrew Letter Dalet With Dagesh\",\n  \"1\": \"One\",\n  \
                   \"\\ud83d\\ude00\": \"Emoji: Grinning Face\",\n  \"\\u0080\": \"Control\",\n  \
                   \"\\u00f6\": \"Latin Small Letter O With Diaeresis\"\n}";
    assert_eq!(
        canonical(sorting),
        "{\"\\r\":\"Carriage Return\",\"1\":\"One\",\"\u{80}\":\"Control\",\
         \"\u{f6}\":\"Latin Small Letter O With Diaeresis\",\"\u{20ac}\":\"Euro Sign\",\
         \"\u{1F600}\":\"Emoji: Grinning Face\",\"\u{fb33}\":\"Hebrew Letter Dalet With Dagesh\"}"
    );
    let strings = r#"{
        "string": "\u20ac$\u000F\u000aA'\u0042\u0022\u005c\\\"\/",
        "literals": [null, true, false]
    }"#;
    assert_eq!(
        canonical(strings),
        "{\"literals\":[null,true,false],\"string\":\"\u{20ac}$\\u000f\\nA'B\\\"\\\\\\\\\\\"/\"}"
    );
    assert_eq!(
        canonical(" { \"b\" : [ 3 , { \"z\" : 1 , \"a\" : 2 } ] ,\n\t\"a\" : { } } "),
        r#"{"a":{},"b":[3,{"a":2,"z":1}]}"#
    );
    assert_eq!(
        canonical(r#""\u001f\u007f\b\f\t\u2028\ud83d\ude00\/""#),
        "\"\\u001f\u{7f}\\b\\f\\t\u{2028}\u{1F600}/\""
    );
    // A repeated name keeps its last value, as JSON.parse does.
    assert_eq!(canonical(r#"{"a":1,"b":2,"a":3}"#), r#"{"a":3,"b":2}"#);
    for invalid in [
        "",
        "[1,]",
        "{'a':1}",
        "01",
        "1.",
        ".5",
        "+1",
        "1e",
        "\"\\ud800\"",
        "\"\\udc00x\"",
        "\"\\x41\"",
        "\"a\u{1}b\"",
        "1 2",
        "NaN",
        "[1] // comment",
        "{\"a\" 1}",
        "\u{feff}{}",
    ] {
        assert!(
            Json::parse(invalid).is_err(),
            "{invalid:?} is refused, not read as JSON"
        );
    }
    let deep = "[".repeat(crate::json::MAX_DEPTH + 1) + &"]".repeat(crate::json::MAX_DEPTH + 1);
    assert!(matches!(Json::parse(&deep), Err(JsonError::TooDeep { .. })));
    let ok = "[".repeat(crate::json::MAX_DEPTH) + &"]".repeat(crate::json::MAX_DEPTH);
    assert!(Json::parse(&ok).is_ok());
}

/// Numbers keep their exact decimal value and get one spelling, in
/// ECMAScript's layout: RFC 8785's number vectors where a double is exact,
/// and exact digits where it would not be.
pub fn exact_decimal_number_vectors() {
    for (input, expected) in [
        ("0", "0"),
        ("-0", "0"),
        ("-0.0e5", "0"),
        ("1", "1"),
        ("1.0", "1"),
        ("1e0", "1"),
        ("10E-1", "1"),
        ("100", "100"),
        ("12E+2", "1200"),
        ("-12.5e-1", "-1.25"),
        ("4.50", "4.5"),
        ("2e-3", "0.002"),
        ("0.000001", "0.000001"),
        ("0.0000001", "1e-7"),
        ("123e-20", "1.23e-18"),
        ("0.000000000000000000000000001", "1e-27"),
        ("1E30", "1e+30"),
        ("1.5e300", "1.5e+300"),
        ("1e20", "100000000000000000000"),
        ("1e21", "1e+21"),
        ("123456789012345678901", "123456789012345678901"),
        ("1234567890123456789012", "1.234567890123456789012e+21"),
        // A double would print 333333333.3333333: the exact digits stay.
        ("333333333.33333329", "333333333.33333329"),
        // 2^53 + 1 and 2^64 + 1: a double rounds both.
        ("9007199254740993", "9007199254740993"),
        ("18446744073709551617", "18446744073709551617"),
        ("-18446744073709551617", "-18446744073709551617"),
        (
            "0.1000000000000000055511151231257827",
            "0.1000000000000000055511151231257827",
        ),
    ] {
        assert_eq!(canonical(input), expected, "{input}");
        assert_eq!(canonical(&format!("[{input}]")), format!("[{expected}]"));
    }
    assert!(matches!(
        Json::parse(&format!(
            "1e{}",
            "9".repeat(crate::json::MAX_EXPONENT_DIGITS + 1)
        )),
        Err(JsonError::ExponentOutOfRange { .. })
    ));
}

/// A snowflake id beyond 2^53 in streamed tool arguments keeps every digit,
/// where a double would round it.
pub fn snowflake_id_survives_canonicalization() {
    let id = "1234567890123456789";
    #[allow(clippy::cast_precision_loss)]
    let rounded = 1_234_567_890_123_456_789_u64 as f64;
    assert_ne!(format!("{rounded}"), id, "a double cannot hold the id");
    let delta = format!(
        r#"{{"type":"content_block_delta","index":0,"delta":{{"type":"input_json_delta","partial_json":"{{\"channel\": {id}, \"after\": -9223372036854775809, \"limit\": 1.50}}"}}}}"#
    );
    let [delta_stop, message_stop] = stop("tool_use");
    let stream = sse(&[
        START,
        (
            "content_block_start",
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"fetch","input":{}}}"#,
        ),
        ("content_block_delta", &delta),
        (
            "content_block_stop",
            r#"{"type":"content_block_stop","index":0}"#,
        ),
        (delta_stop.0, &delta_stop.1),
        (message_stop.0, &message_stop.1),
    ]);
    let normalization = normalize(&streamed(&stream));
    let parts = response_parts(&normalization.exchange);
    let Some(AssistantPart::ToolCall(call)) = parts.first() else {
        panic!("a tool call: {parts:?}");
    };
    assert_eq!(
        call.arguments,
        ToolArguments::Json(crosstalk_spec::observed::message::CanonicalJson(format!(
            r#"{{"after":-9223372036854775809,"channel":{id},"limit":1.5}}"#
        )))
    );
}
