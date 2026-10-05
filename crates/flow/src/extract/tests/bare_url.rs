//! A fetch tool's or HTTP tool's `url` with no scheme: a bare
//! `host[:port][/path…]` reads as `https://`; a file name or a word is
//! still not a URL (`flow.extract.bare-host-url-is-https`).

use serde_json::json;

use crosstalk_spec::derived::flow::access::Extraction::Structured;
use crosstalk_spec::derived::flow::resource::{Host, Locator};
use crosstalk_spec::interfaces::l5_flow::ExtractError;

use super::support::*;
use crate::extract::resource::{tool_url_locator, url_locator};
use crate::extract::{ExtractConfig, ToolExtractors};

fn agentdojo() -> ExtractConfig {
    ExtractConfig::from_json(r#"{"fetch_tools": ["get_webpage"]}"#).expect("valid")
}

/// AgentDojo's `get_webpage {"url": "www.informations.com"}` reads
/// `https://www.informations.com/`.
#[test]
fn agentdojo_bare_host_reads_https() {
    let config = agentdojo();
    let context = context();
    let extractors = ToolExtractors::new(&config, &context);
    let get = call("get_webpage", json!({ "url": "www.informations.com" }));
    assert_eq!(
        extractors.extract_classified(&get, Some(&ok("<html>notice</html>"))),
        Ok(vec![read(https("www.informations.com", "/"), Structured)]),
    );
    // The same page whichever way it is spelled.
    let full = call(
        "get_webpage",
        json!({ "url": "https://www.informations.com" }),
    );
    assert_eq!(
        extractors.extract_classified(&full, Some(&ok("<html>notice</html>"))),
        extractors.extract_classified(&get, Some(&ok("<html>notice</html>"))),
    );
}

/// The built-in fetch tools and the HTTP tool take a bare host too.
#[test]
fn builtin_fetch_and_http_tools_read_a_bare_host() {
    let config = ExtractConfig::default();
    let context = context();
    let extractors = ToolExtractors::new(&config, &context);
    let fetch = call(
        "WebFetch",
        json!({ "url": "docs.example.com/guide?b=2&a=1", "prompt": "summarize" }),
    );
    let Ok(found) = extractors.extract_classified(&fetch, Some(&ok("guide"))) else {
        panic!("not extracted");
    };
    assert_eq!(
        found,
        vec![read(
            Locator::Url {
                scheme: "https".to_owned(),
                host: Host("docs.example.com".to_owned()),
                path: "/guide".to_owned(),
                query: Some("a=1&b=2".to_owned()),
            },
            Structured
        )]
    );
    let get = call(
        "http_request",
        json!({ "method": "GET", "url": "Dead-Drops.example:8443/box/7" }),
    );
    assert_eq!(
        extractors.extract_classified(&get, Some(&ok("meet at 9"))),
        Ok(vec![read(
            Locator::Url {
                scheme: "https".to_owned(),
                host: Host("dead-drops.example:8443".to_owned()),
                path: "/box/7".to_owned(),
                query: None,
            },
            Structured
        )]),
    );
}

/// A file name, a word, a sentence or an address is not a host: the
/// argument stays invalid.
#[test]
fn file_names_and_words_are_not_hosts() {
    let config = agentdojo();
    let context = context();
    let extractors = ToolExtractors::new(&config, &context);
    for url in [
        "README.md",
        "main.rs",
        "notes.txt",
        "hello",
        "informations",
        "www.informations com",
        "e.g.",
        "v1.2",
        "10.0.0.1",
        "localhost:8080",
        "-bad.example",
        "bad-.example",
        "example.com:http",
        "mailto:a@example.com",
        "src/main.rs",
        "",
    ] {
        let get = call("get_webpage", json!({ "url": url }));
        assert!(
            matches!(
                extractors.extract_classified(&get, Some(&ok("x"))),
                Err(ExtractError::Arguments { .. })
            ),
            "{url:?}",
        );
        assert!(tool_url_locator(url).is_err(), "{url:?}");
    }
}

/// Text with a scheme is parsed as it was: the bare rule only applies
/// when parsing fails, and agrees with the explicit `https://` spelling.
#[test]
fn bare_host_matches_the_https_spelling() {
    for (bare, full) in [
        ("www.informations.com", "https://www.informations.com"),
        ("Example.COM/a/./b/../c", "https://example.com/a/c"),
        ("example.com:8080", "https://example.com:8080/"),
        ("www.readme.md", "https://www.readme.md/"),
        ("docs.rs/serde", "https://docs.rs/serde"),
        ("bücher.example/", "https://xn--bcher-kva.example/"),
    ] {
        assert_eq!(tool_url_locator(bare), url_locator(full), "{bare}");
        assert!(tool_url_locator(bare).is_ok(), "{bare}");
    }
    assert_eq!(
        tool_url_locator("http://example.com/a"),
        url_locator("http://example.com/a")
    );
}
