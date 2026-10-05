//! Fetch tools configured by name (`fetch_tools`): a tool whose `url`
//! argument names the page it reads and whose result is the page, such as
//! AgentDojo's `get_webpage`.

use serde_json::json;

use crosstalk_spec::derived::flow::access::Extraction::Structured;
use crosstalk_spec::interfaces::l5_flow::{ExtractError, ResourceExtractor};

use super::support::*;
use crate::extract::{ConfigError, ExtractConfig, ToolExtractors};

fn configured() -> ExtractConfig {
    ExtractConfig::from_json(r#"{"fetch_tools": ["get_webpage"]}"#).expect("valid")
}

#[test]
fn a_configured_fetch_tool_reads_its_url() {
    let config = configured();
    let context = context();
    let extractors = ToolExtractors::new(&config, &context);
    let get = call(
        "get_webpage",
        json!({ "url": "https://Www.Informations.example:443/landlord#top" }),
    );
    let page = https("www.informations.example", "/landlord");
    assert!(extractors.handles(&get));
    assert_eq!(
        extractors.extract_classified(&get, Some(&ok("<html>notice</html>"))),
        Ok(vec![read(page, Structured)]),
    );
    // A read needs a delivered result.
    assert_eq!(extractors.extract_classified(&get, None), Ok(vec![]));
    assert_eq!(
        extractors.extract_classified(&get, Some(&failed("404"))),
        Ok(vec![]),
    );
    // Without a `url` argument the call is not valid.
    assert!(matches!(
        extractors.extract_classified(&call("get_webpage", json!({ "page": "x" })), Some(&ok(""))),
        Err(ExtractError::Arguments { .. })
    ));
}

#[test]
fn a_configured_fetch_tool_meets_site_rules() {
    let config = configured();
    let context = context();
    let extractors = ToolExtractors::new(&config, &context);
    let get = call(
        "get_webpage",
        json!({ "url": "https://en.m.wikipedia.org/wiki/dead_drop" }),
    );
    assert_eq!(
        extractors.extract_classified(&get, Some(&ok("A dead drop is"))),
        Ok(vec![read(
            https("en.wikipedia.org", "/wiki/Dead_drop"),
            Structured
        )]),
    );
}

#[test]
fn fetch_tools_are_configured_not_built_in() {
    let config = ExtractConfig::default();
    assert!(config.fetch_tools().is_empty());
    let context = context();
    let extractors = ToolExtractors::new(&config, &context);
    assert!(!extractors.handles(&call("get_webpage", json!({ "url": "https://x.example/" }))));
    // The built-in fetch tools stay known whatever is configured.
    let config = configured();
    let extractors = ToolExtractors::new(&config, &context);
    assert!(extractors.handles(&call("WebFetch", json!({ "url": "https://x.example/" }))));
}

#[test]
fn fetch_tool_names_are_checked() {
    assert_eq!(
        ExtractConfig::from_json(r#"{"fetch_tools": [""]}"#),
        Err(ConfigError::EmptyName),
    );
    assert_eq!(
        ExtractConfig::from_json(r#"{"fetch_tools": ["fetch"]}"#),
        Err(ConfigError::HttpAndFetch("fetch".to_owned())),
    );
    assert_eq!(
        ExtractConfig::from_json(r#"{"http_tools": ["my_http"], "fetch_tools": ["fetch"]}"#)
            .map(|config| config.fetch_tools().to_vec()),
        Ok(vec!["fetch".to_owned()]),
    );
}

#[test]
fn fetch_tools_round_trip() {
    let config = configured();
    let text = serde_json::to_string(&config).expect("serializes");
    assert_eq!(ExtractConfig::from_json(&text), Ok(config));
}
