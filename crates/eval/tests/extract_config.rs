//! The shipped extract configs parse as the gateway's `ExtractConfig`.

use crosstalk_flow::extract::ExtractConfig;

#[test]
fn the_agentdojo_config_makes_get_webpage_a_fetch_tool() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("extract/agentdojo.json");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{e}"));
    let config = ExtractConfig::from_json(&text).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(config.fetch_tools(), ["get_webpage".to_owned()]);
}
