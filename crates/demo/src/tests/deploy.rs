//! The demo's gateway config is the deployment's, with only the Anthropic
//! upstream pointed at the fake upstream service.

use std::path::PathBuf;

use serde_json::Value;

fn read(relative: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(relative);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

#[test]
fn demo_config_differs_from_the_deployment_only_in_the_upstream_url() {
    let deployed = read("deploy/config/crosstalk.json");
    let mut demo = read("deploy/demo/crosstalk.demo.json");
    let url = demo
        .pointer_mut("/ingress/routes/0/upstream/base_url")
        .expect("anthropic route base_url");
    assert_eq!(url, "http://fake-upstream:8070");
    *url = deployed["ingress"]["routes"][0]["upstream"]["base_url"].clone();
    assert_eq!(demo, deployed);
}
