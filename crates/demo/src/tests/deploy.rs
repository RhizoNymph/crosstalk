//! The demo's gateway config is the deployment's, with only the Anthropic
//! upstream pointed at the fake upstream service and the flow section's
//! windows shortened so the demo's transmissions settle in seconds.

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
    // The demo settles fast: a 10 s evidence window and a 60 s suspected
    // TTL; every other flow key is the deployment's.
    let flow = demo.pointer_mut("/flow").expect("a flow section");
    assert_eq!(flow["evidence_window_ms"], 10_000);
    assert_eq!(flow["suspected_ttl_ms"], 60_000);
    flow["evidence_window_ms"] = deployed["flow"]["evidence_window_ms"].clone();
    flow["suspected_ttl_ms"] = deployed["flow"]["suspected_ttl_ms"].clone();
    assert_eq!(demo, deployed);
}
