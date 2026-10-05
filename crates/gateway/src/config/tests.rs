//! Config parsing: the example, the deployment's config, strictness,
//! checked values and path resolution.

use std::path::{Path, PathBuf};
use std::time::Duration;

use super::*;

const EXAMPLE: &str = include_str!("../../config.example.json");

/// `deploy/config/crosstalk.json` from the deployment contract
/// (`feat/deploy`), verbatim.
const DEPLOY: &str = r#"{
  "ingress": {
    "listen": "0.0.0.0:8080",
    "routes": [
      {
        "name": "anthropic",
        "prefix": "/anthropic",
        "upstream": {
          "id": "anthropic",
          "kind": {"type": "vendor_api", "data": {"type": "anthropic"}},
          "base_url": "https://api.anthropic.com"
        }
      }
    ],
    "secrets": {"current": {"version": 1, "env": "CROSSTALK_SECRET_V1"}},
    "limits": {"upstream_idle_timeout_ms": 600000},
    "capture": {"channel_capacity": 1024}
  },
  "api": {
    "listen": "0.0.0.0:8081",
    "token": {"env": "CROSSTALK_API_TOKEN"}
  },
  "ops": {
    "listen": "0.0.0.0:9464"
  },
  "store": {
    "pool": {"max_connections": 40, "min_connections": 4, "acquire_timeout_ms": 5000}
  },
  "blobs": {
    "root": "/var/lib/crosstalk/blobs"
  },
  "embeddings": {
    "base_url": "https://api.openai.com/v1",
    "model": "text-embedding-3-small",
    "api_key": {"env": "CROSSTALK_EMBEDDINGS_API_KEY"}
  }
}"#;

fn value(text: &str) -> serde_json::Value {
    serde_json::from_str(text).expect("json")
}

fn parses(value: &serde_json::Value) -> bool {
    GatewayConfig::from_json(&value.to_string()).is_ok()
}

#[test]
fn the_example_config_parses() {
    let config = GatewayConfig::from_json(EXAMPLE).expect("the example parses");
    assert_eq!(config.ingress.routes.len(), 1);
    assert_eq!(config.ingress.routes[0].prefix, "/anthropic");
    assert_eq!(config.ingress.secrets.current.env, "CROSSTALK_SECRET_V1");
    assert!(config.store.is_some());
    assert!(config.api.is_some());
    assert!(config.embeddings.is_some());
    assert_eq!(config.pipeline, PipelineConfig::default());
    assert_eq!(config.shutdown, ShutdownConfig::default());
}

#[test]
fn the_deployment_config_parses_with_the_contract_paths() {
    let config = GatewayConfig::from_json(DEPLOY).expect("the deployment config parses");
    assert_eq!(config.ingress.listen.port(), 8080);
    assert_eq!(config.api.as_ref().map(|api| api.listen.port()), Some(8081));
    assert_eq!(config.ops.listen.port(), 9464);
    assert_eq!(
        config.api.as_ref().map(|api| api.token.env.as_str()),
        Some("CROSSTALK_API_TOKEN")
    );
    assert_eq!(
        config.data_dir().expect("a data dir"),
        Path::new("/var/lib/crosstalk")
    );
    assert_eq!(
        config.exchange_log_path().expect("a log path"),
        PathBuf::from("/var/lib/crosstalk/exchanges/exchange-log.jsonl")
    );
    assert_eq!(config.bus, crosstalk_transport::BusConfig::default());
}

#[test]
fn optional_sections_may_be_left_out() {
    let mut config = value(DEPLOY);
    let object = config.as_object_mut().expect("an object");
    for key in ["api", "store", "embeddings"] {
        object.remove(key);
    }
    let parsed = GatewayConfig::from_json(&config.to_string()).expect("parses");
    assert!(parsed.api.is_none() && parsed.store.is_none() && parsed.embeddings.is_none());
    for key in ["ingress", "ops", "blobs"] {
        let mut missing = value(DEPLOY);
        missing.as_object_mut().expect("an object").remove(key);
        assert!(!parses(&missing), "{key} is required");
    }
}

#[test]
fn unknown_fields_are_refused_at_every_level() {
    let pointers = [
        "",
        "/ingress",
        "/api",
        "/api/token",
        "/ops",
        "/store",
        "/store/pool",
        "/blobs",
        "/embeddings",
        "/embeddings/api_key",
    ];
    for pointer in pointers {
        let mut changed = value(DEPLOY);
        changed
            .pointer_mut(pointer)
            .and_then(serde_json::Value::as_object_mut)
            .expect("an object")
            .insert("surprise".to_owned(), serde_json::json!(1));
        assert!(
            !parses(&changed),
            "an unknown field at {pointer:?} was accepted"
        );
    }
    for key in ["bus", "pipeline", "shutdown"] {
        let mut changed = value(DEPLOY);
        changed[key] = serde_json::json!({"surprise": 1});
        assert!(!parses(&changed), "an unknown field in {key} was accepted");
    }
}

#[test]
fn checked_values_are_refused() {
    let cases: [(&str, serde_json::Value); 9] = [
        ("/shutdown", serde_json::json!({"drain_timeout_ms": 0})),
        ("/shutdown", serde_json::json!({"flush_timeout_ms": 0})),
        ("/pipeline", serde_json::json!({"blob_put_attempts": 0})),
        ("/api/token", serde_json::json!({"env": ""})),
        ("/api/token", serde_json::json!({"env": "A=B"})),
        (
            "/embeddings/base_url",
            serde_json::json!("ftp://example.com"),
        ),
        ("/embeddings/base_url", serde_json::json!("not a url")),
        ("/embeddings/model", serde_json::json!(" ")),
        ("/store/pool", serde_json::json!({"max_connections": 0})),
    ];
    for (pointer, bad) in cases {
        let mut changed = value(DEPLOY);
        if pointer == "/shutdown" || pointer == "/pipeline" {
            changed[&pointer[1..]] = bad.clone();
        } else {
            *changed.pointer_mut(pointer).expect("the field exists") = bad.clone();
        }
        assert!(!parses(&changed), "{pointer} = {bad} was accepted");
    }
}

#[test]
fn a_relative_blob_root_is_resolved_against_the_config_file() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let path = dir.path().join("gateway.json");
    let mut config = value(DEPLOY);
    config["blobs"]["root"] = serde_json::json!("data/blobs");
    std::fs::write(&path, config.to_string()).expect("write");
    let loaded = GatewayConfig::load(&path).expect("loads");
    assert_eq!(loaded.blobs.root, dir.path().join("data/blobs"));
    assert_eq!(
        loaded.data_dir().expect("a data dir"),
        dir.path().join("data")
    );

    config["blobs"]["root"] = serde_json::json!("blobs");
    std::fs::write(&path, config.to_string()).expect("write");
    let loaded = GatewayConfig::load(&path).expect("loads");
    assert_eq!(loaded.data_dir().expect("a data dir"), dir.path());

    std::fs::write(&path, DEPLOY).expect("write");
    let loaded = GatewayConfig::load(&path).expect("loads");
    assert_eq!(loaded.blobs.root, PathBuf::from("/var/lib/crosstalk/blobs"));
}

#[test]
fn a_blob_root_without_a_parent_is_refused() {
    let mut config = value(DEPLOY);
    config["blobs"]["root"] = serde_json::json!("/");
    let parsed = GatewayConfig::from_json(&config.to_string()).expect("parses");
    assert!(matches!(parsed.data_dir(), Err(ConfigError::NoDataDir(_))));
}

#[test]
fn a_missing_file_is_a_read_error() {
    let error =
        GatewayConfig::load(Path::new("/nonexistent/crosstalk.json")).expect_err("no such file");
    assert!(matches!(error, ConfigError::Read { .. }));
}

#[test]
fn shutdown_defaults_fit_the_compose_grace_period() {
    let shutdown = ShutdownConfig::default();
    assert!(shutdown.drain_timeout() + shutdown.flush_timeout() < Duration::from_secs(60));
}

/// The checked-in deployment config (with the flow section and the API
/// operator spelled out) parses, and its flow keys are the defaults.
#[test]
fn the_checked_in_deployment_config_spells_out_the_defaults() {
    let text = include_str!("../../../../deploy/config/crosstalk.json");
    let config = GatewayConfig::from_json(text).expect("deploy/config/crosstalk.json parses");
    assert_eq!(config.flow, FlowConfig::default());
    assert_eq!(
        config.api.as_ref().map(|api| api.operator.name.as_str()),
        Some("admin")
    );
}

/// `flow` and `api.operator` are optional, defaulted, strict and checked
/// at start.
#[test]
fn the_flow_section_and_the_api_operator_default() {
    let config = GatewayConfig::from_json(DEPLOY).expect("parses");
    assert_eq!(config.flow, FlowConfig::default());
    assert_eq!(
        config.api.map(|api| api.operator),
        Some(ApiOperator::default())
    );
    let mut with = value(DEPLOY);
    with["flow"] = serde_json::json!({"evidence_window_ms": 10000, "suspected_ttl_ms": 60000});
    with["api"]["operator"] = serde_json::json!({"name": "Ops desk"});
    let config = GatewayConfig::from_json(&with.to_string()).expect("parses");
    assert_eq!(config.flow.evidence_window_ms, 10_000);
    assert_eq!(config.flow.suspected_ttl_ms, 60_000);
    assert_eq!(
        config.flow.correlation_window_ms,
        FlowConfig::default().correlation_window_ms
    );
    assert_eq!(
        config.api.map(|api| api.operator.name.as_str().to_owned()),
        Some("Ops desk".to_owned())
    );
    for (pointer, bad) in [
        ("/flow", serde_json::json!({"surprise": 1})),
        ("/api/operator", serde_json::json!({"name": ""})),
        (
            "/api/operator",
            serde_json::json!({"name": "a", "role": "x"}),
        ),
    ] {
        let mut changed = value(DEPLOY);
        let (parent, key) = pointer.rsplit_once('/').expect("a pointer");
        changed
            .pointer_mut(if parent.is_empty() { "" } else { parent })
            .and_then(serde_json::Value::as_object_mut)
            .expect("an object")
            .insert(key.to_owned(), bad);
        assert!(!parses(&changed), "accepted {pointer}");
    }
}
