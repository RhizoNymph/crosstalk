//! Configuration: JSON through the checked constructors.

use crate::config::{ConfigError, DEFAULT_K, DEFAULT_W, ProvenanceConfig};

#[test]
fn empty_json_is_the_default() {
    let config: ProvenanceConfig = serde_json::from_str("{}").expect("defaults decode");
    assert_eq!(config, ProvenanceConfig::default());
    assert_eq!(config.winnow().k.get(), DEFAULT_K);
    assert_eq!(config.winnow().w.get(), DEFAULT_W);
}

#[test]
fn full_json_decodes() {
    let json = r#"{"winnow": {"k": 24, "w": 8},
        "decode": {"max_depth": 2, "max_layers": 16, "min_encoded_run": 12},
        "index": {"cutoff": 9, "retention_secs": 60, "shards": 4, "owned": [1, 3]},
        "eviction_interval_secs": 5, "semantic_threshold": 0.9}"#;
    let config: ProvenanceConfig = serde_json::from_str(json).expect("decodes");
    assert_eq!(config.winnow().k.get(), 24);
    assert_eq!(config.decode().max_depth(), 2);
    assert_eq!(config.index().cutoff(), 9);
    assert_eq!(config.index().shards().get(), 4);
    assert!(config.index().owned().contains(&3));
    assert_eq!(config.eviction_interval().as_secs(), 5);
}

#[test]
fn invalid_json_is_refused() {
    for (json, why) in [
        (r#"{"winnow": {"k": 2}}"#, "a short shingle"),
        (r#"{"winnow": {"w": 0}}"#, "an empty window"),
        (r#"{"decode": {"max_depth": 0}}"#, "no decoding"),
        (r#"{"decode": {"max_depth": 99}}"#, "unbounded decoding"),
        (r#"{"index": {"retention_secs": 0}}"#, "no retention"),
        (r#"{"index": {"shards": 2, "owned": [2]}}"#, "a shard that does not exist"),
        (r#"{"index": {"owned": []}}"#, "no shard owned"),
        (r#"{"semantic_threshold": 1.5}"#, "a threshold above one"),
        (r#"{"eviction_interval_secs": 0}"#, "no eviction"),
        (r#"{"unknown": 1}"#, "an unknown field"),
    ] {
        assert!(
            serde_json::from_str::<ProvenanceConfig>(json).is_err(),
            "accepted {why}: {json}"
        );
    }
}

#[test]
fn constructors_refuse_invalid_values() {
    assert_eq!(
        crate::config::winnow_params(3, 4),
        Err(ConfigError::ShortShingle { k: 3 })
    );
    assert_eq!(
        crate::config::DecodeLimits::new(1, 1, 4),
        Err(ConfigError::EncodedRun { run: 4 })
    );
}
