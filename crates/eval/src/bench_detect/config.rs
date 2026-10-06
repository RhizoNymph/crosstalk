//! Who wrote a predictions file: the detector's name and variant (what
//! gates select on), the crosstalk commit, and a digest of every setting
//! that changes its output.
//!
//! `config_digest` is the BLAKE3 of the canonical JSON (RFC 8785, the
//! format's `CanonicalJson`) of the settings:
//!
//! | detector | settings |
//! | --- | --- |
//! | `crosstalk-live` | `LiveSettings` (the three windows in ms, seed, forwarding) and the extractor config |
//! | `crosstalk-pipeline` | the seed |
//! | `crosstalk-gateway-export` | none (the gateway ran with its own) |
//!
//! `ct-eval run --predictions-out` names the same detector with the same
//! digest, so the two files are byte-identical (parity stage P5).

use a2a_bench_format::files::DetectorInfo;
use a2a_bench_format::ids::Digest;
use a2a_bench_format::json::CanonicalJson;
use crosstalk_flow::extract::ExtractConfig;
use serde::Serialize;

use crate::detect::live::{Forwarding, LiveSettings};
use crate::golden::manifest::CROSSTALK_COMMIT;

/// The live composition.
pub const LIVE: &str = "crosstalk-live";
/// The bare gateway pipeline.
pub const PIPELINE: &str = "crosstalk-pipeline";
/// The variant of a detector with one configuration.
pub const DEFAULT_VARIANT: &str = "default";

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("encoding the settings: {0}")]
    Encode(#[source] serde_json::Error),
    #[error("the settings are not canonical JSON: {0}")]
    Canonical(String),
}

/// The live settings as their digest reads them.
#[derive(Serialize)]
struct LiveDigested<'a> {
    correlation_window_ms: u64,
    evidence_window_ms: u64,
    suspected_ttl_ms: u64,
    seed: u64,
    forwarding: Forwarding,
    extract: &'a ExtractConfig,
}

#[derive(Serialize)]
struct PipelineDigested {
    seed: u64,
}

/// The BLAKE3 of `value`'s canonical JSON.
pub fn digest(value: &impl Serialize) -> Result<Digest, ConfigError> {
    let text = serde_json::to_string(value).map_err(ConfigError::Encode)?;
    let canonical =
        CanonicalJson::canonicalize(&text).map_err(|error| ConfigError::Canonical(error.to_string()))?;
    Ok(Digest::from_bytes(
        *blake3::hash(canonical.as_str().as_bytes()).as_bytes(),
    ))
}

/// A window in whole milliseconds, saturating.
fn ms(window: std::time::Duration) -> u64 {
    u64::try_from(window.as_millis()).unwrap_or(u64::MAX)
}

/// The variant gates select a live run on.
pub fn live_variant(forwarding: Forwarding) -> &'static str {
    match forwarding {
        Forwarding::Off => "forwarding-off",
        Forwarding::On => "forwarding-on",
    }
}

/// `crosstalk-live` under `settings` and `extract`.
pub fn live_info(
    settings: &LiveSettings,
    extract: &ExtractConfig,
) -> Result<DetectorInfo, ConfigError> {
    let timing = settings.timing;
    let config_digest = digest(&LiveDigested {
        correlation_window_ms: ms(timing.correlation_window()),
        evidence_window_ms: ms(timing.evidence_window()),
        suspected_ttl_ms: ms(timing.suspected_ttl()),
        seed: settings.seed,
        forwarding: settings.forwarding,
        extract,
    })?;
    Ok(DetectorInfo {
        name: LIVE.to_owned(),
        version: CROSSTALK_COMMIT.to_owned(),
        variant: live_variant(settings.forwarding).to_owned(),
        config_digest: Some(config_digest),
    })
}

/// `crosstalk-pipeline` seeded with `seed`.
pub fn pipeline_info(seed: u64) -> Result<DetectorInfo, ConfigError> {
    Ok(DetectorInfo {
        name: PIPELINE.to_owned(),
        version: CROSSTALK_COMMIT.to_owned(),
        variant: DEFAULT_VARIANT.to_owned(),
        config_digest: Some(digest(&PipelineDigested { seed })?),
    })
}
