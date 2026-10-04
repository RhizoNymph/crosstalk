//! What the world's deployment is configured with: the values a host
//! builds its stores from before seeding, all spec types.
//!
//! - **Access.** Authenticated, with two operators: the researcher (every
//!   permission, the same id as the trusted operator in the UI's
//!   `config.json`) and the on-call triager (view, content, triage).
//! - **Sinks.** `soc-webhook`, `#agent-alerts` and `local-log`. The
//!   built-in rules are enabled and deliver to every sink.
//! - **Rules.** The default remap threshold of a watched-topic rule.
//! - **Analysis.** The embedding model, the topic catalog's retention (the
//!   last two activated versions) and lineage floor, the projection frame
//!   retention (three days).
//! - **Topology.** Five-minute buckets and the correlator's timing (a day
//!   to pair a write with a read, fifteen minutes to wait for content, two
//!   days before a suspected transmission expires).
//!
//! Each config document the deployment loaded has its own [`ConfigHash`]
//! ([`document`]); the audit log names it on each change it made.

use std::num::{NonZeroU16, NonZeroU64};
use std::sync::Arc;
use std::time::Duration;

use crosstalk_spec::aggregates::alert::{AlertRuleConfig, BuiltinRule, RuleStatus};
use crosstalk_spec::aggregates::projection::FrameRetention;
use crosstalk_spec::aggregates::retention::RetentionPolicy;
use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::aggregates::topic::EmbeddingModel;
use crosstalk_spec::derived::flow::timing::CorrelationTiming;
use crosstalk_spec::ids::{ConfigHash, OperatorId, SinkId};
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, OperatorConfig, OperatorName,
};
use crosstalk_spec::interfaces::l8_surface::{Permission, PermissionSet, SinkKind};
use crosstalk_spec::support::{Blake3, Similarity};

use crate::clock::{Anchor, BUCKET, MINUTE, WorldClock};
use crate::error::WorldError;
use crate::mint::Mint;

/// The researcher: every permission. The same id as the trusted operator in
/// the UI's `config.json`, so the UI's own actions sit next to the history.
pub const OPERATOR_RESEARCHER: OperatorId =
    OperatorId::from_ulid(0x0192_7f71_f10d_0000_0000_0000_0000_0001);
/// The on-call triager: view, content and triage only.
pub const OPERATOR_ONCALL: OperatorId =
    OperatorId::from_ulid(0x0192_7f71_f10d_0000_0000_0000_0000_0002);

/// The watched-topic rules' default remap threshold.
///
/// The UI fixture used 0.8 with lineage similarity `(cos + 1) / 2`. The
/// memory catalog's lineage similarity is the clamped cosine, under which
/// a two-theme v1 topic links to its themes' v2 topics at about 0.71 and
/// the three-theme "Engineering chatter" at about 0.58. 0.65 keeps the
/// fixture's outcome: every v1 topic carries over except "Engineering
/// chatter", which leaves its rule stale.
pub const REMAP_THRESHOLD: f32 = 0.65;

/// Every lineage link besides an entry's best is at or above this.
pub const LINEAGE_FLOOR: f32 = 0.6;

/// How many activated topic versions retention keeps: the spec's minimum.
pub const KEEP_LAST: u32 = 2;

/// How long a fitted projection frame is kept.
pub const FRAME_RETENTION_DAYS: u16 = 3;

/// The longest text, in characters, the world's embedder takes.
pub const EMBED_MAX_CHARS: usize = 1000;

/// Dimensions of the world's embedding model.
const DIMENSION: u16 = 16;

/// One configured alert sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkDef {
    pub id: SinkId,
    pub kind: SinkKind,
    pub name: String,
}

/// A built-in rule's configured status and sinks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltinDef {
    pub rule: BuiltinRule,
    pub status: RuleStatus,
    pub sinks: Vec<SinkId>,
}

/// Everything a host configures its stores with.
#[derive(Debug, Clone, PartialEq)]
pub struct WorldConfig {
    pub access: AccessConfig,
    pub sinks: Vec<SinkDef>,
    pub builtins: Vec<BuiltinDef>,
    pub rules: AlertRuleConfig,
    pub embedding: EmbeddingModel,
    pub embed_max_chars: usize,
    pub retention: RetentionPolicy,
    pub lineage_floor: Similarity,
    pub frame_retention: FrameRetention,
    pub bucket_width: BucketWidth,
    pub timing: CorrelationTiming,
}

impl WorldConfig {
    /// The configuration of the world seeded from `seed` at `anchor`. Sink
    /// ids are minted at the first config load.
    pub fn new(seed: u64, anchor: Anchor) -> Result<Self, WorldError> {
        let mut mint = Mint::new(seed, "config", Arc::new(WorldClock::Fixed(anchor)));
        let at = anchor.config_at();
        let sinks = vec![
            SinkDef {
                id: mint.at(at)?,
                kind: SinkKind::Webhook,
                name: "soc-webhook".to_owned(),
            },
            SinkDef {
                id: mint.at(at)?,
                kind: SinkKind::Slack,
                name: "#agent-alerts".to_owned(),
            },
            SinkDef {
                id: mint.at(at)?,
                kind: SinkKind::Log,
                name: "local-log".to_owned(),
            },
        ];
        let every_sink: Vec<SinkId> = sinks.iter().map(|sink| sink.id).collect();
        let builtins = BuiltinRule::ALL
            .into_iter()
            .map(|rule| BuiltinDef {
                rule,
                status: RuleStatus::Enabled,
                sinks: every_sink.clone(),
            })
            .collect();
        Ok(Self {
            access: access()?,
            sinks,
            builtins,
            rules: AlertRuleConfig {
                default_remap_threshold: similarity(REMAP_THRESHOLD)?,
            },
            embedding: embedding_model()?,
            embed_max_chars: EMBED_MAX_CHARS,
            retention: RetentionPolicy::new(KEEP_LAST)
                .map_err(|e| WorldError::invalid("RetentionPolicy", e))?,
            lineage_floor: similarity(LINEAGE_FLOOR)?,
            frame_retention: FrameRetention::from_days(
                NonZeroU16::new(FRAME_RETENTION_DAYS)
                    .ok_or_else(|| WorldError::missing("frame retention days"))?,
            ),
            bucket_width: BUCKET,
            timing: timing()?,
        })
    }

    /// The sink of `kind`.
    pub fn sink(&self, kind: SinkKind) -> Result<SinkId, WorldError> {
        self.sinks
            .iter()
            .find(|sink| sink.kind == kind)
            .map(|sink| sink.id)
            .ok_or_else(|| WorldError::missing(format!("sink {kind:?}")))
    }
}

fn similarity(value: f32) -> Result<Similarity, WorldError> {
    Similarity::new(value).map_err(|e| WorldError::invalid("Similarity", e))
}

fn operator(
    id: OperatorId,
    name: &str,
    permissions: PermissionSet,
) -> Result<OperatorConfig, WorldError> {
    Ok(OperatorConfig {
        id,
        name: OperatorName::new(name).map_err(|e| WorldError::invalid("OperatorName", e))?,
        permissions,
    })
}

/// The `access` section: authenticated, the researcher and the on-call
/// operator.
pub fn access() -> Result<AccessConfig, WorldError> {
    Ok(AccessConfig::Authenticated(vec![
        operator(OPERATOR_RESEARCHER, "researcher", PermissionSet::ALL)?,
        operator(
            OPERATOR_ONCALL,
            "oncall",
            PermissionSet::of([Permission::View, Permission::Content, Permission::Triage]),
        )?,
    ]))
}

/// The world's embedding model.
pub fn embedding_model() -> Result<EmbeddingModel, WorldError> {
    Ok(EmbeddingModel {
        name: "fixture-minilm-16".to_owned(),
        dimension: NonZeroU16::new(DIMENSION)
            .ok_or_else(|| WorldError::missing("embedding dimension"))?,
    })
}

/// A day to pair a write with a read, fifteen minutes for content, two
/// days before a suspected transmission expires.
pub fn timing() -> Result<CorrelationTiming, WorldError> {
    CorrelationTiming::new(
        Duration::from_secs(24 * 3600),
        Duration::from_micros(CONTENT_WINDOW),
        Duration::from_micros(EXPIRY),
    )
    .map_err(|e| WorldError::invalid("CorrelationTiming", e))
}

/// How long after a read the correlator waits for content evidence.
pub const CONTENT_WINDOW: u64 = 15 * MINUTE;
/// How long a suspected transmission waits before it is discarded.
pub const EXPIRY: u64 = 2 * crate::clock::DAY;

/// The hash of the `n`th config document the deployment loaded (from 1).
pub fn document(n: u8) -> ConfigHash {
    let text = format!("crosstalk world config document {n}");
    ConfigHash::from_digest(Blake3::of(text.as_bytes()))
}

/// The frame retention as a duration.
pub fn frame_retention_micros(config: &WorldConfig) -> NonZeroU64 {
    let micros =
        u64::try_from(config.frame_retention.as_duration().as_micros()).unwrap_or(u64::MAX);
    NonZeroU64::new(micros).unwrap_or(NonZeroU64::MIN)
}
