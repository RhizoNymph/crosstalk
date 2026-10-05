//! A `LiveConfig` with the surface's defaults, for callers that only choose
//! the clock and the flow timing (eval, the UI).

use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;

use crosstalk_api::InProcessOptions;
use crosstalk_flow::consumer::FlowConfig;
use crosstalk_memory::analysis::catalog::RetentionPolicy;
use crosstalk_memory::model::build::test_model;
use crosstalk_provenance::config::ProvenanceConfig;
use crosstalk_spec::aggregates::projection::FrameRetention;
use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::derived::flow::timing::CorrelationTiming;
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::export::{
    ExportFormat, ExportFormats, ExportLimits, GatewayVersion,
};
use crosstalk_spec::interfaces::l8_surface::live::LiveConfig as FeedConfig;
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, OperatorName, TrustedOperator,
};
use crosstalk_spec::support::Similarity;
use crosstalk_surface::SurfaceConfig;
use crosstalk_transport::BusConfig;

use super::{BlobConfig, LiveClock, LiveConfig, Ticking};
use crate::pipeline::Settings;

/// Which default was out of range: never for the values below, but every
/// checked constructor is checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the default {0} is out of range")]
pub struct DefaultsError(pub &'static str);

/// L7's bucket width by default: five minutes.
pub const DEFAULT_BUCKET: Duration = Duration::from_secs(5 * 60);

impl LiveConfig {
    /// A memory-only process on `clock` with L5's `flow` timing, settled by
    /// [`Live::settle`](super::Live::settle) (`Ticking::OnSettle`), seeded
    /// with `seed`; everything else the surface's defaults: trusted access
    /// (one operator with every permission), five-minute buckets, JSONL
    /// export, the default provenance config. Change any field after.
    pub fn new(clock: LiveClock, flow: FlowConfig, seed: u64) -> Result<Self, DefaultsError> {
        Ok(Self {
            surface: surface(&clock)?,
            clock,
            blobs: BlobConfig::Memory,
            bus: BusConfig::default(),
            pipeline: Settings::default(),
            flow,
            provenance: ProvenanceConfig::default(),
            ticking: Ticking::OnSettle,
            seed,
            capture: None,
        })
    }
}

fn surface(clock: &LiveClock) -> Result<InProcessOptions, DefaultsError> {
    let formats = ExportFormats::new(vec![ExportFormat::Jsonl])
        .map_err(|_| DefaultsError("export formats"))?;
    let live = FeedConfig::new(
        NonZeroU32::MIN.saturating_add(31),
        Duration::from_secs(15),
        Duration::from_secs(600),
    )
    .map_err(|_| DefaultsError("live feed"))?;
    let bucket = u64::try_from(DEFAULT_BUCKET.as_micros())
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or(DefaultsError("bucket width"))?;
    Ok(InProcessOptions {
        clock: clock.reader(),
        seed: 0,
        surface: SurfaceConfig {
            export_formats: formats,
            export_limits: ExportLimits::default(),
            gateway: GatewayVersion::new(env!("CARGO_PKG_VERSION"))
                .map_err(|_| DefaultsError("gateway version"))?,
            default_remap_threshold: Similarity::new(0.8)
                .map_err(|_| DefaultsError("remap threshold"))?,
            frame_retention: FrameRetention::default(),
            live,
        },
        access: AccessConfig::Trusted(TrustedOperator {
            id: OperatorId::from_ulid(1),
            name: OperatorName::new("Operator").map_err(|_| DefaultsError("operator name"))?,
        }),
        bucket_width: BucketWidth::from_micros(bucket),
        // Replaced by the flow config's timing at start.
        timing: CorrelationTiming::new(
            Duration::from_secs(600),
            Duration::from_secs(120),
            Duration::from_secs(1800),
        )
        .map_err(|_| DefaultsError("correlation timing"))?,
        retention: RetentionPolicy::new(3).map_err(|_| DefaultsError("topic retention"))?,
        lineage_floor: Similarity::new(0.5).map_err(|_| DefaultsError("lineage floor"))?,
        embedding_model: test_model("live"),
        sinks: Vec::new(),
        projection_lease: Duration::from_secs(60),
    })
}
