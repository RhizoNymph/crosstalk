//! The configuration the smoke's composition runs with: five-minute
//! buckets, the correlator timing the scenario is shaped for, trusted
//! single-operator access.

use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::Duration;

use crosstalk_api::InProcessOptions;
use crosstalk_memory::analysis::catalog::RetentionPolicy;
use crosstalk_memory::model::build::test_model;
use crosstalk_memory::support::ManualClock;
use crosstalk_spec::aggregates::projection::FrameRetention;
use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::derived::flow::timing::CorrelationTiming;
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::export::{
    ExportFormat, ExportFormats, ExportLimits, GatewayVersion,
};
use crosstalk_spec::interfaces::l8_surface::live::LiveConfig;
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, OperatorName, TrustedOperator,
};
use crosstalk_spec::support::Similarity;
use crosstalk_surface::SurfaceConfig;

/// L7's bucket width: five minutes, as the world seed uses.
pub const BUCKET: Duration = Duration::from_secs(5 * 60);

/// A write and a read pair when they are at most this far apart (the world
/// seed's default).
pub const CORRELATION_WINDOW: Duration = Duration::from_secs(24 * 60 * 60);

/// How long a channel transmission waits for its content.
pub const EVIDENCE_WINDOW: Duration = Duration::from_secs(15 * 60);

/// How long a transmission stays suspected.
pub const SUSPECTED_TTL: Duration = Duration::from_secs(2 * 24 * 60 * 60);

/// Which option was out of range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the {0} option is out of range")]
pub struct OptionsError(pub &'static str);

/// The correlator timing the scenario is shaped for: B's read falls inside
/// the correlation window of A's write, and B's output inside the evidence
/// window of B's read.
pub fn timing() -> Result<CorrelationTiming, OptionsError> {
    CorrelationTiming::new(CORRELATION_WINDOW, EVIDENCE_WINDOW, SUSPECTED_TTL)
        .map_err(|_| OptionsError("correlation timing"))
}

/// The in-process surface's options over `clock`.
pub fn in_process(clock: ManualClock) -> Result<InProcessOptions, OptionsError> {
    let formats = ExportFormats::new(vec![ExportFormat::Jsonl])
        .map_err(|_| OptionsError("export formats"))?;
    let live = LiveConfig::new(
        NonZeroU32::MIN.saturating_add(31),
        Duration::from_secs(15),
        Duration::from_secs(600),
    )
    .map_err(|_| OptionsError("live feed"))?;
    let gateway = GatewayVersion::new("0.1.0-e2e").map_err(|_| OptionsError("gateway version"))?;
    let threshold = Similarity::new(0.8).map_err(|_| OptionsError("remap threshold"))?;
    let retention = RetentionPolicy::new(3).map_err(|_| OptionsError("topic retention"))?;
    let floor = Similarity::new(0.5).map_err(|_| OptionsError("lineage floor"))?;
    let name = OperatorName::new("Smoke").map_err(|_| OptionsError("operator name"))?;
    let bucket = u64::try_from(BUCKET.as_micros())
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or(OptionsError("bucket width"))?;
    Ok(InProcessOptions {
        clock: Arc::new(clock),
        seed: 0xE2E,
        surface: SurfaceConfig {
            export_formats: formats,
            export_limits: ExportLimits::default(),
            gateway,
            default_remap_threshold: threshold,
            frame_retention: FrameRetention::default(),
            live,
        },
        access: AccessConfig::Trusted(TrustedOperator {
            id: OperatorId::from_ulid(0xE2E),
            name,
        }),
        bucket_width: BucketWidth::from_micros(bucket),
        timing: timing()?,
        retention,
        lineage_floor: floor,
        embedding_model: test_model("e2e"),
        sinks: Vec::new(),
        projection_lease: Duration::from_secs(60),
    })
}
