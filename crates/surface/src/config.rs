//! What the surface is configured with: the values `QueryApi::present`
//! reports besides the clock and the stores' own, the export bounds and the
//! live feed's limits.

use crosstalk_spec::aggregates::projection::FrameRetention;
use crosstalk_spec::interfaces::l8_surface::export::{ExportFormats, ExportLimits, GatewayVersion};
use crosstalk_spec::interfaces::l8_surface::live::LiveConfig;
use crosstalk_spec::support::Similarity;

/// The surface's configuration. Every field's type holds its own rule
/// (formats non-empty and distinct, a non-zero row limit and retention, a
/// threshold in `0..=1`, a heartbeat shorter than the feed's retention), so
/// no combination of fields is invalid.
///
/// `default_remap_threshold` and `frame_retention` repeat what config gives
/// the alert store and the projection store: the surface reports them in
/// `present` and cannot read them from the stores' spec traits, so the
/// wiring hands both the same values.
#[derive(Debug, Clone, PartialEq)]
pub struct SurfaceConfig {
    /// The formats `export` writes, in offer order.
    pub export_formats: ExportFormats,
    /// How many rows an export may hold.
    pub export_limits: ExportLimits,
    /// The gateway build every export header names.
    pub gateway: GatewayVersion,
    /// The remap threshold the alert store gives a watched-topic rule
    /// created without one (`AlertRuleConfig::default_remap_threshold`).
    pub default_remap_threshold: Similarity,
    /// How long the projection store keeps a ready frame.
    pub frame_retention: FrameRetention,
    /// The live feed's buffer, heartbeat and retention.
    pub live: LiveConfig,
}
