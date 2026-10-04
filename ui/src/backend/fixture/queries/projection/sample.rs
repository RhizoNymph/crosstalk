//! The fixture's fitter: reads a spec's sample and lays it out.
//!
//! **Sample.** Every transmission confirmed in the spec's window that its
//! pinned filter admits ([`Linked::admitted`]: agents and channels resolved
//! as of the read, self-edges kept). Every confirmed transmission has an
//! embedding from the fixture's one model. When more match than the sample
//! size, the ones with the smallest sample key are kept, in ascending key
//! order (ties to the smaller id), as the spec's bottom-k selection does;
//! the key is SplitMix64 over the seed and the id, standing in for the
//! spec's keyed BLAKE3, so a narrower fit with the same seed keeps every
//! transmission of the wider one's sample that it still admits.
//!
//! **Layout.** A deterministic stand-in for UMAP: each theme is a Gaussian
//! cluster on a ring, outliers are scattered, and every position follows
//! from the seed and the transmission id. A larger minimum distance spreads
//! the clusters.

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::projection::frame::{FrameHeader, ProjectionFrame};
use crosstalk_spec::aggregates::projection::{
    FitFailure, Fitted, ProjectedPoint, ProjectionParams, ProjectionSpec,
};
use crosstalk_spec::ids::{ProjectionId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::support::{Timestamp, Watermark};

use crate::backend::Result;
use crate::backend::fixture::clock::{MINUTE, minus};
use crate::backend::fixture::rng::Rng;
use crate::backend::fixture::text::Theme;

use super::super::Ctx;
use super::super::graph::store_error;
use super::super::linked::{Counted, Linked};

/// What a fit came to.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Fitted(Fitted, Box<ProjectionFrame>),
    Failed(FitFailure),
}

/// The aggregate watermark at `at`: ten minutes behind, as at `NOW`.
pub fn watermark_at(at: Timestamp) -> Watermark {
    Watermark(minus(at, 10 * MINUTE))
}

/// The sample key of `id` under `seed`.
pub fn sample_key(seed: u64, id: TransmissionId) -> u64 {
    let raw = id.as_ulid();
    let low = u64::try_from(raw & u128::from(u64::MAX)).unwrap_or(0);
    let high = u64::try_from(raw >> 64).unwrap_or(0);
    let mut rng = Rng::new(seed ^ low ^ high.rotate_left(32));
    rng.next_u64()
}

fn center(theme: Theme) -> (f64, f64) {
    let angle = std::f64::consts::TAU * theme.index() as f64 / Theme::ALL.len() as f64;
    (6.0 * angle.cos(), 6.0 * angle.sin())
}

/// The stand-in layout of one sampled transmission.
fn position(params: ProjectionParams, counted: &Counted) -> (f32, f32) {
    let raw = counted.record.transmission.id.as_ulid();
    let low = u64::try_from(raw & u128::from(u64::MAX)).unwrap_or(0);
    let high = u64::try_from(raw >> 64).unwrap_or(0);
    let mut rng = Rng::new(params.seed() ^ low ^ high);
    let spread = 0.35 + 0.9 * f64::from(params.min_dist());
    let (x, y) = match counted.topic {
        Some(_) => {
            let (cx, cy) = center(counted.record.theme);
            (cx + rng.gaussian() * spread, cy + rng.gaussian() * spread)
        }
        None => (rng.unit() * 18.0 - 9.0, rng.unit() * 18.0 - 9.0),
    };
    (x as f32, y as f32)
}

/// Fits `spec` as the job `id` started at `at`.
pub fn fit(ctx: &Ctx, spec: &ProjectionSpec, id: ProjectionId, at: Timestamp) -> Result<Outcome> {
    let version = spec.topic_version();
    if !ctx.state.retains(version) {
        return Ok(Outcome::Failed(FitFailure::VersionNotRetained { version }));
    }
    let linked = match Linked::new(ctx, spec.window(), spec.filter()) {
        Ok(linked) => linked,
        Err(QueryError::VersionNotRetained { version }) => {
            return Ok(Outcome::Failed(FitFailure::VersionNotRetained { version }));
        }
        Err(error) => return Err(error),
    };
    let params = spec.params();
    let mut admitted = linked.admitted();
    let matching = u64::try_from(admitted.len()).map_err(|e| store_error("matching", e))?;
    admitted.sort_by_key(|c| {
        let id = c.record.transmission.id;
        (sample_key(params.seed(), id), id)
    });
    let limit = usize::try_from(params.limit().get().get()).unwrap_or(usize::MAX);
    admitted.truncate(limit);
    let got = u64::try_from(admitted.len()).map_err(|e| store_error("points", e))?;
    if got <= u64::from(params.neighbors()) {
        return Ok(Outcome::Failed(FitFailure::TooFewPoints {
            needed: u32::from(params.neighbors()) + 1,
            got,
        }));
    }
    let points: Vec<ProjectedPoint> = admitted
        .iter()
        .map(|counted| {
            let (x, y) = position(params, counted);
            ProjectedPoint {
                transmission: counted.record.transmission.id,
                from: counted.from,
                to: counted.to,
                route: RouteKind::from(&counted.route),
                topic: counted.topic,
                confirmed_at: counted.at,
                x,
                y,
            }
        })
        .collect();
    let watermark = watermark_at(at);
    let header = FrameHeader {
        projection: id,
        topic_version: version,
        watermark,
        limit: params.limit(),
        matching,
    };
    let frame =
        ProjectionFrame::from_points(header, &points).map_err(|e| store_error("frame", e))?;
    let fitted = Fitted {
        started_at: at,
        fitted_at: at,
        watermark,
        matching,
        points: frame.count(),
    };
    Ok(Outcome::Fitted(fitted, Box::new(frame)))
}
