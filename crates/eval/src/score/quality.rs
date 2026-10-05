//! The spec's `DetectionQuality`, built from a detector's transmissions and
//! verdicts the truth implies.
//!
//! Each transmission's matches are judged through the same [`Judge`] the
//! scorer uses. A transmission is `Genuine` when any match aligns with a
//! label, `FalseDetection` when none does and some match is judged false,
//! and has no verdict (unlabeled) otherwise, including every transmission
//! the detector has not confirmed (it names no sender to judge).
//!
//! `DetectionQuality` only counts what the detector opened; the scorer's
//! `missed` counts are what it cannot see.

use crosstalk_spec::aggregates::quality::DetectionQuality;
use crosstalk_spec::aliases::NoAliases;
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::support::TimeWindow;

use super::judge::{Judge, Outcome};
use crate::corpus::World;
use crate::predict::{Directory, PredictError, from_transmission};

/// Each transmission with the verdict the world's truth implies.
pub fn verdicts<'t>(
    world: &World,
    transmissions: &'t [Transmission],
    directory: &impl Directory,
) -> Result<Vec<(&'t Transmission, Option<Verdict>)>, PredictError> {
    let judge = Judge::new(world);
    transmissions
        .iter()
        .map(|transmission| {
            let predictions = from_transmission(transmission, directory)?;
            let outcomes: Vec<Outcome> = predictions
                .iter()
                .map(|prediction| judge.judge(prediction).0)
                .collect();
            let genuine = outcomes
                .iter()
                .any(|outcome| matches!(outcome, Outcome::Correct { .. }));
            let wrong = outcomes
                .iter()
                .any(|outcome| matches!(outcome, Outcome::False { .. }));
            let verdict = match (genuine, wrong) {
                (true, _) => Some(Verdict::Genuine),
                (false, true) => Some(Verdict::FalseDetection),
                (false, false) => None,
            };
            Ok((transmission, verdict))
        })
        .collect()
}

/// `DetectionQuality::tally` over the transmissions opened in `window`, with
/// the verdicts the truth implies. The detector's agents are scored as it
/// recorded them, with no merges applied (`NoAliases`).
pub fn detection_quality(
    window: TimeWindow,
    world: &World,
    transmissions: &[Transmission],
    directory: &impl Directory,
) -> Result<DetectionQuality, PredictError> {
    let judged = verdicts(world, transmissions, directory)?;
    Ok(DetectionQuality::tally(window, judged, NoAliases))
}
