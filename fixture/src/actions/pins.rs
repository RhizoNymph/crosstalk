//! Topic-version pins as `TopicCatalog::pin` and `unpin` define them, over
//! the catalog in the store. An unpin is followed by retention, as the
//! catalog enforces it after an unpin, so a version that only its pin kept
//! is dropped at the acceptance time.

use crosstalk_spec::aggregates::retention::{Pin, PinChange, PinError, RetentionPolicy};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::interfaces::l8_surface::{ActionError, ActionOutcome, ConflictKind};
use crosstalk_spec::support::Timestamp;

use crate::store::State;
use crate::world::catalog::KEEP_LAST;

use super::{Acted, Stamp};

/// How the surface reports a pin refusal, as `PinTopicVersion` documents
/// it. The spec maps the catalog's errors to `QueryError` only (where a
/// dropped version is `VersionNotRetained`, which an action cannot return),
/// so the action's mapping is written here.
pub fn refusal(error: PinError) -> ActionError {
    match error {
        PinError::UnknownVersion(_) => ActionError::NotFound,
        PinError::Fitting(version) => {
            ActionError::Conflict(ConflictKind::TopicVersionFitting { version })
        }
        PinError::Dropped { version, .. } => {
            ActionError::Conflict(ConflictKind::TopicVersionDropped { version })
        }
    }
}

fn outcome(change: PinChange) -> ActionOutcome {
    match change {
        PinChange::Changed => ActionOutcome::Applied,
        PinChange::Unchanged => ActionOutcome::Unchanged,
    }
}

/// Pins `version` for the caller at the acceptance time. A pinned version
/// keeps its existing pin (`Unchanged`).
pub fn pin(state: &mut State, stamp: Stamp, version: TopicModelVersion) -> Acted {
    let pin = Pin {
        by: stamp.by,
        at: stamp.at,
    };
    state
        .catalog
        .pin(version, pin)
        .map(outcome)
        .map_err(refusal)
}

/// Removes `version`'s pin, then drops what retention no longer keeps, in
/// one step: the catalog is replaced only when both succeed. `Unchanged`
/// for a version without a pin, a dropped one included.
pub fn unpin(state: &mut State, at: Timestamp, version: TopicModelVersion) -> Acted {
    let policy = RetentionPolicy::new(KEEP_LAST).map_err(|e| ActionError::Store {
        reason: format!("fixture retention policy: {e:?}"),
    })?;
    let mut catalog = state.catalog.clone();
    let change = catalog.unpin(version).map_err(refusal)?;
    if change == PinChange::Changed {
        for dropped in policy.to_drop(&catalog) {
            catalog
                .mark_dropped(dropped, at, policy)
                .map_err(|e| ActionError::Store {
                    reason: format!("dropping topic version {}: {e:?}", dropped.0),
                })?;
        }
        state.catalog = catalog;
    }
    Ok(outcome(change))
}
