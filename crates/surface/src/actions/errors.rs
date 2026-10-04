//! The two action failures the spec gives no `ActionError::from` for.
//!
//! `query_errors` maps every store error behind an action except the bus's:
//! `ReplayDeadLetter` (`DeadLetterStore::replay`) and the `PolicyChanged`
//! that `SetPolicy` publishes both reach `BusError`, which has a mapping for
//! queries only. The orphan rule keeps a `From` impl for it out of this
//! crate, so it is a function here, mapping as the query mapping does: an
//! unknown dead letter is `NotFound`, everything else a store failure.

use crosstalk_spec::ids::UlidExhausted;
use crosstalk_spec::interfaces::l2_transport::BusError;
use crosstalk_spec::interfaces::l8_surface::{ActionError, QueryError};

/// A bus error reported to an action, as `QueryError::from` maps it for a
/// query, restricted to the variants an action can return: an action takes
/// no cursor, so `InvalidCursor` is a store fault.
pub(super) fn bus_action_error(error: BusError) -> ActionError {
    match QueryError::from(error) {
        QueryError::NotFound => ActionError::NotFound,
        QueryError::Store { reason } => ActionError::Store { reason },
        QueryError::InvalidCursor => ActionError::Store {
            reason: "bus cursor error reported to an action".to_owned(),
        },
        other => ActionError::Store {
            reason: format!("bus error reported to an action: {other:?}"),
        },
    }
}

/// No id could be minted for the envelope an action publishes.
pub(super) fn store_failure(error: UlidExhausted) -> ActionError {
    ActionError::Store {
        reason: error.to_string(),
    }
}
