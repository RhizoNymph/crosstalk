//! The named scenarios the suite's tests run against.
//!
//! Each covers one case of the fixture world that the UI depends on, as
//! facts any implementation can provision. [`everything`] composes them all
//! into the one world most read tests use; tests that act provision the
//! part they act on (the fixture's world holds every part either way).

pub mod declared;
pub mod dropped_bodies;
pub mod hidden_channel;
pub mod hijacked_wiki;
pub mod impersonation;
pub mod late_confirmation;
pub mod lone_resource;
pub mod merges;
pub mod pipeline;
pub mod policies;
pub mod promotion;
pub mod registered;
pub mod routes;
pub mod suspected;
pub mod topics;
pub mod verdicts;

use crosstalk_spec::derived::flow::resource::{Host, Locator};

use super::{Scenario, ScenarioError};

/// The name of [`everything`].
pub const EVERYTHING: &str = "everything";

/// Every named scenario, each on its own.
pub fn all() -> Result<Vec<Scenario>, ScenarioError> {
    Ok(vec![
        hijacked_wiki::scenario()?,
        late_confirmation::scenario()?,
        impersonation::scenario()?,
        merges::scenario()?,
        hidden_channel::scenario()?,
        suspected::scenario()?,
        declared::scenario()?,
        lone_resource::scenario()?,
        promotion::scenario()?,
        policies::scenario()?,
        topics::scenario()?,
        verdicts::scenario()?,
        dropped_bodies::scenario()?,
        pipeline::scenario()?,
        routes::scenario()?,
        registered::scenario()?,
    ])
}

/// One world holding every named scenario.
pub fn everything() -> Result<Scenario, ScenarioError> {
    Scenario::compose(EVERYTHING, all()?)
}

/// An `https` URL without a query.
pub(crate) fn https(host: &str, path: &str) -> Locator {
    Locator::Url {
        scheme: "https".to_owned(),
        host: Host(host.to_owned()),
        path: path.to_owned(),
        query: None,
    }
}
