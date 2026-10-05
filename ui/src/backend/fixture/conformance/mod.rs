//! The fixture as a conformance harness: [`FixtureHarness`] implements
//! `crosstalk_conformance::Harness`, so the L8 conformance suite runs
//! against the fixture (`suite`, under `cargo test`). Test-only.
//!
//! The fixture cannot build arbitrary worlds: it generates one week from a
//! seed. It provisions a scenario by binding: every named scenario of the
//! suite describes a case the generated world already contains, and
//! [`bind`] finds the agents, channels, transmissions, merges and rules
//! that satisfy its facts (the suite then checks each fact through L8
//! before relying on it). A scenario that is not one of the named ones is
//! `Unsupported`.

mod bind;
mod find;
mod suite;

use crosstalk_conformance::harness::{Harness, Provision, ProvisionError, Provisioned};
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::PermissionSet;
use crosstalk_spec::support::{EmptyWindow, TimeWindow};

use crate::backend::fixture::FixtureBackend;
use crate::backend::fixture::clock::all_time;
use crate::backend::fixture::export::digest::RowDigest;
use crate::backend::fixture::world::{OPERATOR_ONCALL, OPERATOR_RESEARCHER};

/// The seed every provisioned world is generated from.
pub const SEED: u64 = 7;

/// Provisions the fixture's generated world for the conformance suite.
#[derive(Debug, Clone, Copy)]
pub struct FixtureHarness {
    seed: u64,
    extent: TimeWindow,
}

impl FixtureHarness {
    /// The harness for [`SEED`]. Fails only if the fixture's clock
    /// constants are broken.
    pub fn new() -> Result<Self, EmptyWindow> {
        Ok(Self {
            seed: SEED,
            extent: all_time()?,
        })
    }
}

impl Harness for FixtureHarness {
    type Backend = FixtureBackend;
    type Hasher = RowDigest;

    async fn provision(
        &self,
        request: Provision<'_>,
    ) -> Result<Provisioned<FixtureBackend>, ProvisionError> {
        let scenario = request.scenario;
        let failed = |reason: String| ProvisionError::Failed {
            scenario: scenario.name(),
            reason,
        };
        let mut backend = FixtureBackend::try_new(self.seed).map_err(|e| failed(e.to_string()))?;
        if let Some(live) = request.knobs.live {
            backend = backend.with_live_config(live);
        }
        if let Some(limits) = request.knobs.export {
            backend = backend.with_export_limits(limits);
        }
        let bindings = {
            let state = backend.state.read().await;
            bind::scenario(&backend.world, &state, scenario)?
        };
        Ok(Provisioned { backend, bindings })
    }

    /// The researcher for every permission, the on-call operator for any
    /// other set: the fixture checks only the caller's permissions.
    fn operator(&self, holds: PermissionSet) -> OperatorId {
        if holds == PermissionSet::ALL {
            OPERATOR_RESEARCHER
        } else {
            OPERATOR_ONCALL
        }
    }

    /// `[START, NOW + BUCKET)`: every generated access and confirmation.
    async fn extent(&self, _backend: &FixtureBackend) -> TimeWindow {
        self.extent
    }

    fn row_hasher(&self) -> RowDigest {
        RowDigest::new()
    }
}
