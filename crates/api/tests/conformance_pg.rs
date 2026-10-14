//! The L8 conformance suite against the in-process surface over the
//! Postgres stores: `crosstalk-surface` hosted by `InProcess::host` over
//! `PgStores` on a fresh migrated database per world, the synthetic world
//! seeded through the spec's write traits on those stores
//! ([`pg_world`]). The memory harness is `conformance.rs`; the same suite
//! over HTTP on Postgres is `crosstalk-client`'s `conformance_pg.rs`.
//!
//! Needs the test database (`TEST_DATABASE_URL`, found as `TestDb` finds
//! it); without one every test skips, passing.

#[path = "support/pg_world.rs"]
mod pg_world;

use crosstalk_conformance::harness::{
    ExpectedFailure, Harness, Provision, ProvisionError, Provisioned,
};
use crosstalk_conformance::routed::{Route, Routed};
use crosstalk_conformance::world::SURFACE_FAILURES;
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::{Caller, PermissionSet};
use crosstalk_spec::support::TimeWindow;
use crosstalk_surface::export::Blake3RowHasher;
use crosstalk_world::config::{OPERATOR_ONCALL, OPERATOR_RESEARCHER};

use pg_world::{Databases, PgWorld, Stores};

/// The surface over the Postgres stores.
type PgSurface = crosstalk_surface::Surface<Stores>;

/// The in-process surface over the Postgres stores of a seeded world.
struct PgHarness(Databases);

/// Every call goes to the world's one surface.
struct OneSurface(PgWorld);

impl Route for OneSurface {
    type Target = PgSurface;

    fn route(&self, _caller: &Caller) -> &Self::Target {
        &self.0.in_process.surface
    }
}

impl Harness for PgHarness {
    type Backend = Routed<OneSurface>;
    type Hasher = Blake3RowHasher;

    async fn provision(
        &self,
        request: Provision<'_>,
    ) -> Result<Provisioned<Self::Backend>, ProvisionError> {
        let (world, bindings) = pg_world::provision(&self.0, request, Vec::new()).await?;
        Ok(Provisioned {
            backend: Routed(OneSurface(world)),
            bindings,
        })
    }

    fn operator(&self, holds: PermissionSet) -> OperatorId {
        if holds == PermissionSet::ALL {
            OPERATOR_RESEARCHER
        } else {
            OPERATOR_ONCALL
        }
    }

    async fn extent(&self, _backend: &Self::Backend) -> TimeWindow {
        pg_world::extent()
    }

    fn row_hasher(&self) -> Blake3RowHasher {
        Blake3RowHasher::default()
    }

    fn expected_failures(&self) -> &[ExpectedFailure] {
        SURFACE_FAILURES
    }

    fn unavailable(&self) -> Option<String> {
        self.0.unavailable()
    }
}

crosstalk_conformance::suite!(crate::PgHarness(crate::pg_world::Databases::new()));
