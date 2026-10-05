//! A provisioned world as a test sees it: the backend, the bindings, the
//! harness's answers and ready-made callers.

use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use crosstalk_spec::support::{TimeWindow, Timestamp};

/// A day in microseconds.
const DAY: u64 = 24 * 3600 * 1_000_000;

use crate::harness::{Callers, Harness, Knobs, Provision};
use crate::scenario::{Role, RoleKind, Scenario};

/// One provisioned world. Tests panic on anything unexpected: a world
/// that cannot be opened is a failed test, with the reason.
pub struct World<'h, H: Harness> {
    pub harness: &'h H,
    pub backend: H::Backend,
    pub scenario: Scenario,
    pub bindings: crate::scenario::Bindings,
    pub callers: Callers,
    /// The lead operator with every permission.
    pub lead: Caller,
    /// Covers every provisioned fact; on bucket boundaries.
    pub extent: TimeWindow,
    pub bucket: BucketWidth,
}

impl<'h, H: Harness> World<'h, H> {
    /// `scenario` under the implementation's default configuration.
    pub async fn open(harness: &'h H, scenario: Scenario) -> Self {
        Self::open_with(harness, scenario, Knobs::default()).await
    }

    /// `scenario` configured by `knobs`.
    pub async fn open_with(harness: &'h H, scenario: Scenario, knobs: Knobs) -> Self {
        let provisioned = harness
            .provision(Provision {
                scenario: &scenario,
                knobs,
            })
            .await
            .unwrap_or_else(|e| panic!("provision {}: {e}", scenario.name()));
        let callers = Callers::new(harness.operators());
        let lead = callers
            .lead()
            .unwrap_or_else(|e| panic!("lead caller: {e}"));
        let extent = harness.extent(&provisioned.backend).await;
        let bucket = harness.bucket_width();
        assert!(
            bucket.is_boundary(extent.start()) && bucket.is_boundary(extent.end()),
            "the harness's extent {extent:?} is on bucket boundaries"
        );
        Self {
            harness,
            backend: provisioned.backend,
            scenario,
            bindings: provisioned.bindings,
            callers,
            lead,
            extent,
            bucket,
        }
    }

    /// The world of every named scenario.
    pub async fn everything(harness: &'h H) -> Self {
        let scenario = crate::scenario::named::everything().unwrap_or_else(|e| panic!("{e}"));
        Self::open(harness, scenario).await
    }

    /// One named scenario's world.
    pub async fn of(
        harness: &'h H,
        scenario: Result<Scenario, crate::scenario::ScenarioError>,
    ) -> Self {
        Self::open(harness, scenario.unwrap_or_else(|e| panic!("{e}"))).await
    }

    /// The last day of the extent: the UI's default view.
    pub fn day(&self) -> TimeWindow {
        super::windows::tail(self.bucket, self.extent, DAY / self.bucket.as_micros())
    }

    /// The id `role` is bound to.
    pub fn id<K: RoleKind>(&self, role: Role<K>) -> K::Id {
        self.bindings
            .get(role)
            .unwrap_or_else(|e| panic!("{e} in {}", self.scenario.name()))
    }

    /// The other operator holding exactly `permissions`.
    pub fn caller(&self, permissions: &[Permission]) -> Caller {
        self.callers
            .with(permissions)
            .unwrap_or_else(|e| panic!("caller {permissions:?}: {e}"))
    }

    /// The other operator with every permission but `missing`.
    pub fn without(&self, missing: Permission) -> Caller {
        let held: Vec<Permission> = Permission::ALL
            .into_iter()
            .filter(|p| *p != missing)
            .collect();
        self.caller(&held)
    }

    /// The backend's present.
    pub async fn now(&self) -> Timestamp {
        self.harness.now(&self.backend).await
    }
}
