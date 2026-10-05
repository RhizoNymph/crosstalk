//! The harness: what an implementation provides so the suite can run
//! against it.
//!
//! A harness provisions worlds. Given a [`Scenario`] it returns a fresh
//! backend whose world contains every fact of the scenario, and
//! [`Bindings`] from the scenario's roles to the ids the backend gave them.
//! The world may hold more than the scenario (the fixture's always holds
//! its whole generated week), so the suite asserts what the facts imply and
//! relations between reads, never totals of a particular world.
//!
//! Besides worlds, a harness answers what the L8 traits cannot yet say
//! (the bucket width, the present, the export digest's hasher) and names
//! the operators callers are built for.

mod callers;

use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::export::{ExportLimits, RowHasher};
use crosstalk_spec::interfaces::l8_surface::live::{LiveConfig, LiveFeed};
use crosstalk_spec::interfaces::l8_surface::{OperatorActions, QueryApi};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use crate::scenario::{Bindings, Scenario};

pub use callers::{CallerError, Callers};

/// An implementation of the L8 surface under test.
///
/// The methods are native `async fn`s without `Send` bounds, like the
/// spec's traits: the suite runs every test on a current-thread runtime
/// and never spawns a backend future, so neither the harness nor the
/// backend needs to be `Send`.
// Native `async fn`, like the spec's traits (the spec allows the lint crate-wide).
#[allow(async_fn_in_trait)]
pub trait Harness {
    /// The backend a provisioned world is served by.
    type Backend: QueryApi + OperatorActions + LiveFeed;

    /// The export digest's hasher: the spec's BLAKE3 in the gateway, the
    /// implementation's stand-in elsewhere. `verify_export` runs with it.
    type Hasher: RowHasher;

    /// A fresh backend whose world contains every fact of
    /// `request.scenario`, with every role bound, configured by
    /// `request.knobs`.
    ///
    /// After this returns, nothing changes the world but the test's own
    /// calls: no background activity publishes to the live feed or moves
    /// the watermark, and every fact settled before the watermark the
    /// backend reports (in-flight evidence aside: awaiting content,
    /// detected).
    async fn provision(
        &self,
        request: Provision<'_>,
    ) -> Result<Provisioned<Self::Backend>, ProvisionError>;

    /// Two distinct operators the backend's directory defines; the suite
    /// builds every caller for one of them ([`Callers`]).
    fn operators(&self) -> Operators;

    /// The edge store's bucket width (`EdgeStore::bucket_width`), which
    /// `QueryApi` does not expose.
    fn bucket_width(&self) -> BucketWidth;

    /// The backend's present: the time an action it accepted now would be
    /// stamped with. Monotone; a fixed clock may return one instant.
    async fn now(&self, backend: &Self::Backend) -> Timestamp;

    /// A window on bucket boundaries covering every fact provisioned into
    /// `backend`, ending at or after its watermark.
    async fn extent(&self, backend: &Self::Backend) -> TimeWindow;

    /// A fresh export digest hasher.
    fn row_hasher(&self) -> Self::Hasher;
}

/// What to provision.
#[derive(Debug, Clone, Copy)]
pub struct Provision<'a> {
    pub scenario: &'a Scenario,
    pub knobs: Knobs,
}

impl<'a> Provision<'a> {
    /// `scenario` under the implementation's default configuration.
    pub fn of(scenario: &'a Scenario) -> Self {
        Self {
            scenario,
            knobs: Knobs::default(),
        }
    }
}

/// Configuration a test needs other than the implementation's default,
/// in the spec's own config types (what the gateway's config holds too).
#[derive(Debug, Clone, Copy, Default)]
pub struct Knobs {
    /// The live feed's buffer, heartbeat and retention.
    pub live: Option<LiveConfig>,
    /// `export.max_rows`.
    pub export: Option<ExportLimits>,
}

/// A provisioned world.
#[derive(Debug)]
pub struct Provisioned<B> {
    pub backend: B,
    pub bindings: Bindings,
}

/// Why a world could not be provisioned.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProvisionError {
    /// The harness cannot build this scenario (the fixture builds only the
    /// named scenarios its generated world contains).
    #[error("the harness cannot provision scenario {scenario}")]
    Unsupported { scenario: &'static str },
    /// The harness tried and failed.
    #[error("provisioning {scenario} failed: {reason}")]
    Failed {
        scenario: &'static str,
        reason: String,
    },
}

/// The two operators callers are built for: `lead` for actions whose
/// author tests compare, `other` for a second operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Operators {
    pub lead: OperatorId,
    pub other: OperatorId,
}
