//! The L8 conformance suite against the in-process surface: the
//! `crosstalk-surface` service over the memory stores, seeded with the
//! synthetic world through the spec's write traits. The callers travel
//! with each call. (`crosstalk-client`'s tests run the same suite over
//! HTTP.)

use crosstalk_api::MemoryStores;
use crosstalk_api::world::{SeededWorld, WorldOptions, seed_world};
use crosstalk_conformance::harness::{
    ExpectedFailure, Harness, Provision, ProvisionError, Provisioned,
};
use crosstalk_conformance::routed::{Route, Routed};
use crosstalk_conformance::world::{WorldReads, bind};
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::{Caller, PermissionSet, QueryApi};
use crosstalk_spec::support::{TimeWindow, Timestamp};
use crosstalk_surface::Surface;
use crosstalk_surface::export::Blake3RowHasher;
use crosstalk_world::clock::{BUCKET, DAY, UI_ANCHOR, minus, plus};
use crosstalk_world::config::{OPERATOR_ONCALL, OPERATOR_RESEARCHER};

/// The seed every world is generated from.
const SEED: u64 = 7;

/// The in-process surface over a seeded world.
struct InProcessHarness;

/// The seeded world; every call goes to its one surface.
struct OneSurface(SeededWorld);

impl Route for OneSurface {
    type Target = Surface<MemoryStores>;

    fn route(&self, _caller: &Caller) -> &Self::Target {
        &self.0.in_process.surface
    }
}

impl Harness for InProcessHarness {
    type Backend = Routed<OneSurface>;
    type Hasher = Blake3RowHasher;

    async fn provision(
        &self,
        request: Provision<'_>,
    ) -> Result<Provisioned<Self::Backend>, ProvisionError> {
        let scenario = request.scenario;
        let failed = |reason: String| ProvisionError::Failed {
            scenario: scenario.name(),
            reason,
        };
        let mut options = WorldOptions::new(SEED, UI_ANCHOR).map_err(|e| failed(e.to_string()))?;
        if let Some(live) = request.knobs.live {
            options.live = live;
        }
        if let Some(limits) = request.knobs.export {
            options.export_limits = limits;
        }
        let world = seed_world(options)
            .await
            .map_err(|e| failed(e.to_string()))?;
        let lead = crosstalk_conformance::harness::caller(OPERATOR_RESEARCHER, PermissionSet::ALL)
            .map_err(|e| failed(e.to_string()))?;
        let watermark = world
            .in_process
            .surface
            .watermark(&lead)
            .await
            .map_err(|e| failed(format!("{e:?}")))?;
        let stores = &world.in_process.stores;
        let bindings = bind(
            WorldReads {
                agents: &stores.agents,
                channels: &stores.channels,
                transmissions: &stores.transmissions,
                world: &world.scenario,
                watermark: watermark.at(),
                bucket: BUCKET,
            },
            scenario,
        )
        .await?;
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

    /// The world's week and a day before it, to the bucket after the
    /// anchor.
    async fn extent(&self, _backend: &Self::Backend) -> TimeWindow {
        extent()
    }

    fn row_hasher(&self) -> Blake3RowHasher {
        Blake3RowHasher::default()
    }

    fn expected_failures(&self) -> &[ExpectedFailure] {
        EXPECTED_FAILURES
    }
}

/// What the in-process surface over the seeded world is known to get
/// wrong or lack; see `docs/features/conformance.md`, "Findings".
pub const EXPECTED_FAILURES: &[ExpectedFailure] = &[
    ExpectedFailure {
        test: "graph::the_channel_centred_view_shares_the_topology_edges",
        reason: ACCESS_EDGES,
    },
    ExpectedFailure {
        test: "graph::channel_graph_draws_only_listed_channels",
        reason: ACCESS_EDGES,
    },
    ExpectedFailure {
        test: "graph::route_and_topic_filters_and_their_conjunction",
        reason: TOPIC_FILTER,
    },
    ExpectedFailure {
        test: "scenarios::dropped_bodies",
        reason: DROPPED_BODY,
    },
    ExpectedFailure {
        test: "scenarios::everything",
        reason: DROPPED_BODY,
    },
    ExpectedFailure {
        test: "projections::every_fit_is_a_new_job_with_a_reproducible_frame",
        reason: NO_FITTER,
    },
    ExpectedFailure {
        test: "projections::samples_honour_the_window_and_filter",
        reason: NO_FITTER,
    },
    ExpectedFailure {
        test: "projections::a_narrower_fit_keeps_what_it_admits_of_a_wider_sample",
        reason: NO_FITTER,
    },
    ExpectedFailure {
        test: "projections::too_few_points_fail_the_job",
        reason: NO_FITTER,
    },
    ExpectedFailure {
        test: "projections::jobs_list_newest_first",
        reason: NO_FITTER,
    },
];

const ACCESS_EDGES: &str = "channel_topology draws no access edges for a discovered channel's \
    resources (accessed before the channel existed), so the hijacked wiki has none and the \
    unconfirmed S3 channel is not drawn (INV-860, INV-861)";
const TOPIC_FILTER: &str = "a topic filter keeps transmissions whose transmissions_by_id rows are \
    Unassigned under the pinned version: the edge store's topic buckets and the rows' topics \
    disagree (INV-345, INV-400)";
const DROPPED_BODY: &str = "transmission_evidence for a transmission whose body retention dropped \
    fails with Store(\"span missing\") instead of answering BodyDropped on that side (INV-698)";
const NO_FITTER: &str = "no projection fitter runs in the in-process composition: a fit_projection \
    job never leaves the queue";

/// `[anchor - 8 days, anchor + one bucket)`: the anchor is on a bucket
/// boundary, so both ends are.
fn extent() -> TimeWindow {
    let start: Timestamp = minus(UI_ANCHOR, 8 * DAY);
    let end = plus(UI_ANCHOR, BUCKET.as_micros().get());
    TimeWindow::new(start, end).unwrap_or_else(|_| panic!("the world's extent"))
}

crosstalk_conformance::suite!(crate::InProcessHarness);
