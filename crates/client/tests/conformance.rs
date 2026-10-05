//! The L8 conformance suite over HTTP: `HttpClient` against
//! `crosstalk-api`'s server, served in process on a loopback port over the
//! `crosstalk-surface` service and the memory stores seeded with the
//! synthetic world. No gateway, no Postgres.
//!
//! The surface derives the caller from the bearer token, never from the
//! call, so each permission set a test asks for is its own operator in the
//! server's directory with its own token, and every call goes through the
//! client of the caller's operator ([`Routed`]).

use std::collections::BTreeMap;

use crosstalk_api::http::BearerToken as ServerToken;
use crosstalk_api::world::{HttpWorld, WorldOptions, seed_world, serve_world};
use crosstalk_client::{BaseUrl, BearerToken, Blake3RowHasher, ClientConfig, HttpClient};
use crosstalk_conformance::harness::{
    ExpectedFailure, Harness, Provision, ProvisionError, Provisioned, caller,
};
use crosstalk_conformance::routed::{Route, Routed};
use crosstalk_conformance::world::{SURFACE_FAILURES, WorldReads, bind};
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::operators::{OperatorConfig, OperatorName};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, PermissionSet, QueryApi};
use crosstalk_spec::support::TimeWindow;
use crosstalk_world::clock::{BUCKET, DAY, UI_ANCHOR, minus, plus};
use crosstalk_world::config::OPERATOR_RESEARCHER;

/// The seed every world is generated from.
const SEED: u64 = 7;

/// The ids of the operators the harness adds, one per permission set: this
/// base plus the set's bits.
const OPERATOR_BASE: u128 = 0x0c0f_0000_0000_0000_0000_0000_0000_0000;

/// The seeded world served over HTTP.
struct HttpHarness;

/// The served world and one client per operator.
struct ByOperator {
    clients: BTreeMap<OperatorId, HttpClient<Blake3RowHasher>>,
    lead: HttpClient<Blake3RowHasher>,
    // Kept alive while the backend is: the server and the surface.
    _world: HttpWorld,
}

impl Route for ByOperator {
    type Target = HttpClient<Blake3RowHasher>;

    fn route(&self, caller: &Caller) -> &Self::Target {
        self.clients.get(&caller.operator()).unwrap_or(&self.lead)
    }
}

/// The bits of a permission set, in `Permission::ALL` order.
fn bits(holds: PermissionSet) -> u128 {
    Permission::ALL
        .iter()
        .enumerate()
        .filter(|(_, p)| holds.contains(**p))
        .fold(0, |acc, (i, _)| acc | (1 << i))
}

/// Every non-empty permission set but the full one, which the lead holds.
fn partial_sets() -> impl Iterator<Item = PermissionSet> {
    let n = Permission::ALL.len();
    (1u32..(1 << n) - 1).map(move |mask| {
        PermissionSet::of(
            Permission::ALL
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1 << i) != 0)
                .map(|(_, p)| *p),
        )
    })
}

fn operator_for(holds: PermissionSet) -> OperatorId {
    if holds == PermissionSet::ALL {
        OPERATOR_RESEARCHER
    } else {
        OperatorId::from_ulid(OPERATOR_BASE | bits(holds))
    }
}

/// A token of at least the minimum length, distinct per operator.
fn token_text(operator: OperatorId) -> String {
    format!("conformance-{:032x}", operator.as_ulid())
}

impl Harness for HttpHarness {
    type Backend = Routed<ByOperator>;
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
        for holds in partial_sets() {
            let id = operator_for(holds);
            let name = OperatorName::new(&format!("conformance-{:02x}", bits(holds)))
                .map_err(|e| failed(format!("{e:?}")))?;
            options.operators.push(OperatorConfig {
                id,
                name,
                permissions: holds,
            });
        }
        let operators: Vec<OperatorId> = std::iter::once(OPERATOR_RESEARCHER)
            .chain(partial_sets().map(operator_for))
            .collect();
        let world = seed_world(options)
            .await
            .map_err(|e| failed(e.to_string()))?;
        let lead_caller =
            caller(OPERATOR_RESEARCHER, PermissionSet::ALL).map_err(|e| failed(e.to_string()))?;
        let watermark = world
            .in_process
            .surface
            .watermark(&lead_caller)
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
        let mut server_tokens = Vec::new();
        for id in &operators {
            let token = ServerToken::new(&token_text(*id)).map_err(|e| failed(e.to_string()))?;
            server_tokens.push((token, *id));
        }
        let served = serve_world(world, server_tokens)
            .await
            .map_err(|e| failed(e.to_string()))?;
        let base = BaseUrl::parse(&served.base_url()).map_err(|e| failed(format!("{e:?}")))?;
        let anonymous = HttpClient::new(base, ClientConfig::default());
        let mut clients = BTreeMap::new();
        for id in &operators {
            let token = BearerToken::new(&token_text(*id)).map_err(|e| failed(format!("{e:?}")))?;
            clients.insert(*id, anonymous.with_token(token));
        }
        let lead = clients
            .get(&OPERATOR_RESEARCHER)
            .cloned()
            .ok_or_else(|| failed("no client for the lead".to_owned()))?;
        Ok(Provisioned {
            backend: Routed(ByOperator {
                clients,
                lead,
                _world: served,
            }),
            bindings,
        })
    }

    fn operator(&self, holds: PermissionSet) -> OperatorId {
        operator_for(holds)
    }

    async fn extent(&self, _backend: &Self::Backend) -> TimeWindow {
        let start = minus(UI_ANCHOR, 8 * DAY);
        let end = plus(UI_ANCHOR, BUCKET.as_micros().get());
        TimeWindow::new(start, end).unwrap_or_else(|_| panic!("the world's extent"))
    }

    fn row_hasher(&self) -> Blake3RowHasher {
        Blake3RowHasher::default()
    }

    fn expected_failures(&self) -> &[ExpectedFailure] {
        SURFACE_FAILURES
    }
}

crosstalk_conformance::suite!(crate::HttpHarness);
