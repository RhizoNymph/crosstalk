//! The L8 conformance suite over HTTP on the Postgres stores: `HttpClient`
//! against `crosstalk-api`'s server, served in process on a loopback port
//! over the surface hosted on `PgStores`, the synthetic world seeded into
//! a fresh migrated database per world (`crosstalk-api`'s
//! `tests/support/pg_world.rs`, included by path). The directory the
//! server authenticates against is the one stored in Postgres.
//!
//! As in `conformance.rs`, each permission set a test asks for is its own
//! operator in the directory with its own token, and every call goes
//! through the client of the caller's operator ([`Routed`]).
//!
//! Needs the test database (`TEST_DATABASE_URL`); without one every test
//! skips, passing.

#[path = "../../api/tests/support/pg_world.rs"]
mod pg_world;

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use crosstalk_api::http::{
    Auth, BearerToken as ServerToken, HttpApi, HttpConfig, StaticTokens, bind, serve,
};
use crosstalk_client::{BaseUrl, BearerToken, Blake3RowHasher, ClientConfig, HttpClient};
use crosstalk_conformance::harness::{
    ExpectedFailure, Harness, Provision, ProvisionError, Provisioned,
};
use crosstalk_conformance::routed::{Route, Routed};
use crosstalk_conformance::world::SURFACE_FAILURES;
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::operators::{OperatorConfig, OperatorName};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, PermissionSet};
use crosstalk_spec::support::TimeWindow;
use crosstalk_world::config::OPERATOR_RESEARCHER;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use pg_world::{Databases, PgWorld};

/// The ids of the operators the harness adds, one per permission set: this
/// base plus the set's bits.
const OPERATOR_BASE: u128 = 0x0c0f_0000_0000_0000_0000_0000_0000_0000;

/// How long reading the stored directory may take.
const WAIT: Duration = Duration::from_secs(300);

/// The Postgres-backed world served over HTTP.
struct HttpPgHarness(Databases);

/// The served world and one client per operator.
struct ByOperator {
    clients: BTreeMap<OperatorId, HttpClient<Blake3RowHasher>>,
    lead: HttpClient<Blake3RowHasher>,
    _served: Served,
}

/// The server and the world behind it; dropped, the server stops.
struct Served {
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
    _world: PgWorld,
}

impl Drop for Served {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.abort();
    }
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

impl Harness for HttpPgHarness {
    type Backend = Routed<ByOperator>;
    type Hasher = Blake3RowHasher;

    async fn provision(
        &self,
        request: Provision<'_>,
    ) -> Result<Provisioned<Self::Backend>, ProvisionError> {
        let name = request.scenario.name();
        let failed = |reason: String| ProvisionError::Failed {
            scenario: name,
            reason,
        };
        let mut extra = Vec::new();
        for holds in partial_sets() {
            let operator = OperatorName::new(&format!("conformance-{:02x}", bits(holds)))
                .map_err(|e| failed(format!("{e:?}")))?;
            extra.push(OperatorConfig {
                id: operator_for(holds),
                name: operator,
                permissions: holds,
            });
        }
        let operators: Vec<OperatorId> = std::iter::once(OPERATOR_RESEARCHER)
            .chain(partial_sets().map(operator_for))
            .collect();
        let (world, bindings) = pg_world::provision(&self.0, request, extra).await?;
        let directory = tokio::time::timeout(WAIT, world.in_process.stores.operators.directory())
            .await
            .map_err(|_| failed(format!("reading the stored directory took over {WAIT:?}")))?
            .map_err(|e| failed(format!("reading the stored directory: {e:?}")))?
            .ok_or_else(|| failed("no operator directory stored".to_owned()))?;
        let mut server_tokens = Vec::new();
        for id in &operators {
            let token = ServerToken::new(&token_text(*id)).map_err(|e| failed(e.to_string()))?;
            server_tokens.push((token, *id));
        }
        let auth = Auth::fixed(directory, StaticTokens::new(server_tokens));
        let config = HttpConfig {
            frame_retention: world.frame_retention,
            clock: Arc::clone(&world.clock),
        };
        let router = HttpApi::new(Arc::clone(&world.in_process.surface), auth, config).router();
        let listener = bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .map_err(|e| failed(e.to_string()))?;
        let addr = listener
            .local_addr()
            .map_err(|e| failed(format!("the listener's address: {e}")))?;
        let (shutdown, stop) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let stopped = async move {
                let _ = stop.await;
            };
            if let Err(error) = serve(listener, router, stopped).await {
                eprintln!("conformance_pg: the server failed: {error}");
            }
        });
        let served = Served {
            shutdown: Some(shutdown),
            task,
            _world: world,
        };
        let base =
            BaseUrl::parse(&format!("http://{addr}")).map_err(|e| failed(format!("{e:?}")))?;
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
                _served: served,
            }),
            bindings,
        })
    }

    fn operator(&self, holds: PermissionSet) -> OperatorId {
        operator_for(holds)
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

crosstalk_conformance::suite!(crate::HttpPgHarness(crate::pg_world::Databases::new()));
