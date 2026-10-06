//! The fixture's surface served over HTTP by `crosstalk_api`'s binding: a
//! real gateway API (authentication, routes, SSE) up in well under a
//! second, for the http backend's tests that need the transport but none
//! of the world's data.
//!
//! Seeding the world (`testing::http::HttpWorld`) costs about 15 s of CPU
//! in a debug build, which a full-parallel workspace run stretches past a
//! minute. The gateway-failure tests only need a server that refuses a
//! token, goes away, or streams the live feed, so they serve the
//! deterministic fixture (`FixtureBackend`) instead.
//!
//! ```text
//! UI router ─▶ AppBackend::Http(HttpClient) ──HTTP──▶ HttpApi (Auth: StaticTokens) ─▶ FixtureBackend
//! ```
//!
//! [`FixtureApi`] has `HttpWorld`'s methods (`access`, `router`, `base`,
//! `addr`, `stop`), so a test switches harness by its first line.
//!
//! Exports do not verify over it: the fixture's row digest
//! (`backend::fixture::export::digest::RowDigest`) is a stand-in for the
//! surface's BLAKE3, which the client checks the trailer against. A test of
//! an export over HTTP uses `HttpWorld`.

use std::net::SocketAddr;
use std::sync::Arc;

use crosstalk_api::http::{
    Auth, BearerToken as ServerToken, HttpApi, HttpConfig, ServeError, StaticTokens, bind, serve,
};
use crosstalk_client::{BaseUrl, BearerToken, ClientConfig, HttpClient};
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, OperatorConfig, OperatorDirectory, OperatorName,
};
use crosstalk_spec::interfaces::l8_surface::{Permission, PermissionSet, QueryApi};
use crosstalk_spec::support::{Clock, Timestamp};
use crosstalk_world::config::{OPERATOR_ONCALL, OPERATOR_RESEARCHER};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use topcoat::router::Router;

use super::SEED;
use super::http::{ONCALL_TOKEN, RESEARCHER_TOKEN};
use crate::backend::AppBackend;
use crate::backend::fixture::FixtureBackend;
use crate::backend::http::identity::{IdentityError, resolve};
use crate::config::Access;
use crate::identity::Identity;

/// The present's `now`, still: frame `max-age` is all the server reads
/// from it.
#[derive(Debug)]
struct Still(Timestamp);

impl Clock for Still {
    fn now(&self) -> Timestamp {
        self.0
    }
}

/// The fixture behind a running HTTP server.
pub struct FixtureApi {
    pub base: BaseUrl,
    addr: SocketAddr,
    stop: oneshot::Sender<()>,
    task: JoinHandle<Result<(), ServeError>>,
}

impl FixtureApi {
    /// Generates the fixture and serves it on `127.0.0.1:0`, each token
    /// signing in as the fixture's operator of the same name (the
    /// researcher with every permission, on-call with view, content and
    /// triage), as `HttpWorld` does for the world's.
    pub async fn start() -> Self {
        let fixture = FixtureBackend::try_new(SEED).expect("fixture generates");
        let present = fixture
            .present(&super::operator().caller())
            .await
            .expect("present");
        let operator = |id, name, permissions| OperatorConfig {
            id,
            name: OperatorName::new(name).expect("name"),
            permissions,
        };
        let (directory, _) = OperatorDirectory::load(
            None,
            &AccessConfig::Authenticated(vec![
                operator(OPERATOR_RESEARCHER, "researcher", PermissionSet::ALL),
                operator(
                    OPERATOR_ONCALL,
                    "oncall",
                    PermissionSet::of([Permission::View, Permission::Content, Permission::Triage]),
                ),
            ]),
        )
        .expect("directory");
        let tokens = StaticTokens::new([
            (
                ServerToken::new(RESEARCHER_TOKEN).expect("token"),
                OPERATOR_RESEARCHER,
            ),
            (
                ServerToken::new(ONCALL_TOKEN).expect("token"),
                OPERATOR_ONCALL,
            ),
        ]);
        let api = HttpApi::new(
            Arc::new(fixture),
            Auth::fixed(directory, tokens),
            HttpConfig {
                frame_retention: present.frame_retention_micros,
                clock: Arc::new(Still(present.now)),
            },
        );
        let listener = bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("address");
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(serve(listener, api.router(), async move {
            let _ = stopped.await;
        }));
        Self {
            base: BaseUrl::parse(&format!("http://{addr}")).expect("base url"),
            addr,
            stop,
            task,
        }
    }

    /// Where the server listens, for a proxy in front of it.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// A client presenting `token`.
    pub fn client(&self, token: &str) -> HttpClient {
        HttpClient::new(self.base.clone(), ClientConfig::default())
            .with_token(BearerToken::new(token).expect("token"))
    }

    /// Who `token` is, as the http backend learns it at startup.
    pub async fn access(&self, token: &str) -> Result<Access, IdentityError> {
        resolve(&self.client(token)).await
    }

    /// The UI's router over the http backend, acting as `access`.
    pub fn router(&self, token: &str, access: Access) -> Router {
        super::router_with(
            AppBackend::Http(self.client(token)),
            Identity::fixed(access),
        )
    }

    /// Stops the server; requests in flight finish first.
    pub async fn stop(self) {
        let _ = self.stop.send(());
        let _ = self.task.await;
    }
}
