//! A real HTTP server in this process for the http backend's tests: the
//! world's surface (`crosstalk_api::InProcess` over the memory stores,
//! seeded by `crosstalk-world`, as the world backend runs it) served by
//! `crosstalk_api::http` on an ephemeral port, with a fixed bearer token
//! per world operator, and the UI's router over a `crosstalk_client`
//! client of it.
//!
//! ```text
//! UI router ─▶ AppBackend::Http(HttpClient) ──HTTP──▶ HttpApi (Auth: StaticTokens) ─▶ Surface<MemoryStores>
//! ```

use std::sync::Arc;

use crosstalk_api::InProcess;
use crosstalk_api::http::{
    Auth, BearerToken as ServerToken, HttpApi, HttpConfig, StaticTokens, bind, serve,
};
use crosstalk_client::{BaseUrl, BearerToken, ClientConfig, HttpClient};
use crosstalk_spec::ids::{ConfigHash, OperatorId};
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, OperatorConfig, OperatorDirectory, OperatorStore, RequestIdentity,
};
use crosstalk_spec::interfaces::l8_surface::{Caller, QueryApi};
use crosstalk_spec::support::Blake3;
use crosstalk_world::config::{OPERATOR_ONCALL, OPERATOR_RESEARCHER};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use topcoat::router::Router;

use super::SEED;
use crate::backend::AppBackend;
use crate::backend::http::identity::{IdentityError, resolve};
use crate::backend::world::{WorldBackend, WorldSurface};
use crate::config::{Access, OperatorPick};
use crate::identity::Identity;
use crate::url::ulid::UlidId;

/// The researcher's token (every permission).
pub const RESEARCHER_TOKEN: &str = "ui-http-test-researcher-0123456789";
/// The on-call triager's token (view, content, triage).
pub const ONCALL_TOKEN: &str = "ui-http-test-oncall-0123456789";
/// A well-formed token the server does not know.
pub const UNKNOWN_TOKEN: &str = "ui-http-test-unknown-0123456789";

/// The world's surface behind a running HTTP server.
pub struct HttpWorld {
    pub base: BaseUrl,
    pub surface: Arc<WorldSurface>,
    in_process: InProcess,
    stop: oneshot::Sender<()>,
    server: JoinHandle<()>,
}

impl HttpWorld {
    /// Seeds the world into an in-process surface and serves it over HTTP
    /// on `127.0.0.1:0`.
    pub async fn start() -> Self {
        let (world, in_process) = WorldBackend::start(SEED).await.expect("world starts");
        let researcher = in_process
            .caller(RequestIdentity::Verified(OPERATOR_RESEARCHER))
            .await
            .expect("the researcher's caller");
        let surface = Arc::clone(&in_process.surface);
        // The server authenticates against the surface's own directory.
        let current = surface
            .operators(&researcher)
            .await
            .expect("operators")
            .into_iter()
            .filter(|o| !o.permissions.is_empty())
            .map(|o| OperatorConfig {
                id: o.id,
                name: o.name,
                permissions: o.permissions,
            })
            .collect();
        let (directory, _) = OperatorDirectory::load(None, &AccessConfig::Authenticated(current))
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
        let present = surface.present(&researcher).await.expect("present");
        let api = HttpApi::new(
            Arc::clone(&surface),
            Auth::fixed(directory, tokens),
            HttpConfig {
                frame_retention: present.frame_retention_micros,
                clock: world.clock(),
            },
        );
        let listener = bind(([127, 0, 0, 1], 0).into()).await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let (stop, stopped) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let shutdown = async move {
                let _ = stopped.await;
            };
            serve(listener, api.router(), shutdown)
                .await
                .expect("the api serves");
        });
        Self {
            base: BaseUrl::parse(&format!("http://{addr}")).expect("base url"),
            surface,
            in_process,
            stop,
            server,
        }
    }

    /// A client presenting `token`.
    pub fn client(&self, token: &str) -> HttpClient {
        HttpClient::new(self.base.clone(), ClientConfig::default())
            .with_token(BearerToken::new(token).expect("token"))
    }

    /// Who `token` is, as the http backend learns it at startup.
    pub async fn access(&self, token: &str, pick: OperatorPick) -> Result<Access, IdentityError> {
        resolve(&self.client(token), pick).await
    }

    /// The UI's router over the http backend, acting as `access`.
    pub fn router(&self, token: &str, access: Access) -> Router {
        super::router_with(
            AppBackend::Http(self.client(token)),
            Identity::fixed(access),
        )
    }

    /// The researcher's caller on the surface itself, for arranging state
    /// behind the server's back.
    pub async fn researcher(&self) -> Caller {
        self.in_process
            .caller(RequestIdentity::Verified(OPERATOR_RESEARCHER))
            .await
            .expect("caller")
    }

    /// Loads `config` into the surface's operator store, as a config
    /// reload on the server would.
    pub async fn load_access(&self, config: &AccessConfig) {
        let at = self
            .surface
            .present(&self.researcher().await)
            .await
            .expect("present")
            .now;
        let mut operators = self.in_process.stores.operators.clone();
        OperatorStore::load(
            &mut operators,
            config,
            ConfigHash::from_digest(Blake3::of(b"ui http test access")),
            at,
        )
        .await
        .expect("the access config loads");
    }

    /// Stops the server, then the surface.
    pub async fn stop(self) {
        let _ = self.stop.send(());
        let _ = self.server.await;
        self.in_process.shutdown().await;
    }
}

/// The researcher's id, as the UI config names it.
pub fn researcher() -> OperatorPick {
    OperatorPick::Id(OPERATOR_RESEARCHER)
}

/// The on-call operator's id.
pub fn oncall() -> OperatorPick {
    OperatorPick::Id(OPERATOR_ONCALL)
}

/// An operator id the world does not define.
pub fn stranger() -> OperatorPick {
    OperatorPick::Id(OperatorId::from_raw(0xdead))
}
