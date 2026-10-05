//! A real HTTP server in this process for the http backend's tests:
//! `crosstalk_api::world` seeds the world into the in-process surface (as
//! the world backend does) and serves it on `127.0.0.1:0` with a fixed
//! bearer token per world operator; the UI's router reads it through a
//! `crosstalk_client` client.
//!
//! ```text
//! UI router ─▶ AppBackend::Http(HttpClient) ──HTTP──▶ serve_world (StaticTokens) ─▶ Surface<MemoryStores>
//! ```

use std::sync::Arc;

use crosstalk_api::http::BearerToken as ServerToken;
use crosstalk_api::world::{self, seed_world, serve_world};
use crosstalk_client::{BaseUrl, BearerToken, ClientConfig, HttpClient};
use crosstalk_spec::ids::{ConfigHash, OperatorId};
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, OperatorStore, RequestIdentity,
};
use crosstalk_spec::interfaces::l8_surface::{Caller, QueryApi};
use crosstalk_spec::support::Blake3;
use crosstalk_world::config::{OPERATOR_ONCALL, OPERATOR_RESEARCHER};
use topcoat::router::Router;

use super::SEED;
use crate::backend::AppBackend;
use crate::backend::http::identity::{IdentityError, resolve};
use crate::backend::world::{WorldSurface, options};
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
    served: world::HttpWorld,
}

impl HttpWorld {
    /// Seeds the world and serves it over HTTP on `127.0.0.1:0`.
    pub async fn start() -> Self {
        let seeded = seed_world(options(SEED).expect("world options"))
            .await
            .expect("world seeds");
        let surface = Arc::clone(&seeded.in_process.surface);
        let tokens = vec![
            (
                ServerToken::new(RESEARCHER_TOKEN).expect("token"),
                OPERATOR_RESEARCHER,
            ),
            (
                ServerToken::new(ONCALL_TOKEN).expect("token"),
                OPERATOR_ONCALL,
            ),
        ];
        // `seed_world` settles before returning (`InProcess::settle`): the
        // relay has applied every seeded change to the live feed, so a
        // test's stream does not start with the seed's backlog.
        let served = serve_world(seeded, tokens).await.expect("the api serves");
        Self {
            base: BaseUrl::parse(&served.base_url()).expect("base url"),
            surface,
            served,
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
        self.served
            .world
            .in_process
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
        let mut operators = self.served.world.in_process.stores.operators.clone();
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
        self.served.shutdown().await;
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
