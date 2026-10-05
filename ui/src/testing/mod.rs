//! Test helpers: a router over the fixture backend and request shortcuts.

use std::sync::{Arc, OnceLock};
use topcoat::context::{AppContext, Cx};
use topcoat::router::request::Request;
use topcoat::router::{Body, HeaderMap, Router, RouterBuilderDiscoverExt, StatusCode, to_bytes};

use topcoat::asset::{
    AssetConfig, AssetId, MANIFEST_VERSION, Manifest, ManifestEntry, RawAsset,
    RouterBuilderAssetExt,
};
use topcoat::runtime::RouterBuilderRuntimeExt;
use topcoat::view::{View, ViewExt};

use crosstalk_spec::ids::{AgentId, ChannelId, OperatorId};
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, OperatorConfig, OperatorDirectory, OperatorName, RequestIdentity, TrustedOperator,
};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, PermissionSet};

use crate::backend::fixture::{ChannelKey, FixtureBackend};
use crate::config::Access;
use crate::url::ulid::UlidId;

/// The seed every harness backend is generated from.
pub const SEED: u64 = 7;

/// What a test sees of a response.
pub struct Reply {
    pub status: StatusCode,
    pub location: Option<String>,
    pub headers: HeaderMap,
    pub body: String,
}

pub fn operator() -> Access {
    let name = OperatorName::new("tester").expect("name");
    Access::trusted(TrustedOperator {
        id: OperatorId::from_raw(1),
        name,
    })
    .expect("trusted access")
}

/// A caller holding exactly `permissions` (at least one), as an
/// authenticated directory gives it to operator 1.
#[allow(dead_code)]
pub fn caller_with(permissions: &[Permission]) -> Caller {
    caller_of(OperatorId::from_raw(1), permissions)
}

/// A caller for `operator` holding exactly `permissions` (at least one).
pub fn caller_of(operator: OperatorId, permissions: &[Permission]) -> Caller {
    let config = OperatorConfig {
        id: operator,
        name: OperatorName::new("test operator").expect("name"),
        permissions: PermissionSet::of(permissions.iter().copied()),
    };
    let (directory, _) = OperatorDirectory::load(None, &AccessConfig::Authenticated(vec![config]))
        .expect("directory");
    directory
        .caller(RequestIdentity::Verified(operator))
        .expect("caller")
}

pub fn router() -> Router {
    router_over(FixtureBackend::try_new(SEED).expect("fixture generates"))
}

/// The router over `backend`.
pub fn router_over(backend: FixtureBackend) -> Router {
    Router::builder()
        .discover()
        .app_context(operator())
        .app_context(backend)
        .assets(assets())
        .runtime()
        .build()
}

/// A catalog of every asset declared in this test binary, hosted nowhere:
/// tests read markup, not stylesheets.
fn assets() -> AssetConfig {
    static IDS: OnceLock<Vec<AssetId>> = OnceLock::new();
    let ids = IDS.get_or_init(|| {
        let exe = std::env::current_exe().expect("test binary path");
        let binary = std::fs::read(exe).expect("test binary");
        RawAsset::find_in_binary(&binary)
            .iter()
            .map(RawAsset::id)
            .collect()
    });
    let manifest = Manifest {
        version: MANIFEST_VERSION,
        assets: ids
            .iter()
            .enumerate()
            .map(|(i, id)| ManifestEntry {
                id: *id,
                file: format!("asset-{i}"),
                hash: String::new(),
                content_type: "application/octet-stream".to_owned(),
            })
            .collect(),
    };
    AssetConfig::hosted_at("/assets", manifest)
}

/// A request context over the fixture backend, for rendering components.
pub fn cx() -> Cx {
    let mut app = AppContext::new();
    app.insert(operator());
    app.insert(FixtureBackend::try_new(SEED).expect("fixture generates"));
    Cx::new(Arc::new(app))
}

/// Renders a component view to HTML.
pub async fn render(view: impl View, cx: &Cx) -> String {
    view.first().await.expect("view renders").render(cx)
}

/// A copy of the harness world, for finding the ids of its scenario
/// entities and what the backend says about them. Same seed, same ids.
/// Never act on it: tests share it.
pub fn world() -> &'static FixtureBackend {
    static WORLD: OnceLock<FixtureBackend> = OnceLock::new();
    WORLD.get_or_init(|| FixtureBackend::try_new(SEED).expect("fixture generates"))
}

/// The id of a scenario agent by its fixture key (`pi2`, `al3`, …).
pub fn agent_id(key: &str) -> AgentId {
    world()
        .scenario()
        .agent(key)
        .unwrap_or_else(|| panic!("no fixture agent {key}"))
}

/// The id of a scenario channel.
pub fn channel_id(key: ChannelKey) -> ChannelId {
    world()
        .scenario()
        .channel(key)
        .unwrap_or_else(|| panic!("no fixture channel {key:?}"))
}

/// One router, so the backend's state carries over between requests (an
/// action posted, then the page that shows it). [`get`] and [`post`] build
/// a fresh world per request.
pub struct Session(Router);

impl Session {
    pub fn new() -> Self {
        Self(router())
    }

    pub async fn get(&self, uri: &str) -> Reply {
        send_to(&self.0, get_request(uri)).await
    }

    pub async fn post(&self, uri: &str, form: &str) -> Reply {
        send_to(&self.0, post_request(uri, form)).await
    }
}

async fn send(request: Request) -> Reply {
    send_to(&router(), request).await
}

/// A GET through `router`.
pub async fn get_from(router: &Router, uri: &str) -> Reply {
    send_to(router, get_request(uri)).await
}

async fn send_to(router: &Router, request: Request) -> Reply {
    let response = router.handle(request).await;
    let status = response.status();
    let headers = response.headers().clone();
    let location = response
        .headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let bytes = to_bytes(response.into_body(), 1 << 22).await.expect("body");
    Reply {
        status,
        location,
        headers,
        body: String::from_utf8_lossy(&bytes).into_owned(),
    }
}

fn get_request(uri: &str) -> Request {
    Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .expect("request")
}

fn post_request(uri: &str, form: &str) -> Request {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from(form.to_owned()))
        .expect("request")
}

pub async fn get(uri: &str) -> Reply {
    send(get_request(uri)).await
}

pub async fn post(uri: &str, form: &str) -> Reply {
    send(post_request(uri, form)).await
}

#[tokio::test]
async fn overview_renders() {
    let reply = get("/").await;
    assert_eq!(
        reply.status,
        StatusCode::TEMPORARY_REDIRECT,
        "{}",
        reply.body
    );
    let reply = get(reply.location.as_deref().expect("location")).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert!(reply.body.contains("Overview"));
}
