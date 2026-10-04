//! Test helpers: a router over the fixture backend and request shortcuts.

use std::sync::OnceLock;
use topcoat::router::request::Request;
use topcoat::router::{Body, Router, RouterBuilderDiscoverExt, StatusCode, to_bytes};

use topcoat::asset::{
    AssetConfig, AssetId, MANIFEST_VERSION, Manifest, ManifestEntry, RawAsset,
    RouterBuilderAssetExt,
};
use topcoat::runtime::RouterBuilderRuntimeExt;

use crate::backend::fixture::FixtureBackend;
use crate::config::TrustedOperator;
use crate::url::ulid::UlidId;

/// What a test sees of a response.
pub struct Reply {
    pub status: StatusCode,
    pub location: Option<String>,
    pub body: String,
}

pub fn operator() -> TrustedOperator {
    TrustedOperator {
        id: crosstalk_spec::ids::OperatorId::from_raw(1),
        name: "tester".to_owned(),
    }
}

pub fn router() -> Router {
    Router::builder()
        .discover()
        .app_context(operator())
        .app_context(FixtureBackend::new(7))
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

async fn send(request: Request) -> Reply {
    let response = router().handle(request).await;
    let status = response.status();
    let location = response
        .headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let bytes = to_bytes(response.into_body(), 1 << 22).await.expect("body");
    Reply {
        status,
        location,
        body: String::from_utf8_lossy(&bytes).into_owned(),
    }
}

pub async fn get(uri: &str) -> Reply {
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .expect("request");
    send(request).await
}

pub async fn post(uri: &str, form: &str) -> Reply {
    let request = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from(form.to_owned()))
        .expect("request");
    send(request).await
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
