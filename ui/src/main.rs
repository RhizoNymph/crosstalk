//! The crosstalk operator UI.

mod app;
mod backend;
mod components;
mod config;
mod contract;
mod data;
mod pages;
mod url;

use topcoat::asset::{AssetBundle, RouterBuilderAssetExt};
use topcoat::router::{Router, RouterBuilderDiscoverExt};
use topcoat::runtime::RouterBuilderRuntimeExt;
use tracing_subscriber::EnvFilter;

use crate::backend::fixture::FixtureBackend;
use crate::config::{BackendConfig, Config};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config = Config::from_env()?;
    let backend = match config.backend {
        BackendConfig::Fixture { seed } => FixtureBackend::new(seed),
    };
    let router = Router::builder()
        .discover()
        .app_context(config.operator.clone())
        .app_context(backend)
        .assets(AssetBundle::load()?)
        .runtime()
        .build();

    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    tracing::info!(listen = %config.listen, operator = %config.operator.name, "serving");
    topcoat::serve(listener, router).await?;
    Ok(())
}
