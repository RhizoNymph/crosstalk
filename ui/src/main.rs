//! The crosstalk operator UI.

// Pages that embed shards nest component futures deeply enough that
// checking they are `Send` exceeds the default limit of 128.
#![recursion_limit = "256"]

mod app;
mod backend;
mod components;
mod config;
mod contract;
mod data;
mod error;
mod pages;
#[cfg(test)]
mod testing;
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
        BackendConfig::Fixture { seed } => FixtureBackend::try_live(seed)?,
    };
    let router = Router::builder()
        .discover()
        .app_context(config.access.clone())
        .app_context(backend)
        .assets(AssetBundle::load()?)
        .runtime()
        .build();

    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    tracing::info!(listen = %config.listen, operator = config.access.name(), "serving");
    topcoat::serve(listener, router).await?;
    Ok(())
}
