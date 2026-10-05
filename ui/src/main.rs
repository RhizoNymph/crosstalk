//! The crosstalk operator UI.

// Pages that embed shards nest component futures deeply enough that
// checking they are `Send` exceeds the default limit of 128.
#![recursion_limit = "256"]

mod app;
mod backend;
mod components;
mod config;
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

use crate::backend::Started;
use crate::config::Config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config = Config::from_env()?;
    // The service (a replay ticker, the in-process surface's relay and
    // feed) runs for as long as the server does and is shut down after it.
    let Started { backend, service } = backend::start(&config.backend).await?;
    let router = Router::builder()
        .discover()
        .app_context(config.access.clone())
        .app_context(backend)
        .assets(AssetBundle::load()?)
        .runtime()
        .build();

    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    tracing::info!(listen = %config.listen, operator = config.access.name(), "serving");
    // Returns once Ctrl+C or SIGTERM has drained the server.
    let served = topcoat::serve(listener, router).await;
    service.shutdown().await;
    tracing::info!("stopped");
    served?;
    Ok(())
}
