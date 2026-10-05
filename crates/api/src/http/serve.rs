//! Running the router on a listener: the gateway's `--role api`, bound to
//! `api.listen` (8081 in the deployment).

use std::future::Future;
use std::net::SocketAddr;

use axum::Router;
use tokio::net::TcpListener;

/// Why the API could not listen or stopped serving.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("cannot listen on {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("the API listener failed: {0}")]
    Serve(#[source] std::io::Error),
}

/// A listener on `addr` (`api.listen`).
pub async fn bind(addr: SocketAddr) -> Result<TcpListener, ServeError> {
    TcpListener::bind(addr)
        .await
        .map_err(|source| ServeError::Bind { addr, source })
}

/// Serves `router` on `listener` until `shutdown` completes, then lets the
/// requests in flight finish (a live stream ends when its surface stream
/// does).
pub async fn serve(
    listener: TcpListener,
    router: Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), ServeError> {
    let local = listener.local_addr().ok();
    tracing::info!(listen = ?local, "api listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(ServeError::Serve)
}
