//! Serving connections: an accept loop over a TCP listener, and one
//! HTTP/1.1 connection over any byte stream (a TCP socket, or an in-memory
//! duplex in simulation).

use std::convert::Infallible;
use std::future::Future;

use crosstalk_spec::interfaces::l0_ingress::ProviderAdapter;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::client::legacy::connect::Connect;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::task::JoinSet;

use super::Proxy;
use crate::decode::RequestDecoder;

/// Why serving stopped.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("reading the listener's address: {0}")]
    Listener(std::io::Error),
}

impl<A, D, C> Proxy<A, D, C>
where
    A: ProviderAdapter + Send + Sync + 'static,
    A::Framer: Unpin,
    D: RequestDecoder,
    C: Connect + Clone + Send + Sync + 'static,
{
    /// Serve one client connection until it closes. The proxy sends no
    /// `Date` of its own: the client gets the upstream's headers only.
    pub async fn serve_connection<I>(&self, io: I)
    where
        I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let proxy = self.clone();
        let service = service_fn(move |request| {
            let proxy = proxy.clone();
            async move { Ok::<_, Infallible>(proxy.handle(request).await) }
        });
        let mut builder = http1::Builder::new();
        builder.auto_date_header(false);
        if let Err(error) = builder.serve_connection(TokioIo::new(io), service).await {
            tracing::debug!(error = %error, "client connection ended with an error");
        }
    }

    /// Accept connections on `listener` until `shutdown` resolves, serving
    /// each on its own task. Connections still open at shutdown are dropped
    /// with the task set.
    pub async fn serve(
        &self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), ServeError> {
        let addr = listener.local_addr().map_err(ServeError::Listener)?;
        tracing::info!(%addr, "ingress proxy listening");
        let mut connections = JoinSet::new();
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => break,
                accepted = listener.accept() => match accepted {
                    Ok((stream, peer)) => {
                        tracing::debug!(%peer, "client connected");
                        if let Err(error) = stream.set_nodelay(true) {
                            tracing::debug!(error = %error, "could not set TCP_NODELAY");
                        }
                        let proxy = self.clone();
                        connections.spawn(async move { proxy.serve_connection(stream).await });
                    }
                    Err(error) => tracing::warn!(error = %error, "accept failed"),
                },
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
            }
        }
        tracing::info!(%addr, "ingress proxy stopped");
        Ok(())
    }
}
