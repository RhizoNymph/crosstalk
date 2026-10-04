//! An HTTP/1.1 accept loop with a graceful, bounded drain, shared by the
//! proxy and the admin listener.
//!
//! [`serve`] accepts connections until `stop` fires and serves each on its
//! own task. On stop it closes the listener (new connections are refused),
//! asks every open connection to finish gracefully (hyper's
//! `graceful_shutdown`: an idle keep-alive connection closes at once, one
//! with a request in flight closes after its response ends), and waits up
//! to `drain` for them. Connections still open then are aborted: their
//! responses end with an error and, on the proxy, their exchanges are
//! captured as `client_disconnected`.

use std::convert::Infallible;
use std::future::Future;
use std::time::Duration;

use hyper::body::{Body, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinSet;

/// What a drain found.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DrainReport {
    /// Connections open when the listener closed.
    pub open_at_stop: usize,
    /// Connections still open at the drain deadline, which were aborted.
    pub cut: usize,
}

/// How one listener is served.
#[derive(Debug, Clone, Copy)]
pub struct ServeOptions {
    /// The listener's name in logs (`proxy`, `admin`).
    pub name: &'static str,
    /// The longest the drain waits for open connections.
    pub drain: Duration,
    /// Whether hyper adds a `Date` header. The proxy adds nothing of its
    /// own to a response, so it turns this off.
    pub date_header: bool,
}

/// Serve `listener` with `handler` until `stop` becomes `true` (or its
/// sender is dropped), then drain.
pub async fn serve<H, Fut, B>(
    listener: TcpListener,
    handler: H,
    mut stop: watch::Receiver<bool>,
    options: ServeOptions,
) -> DrainReport
where
    H: Fn(Request<Incoming>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Response<B>> + Send + 'static,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let name = options.name;
    match listener.local_addr() {
        Ok(addr) => tracing::info!(listener = name, %addr, "listening"),
        Err(error) => {
            tracing::info!(listener = name, error = %error, "listening on an unknown address")
        }
    }
    let (graceful, graceful_watch) = watch::channel(false);
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            () = stopped(&mut stop) => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    tracing::debug!(listener = name, %peer, "connection accepted");
                    connections.spawn(connection(
                        stream,
                        handler.clone(),
                        graceful_watch.clone(),
                        options,
                    ));
                }
                Err(error) => tracing::warn!(listener = name, error = %error, "accept failed"),
            },
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
    drop(listener);
    let open_at_stop = connections.len();
    tracing::info!(
        listener = name,
        open = open_at_stop,
        drain_ms = u64::try_from(options.drain.as_millis()).unwrap_or(u64::MAX),
        "listener closed; draining connections"
    );
    // Fails only when every connection task has already ended.
    let _ = graceful.send(true);
    let drained = tokio::time::timeout(options.drain, async {
        while connections.join_next().await.is_some() {}
    })
    .await
    .is_ok();
    let cut = if drained {
        0
    } else {
        let cut = connections.len();
        tracing::warn!(
            listener = name,
            connections = cut,
            "drain deadline passed; closing open connections"
        );
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        cut
    };
    tracing::info!(listener = name, cut, "listener stopped");
    DrainReport { open_at_stop, cut }
}

/// Resolves once `flag` is `true` or its sender is gone. Holds no borrow
/// of the value across an await, so the future stays `Send`.
async fn stopped(flag: &mut watch::Receiver<bool>) {
    let _ = flag.wait_for(|stopped| *stopped).await.map(|_| ());
}

async fn connection<H, Fut, B>(
    stream: TcpStream,
    handler: H,
    mut graceful: watch::Receiver<bool>,
    options: ServeOptions,
) where
    H: Fn(Request<Incoming>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Response<B>> + Send + 'static,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    if let Err(error) = stream.set_nodelay(true) {
        tracing::debug!(listener = options.name, error = %error, "could not set TCP_NODELAY");
    }
    let service = service_fn(move |request| {
        let response = handler(request);
        async move { Ok::<_, Infallible>(response.await) }
    });
    let mut builder = http1::Builder::new();
    builder.auto_date_header(options.date_header);
    let served = builder.serve_connection(TokioIo::new(stream), service);
    tokio::pin!(served);
    let result = tokio::select! {
        result = served.as_mut() => result,
        () = stopped(&mut graceful) => {
            served.as_mut().graceful_shutdown();
            served.await
        }
    };
    if let Err(error) = result {
        tracing::debug!(listener = options.name, error = %error, "connection ended with an error");
    }
}
