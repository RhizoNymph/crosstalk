//! A TCP proxy in front of a test server, which a test can cut: the
//! gateway gone from under open connections, and nothing listening when
//! the client tries again.
//!
//! ```text
//! client ─▶ Proxy (127.0.0.1:0) ─▶ target      cut(): listener and every connection dropped
//! ```

use std::net::SocketAddr;

use crosstalk_client::BaseUrl;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};

/// A running proxy to one target.
pub struct Proxy {
    pub url: BaseUrl,
    /// Owns the listener and, in its `JoinSet`, every connection: aborting
    /// it drops them all.
    task: JoinHandle<()>,
}

impl Proxy {
    /// Forwards every connection to `127.0.0.1:<port>` on to `target`.
    pub async fn start(target: SocketAddr) -> Self {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .expect("bind the proxy");
        let addr = listener.local_addr().expect("proxy address");
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            while let Ok((mut inbound, _)) = listener.accept().await {
                connections.spawn(async move {
                    if let Ok(mut outbound) = TcpStream::connect(target).await {
                        // Ends when either side closes or the proxy is cut.
                        let _ = copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
            }
        });
        Self {
            url: BaseUrl::parse(&format!("http://{addr}")).expect("proxy url"),
            task,
        }
    }

    /// Drops the listener and every open connection.
    pub async fn cut(self) {
        self.task.abort();
        // Wait until the abort has dropped the sockets.
        let _ = self.task.await;
    }
}
