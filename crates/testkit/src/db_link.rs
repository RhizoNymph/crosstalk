//! [`DbLink`]: a cuttable loopback TCP relay between a test's connection
//! pool and the test database server, to simulate a database outage
//! without stopping the server (which other tests share).
//!
//! The test points its pool at [`DbLink::host`] and [`DbLink::port`]
//! instead of the server. While the link is up, every connection is
//! relayed byte for byte. [`DbLink::cut`] resets every relayed connection
//! and resets every new one as soon as it is accepted, which a client sees
//! as the server gone; [`DbLink::restore`] relays again. Dropping the link
//! cuts it for good.
//!
//! It speaks TCP only, so it knows nothing of Postgres (or of the URL and
//! its credentials): the caller resolves the server's host and port.

use std::io;
use std::net::{Ipv4Addr, SocketAddr};

use tokio::net::{TcpListener, TcpStream, lookup_host};
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// Whether the link relays.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Link {
    Up,
    Cut,
}

/// A cuttable relay to one upstream server. See the [module docs](self).
#[derive(Debug)]
pub struct DbLink {
    addr: SocketAddr,
    upstream: SocketAddr,
    state: watch::Sender<Link>,
    accept: JoinHandle<()>,
}

impl DbLink {
    /// Listen on `127.0.0.1:0` and relay to `host:port`, up.
    pub async fn start(host: &str, port: u16) -> io::Result<Self> {
        let upstream = lookup_host((host, port)).await?.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "the upstream host resolves to nothing",
            )
        })?;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addr = listener.local_addr()?;
        let state = watch::Sender::new(Link::Up);
        let accept = tokio::spawn(accept_loop(listener, upstream, state.subscribe()));
        tracing::debug!(%addr, %upstream, "db link started");
        Ok(Self {
            addr,
            upstream,
            state,
            accept,
        })
    }

    /// The host to connect to instead of the server: `127.0.0.1`.
    pub fn host(&self) -> String {
        self.addr.ip().to_string()
    }

    /// The port to connect to instead of the server's.
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// The local address the link listens on.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The server the link relays to.
    pub fn upstream(&self) -> SocketAddr {
        self.upstream
    }

    /// Reset every relayed connection, and every new one until
    /// [`DbLink::restore`].
    pub fn cut(&self) {
        self.state.send_replace(Link::Cut);
        tracing::debug!(addr = %self.addr, "db link cut");
    }

    /// Relay new connections again.
    pub fn restore(&self) {
        self.state.send_replace(Link::Up);
        tracing::debug!(addr = %self.addr, "db link restored");
    }

    /// Whether the link is cut.
    pub fn is_cut(&self) -> bool {
        *self.state.borrow() == Link::Cut
    }
}

impl Drop for DbLink {
    fn drop(&mut self) {
        self.state.send_replace(Link::Cut);
        self.accept.abort();
    }
}

/// Reset rather than close: the peer sees the connection fail at once.
fn reset(stream: TcpStream) {
    // Best effort: without the option the drop still closes the socket.
    let _ = stream.set_zero_linger();
    drop(stream);
}

async fn accept_loop(listener: TcpListener, upstream: SocketAddr, state: watch::Receiver<Link>) {
    loop {
        let client = match listener.accept().await {
            Ok((client, _)) => client,
            Err(error) => {
                tracing::debug!(%error, "db link accept failed");
                tokio::task::yield_now().await;
                continue;
            }
        };
        if *state.borrow() == Link::Cut {
            reset(client);
            continue;
        }
        tokio::spawn(relay(client, upstream, state.clone()));
    }
}

/// Resolves once the link is cut (or the link is gone).
async fn cut(mut state: watch::Receiver<Link>) {
    loop {
        if *state.borrow_and_update() == Link::Cut {
            return;
        }
        if state.changed().await.is_err() {
            return;
        }
    }
}

async fn relay(mut client: TcpStream, upstream: SocketAddr, state: watch::Receiver<Link>) {
    let mut server = match TcpStream::connect(upstream).await {
        Ok(server) => server,
        Err(error) => {
            tracing::debug!(%error, "db link could not reach the upstream");
            reset(client);
            return;
        }
    };
    tokio::select! {
        copied = tokio::io::copy_bidirectional(&mut client, &mut server) => {
            if let Err(error) = copied {
                tracing::debug!(%error, "db link connection ended");
            }
        }
        () = cut(state) => {
            reset(client);
            reset(server);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    /// An echo server on loopback.
    async fn echo() -> SocketAddr {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 64];
                    while let Ok(n) = socket.read(&mut buf).await {
                        if n == 0 || socket.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        addr
    }

    async fn round_trip(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
        stream.write_all(b"ping").await?;
        let mut buf = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut buf))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "no echo"))??;
        Ok(buf.to_vec())
    }

    #[tokio::test]
    async fn relays_cuts_and_restores() {
        let server = echo().await;
        let link = DbLink::start("127.0.0.1", server.port())
            .await
            .expect("link");
        assert_eq!(link.upstream(), server);

        let mut first = TcpStream::connect(link.addr()).await.expect("connect");
        assert_eq!(round_trip(&mut first).await.expect("relayed"), b"ping");

        // Cut: the open connection fails, and a new one is reset.
        link.cut();
        assert!(link.is_cut());
        assert!(round_trip(&mut first).await.is_err(), "cut connection");
        if let Ok(mut second) = TcpStream::connect(link.addr()).await {
            assert!(round_trip(&mut second).await.is_err(), "refused while cut");
        }

        // Restored: new connections relay again.
        link.restore();
        let mut third = TcpStream::connect(link.addr()).await.expect("connect");
        assert_eq!(round_trip(&mut third).await.expect("relayed"), b"ping");
    }
}
