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
//! [`DbLink::partition`] is a silent network partition instead: nothing
//! is reset or closed, nothing is relayed. Every open connection stays
//! established on both sides (loopback acknowledges what a client writes)
//! but no byte, and no close, crosses the link, so a client's next query
//! waits for an answer that never comes, as across a partition that drops
//! every packet. A connection accepted meanwhile is held unanswered (the
//! upstream is not even dialled). [`DbLink::restore`] heals it: the held
//! bytes, and any close a side sent meanwhile, cross in order, as TCP
//! retransmissions would deliver them; [`DbLink::cut`] resets everything.
//!
//! It speaks TCP only, so it knows nothing of Postgres (or of the URL and
//! its credentials): the caller resolves the server's host and port.
//!
//! [`DbLink::start_lossy`] adds the faults a lossy link shows a TCP
//! client (packet loss seen as retransmission stalls, and jitter): every
//! chunk relayed in either direction waits a delay drawn from a seeded
//! stream, mostly a little jitter and, with the configured probability, a
//! retransmission timeout of 200 ms or more. The same seed gives the same
//! delays in the same order per direction and connection.

use std::io;
use std::net::{Ipv4Addr, SocketAddr};

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, lookup_host};
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// Whether the link relays.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Link {
    Up,
    /// Silent: nothing crosses, nothing is reset or closed.
    Partitioned,
    Cut,
}

/// The faults [`DbLink::start_lossy`] injects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lossy {
    /// The seed of the delay stream (each connection and direction draws
    /// from its own stream derived from it).
    pub seed: u64,
    /// Out of 1000 chunks, how many stall as a lost packet would: a
    /// retransmission timeout of `rto` up to twice it.
    pub loss_per_mille: u16,
    /// The minimum retransmission timeout (Linux: 200 ms).
    pub rto: Duration,
    /// Every other chunk waits up to this much.
    pub jitter: Duration,
}

impl Lossy {
    /// Roughly the link the node0 loop ran over: 15% of chunks stalled by
    /// a 200 to 400 ms retransmission, the rest up to 5 ms of jitter.
    pub fn tailscale_bad_day(seed: u64) -> Self {
        Self {
            seed,
            loss_per_mille: 150,
            rto: Duration::from_millis(200),
            jitter: Duration::from_millis(5),
        }
    }
}

/// A seeded delay stream (SplitMix64).
#[derive(Debug, Clone)]
struct Delays {
    state: u64,
    lossy: Lossy,
}

impl Delays {
    fn new(lossy: Lossy, stream: u64) -> Self {
        Self {
            state: lossy.seed ^ stream.wrapping_mul(0x9E37_79B9_7F4A_7C15),
            lossy,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// The next chunk's delay.
    fn next(&mut self) -> Duration {
        let roll = self.next_u64() % 1000;
        let fraction = |span: Duration, draw: u64| {
            let micros = u64::try_from(span.as_micros()).unwrap_or(u64::MAX);
            Duration::from_micros(match micros {
                0 => 0,
                micros => draw % (micros + 1),
            })
        };
        let draw = self.next_u64();
        match roll < u64::from(self.lossy.loss_per_mille) {
            true => self.lossy.rto + fraction(self.lossy.rto, draw),
            false => fraction(self.lossy.jitter, draw),
        }
    }
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
        Self::start_with(host, port, None).await
    }

    /// [`DbLink::start`] whose relayed chunks wait as on a lossy link (see
    /// [`Lossy`]).
    pub async fn start_lossy(host: &str, port: u16, lossy: Lossy) -> io::Result<Self> {
        Self::start_with(host, port, Some(lossy)).await
    }

    async fn start_with(host: &str, port: u16, lossy: Option<Lossy>) -> io::Result<Self> {
        let upstream = lookup_host((host, port)).await?.next().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "the upstream host resolves to nothing",
            )
        })?;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addr = listener.local_addr()?;
        let state = watch::Sender::new(Link::Up);
        let accept = tokio::spawn(accept_loop(listener, upstream, state.subscribe(), lossy));
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

    /// Partition the link silently: every connection, open or new, stays
    /// established but nothing crosses it (no byte, no close, no reset)
    /// until [`DbLink::restore`] or [`DbLink::cut`].
    pub fn partition(&self) {
        self.state.send_replace(Link::Partitioned);
        tracing::debug!(addr = %self.addr, "db link partitioned");
    }

    /// Relay again: new connections after a cut, held bytes and new
    /// connections after a partition.
    pub fn restore(&self) {
        self.state.send_replace(Link::Up);
        tracing::debug!(addr = %self.addr, "db link restored");
    }

    /// Whether the link is cut.
    pub fn is_cut(&self) -> bool {
        *self.state.borrow() == Link::Cut
    }

    /// Whether the link is partitioned.
    pub fn is_partitioned(&self) -> bool {
        *self.state.borrow() == Link::Partitioned
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

async fn accept_loop(
    listener: TcpListener,
    upstream: SocketAddr,
    state: watch::Receiver<Link>,
    lossy: Option<Lossy>,
) {
    let mut connections: u64 = 0;
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
        connections += 1;
        let delays = lossy.map(|lossy| {
            (
                Delays::new(lossy, connections * 2),
                Delays::new(lossy, connections * 2 + 1),
            )
        });
        tokio::spawn(relay(client, upstream, state.clone(), delays));
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

/// Wait out a partition: `true` once the link is up, `false` once it is
/// cut (or gone).
async fn passable(state: &mut watch::Receiver<Link>) -> bool {
    loop {
        match *state.borrow_and_update() {
            Link::Up => return true,
            Link::Cut => return false,
            Link::Partitioned => {}
        }
        if state.changed().await.is_err() {
            return false;
        }
    }
}

async fn relay(
    mut client: TcpStream,
    upstream: SocketAddr,
    state: watch::Receiver<Link>,
    delays: Option<(Delays, Delays)>,
) {
    // Accepted during a partition: held unanswered, the upstream not
    // dialled, until the link heals or is cut.
    if !passable(&mut state.clone()).await {
        reset(client);
        return;
    }
    let mut server = match TcpStream::connect(upstream).await {
        Ok(server) => server,
        Err(error) => {
            tracing::debug!(%error, "db link could not reach the upstream");
            reset(client);
            return;
        }
    };
    let lossy = delays.is_some();
    let (up, down) = match delays {
        Some((up, down)) => (Some(up), Some(down)),
        None => (None, None),
    };
    let was_cut = {
        let (client_read, client_write) = client.split();
        let (server_read, server_write) = server.split();
        let forward = pump(client_read, server_write, up, state.clone());
        let backward = pump(server_read, client_write, down, state.clone());
        tokio::select! {
            () = async { tokio::join!(forward, backward); } => false,
            () = cut(state) => true,
        }
    };
    // A cut resets both sides on the plain relay; a lossy link is for
    // slowness, so its sockets just close.
    if was_cut && !lossy {
        reset(client);
        reset(server);
    }
}

/// Relay `from` to `to`, each chunk after its delay (on a lossy link) and
/// only while the link is up: a partition holds the chunk, and the close,
/// until it heals; a cut ends the pump.
async fn pump<R, W>(
    mut from: R,
    mut to: W,
    mut delays: Option<Delays>,
    mut state: watch::Receiver<Link>,
) where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut buffer = vec![0u8; 16 * 1024];
    loop {
        let read = match from.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        if let Some(delays) = delays.as_mut() {
            tokio::time::sleep(delays.next()).await;
        }
        if !passable(&mut state).await {
            return;
        }
        if to.write_all(&buffer[..read]).await.is_err() {
            break;
        }
    }
    if passable(&mut state).await {
        let _ = to.shutdown().await;
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

    /// A partition is silent: the client's write succeeds, nothing comes
    /// back, and the connection neither resets nor closes (a read just
    /// waits). Healed, the held bytes cross and the echo arrives.
    #[tokio::test]
    async fn a_partition_stalls_silently_and_heals() {
        let server = echo().await;
        let link = DbLink::start("127.0.0.1", server.port())
            .await
            .expect("link");
        let mut stream = TcpStream::connect(link.addr()).await.expect("connect");
        assert_eq!(round_trip(&mut stream).await.expect("relayed"), b"ping");

        link.partition();
        assert!(link.is_partitioned());
        stream
            .write_all(b"held")
            .await
            .expect("loopback accepts the write");
        let mut back = [0u8; 4];
        let stalled =
            tokio::time::timeout(Duration::from_millis(500), stream.read_exact(&mut back)).await;
        assert!(
            stalled.is_err(),
            "a partition neither answers, resets nor closes: {stalled:?}"
        );
        // A connection opened during the partition is held unanswered too.
        let mut late = TcpStream::connect(link.addr()).await.expect("accepted");
        late.write_all(b"late").await.expect("written");
        let mut late_back = [0u8; 4];
        assert!(
            tokio::time::timeout(Duration::from_millis(300), late.read_exact(&mut late_back))
                .await
                .is_err(),
            "held unanswered"
        );

        link.restore();
        tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut back))
            .await
            .expect("the held bytes cross once healed")
            .expect("read");
        assert_eq!(&back, b"held");
        tokio::time::timeout(Duration::from_secs(5), late.read_exact(&mut late_back))
            .await
            .expect("the late connection relays once healed")
            .expect("read");
        assert_eq!(&late_back, b"late");
    }

    /// A partition then a cut: the stalled connection is reset.
    #[tokio::test]
    async fn a_cut_ends_a_partition_with_a_reset() {
        let server = echo().await;
        let link = DbLink::start("127.0.0.1", server.port())
            .await
            .expect("link");
        let mut stream = TcpStream::connect(link.addr()).await.expect("connect");
        assert_eq!(round_trip(&mut stream).await.expect("relayed"), b"ping");
        link.partition();
        stream.write_all(b"lost").await.expect("written");
        link.cut();
        let mut back = [0u8; 4];
        let ended = tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut back))
            .await
            .expect("the cut ends the stall");
        assert!(ended.is_err(), "reset, not answered: {ended:?}");
    }

    #[test]
    fn a_lossy_link_draws_the_same_delays_from_the_same_seed() {
        let lossy = Lossy::tailscale_bad_day(7);
        let draw = |stream| {
            let mut delays = Delays::new(lossy, stream);
            (0..2000).map(|_| delays.next()).collect::<Vec<_>>()
        };
        assert_eq!(draw(3), draw(3));
        assert_ne!(draw(3), draw(4));
        let stalls = draw(3)
            .iter()
            .filter(|delay| **delay >= Duration::from_millis(200))
            .count();
        assert!((200..400).contains(&stalls), "about 15% stall: {stalls}");
        assert!(
            draw(3)
                .iter()
                .all(|delay| *delay <= Duration::from_millis(400))
        );
    }

    #[tokio::test]
    async fn a_lossy_link_relays_every_byte_in_order() {
        let server = echo().await;
        let lossy = Lossy {
            seed: 11,
            loss_per_mille: 500,
            rto: Duration::from_millis(20),
            jitter: Duration::from_millis(2),
        };
        let link = DbLink::start_lossy("127.0.0.1", server.port(), lossy)
            .await
            .expect("link");
        let mut stream = TcpStream::connect(link.addr()).await.expect("connect");
        for round in 0..5u8 {
            let sent = [round; 4];
            stream.write_all(&sent).await.expect("written");
            let mut back = [0u8; 4];
            tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut back))
                .await
                .expect("echoed within the bound")
                .expect("read");
            assert_eq!(back, sent);
        }
    }
}
