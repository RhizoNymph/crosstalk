//! Upstream connections.
//!
//! [`https`] is the production connector: TCP with `TCP_NODELAY`, TLS
//! through rustls (ring) with the Mozilla roots from `webpki-roots`, and
//! plain HTTP for `http://` upstreams (self-hosted servers, test fakes).
//! HTTP/1.1 only. The proxy is generic over the connector, so a simulation
//! can connect through in-memory pipes instead.

use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::connect::HttpConnector;

/// The production connector type.
pub type Https = HttpsConnector<HttpConnector>;

/// TLS for `https://` upstreams, plain TCP for `http://` ones.
pub fn https() -> Https {
    let mut http = HttpConnector::new();
    http.enforce_http(false);
    http.set_nodelay(true);
    HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .wrap_connector(http)
}
