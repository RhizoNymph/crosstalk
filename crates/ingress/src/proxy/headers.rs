//! Hop-by-hop headers, and the spec's head views of hyper's messages.
//!
//! RFC 9110 section 7.6.1: a proxy removes `Connection`, every field
//! `Connection` names, and `Proxy-Connection`, `Keep-Alive`, `TE`,
//! `Transfer-Encoding` and `Upgrade`, which describe one connection, not the
//! message. A request also loses `Host`, which names the proxy; the client
//! sets the upstream's. Every other field is forwarded as received, in
//! order, including `content-length`, credentials and `anthropic-*`.

use crosstalk_spec::interfaces::l0_ingress::{RequestHead, ResponseHead};
use hyper::HeaderMap;
use hyper::header::{CONNECTION, HOST, HeaderName};

/// Connection-scoped fields, besides those `Connection` names.
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "proxy-connection",
    "keep-alive",
    "te",
    "transfer-encoding",
    "upgrade",
];

/// Remove hop-by-hop fields.
pub fn strip_hop_by_hop(headers: &mut HeaderMap) {
    let named: Vec<HeaderName> = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect();
    for name in named {
        headers.remove(name);
    }
    for name in HOP_BY_HOP {
        headers.remove(*name);
    }
}

/// Remove hop-by-hop fields and `Host` from a request.
pub fn strip_request(headers: &mut HeaderMap) {
    strip_hop_by_hop(headers);
    headers.remove(HOST);
}

/// Headers as the spec's name and value strings, in order. A value that is
/// not UTF-8 is read lossily: these views are for classification and
/// identity, never for forwarding.
pub fn pairs(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect()
}

pub fn request_head(
    method: &hyper::Method,
    path: &str,
    query: Option<&str>,
    headers: &HeaderMap,
) -> RequestHead {
    RequestHead {
        method: method.as_str().to_owned(),
        path: path.to_owned(),
        query: query.map(str::to_owned),
        headers: pairs(headers),
    }
}

pub fn response_head(status: hyper::StatusCode, headers: &HeaderMap) -> ResponseHead {
    ResponseHead {
        status: status.as_u16(),
        headers: pairs(headers),
    }
}
