//! Telling a gateway that cannot be reached, or that refused the token,
//! apart from a failure the gateway itself reported.
//!
//! `crosstalk-client` folds what the spec's errors have no variant for
//! into `QueryError::Store { reason }`, the reason being its
//! `ClientError`'s display, and exposes no typed accessor through the
//! traits. [`classify`] reads that display, in this one place:
//!
//! | `ClientError` | Reason starts with | [`GatewayFailure`] |
//! | --- | --- | --- |
//! | `Unauthenticated` (`401`) | `no caller: ` | `TokenRefused` |
//! | `Transport(Send)` (connect, send, response head) | `sending the request: ` | `Unreachable` |
//! | `Transport(Body)` (the connection cut) | `reading the body: ` | `Unreachable` |
//! | `Transport(Timeout)` | `no response within ` | `Unreachable` |
//! | anything else, the gateway's own `Store` included | | none |
//!
//! The router tests pin it against the client's real errors (a refused
//! token, a stopped server), so a change to the client's wording fails
//! them rather than the page.

use crosstalk_client::BaseUrl;
use crosstalk_spec::interfaces::l8_surface::QueryError;

/// Why the http backend could not get an answer from the gateway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayFailure {
    /// No connection, the connection cut, or no answer in time.
    Unreachable,
    /// The gateway answered `401`: it does not accept the token.
    TokenRefused,
}

/// The gateway failure a call over HTTP failed with, if it was one.
pub fn classify(error: &QueryError) -> Option<GatewayFailure> {
    let QueryError::Store { reason } = error else {
        return None;
    };
    if reason.starts_with("no caller: ") {
        Some(GatewayFailure::TokenRefused)
    } else if [
        "sending the request: ",
        "reading the body: ",
        "no response within ",
    ]
    .iter()
    .any(|prefix| reason.starts_with(prefix))
    {
        Some(GatewayFailure::Unreachable)
    } else {
        None
    }
}

/// `url` as an operator may see it: `http://host[:port][/prefix]`, without
/// any `user:password@` the configured URL carried.
pub fn public_url(url: &BaseUrl) -> String {
    let text = url.to_string();
    let Some(rest) = text.strip_prefix("http://") else {
        return text;
    };
    let (authority, path) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    format!("http://{host}{path}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(reason: &str) -> QueryError {
        QueryError::Store {
            reason: reason.to_owned(),
        }
    }

    #[test]
    fn a_401_is_a_refused_token() {
        assert_eq!(
            classify(&store("no caller: AuthError { reason: InvalidCredential }")),
            Some(GatewayFailure::TokenRefused)
        );
    }

    #[test]
    fn transport_failures_are_unreachable() {
        for reason in [
            "sending the request: client error (Connect)",
            "reading the body: connection reset",
            "no response within 30000 ms",
        ] {
            assert_eq!(
                classify(&store(reason)),
                Some(GatewayFailure::Unreachable),
                "{reason}"
            );
        }
    }

    #[test]
    fn the_gateways_own_failures_and_other_errors_are_not_gateway_failures() {
        assert_eq!(classify(&store("the edge store is down")), None);
        assert_eq!(classify(&QueryError::NotFound), None);
        assert_eq!(
            classify(&QueryError::Forbidden {
                missing: crosstalk_spec::interfaces::l8_surface::Permission::Audit
            }),
            None
        );
    }

    #[test]
    fn the_public_url_drops_credentials() {
        let url = BaseUrl::parse("http://ops:secret@crosstalk:8081/api").expect("url");
        assert_eq!(public_url(&url), "http://crosstalk:8081/api");
        let plain = BaseUrl::parse("http://crosstalk:8081").expect("url");
        assert_eq!(public_url(&plain), "http://crosstalk:8081");
    }
}
