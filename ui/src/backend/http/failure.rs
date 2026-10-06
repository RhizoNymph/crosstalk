//! Telling a gateway that cannot be reached, or that refused the token,
//! apart from a failure the gateway itself reported.
//!
//! `crosstalk-client` returns a call that never reached a surface that
//! answered it as the client-only `Unavailable { kind, reason }`
//! (`QueryError` and `ActionError` alike). [`classify`] maps its `kind`:
//!
//! | Error | [`GatewayFailure`] |
//! | --- | --- |
//! | `Unavailable { kind: Unauthenticated }` (a `401`) | `TokenRefused` |
//! | `Unavailable { kind: Transport \| Body \| Timeout }` | `Unreachable` |
//! | anything else, the gateway's own `Store` included | none: the generic 500 |
//!
//! The router tests pin it end to end against the client's real errors (a
//! refused token, a stopped server).

use crosstalk_client::BaseUrl;
use crosstalk_spec::interfaces::l8_surface::{ActionError, QueryError, UnavailableKind};

/// Why the http backend could not get an answer from the gateway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayFailure {
    /// No connection, the connection cut, or no answer in time.
    Unreachable,
    /// The gateway answered `401`: it does not accept the token.
    TokenRefused,
}

impl From<UnavailableKind> for GatewayFailure {
    fn from(kind: UnavailableKind) -> Self {
        match kind {
            UnavailableKind::Unauthenticated => Self::TokenRefused,
            UnavailableKind::Transport | UnavailableKind::Body | UnavailableKind::Timeout => {
                Self::Unreachable
            }
        }
    }
}

/// An error a call to the gateway can fail with.
pub trait CallFailure {
    /// The `kind` of an `Unavailable`, if the error is one.
    fn unavailable(&self) -> Option<UnavailableKind>;
}

impl CallFailure for QueryError {
    fn unavailable(&self) -> Option<UnavailableKind> {
        match self {
            Self::Unavailable { kind, .. } => Some(*kind),
            _ => None,
        }
    }
}

impl CallFailure for ActionError {
    fn unavailable(&self) -> Option<UnavailableKind> {
        match self {
            Self::Unavailable { kind, .. } => Some(*kind),
            _ => None,
        }
    }
}

/// The gateway failure a call over HTTP failed with, if it was one.
pub fn classify(error: &impl CallFailure) -> Option<GatewayFailure> {
    error.unavailable().map(GatewayFailure::from)
}

/// `url` as an operator may see it: without the username and password a
/// configured URL may carry, for any scheme. A URL without them is
/// returned byte for byte. One that cannot be parsed is not shown at all,
/// since its credentials could not be found.
pub fn public_url(url: &BaseUrl) -> String {
    redact(&url.to_string())
}

/// What a URL that cannot be parsed shows instead.
const UNPARSEABLE: &str = "(the gateway URL could not be shown)";

/// [`public_url`] over text.
fn redact(text: &str) -> String {
    let Ok(mut parsed) = url::Url::parse(text) else {
        return UNPARSEABLE.to_owned();
    };
    if parsed.username().is_empty() && parsed.password().is_none() {
        return text.to_owned();
    }
    // Both setters fail only for a URL that cannot have credentials, which
    // then has none to remove.
    let cleared = parsed.set_username("").is_ok() && parsed.set_password(None).is_ok();
    if !cleared {
        return UNPARSEABLE.to_owned();
    }
    let mut shown = parsed.to_string();
    // `Url` writes an empty path as `/`; keep the configured form.
    if parsed.path() == "/"
        && !text.ends_with('/')
        && parsed.query().is_none()
        && parsed.fragment().is_none()
    {
        shown.pop();
    }
    shown
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(kind: UnavailableKind) -> QueryError {
        QueryError::Unavailable {
            kind,
            reason: "the client's reason".to_owned(),
        }
    }

    fn action(kind: UnavailableKind) -> ActionError {
        ActionError::Unavailable {
            kind,
            reason: "the client's reason".to_owned(),
        }
    }

    #[test]
    fn unauthenticated_is_a_refused_token() {
        let kind = UnavailableKind::Unauthenticated;
        assert_eq!(classify(&query(kind)), Some(GatewayFailure::TokenRefused));
        assert_eq!(classify(&action(kind)), Some(GatewayFailure::TokenRefused));
    }

    #[test]
    fn transport_body_and_timeout_are_unreachable() {
        for kind in [
            UnavailableKind::Transport,
            UnavailableKind::Body,
            UnavailableKind::Timeout,
        ] {
            assert_eq!(
                classify(&query(kind)),
                Some(GatewayFailure::Unreachable),
                "{kind:?}"
            );
            assert_eq!(
                classify(&action(kind)),
                Some(GatewayFailure::Unreachable),
                "{kind:?}"
            );
        }
    }

    #[test]
    fn every_kind_is_a_gateway_failure() {
        for kind in UnavailableKind::ALL {
            assert!(classify(&query(kind)).is_some(), "{kind:?}");
        }
    }

    #[test]
    fn store_and_other_errors_are_not_gateway_failures() {
        // A store failure, whatever its text, is the gateway's own: the
        // generic 500, not the gateway page.
        for reason in ["the edge store is down", "no caller: looks like a 401"] {
            let store = QueryError::Store {
                reason: reason.to_owned(),
            };
            assert_eq!(classify(&store), None, "{reason}");
            let action_store = ActionError::Store {
                reason: reason.to_owned(),
            };
            assert_eq!(classify(&action_store), None, "{reason}");
        }
        assert_eq!(classify(&QueryError::NotFound), None);
        assert_eq!(
            classify(&QueryError::Forbidden {
                missing: crosstalk_spec::interfaces::l8_surface::Permission::Audit
            }),
            None
        );
        assert_eq!(classify(&ActionError::NotFound), None);
    }

    #[test]
    fn the_public_url_drops_credentials() {
        let url = BaseUrl::parse("http://ops:secret@crosstalk:8081/api").expect("url");
        assert_eq!(public_url(&url), "http://crosstalk:8081/api");
        let plain = BaseUrl::parse("http://crosstalk:8081").expect("url");
        assert_eq!(public_url(&plain), "http://crosstalk:8081");
    }

    #[test]
    fn credentials_are_dropped_for_any_scheme() {
        assert_eq!(
            redact("https://user:pass@gateway.example:8443/api"),
            "https://gateway.example:8443/api"
        );
        assert_eq!(
            redact("https://user:pass@gateway.example"),
            "https://gateway.example"
        );
    }

    #[test]
    fn a_user_without_a_password_is_dropped() {
        assert_eq!(
            redact("http://ops@crosstalk:8081/api"),
            "http://crosstalk:8081/api"
        );
    }

    #[test]
    fn an_ipv6_host_keeps_its_brackets_and_port() {
        assert_eq!(
            redact("http://ops:secret@[::1]:8081/api"),
            "http://[::1]:8081/api"
        );
        assert_eq!(redact("http://[::1]:8081"), "http://[::1]:8081");
    }

    #[test]
    fn a_url_without_credentials_is_byte_identical() {
        for text in [
            "http://crosstalk:8081",
            "http://crosstalk:8081/api",
            "http://CROSSTALK:8081/api/",
            "http://[::1]:8081/a%20b",
        ] {
            assert_eq!(redact(text), text);
        }
    }

    #[test]
    fn a_fragment_is_kept_whole() {
        assert_eq!(
            redact("https://u:p@gateway.example#frag"),
            "https://gateway.example/#frag"
        );
        assert_eq!(
            redact("https://u:p@gateway.example/api#frag"),
            "https://gateway.example/api#frag"
        );
        assert_eq!(
            redact("https://gateway.example#frag"),
            "https://gateway.example#frag"
        );
    }

    #[test]
    fn an_unparseable_url_is_not_shown() {
        assert_eq!(redact("http://user:pass@"), UNPARSEABLE);
    }
}
