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
