//! The credential: a bearer token in `Authorization` exactly as the
//! binding reads one, never anything else, never in `Debug`.

use crosstalk_spec::interfaces::l8_surface::QueryApi;
use crosstalk_spec::interfaces::l8_surface::http::auth::{Credential, CredentialHeaders, Field};

use super::caller;
use super::stub::{Reply, Stub};
use crate::{BearerToken, InvalidToken};

const TOKEN: &str = "abc.DEF-123~_+/xyz==";

fn token() -> BearerToken {
    BearerToken::new(TOKEN).unwrap_or_else(|error| panic!("{error}"))
}

/// With a token, every request carries `Authorization: Bearer <token>`,
/// which the binding's credential reader takes as that bearer token; the
/// session cookie is never sent.
#[tokio::test]
async fn a_token_travels_as_the_bearer_credential() {
    let mut stub = Stub::always(Reply::json(404, r#"{"type":"not_found"}"#)).await;
    let client = stub.client().with_token(token());
    let _ = client.watermark(&caller()).await;
    let request = stub.only_request();
    let authorization = request
        .header("authorization")
        .unwrap_or_else(|| panic!("an Authorization header"));
    assert_eq!(authorization, format!("Bearer {TOKEN}"));
    assert_eq!(request.headers.get_all("authorization").iter().count(), 1);
    assert_eq!(request.header("cookie"), None);
    let headers = CredentialHeaders {
        authorization: Field::One(authorization),
        cookie: None,
    };
    match headers.credential() {
        Ok(Some(Credential::Bearer(secret))) => assert_eq!(secret.expose(), TOKEN),
        other => panic!("{other:?}"),
    }
}

/// Without a token no credential travels; `with_token` and
/// `without_token` change only the new client.
#[tokio::test]
async fn tokens_belong_to_one_client() {
    let mut stub = Stub::always(Reply::json(404, r#"{"type":"not_found"}"#)).await;
    let anonymous = stub.client();
    let signed_in = anonymous.with_token(token());
    let signed_out = signed_in.without_token();
    let _ = anonymous.watermark(&caller()).await;
    let _ = signed_in.watermark(&caller()).await;
    let _ = signed_out.watermark(&caller()).await;
    let sent: Vec<Option<String>> = stub
        .requests()
        .iter()
        .map(|request| request.header("authorization").map(str::to_owned))
        .collect();
    assert_eq!(sent, vec![None, Some(format!("Bearer {TOKEN}")), None]);
}

/// A token is accepted exactly when the binding would read it as a bearer
/// token: `b64token` text, `=` only as trailing padding.
#[test]
fn tokens_are_b64token_text() {
    for good in ["a", "abc==", "A-z.0_9~+/", TOKEN] {
        assert!(BearerToken::new(good).is_ok(), "{good}");
    }
    for bad in [
        "",
        "=",
        "a=b",
        "a b",
        " a",
        "a\n",
        "tok\u{e9}n",
        "a,b",
        "\"a\"",
    ] {
        assert_eq!(BearerToken::new(bad), Err(InvalidToken), "{bad:?}");
    }
}

/// Neither the token nor the client prints the secret, and the header
/// value is marked sensitive (`surface.api.no-raw-credentials`).
#[tokio::test]
async fn the_secret_is_never_printed() {
    let token = token();
    assert_eq!(format!("{token:?}"), "BearerToken(<redacted>)");
    assert!(token.header().is_sensitive());
    let stub = Stub::always(Reply::json(200, "null")).await;
    let client = stub.client().with_token(token);
    assert!(!format!("{client:?}").contains("abc.DEF"), "{client:?}");
}

/// The User-Agent names the crate and its version, nothing about the host
/// or the operator.
#[tokio::test]
async fn the_user_agent_is_neutral() {
    let mut stub = Stub::always(Reply::json(404, r#"{"type":"not_found"}"#)).await;
    let _ = stub.client().with_token(token()).watermark(&caller()).await;
    assert_eq!(
        stub.only_request().header("user-agent"),
        Some(concat!("crosstalk-client/", env!("CARGO_PKG_VERSION")))
    );
}
