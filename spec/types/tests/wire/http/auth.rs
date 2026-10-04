//! Authentication: the credential comes from `Authorization` or the session
//! cookie only, verification becomes the directory's identity, and a
//! request with no caller is a 401 with an `AuthError` body.

use super::super::harness::{assert_golden, assert_rejected};
use super::AREA;
use crate::ids::OperatorId;
use crate::interfaces::l8_surface::http::auth::{
    Field, MalformedCredential, SESSION_COOKIE, authenticate,
};
use crate::interfaces::l8_surface::http::{
    AuthError, AuthFailure, Credential, CredentialHeaders, ErrorStatus, Status, Verification,
};
use crate::interfaces::l8_surface::operators::{
    AccessConfig, OperatorConfig, OperatorDirectory, OperatorName, RequestIdentity, TrustedOperator,
};
use crate::interfaces::l8_surface::{Permission, PermissionSet, QueryError};

fn operator(n: u128) -> OperatorId {
    OperatorId::from_ulid(n)
}

fn name(text: &str) -> OperatorName {
    OperatorName::new(text).unwrap_or_else(|error| panic!("{error:?}"))
}

/// Operator 1 holds View; operator 2 was configured once and is now
/// former.
fn authenticated() -> OperatorDirectory {
    let config = |ids: &[u128]| {
        AccessConfig::Authenticated(
            ids.iter()
                .map(|n| OperatorConfig {
                    id: operator(*n),
                    name: name("op"),
                    permissions: PermissionSet::of([Permission::View]),
                })
                .collect(),
        )
    };
    let (before, _) =
        OperatorDirectory::load(None, &config(&[1, 2])).unwrap_or_else(|e| panic!("{e:?}"));
    OperatorDirectory::load(Some(&before), &config(&[1]))
        .unwrap_or_else(|e| panic!("{e:?}"))
        .0
}

fn trusted() -> OperatorDirectory {
    let config = AccessConfig::Trusted(TrustedOperator {
        id: operator(9),
        name: name("me"),
    });
    OperatorDirectory::load(None, &config)
        .unwrap_or_else(|e| panic!("{e:?}"))
        .0
}

fn headers<'a>(authorization: Field<'a>, cookie: Option<&'a str>) -> CredentialHeaders<'a> {
    CredentialHeaders {
        authorization,
        cookie,
    }
}

fn bearer(token: &str) -> String {
    match headers(Field::One(&format!("Bearer {token}")), None).credential() {
        Ok(Some(Credential::Bearer(secret))) => secret.expose().to_owned(),
        other => panic!("{token}: {other:?}"),
    }
}

#[test]
fn the_bearer_token_is_the_credential() {
    assert_eq!(bearer("abc.DEF-123_~+/=="), "abc.DEF-123_~+/==");
    let lower = headers(Field::One("bearer tok"), None).credential();
    assert!(
        matches!(lower, Ok(Some(Credential::Bearer(_)))),
        "{lower:?}"
    );
}

#[test]
fn authorization_wins_over_the_cookie() {
    let cookie = format!("theme=dark; {SESSION_COOKIE}=sess1");
    let both = headers(Field::One("Bearer tok"), Some(&cookie)).credential();
    assert!(matches!(both, Ok(Some(Credential::Bearer(_)))), "{both:?}");
    let only_cookie = headers(Field::Absent, Some(&cookie)).credential();
    match only_cookie {
        Ok(Some(Credential::Session(secret))) => assert_eq!(secret.expose(), "sess1"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn no_credential_is_none() {
    assert_eq!(headers(Field::Absent, None).credential(), Ok(None));
    assert_eq!(
        headers(Field::Absent, Some("theme=dark; crosstalk-session=x")).credential(),
        Ok(None),
        "a cookie without the __Host- prefix is not the session cookie"
    );
}

#[test]
fn malformed_credentials_are_refused() {
    let cases = [
        (
            headers(Field::Repeated, None),
            MalformedCredential::RepeatedAuthorization,
        ),
        (
            headers(Field::One("Basic dXNlcjpwdw=="), None),
            MalformedCredential::NotBearer,
        ),
        (
            headers(Field::One("Bearer"), None),
            MalformedCredential::NotBearer,
        ),
        (
            headers(Field::One("Bearer "), None),
            MalformedCredential::NotBearer,
        ),
        (
            headers(Field::One("Bearer a b"), None),
            MalformedCredential::InvalidToken,
        ),
        (
            headers(Field::One("Bearer \"tok\""), None),
            MalformedCredential::InvalidToken,
        ),
        (
            headers(Field::One("Bearer ==="), None),
            MalformedCredential::InvalidToken,
        ),
    ];
    for (headers, expected) in cases {
        assert_eq!(headers.credential(), Err(expected), "{headers:?}");
    }
    let twice = format!("{SESSION_COOKIE}=a; {SESSION_COOKIE}=b");
    assert_eq!(
        headers(Field::Absent, Some(&twice)).credential(),
        Err(MalformedCredential::RepeatedCookie)
    );
    let quoted = format!("{SESSION_COOKIE}=\"a\"");
    assert_eq!(
        headers(Field::Absent, Some(&quoted)).credential(),
        Err(MalformedCredential::InvalidToken)
    );
}

/// A credential's `Debug` never shows the secret.
#[test]
fn a_credential_never_prints_its_secret() {
    let credential = headers(Field::One("Bearer s3cr3t-token"), None).credential();
    let printed = format!("{credential:?}");
    assert!(!printed.contains("s3cr3t"), "{printed}");
    assert!(printed.contains("redacted"), "{printed}");
}

#[test]
fn verification_becomes_the_directory_identity() {
    let verify_as = |op: Option<OperatorId>| move |_: &Credential| op;
    let none = headers(Field::Absent, None);
    let token = headers(Field::One("Bearer tok"), None);
    let malformed = headers(Field::One("Basic x"), None);
    assert_eq!(
        Verification::of(&none, verify_as(Some(operator(1)))),
        Verification::NoCredential
    );
    assert_eq!(
        Verification::of(&malformed, verify_as(Some(operator(1)))),
        Verification::Rejected
    );
    assert_eq!(
        Verification::of(&token, verify_as(None)),
        Verification::Rejected
    );
    assert_eq!(
        Verification::of(&token, verify_as(Some(operator(1)))),
        Verification::Verified(operator(1))
    );
    assert_eq!(
        Verification::NoCredential.identity(),
        RequestIdentity::Anonymous
    );
    assert_eq!(
        Verification::Rejected.identity(),
        RequestIdentity::Anonymous
    );
    assert_eq!(
        Verification::Verified(operator(1)).identity(),
        RequestIdentity::Verified(operator(1))
    );
}

/// The caller is the directory's caller of the verified operator, with its
/// configured permissions; every other verification is a 401 saying why.
#[test]
fn authenticated_mode_answers_401_without_a_caller() {
    let directory = authenticated();
    let caller = authenticate(&directory, Verification::Verified(operator(1)))
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(caller.operator(), operator(1));
    assert_eq!(caller.permissions(), PermissionSet::of([Permission::View]));
    let cases = [
        (Verification::NoCredential, AuthFailure::NoCredential),
        (Verification::Rejected, AuthFailure::InvalidCredential),
        (
            Verification::Verified(operator(7)),
            AuthFailure::UnknownOperator,
        ),
        (
            Verification::Verified(operator(2)),
            AuthFailure::FormerOperator,
        ),
    ];
    for (verification, reason) in cases {
        let error = authenticate(&directory, verification).err();
        assert_eq!(error, Some(AuthError { reason }), "{verification:?}");
    }
}

/// Trusted mode needs no credential: every verification is the trusted
/// operator with every permission.
#[test]
fn trusted_mode_ignores_the_credential() {
    let directory = trusted();
    for verification in [
        Verification::NoCredential,
        Verification::Rejected,
        Verification::Verified(operator(1)),
    ] {
        let caller = authenticate(&directory, verification)
            .unwrap_or_else(|error| panic!("{verification:?}: {error:?}"));
        assert_eq!(caller.operator(), operator(9));
        assert_eq!(caller.permissions(), PermissionSet::ALL);
    }
}

/// 401 is no caller; 403 is a caller without the permission.
#[test]
fn unauthorized_is_not_forbidden() {
    let error = AuthError {
        reason: AuthFailure::InvalidCredential,
    };
    assert_eq!(error.status(), Status::Unauthorized);
    assert_eq!(error.status().code(), 401);
    let forbidden = QueryError::Forbidden {
        missing: Permission::View,
    };
    assert_eq!(forbidden.status().code(), 403);
}

#[test]
fn a_401_challenges_for_a_bearer_token() {
    let challenge = |reason| AuthError { reason }.www_authenticate();
    assert_eq!(
        challenge(AuthFailure::NoCredential),
        r#"Bearer realm="crosstalk""#
    );
    for reason in [
        AuthFailure::InvalidCredential,
        AuthFailure::UnknownOperator,
        AuthFailure::FormerOperator,
    ] {
        assert_eq!(
            challenge(reason),
            r#"Bearer realm="crosstalk", error="invalid_token""#
        );
    }
}

fn every_auth_error() -> Vec<AuthError> {
    fn declared(reason: AuthFailure) -> AuthFailure {
        match reason {
            AuthFailure::NoCredential
            | AuthFailure::InvalidCredential
            | AuthFailure::UnknownOperator
            | AuthFailure::FormerOperator => reason,
        }
    }
    [
        AuthFailure::NoCredential,
        AuthFailure::InvalidCredential,
        AuthFailure::UnknownOperator,
        AuthFailure::FormerOperator,
    ]
    .into_iter()
    .map(|reason| AuthError {
        reason: declared(reason),
    })
    .collect()
}

#[test]
fn auth_errors_golden_with_every_reason() {
    assert_golden(AREA, "auth_errors", &every_auth_error());
}

#[test]
fn auth_errors_decode_strictly() {
    assert_rejected::<AuthError>(r#"{"reason": "expired"}"#, "unknown variant `expired`");
    assert_rejected::<AuthError>(
        r#"{"reason": "no_credential", "operator": "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA"}"#,
        "unknown field `operator`",
    );
    assert_rejected::<AuthError>(r#"{}"#, "missing field `reason`");
}
