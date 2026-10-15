//! Client input: what `decode_request` accepts, how its refusals reach the
//! client, and that no request carries a value the server stamps.

use serde_json::json;

use super::harness::authority_key;
use super::{ULID_B, ULID_C, id};
use crate::ids::{AgentId, ChannelId};
use crate::interfaces::l8_surface::{
    ActionError, AlertFilter, AlertStateKind, InputError, QueryError,
};
use crate::paging::{AlertList, PageRequest};
use crate::support::TimeWindow;
use crate::wire::{DecodeError, DecodeErrorKind, decode_request};

#[test]
fn decode_request_decodes_request_types() {
    let filter = decode_request::<AlertFilter>(
        format!(r#"{{"states": ["suppressed"], "channel": "{ULID_B}"}}"#).as_bytes(),
    );
    assert_eq!(
        filter,
        Ok(AlertFilter {
            states: vec![AlertStateKind::Suppressed],
            channel: Some(id(ChannelId::from_ulid_text, ULID_B)),
        })
    );
    let agent = decode_request::<AgentId>(format!("\"{ULID_C}\"").as_bytes());
    assert_eq!(agent, Ok(id(AgentId::from_ulid_text, ULID_C)));
    let window = decode_request::<Option<TimeWindow>>(b"null");
    assert_eq!(window, Ok(None));
}

#[test]
fn decode_request_classifies_what_it_refuses() {
    let kind = |json: &[u8]| decode_request::<PageRequest<AlertList>>(json).map_err(|e| e.kind);
    assert_eq!(
        kind(br#"{"size": 50, "after": nul}"#),
        Err(DecodeErrorKind::Syntax)
    );
    assert_eq!(kind(br#"{"size": 50, "after""#), Err(DecodeErrorKind::Eof));
    assert_eq!(
        kind(br#"{"size": 0, "after": null}"#),
        Err(DecodeErrorKind::Data)
    );
    assert_eq!(
        kind(br#"{"size": 50, "after": null, "offset": 0}"#),
        Err(DecodeErrorKind::Data)
    );
    assert_eq!(
        kind(br#"{"size": 50, "after": null} {}"#),
        Err(DecodeErrorKind::Syntax)
    );
}

/// A request that does not decode reaches the client as
/// `InvalidInput(MalformedRequest)`, from a query route and an action
/// route alike, carrying the decoder's kind and reason.
#[test]
fn undecodable_requests_are_malformed_request_input_errors() {
    let error = decode_request::<AlertFilter>(br#"{"stats": []}"#)
        .err()
        .unwrap_or_else(|| panic!("an unknown field must be refused"));
    assert_eq!(error.kind, DecodeErrorKind::Data);
    assert!(
        error.reason.contains("unknown field `stats`"),
        "{}",
        error.reason
    );
    let expected = InputError::MalformedRequest {
        kind: DecodeErrorKind::Data,
        reason: error.reason.clone(),
    };
    assert_eq!(
        QueryError::from(error.clone()),
        QueryError::InvalidInput(expected.clone())
    );
    assert_eq!(
        ActionError::from(error),
        ActionError::InvalidInput(expected)
    );
    let syntax = DecodeError {
        kind: DecodeErrorKind::Syntax,
        reason: "expected value at line 1 column 1".into(),
    };
    assert_eq!(
        QueryError::from(syntax),
        QueryError::InvalidInput(InputError::MalformedRequest {
            kind: DecodeErrorKind::Syntax,
            reason: "expected value at line 1 column 1".into(),
        })
    );
}

/// Strict decoding is what keeps an author out of a request: a client that
/// adds one gets a decode error, not a value with its field ignored.
#[test]
fn a_smuggled_author_is_a_decode_error() {
    for field in ["by", "at", "caller", "permissions"] {
        let json = format!(r#"{{"states": [], "channel": null, "{field}": "{ULID_C}"}}"#);
        let error = decode_request::<AlertFilter>(json.as_bytes())
            .err()
            .unwrap_or_else(|| panic!("{json} must be refused"));
        assert_eq!(error.kind, DecodeErrorKind::Data, "{json}");
        assert!(error.reason.contains(&format!("unknown field `{field}`")));
    }
}

/// The request goldens' authority check finds stamped keys at any depth.
#[test]
fn authority_keys_are_found_at_any_depth() {
    assert_eq!(authority_key(&json!({"states": [], "channel": null})), None);
    assert_eq!(authority_key(&json!({"rule": {"by": "x"}})), Some("by"));
    assert_eq!(authority_key(&json!([{"ok": 1}, {"at": "t"}])), Some("at"));
    assert_eq!(
        authority_key(&json!({"type": "x", "data": {"requested_by": "y"}})),
        Some("requested_by")
    );
}
