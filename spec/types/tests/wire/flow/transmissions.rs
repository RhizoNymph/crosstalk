//! Transmissions in every state, their routes, the co-access evidence and
//! the confirmed content.

use serde_json::{Value, json};

use super::super::harness::{assert_golden, assert_rejected};
use super::{
    AREA, ULID_A, ULID_C, classification, co_access, coder, confirmed, content_match,
    content_match_to, planner, read_at, transmission_id, wiki,
};
use crate::derived::flow::evidence::CoAccess;
use crate::derived::flow::transmission::{
    Confirmed, DelegationDirection, DirectCarrier, Route, Transmission, TransmissionState,
};
use crate::ids::AgentId;
use crate::observed::message::ToolName;
use crate::support::NonEmpty;
use crate::tests::wire::{id, ts};

/// One transmission state of every variant.
fn every_state() -> Vec<(&'static str, TransmissionState)> {
    fn declared(state: TransmissionState) -> TransmissionState {
        match state {
            TransmissionState::Detected
            | TransmissionState::AwaitingContent { .. }
            | TransmissionState::Suspected { .. }
            | TransmissionState::Confirmed(_)
            | TransmissionState::Classified { .. }
            | TransmissionState::Aggregated { .. }
            | TransmissionState::Discarded { .. } => state,
        }
    }
    [
        ("transmission_detected", TransmissionState::Detected),
        (
            "transmission_awaiting_content",
            TransmissionState::AwaitingContent {
                co_access: co_access(),
                window_closes_at: ts("2026-10-04T12:05:30.250000Z"),
            },
        ),
        (
            "transmission_suspected",
            TransmissionState::Suspected {
                co_access: NonEmpty::new(co_access()),
                since: ts("2026-10-04T12:05:30.250000Z"),
            },
        ),
        (
            "transmission_confirmed",
            TransmissionState::Confirmed(confirmed()),
        ),
        (
            "transmission_classified",
            TransmissionState::Classified {
                confirmed: confirmed(),
                classification: classification(),
            },
        ),
        (
            "transmission_aggregated",
            TransmissionState::Aggregated {
                confirmed: confirmed(),
                classification: classification(),
            },
        ),
        (
            "transmission_discarded",
            TransmissionState::Discarded {
                at: ts("2026-10-04T13:05:30.250000Z"),
                co_access: NonEmpty::new(co_access()),
            },
        ),
    ]
    .into_iter()
    .map(|(name, state)| (name, declared(state)))
    .collect()
}

fn transmission(state: TransmissionState) -> Transmission {
    Transmission {
        id: transmission_id(),
        to: coder(),
        route: Route::Channel(wiki()),
        opened_at: read_at(),
        state,
    }
}

#[test]
fn transmissions_golden_in_every_state() {
    for (name, state) in every_state() {
        assert_golden(AREA, name, &transmission(state));
    }
}

#[test]
fn routes_golden_with_every_variant() {
    fn route(route: Route) -> Route {
        match route {
            Route::Channel(_) | Route::Delegation(_) | Route::Direct(_) | Route::Unobserved => {
                route
            }
        }
    }
    fn carrier(carrier: DirectCarrier) -> DirectCarrier {
        match carrier {
            DirectCarrier::UserTurn
            | DirectCarrier::SystemPrompt
            | DirectCarrier::ToolResult(_) => carrier,
        }
    }
    fn direction(direction: DelegationDirection) -> DelegationDirection {
        match direction {
            DelegationDirection::ParentToChild | DelegationDirection::ChildToParent => direction,
        }
    }
    let routes = [
        Route::Channel(wiki()),
        Route::Delegation(DelegationDirection::ParentToChild),
        Route::Direct(DirectCarrier::ToolResult(ToolName("web_fetch".into()))),
        Route::Unobserved,
    ]
    .map(route);
    assert_golden(AREA, "routes", &routes.to_vec());
    let carriers = [
        DirectCarrier::UserTurn,
        DirectCarrier::SystemPrompt,
        DirectCarrier::ToolResult(ToolName("web_fetch".into())),
    ]
    .map(carrier);
    assert_golden(AREA, "direct_carriers", &carriers.to_vec());
    let directions = [
        DelegationDirection::ParentToChild,
        DelegationDirection::ChildToParent,
    ]
    .map(direction);
    assert_golden(AREA, "delegation_directions", &directions.to_vec());
}

/// The lag is whole microseconds under `lag_micros`, not serde's
/// `{"secs", "nanos"}`.
#[test]
fn co_access_golden_carries_the_lag_in_micros() {
    let co = co_access();
    assert_eq!(co.lag().as_micros(), 30_250_000);
    assert_golden(AREA, "co_access", &co);
    let json = serde_json::to_value(co).expect("a co-access encodes");
    assert_eq!(json["lag_micros"], json!(30_250_000));
    assert!(json.get("lag").is_none());
}

#[test]
fn co_access_refuses_what_it_can_check_and_other_shapes() {
    let write = ULID_A;
    let read = ULID_C;
    let writer = ULID_A;
    assert_rejected::<CoAccess>(
        &format!(
            r#"{{"write": "{write}", "writer": "{writer}", "read": "{write}", "lag_micros": 30250000}}"#
        ),
        "invalid co-access: WrongOperations",
    );
    assert_rejected::<CoAccess>(
        &format!(
            r#"{{"write": "{write}", "writer": "{writer}", "read": "{read}", "lag_micros": 0}}"#
        ),
        "invalid co-access: ReadNotAfterWrite",
    );
    assert_rejected::<CoAccess>(
        &format!(
            r#"{{"write": "{write}", "writer": "{writer}", "read": "{read}", "lag": {{"secs": 30, "nanos": 250000000}}}}"#
        ),
        "unknown field `lag`",
    );
    assert_rejected::<CoAccess>(
        &format!(
            r#"{{"write": "{write}", "writer": "{writer}", "read": "{read}", "lag_micros": -1}}"#
        ),
        "invalid value",
    );
    assert_rejected::<CoAccess>(
        &format!(
            r#"{{"write": "{write}", "writer": "{writer}", "read": "{read}", "lag_micros": 30.25}}"#
        ),
        "invalid type",
    );
    assert_rejected::<CoAccess>(
        &format!(
            r#"{{"write": "{write}", "writer": "{writer}", "read": "{read}", "lag_micros": 1, "resource": "{write}"}}"#
        ),
        "unknown field `resource`",
    );
}

/// `Confirmed` writes no sender: decoding takes it from the content's
/// origin agent through `Confirmed::new`.
#[test]
fn confirmed_golden_rebuilds_its_sender_on_decode() {
    let confirmed = confirmed();
    assert_eq!(confirmed.from(), planner());
    assert_golden(AREA, "confirmed", &confirmed);
    let json = serde_json::to_value(&confirmed).expect("a confirmed transmission encodes");
    let keys: Vec<&str> = json
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(keys, ["at", "co_access", "content"], "no `from` key");
    let decoded: Confirmed = serde_json::from_value(json.clone()).expect("the encoding decodes");
    assert_eq!(decoded.from(), planner());
    assert_eq!(decoded, confirmed);

    let mut with_sender = json;
    with_sender["from"] = json!(planner());
    assert_rejected::<Confirmed>(&with_sender.to_string(), "unknown field `from`");
}

#[test]
fn confirmed_refuses_mixed_matches() {
    let valid = serde_json::to_value(confirmed()).expect("a confirmed transmission encodes");
    let with_content = |second: &Value| {
        let mut value = valid.clone();
        let first = value["content"][0].clone();
        value["content"] = json!([first, second]);
        value.to_string()
    };
    let mut other_origin = serde_json::to_value(content_match()).expect("a content match encodes");
    other_origin["origin_agent"] = json!(ULID_C);
    assert_rejected::<Confirmed>(
        &with_content(&other_origin),
        "invalid confirmed transmission: SeveralOrigins",
    );
    let reviewer = id(AgentId::from_ulid_text, ULID_C);
    let other_reader =
        serde_json::to_value(content_match_to(reviewer)).expect("a content match encodes");
    assert_rejected::<Confirmed>(
        &with_content(&other_reader),
        "invalid confirmed transmission: SeveralReaders",
    );
    let mut empty = valid;
    empty["content"] = json!([]);
    assert_rejected::<Confirmed>(&empty.to_string(), "invalid non-empty list");
}

#[test]
fn transmissions_refuse_unknown_fields_and_variants() {
    let valid = serde_json::to_value(transmission(TransmissionState::Detected)).expect("encodes");
    let mut state = valid.clone();
    state["state"] = json!({"type": "rejected"});
    assert_rejected::<Transmission>(&state.to_string(), "unknown variant `rejected`");
    let mut extra = valid;
    extra["from"] = json!(planner());
    assert_rejected::<Transmission>(&extra.to_string(), "unknown field `from`");
    assert_rejected::<TransmissionState>(
        r#"{"type": "suspected", "data": {"co_access": [], "since": "2026-10-04T12:05:30.250000Z"}}"#,
        "invalid non-empty list",
    );
    assert_rejected::<Route>(r#"{"type": "broadcast"}"#, "unknown variant `broadcast`");
    assert_rejected::<DirectCarrier>(r#"{"type": "email"}"#, "unknown variant `email`");
    assert_rejected::<DelegationDirection>(r#""sibling""#, "unknown variant `sibling`");
}
