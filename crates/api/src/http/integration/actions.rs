//! `POST /actions`: every action kind's request golden becomes the action
//! the caller is stamped into, and every outcome golden is answered as is.

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::http::StatusCode;
use axum::http::header::CACHE_CONTROL;
use crosstalk_spec::interfaces::l8_surface::http::{RequestBuilder, Route};
use crosstalk_spec::interfaces::l8_surface::operators::RequestIdentity;
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionKind, ActionOutcome, ActionRequest,
};
use serde_json::Value;

use super::fake::Fake;
use super::{
    FULL, authenticated, decode, directory, error_json, golden, golden_text, operator, request,
    send, server, without,
};

const REQUESTS: [&str; 16] = [
    "acknowledge",
    "create_rule",
    "merge_agents",
    "pin_topic_version",
    "promote_channel",
    "rename_agent",
    "rename_agent_clear",
    "replay_dead_letter",
    "resolve",
    "set_policy",
    "set_rule_enabled",
    "set_verdict",
    "set_verdict_withdraw",
    "unmerge",
    "unpin_topic_version",
    "update_rule",
];

fn action_request(name: &str) -> ActionRequest {
    golden(&format!("surface_actions/actions/request_{name}"))
}

/// Every kind's request reaches `act` as `into_action` of it with the
/// caller, and the outcome is answered 200 with its JSON.
#[tokio::test]
async fn every_action_kind_is_served() {
    let outcomes: Vec<Value> = decode(&golden_text("surface_actions/actions/action_outcomes"));
    let caller = directory(&authenticated())
        .caller(RequestIdentity::Verified(operator(FULL)))
        .expect("the full operator");
    let mut kinds = BTreeSet::new();
    for (index, name) in REQUESTS.iter().enumerate() {
        let action = action_request(name);
        let outcome_json = &outcomes[index % outcomes.len()];
        let outcome: ActionOutcome =
            serde_json::from_value(outcome_json.clone()).expect("an outcome");
        let fake = Arc::new(Fake::default());
        fake.act_with(Ok(outcome));
        let encoded = RequestBuilder::new(Route::Action(action.kind()))
            .body(&action)
            .build()
            .expect("a request");
        let reply = send(&server(&fake), request(&encoded, FULL)).await;
        assert_eq!(reply.status, StatusCode::OK, "{name}: {reply:?}");
        assert_eq!(&reply.json(), outcome_json, "{name}: the outcome as is");
        assert_eq!(reply.header(CACHE_CONTROL.as_str()), Some("no-store"));
        let expected = action
            .clone()
            .into_action(&caller)
            .expect("not a self-merge");
        assert_eq!(fake.actions(), vec![(operator(FULL), expected)], "{name}");
        kinds.insert(action.kind().index());
    }
    assert_eq!(kinds.len(), ActionKind::ALL.len(), "every kind is served");
}

/// A caller without an action's permission is a 403 naming it.
#[tokio::test]
async fn every_action_refuses_a_caller_without_its_permission() {
    for name in REQUESTS {
        let action = action_request(name);
        let needed = action.kind().required_permission();
        let fake = Arc::new(Fake::default());
        let encoded = RequestBuilder::new(Route::Action(action.kind()))
            .body(&action)
            .build()
            .expect("a request");
        let reply = send(&server(&fake), request(&encoded, without(needed))).await;
        assert_eq!(reply.status, StatusCode::FORBIDDEN, "{name}");
        assert_eq!(
            reply.json(),
            error_json(&ActionError::Forbidden { missing: needed })
        );
        assert!(fake.actions().is_empty(), "{name}: nothing applied");
    }
}
