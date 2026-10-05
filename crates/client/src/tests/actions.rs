//! `act` sends the action's request to `POST /actions`: every request
//! golden comes out of the action it stamps into, byte for byte in JSON,
//! and every outcome golden comes back as itself.

use crosstalk_spec::interfaces::l8_surface::http::request::check_body;
use crosstalk_spec::interfaces::l8_surface::http::{Method, Route, Target, resolve};
use crosstalk_spec::interfaces::l8_surface::{
    ActionOutcome, ActionRequest, OperatorAction, OperatorActions,
};

use super::stub::{Reply, Stub};
use super::{ULID_A, caller, golden_json, golden_value, id};

const REQUESTS: [&str; 16] = [
    "request_acknowledge",
    "request_create_rule",
    "request_merge_agents",
    "request_pin_topic_version",
    "request_promote_channel",
    "request_rename_agent",
    "request_rename_agent_clear",
    "request_replay_dead_letter",
    "request_resolve",
    "request_set_policy",
    "request_set_rule_enabled",
    "request_set_verdict",
    "request_set_verdict_withdraw",
    "request_unmerge",
    "request_unpin_topic_version",
    "request_update_rule",
];

/// The action a golden request stamps into, sent back by the client as the
/// golden request: nothing the surface stamps (a merge's author) travels.
#[tokio::test]
async fn every_action_sends_its_request_golden() {
    let c = caller();
    for name in REQUESTS {
        let path = format!("surface_actions/actions/{name}.json");
        let request: ActionRequest = golden_value(&path);
        let kind = request.kind();
        let action: OperatorAction = request
            .into_action(&c)
            .unwrap_or_else(|error| panic!("{name}: {error:?}"));
        let mut stub = Stub::always(Reply::value(200, &ActionOutcome::Applied)).await;
        let outcome = stub.client().act(&c, action).await;
        assert_eq!(outcome, Ok(ActionOutcome::Applied), "{name}");
        let sent = stub.only_request();
        assert_eq!(sent.method, "POST", "{name}");
        assert_eq!(
            resolve(Method::Post, &sent.path).map(|(target, _)| target),
            Some(Target::Actions),
            "{name}"
        );
        assert_eq!(Route::Action(kind).path(), sent.path, "{name}");
        let body = check_body(Route::Action(kind), sent.header("content-type"), &sent.body)
            .unwrap_or_else(|error| panic!("{name}: {error:?}"));
        assert!(body.is_some(), "{name}");
        assert_eq!(sent.json(), golden_json(&path), "{name}");
        assert!(sent.query.is_empty(), "{name}");
    }
}

/// Every outcome of the golden comes back as itself.
#[tokio::test]
async fn every_outcome_golden_decodes() {
    let outcomes: Vec<ActionOutcome> = golden_value("surface_actions/actions/action_outcomes.json");
    assert_eq!(outcomes.len(), 5);
    for outcome in outcomes {
        let stub = Stub::always(Reply::value(200, &outcome)).await;
        let result = stub
            .client()
            .act(&caller(), OperatorAction::Acknowledge { alert: id(ULID_A) })
            .await;
        assert_eq!(result, Ok(outcome));
    }
}
