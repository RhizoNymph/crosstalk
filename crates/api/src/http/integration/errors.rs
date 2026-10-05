//! Every error of the status goldens, answered by the surface, reaches the
//! client with its status and its wire JSON.

use std::sync::Arc;

use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use crosstalk_spec::interfaces::l8_surface::http::{ErrorStatus, RequestBuilder, Route};
use crosstalk_spec::interfaces::l8_surface::{ActionError, ActionRequest, QueryError};
use serde_json::Value;

use super::fake::Fake;
use super::{FULL, decode, golden, golden_text, request, send, server};

/// `{"status": 409, "error": {..}}` rows of a status golden.
fn rows(name: &str) -> Vec<(u16, Value)> {
    let rows: Vec<Value> = decode(&golden_text(name));
    rows.into_iter()
        .map(|row| {
            let status = row["status"].as_u64().expect("a status") as u16;
            (status, row["error"].clone())
        })
        .collect()
}

#[tokio::test]
async fn errors_answer_with_their_status_and_json() {
    let query_rows = rows("http/query_error_statuses");
    assert!(!query_rows.is_empty());
    for (status, json) in query_rows {
        let error: QueryError = serde_json::from_value(json.clone()).expect("a query error");
        assert_eq!(error.status().code(), status, "the spec's table: {json}");
        let fake = Arc::new(Fake::default());
        fake.fail_with(error);
        let watermark = RequestBuilder::new(Route::Watermark)
            .build()
            .expect("a request");
        let reply = send(&server(&fake), request(&watermark, FULL)).await;
        assert_eq!(reply.status.as_u16(), status, "{json}");
        assert_eq!(reply.json(), json, "the body is the error's JSON");
        assert_eq!(
            reply.header(CONTENT_TYPE.as_str()),
            Some("application/json")
        );
        assert_eq!(reply.header(CACHE_CONTROL.as_str()), Some("no-store"));
    }
    let action_rows = rows("http/action_error_statuses");
    assert!(!action_rows.is_empty());
    let acknowledge: ActionRequest = golden("surface_actions/actions/request_acknowledge");
    let act = RequestBuilder::new(Route::Action(acknowledge.kind()))
        .body(&acknowledge)
        .build()
        .expect("a request");
    for (status, json) in action_rows {
        let error: ActionError = serde_json::from_value(json.clone()).expect("an action error");
        assert_eq!(error.status().code(), status, "the spec's table: {json}");
        let fake = Arc::new(Fake::default());
        fake.act_with(Err(error));
        let reply = send(&server(&fake), request(&act, FULL)).await;
        assert_eq!(reply.status.as_u16(), status, "{json}");
        assert_eq!(reply.json(), json, "the body is the error's JSON");
        assert_eq!(reply.header(CACHE_CONTROL.as_str()), Some("no-store"));
    }
}
