//! `POST /actions`: the one endpoint of every `Route::Action(kind)`, told
//! apart by the `ActionRequest`'s `type`.
//!
//! ```text
//! body ─decode_request─▶ ActionRequest ── refused ─▶ 400 ActionError::InvalidInput(MalformedRequest)
//!      ─into_action(&caller)─▶ OperatorAction ── SelfMerge ─▶ 422 InvalidInput(SelfMerge)
//!      ─▶ OperatorActions::act(caller, action) ─▶ 200 ActionOutcome | e.status(), e's JSON
//! ```
//!
//! The author of a merge is the caller's operator, stamped by
//! [`ActionRequest::into_action`]; a request that does not decode or is a
//! self-merge never becomes an action, so `act` never sees it and nothing
//! is audited.

use axum::body::Body;
use axum::http::request::Parts;
use axum::response::Response;
use crosstalk_spec::interfaces::l8_surface::http::{Route, Status};
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionKind, ActionRequest, Caller, QueryError,
};

use super::input::{Input, Unread};
use super::{Surface, respond};

pub(super) async fn serve<S: Surface>(
    surface: &S,
    caller: &Caller,
    parts: Parts,
    body: Body,
) -> Response {
    match act(surface, caller, parts, body).await {
        Ok(response) => response,
        Err(error) => respond::error(&error),
    }
}

async fn act<S: Surface>(
    surface: &S,
    caller: &Caller,
    parts: Parts,
    body: Body,
) -> Result<Response, ActionError> {
    // Every action route has the same arguments (one JSON body), so any
    // kind's row reads the request; the body then says which kind it is.
    let route = Route::Action(ActionKind::ALL[0]);
    let input = Input::read(route, parts, body)
        .await
        .map_err(|unread| match unread {
            Unread::NoRoute => ActionError::NotFound,
            Unread::Malformed(error) => ActionError::from(error),
        })?;
    let request: ActionRequest = input.body()?;
    let action = request.into_action(caller)?;
    let outcome = surface.act(caller, action).await?;
    respond::json(Status::Ok, &outcome).map_err(|error| ActionError::Store {
        reason: match error {
            QueryError::Store { reason } => reason,
            other => format!("{other:?}"),
        },
    })
}
