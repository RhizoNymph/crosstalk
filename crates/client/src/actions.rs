//! `OperatorActions` over HTTP: `act` sends the action's
//! [`ActionRequest`] to `POST /actions`, the route of its kind
//! (`Route::Action(kind)`), and decodes the `ActionOutcome` or the
//! `ActionError`.
//!
//! A client never sends an `OperatorAction`: it sends the request of the
//! same variant without anything the surface stamps
//! ([`ActionRequest::of`]). A merge's author is the caller the surface
//! derives from this client's credential, whoever the action names.

use crosstalk_spec::interfaces::l8_surface::http::Route;
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionOutcome, ActionRequest, Caller, OperatorAction, OperatorActions,
};

use crate::client::HttpClient;

impl<H> OperatorActions for HttpClient<H> {
    async fn act(&self, _: &Caller, action: OperatorAction) -> Result<ActionOutcome, ActionError> {
        let route = Route::Action(action.kind());
        let request = ActionRequest::of(&action);
        tracing::info!(kind = ?action.kind(), "acting");
        self.call::<ActionOutcome, ActionError>(route, |b| b.body(&request))
            .await
            .map_err(ActionError::from)
    }
}
