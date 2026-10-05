//! Running an operator action from a form post.
//!
//! A post validates its fields into an [`OperatorAction`], runs it through
//! [`perform`], and on success redirects (303) to a page with a flash code.
//! On failure the handler renders the page again with the error next to the
//! form, under the status [`status_of`] gives.

use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use topcoat::context::Cx;
use topcoat::router::StatusCode;
use topcoat::router::error::see_other;

use super::flash::{self, Flash};
use super::form::FormFields;
use crate::app::{backend, caller, can};
use crate::components::href;
use crate::error::UiError;
use crate::url::view_state::ViewState;
use crosstalk_spec::interfaces::l8_surface::OperatorActions;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::interfaces::l8_surface::{ActionOutcome, OperatorAction};

/// A rejected form post: which form (`None` when the post named no known
/// form), why, and what was submitted, so the page can show the error next
/// to that form with the input kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure<F> {
    pub form: Option<F>,
    pub error: UiError,
    pub fields: FormFields,
}

impl<F: Copy + PartialEq> Failure<F> {
    pub fn new(form: Option<F>, error: UiError, fields: FormFields) -> Self {
        Self {
            form,
            error,
            fields,
        }
    }

    pub fn status(&self) -> StatusCode {
        status_of(&self.error)
    }
}

/// The error to show next to `form`, if the failure was there.
pub fn error_for<F: Copy + PartialEq>(failure: Option<&Failure<F>>, form: F) -> Option<UiError> {
    failure
        .filter(|f| f.form == Some(form))
        .map(|f| f.error.clone())
}

/// The fields to refill `form` with, if the failure was there.
pub fn fields_for<F: Copy + PartialEq>(
    failure: Option<&Failure<F>>,
    form: F,
) -> Option<FormFields> {
    failure
        .filter(|f| f.form == Some(form))
        .map(|f| f.fields.clone())
}

/// The error of a post that named no known form, shown at the top.
pub fn general_error<F: Copy + PartialEq>(failure: Option<&Failure<F>>) -> Option<UiError> {
    failure
        .filter(|f| f.form.is_none())
        .map(|f| f.error.clone())
}

/// `Err(Forbidden)` unless the caller holds `permission`.
pub fn require(caller: &Caller, permission: Permission) -> Result<(), UiError> {
    if can(caller, permission) {
        Ok(())
    } else {
        Err(UiError::Query(QueryError::Forbidden {
            missing: permission,
        }))
    }
}

/// Checks the action's one required permission, then runs it; the
/// surface's `ActionError` becomes the page's `UiError`.
pub async fn perform(cx: &Cx, action: OperatorAction) -> Result<ActionOutcome, UiError> {
    let caller = caller(cx);
    require(&caller, action.required_permission())?;
    Ok(backend(cx).act(&caller, action).await?)
}

/// The flash after an accepted action: `applied` when it changed state,
/// [`Flash::Unchanged`] when the state already matched.
pub fn settled(outcome: &ActionOutcome, applied: Flash) -> Flash {
    match outcome {
        ActionOutcome::Unchanged => Flash::Unchanged,
        _ => applied,
    }
}

/// The response status for a failed action or read.
pub fn status_of(error: &UiError) -> StatusCode {
    match error {
        UiError::Field { .. } => StatusCode::UNPROCESSABLE_ENTITY,
        UiError::Query(query) => match query {
            QueryError::InvalidInput(_) => StatusCode::UNPROCESSABLE_ENTITY,
            QueryError::InvalidCursor => StatusCode::BAD_REQUEST,
            QueryError::Forbidden { .. } => StatusCode::FORBIDDEN,
            QueryError::NotFound | QueryError::ProjectionNotRetained { .. } => {
                StatusCode::NOT_FOUND
            }
            QueryError::Conflict(_) | QueryError::VersionNotRetained { .. } => StatusCode::CONFLICT,
            QueryError::Store { .. } | QueryError::Unavailable { .. } => StatusCode::BAD_GATEWAY,
        },
    }
}

/// Where a successful action sends the browser: `path` with the view state,
/// the page's own pairs and the flash code.
pub fn done_url(path: &str, state: &ViewState, extra: &[(&str, &str)], flash: Flash) -> String {
    let code = flash.code();
    let mut pairs = extra.to_vec();
    pairs.push((flash::KEY, code.as_str()));
    href(path, state, &pairs)
}

/// The 303 redirect of a successful action, returned through `Err` from a
/// page handler.
pub fn done(path: &str, state: &ViewState, extra: &[(&str, &str)], flash: Flash) -> topcoat::Error {
    see_other(done_url(path, state, extra, flash)).into()
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::ids::OperatorId;

    use super::*;
    use crate::components::href::tests::state;
    use crosstalk_spec::interfaces::l8_surface::ConflictKind;

    #[test]
    fn statuses_follow_the_error() {
        assert_eq!(
            status_of(&UiError::Query(QueryError::Conflict(
                ConflictKind::AgentMerged {
                    agent: crosstalk_spec::ids::AgentId::from_ulid(1),
                    into: crosstalk_spec::ids::AgentId::from_ulid(2),
                }
            ))),
            StatusCode::CONFLICT
        );
        assert_eq!(
            status_of(&UiError::Query(QueryError::Forbidden {
                missing: Permission::Govern
            })),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            status_of(&UiError::Query(QueryError::NotFound)),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn unchanged_outcomes_say_so() {
        assert_eq!(
            settled(&ActionOutcome::Unchanged, Flash::Acknowledged),
            Flash::Unchanged
        );
        assert_eq!(
            settled(&ActionOutcome::Applied, Flash::Acknowledged),
            Flash::Acknowledged
        );
    }

    #[test]
    fn done_url_ends_with_the_flash() {
        let url = done_url("/alerts", &state(), &[("tab", "open")], Flash::Resolved);
        assert!(url.starts_with("/alerts?from="));
        assert!(url.ends_with("&tab=open&flash=resolved"));
    }

    #[test]
    fn failures_belong_to_one_form() {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum Forms {
            A,
            B,
        }
        let failure = Failure::new(
            Some(Forms::A),
            UiError::Query(QueryError::NotFound),
            FormFields::default(),
        );
        assert_eq!(
            error_for(Some(&failure), Forms::A),
            Some(UiError::Query(QueryError::NotFound))
        );
        assert_eq!(error_for(Some(&failure), Forms::B), None);
        assert_eq!(general_error(Some(&failure)), None);
        let general: Failure<Forms> = Failure::new(
            None,
            UiError::Query(QueryError::NotFound),
            FormFields::default(),
        );
        assert_eq!(
            general_error(Some(&general)),
            Some(UiError::Query(QueryError::NotFound))
        );
        assert_eq!(failure.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn require_names_the_missing_permission() {
        let caller = crate::testing::caller_of(OperatorId::from_ulid(1), &[Permission::View]);
        assert_eq!(require(&caller, Permission::View), Ok(()));
        assert_eq!(
            require(&caller, Permission::Triage),
            Err(UiError::Query(QueryError::Forbidden {
                missing: Permission::Triage
            }))
        );
    }
}
