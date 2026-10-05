//! The agent page's forms: rename, clear the label, revert a merge, and the
//! merge confirmation.

use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l8_surface::{ActionError, Caller};
use crosstalk_spec::observed::agent::AgentLabel;
use crosstalk_spec::support::InvalidText;

use crate::error::UiError;
use crate::pages::common::flash::Flash;
use crate::pages::common::form::{FormFields, id, invalid, required};
use crosstalk_spec::ids::MergeId;
use crosstalk_spec::interfaces::l8_surface::OperatorAction;

/// The forms on the agent page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentForm {
    Rename,
    Unmerge,
}

/// Why a label was refused, in words.
pub fn label_error(error: InvalidText) -> String {
    match error {
        InvalidText::Blank => "label is blank".to_owned(),
        InvalidText::TooLong { max, .. } => format!("label is longer than {max} characters"),
        InvalidText::ControlCharacter => "label holds a control character".to_owned(),
    }
}

/// A validated post of the agent page, and the flash it ends with.
pub fn parse(
    agent: AgentId,
    fields: &FormFields,
) -> Result<(AgentForm, OperatorAction, Flash), (Option<AgentForm>, UiError)> {
    match fields.text("action") {
        Some("rename") => {
            let label = required(fields, "label")
                .and_then(|raw| AgentLabel::new(raw).map_err(|e| invalid("label", label_error(e))))
                .map_err(|e| (Some(AgentForm::Rename), e))?;
            Ok((
                AgentForm::Rename,
                OperatorAction::RenameAgent {
                    agent,
                    label: Some(label),
                },
                Flash::AgentRenamed,
            ))
        }
        Some("clear-label") => Ok((
            AgentForm::Rename,
            OperatorAction::RenameAgent { agent, label: None },
            Flash::LabelCleared,
        )),
        Some("unmerge") => {
            let merge =
                id::<MergeId>(fields, "merge").map_err(|e| (Some(AgentForm::Unmerge), e))?;
            Ok((
                AgentForm::Unmerge,
                OperatorAction::Unmerge { merge },
                Flash::Unmerged,
            ))
        }
        _ => Err((None, invalid("action", "unknown action"))),
    }
}

/// The merge the caller confirms: `from` becomes `into`, authored by the
/// caller (`OperatorAction::merge_agents`). One id twice is refused here,
/// before any call and without an audit entry, as `InvalidInput(SelfMerge)`.
pub fn merge_action(
    caller: &Caller,
    from: AgentId,
    into: AgentId,
) -> Result<OperatorAction, UiError> {
    OperatorAction::merge_agents(caller, from, into).map_err(|e| ActionError::from(e).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::url::ulid::UlidId;

    fn agent() -> AgentId {
        AgentId::from_ulid(1)
    }

    #[test]
    fn rename_validates_the_label() {
        let fields = FormFields::from_pairs(&[("action", "rename"), ("label", "  planner ")]);
        let (form, action, flash) = parse(agent(), &fields).expect("valid");
        assert_eq!(form, AgentForm::Rename);
        assert_eq!(flash, Flash::AgentRenamed);
        assert_eq!(
            action,
            OperatorAction::RenameAgent {
                agent: agent(),
                label: AgentLabel::new("planner").ok()
            }
        );
        let blank = FormFields::from_pairs(&[("action", "rename"), ("label", "   ")]);
        assert_eq!(
            parse(agent(), &blank),
            Err((Some(AgentForm::Rename), invalid("label", "required")))
        );
        let long = "x".repeat(AgentLabel::MAX_CHARS + 1);
        let too_long = FormFields::from_pairs(&[("action", "rename"), ("label", &long)]);
        assert_eq!(
            parse(agent(), &too_long),
            Err((
                Some(AgentForm::Rename),
                invalid("label", "label is longer than 64 characters")
            ))
        );
        let control = FormFields::from_pairs(&[("action", "rename"), ("label", "a\u{7}b")]);
        assert_eq!(
            parse(agent(), &control),
            Err((
                Some(AgentForm::Rename),
                invalid("label", "label holds a control character")
            ))
        );
    }

    #[test]
    fn clearing_sends_no_label() {
        let fields = FormFields::from_pairs(&[("action", "clear-label")]);
        let (_, action, flash) = parse(agent(), &fields).expect("valid");
        assert_eq!(
            action,
            OperatorAction::RenameAgent {
                agent: agent(),
                label: None
            }
        );
        assert_eq!(flash, Flash::LabelCleared);
    }

    #[test]
    fn unmerge_needs_a_merge_id() {
        let merge = MergeId::from_ulid(9);
        let fields = FormFields::from_pairs(&[("action", "unmerge"), ("merge", &merge.to_ulid())]);
        let (_, action, _) = parse(agent(), &fields).expect("valid");
        assert_eq!(action, OperatorAction::Unmerge { merge });
        let bad = FormFields::from_pairs(&[("action", "unmerge"), ("merge", "x")]);
        assert!(matches!(
            parse(agent(), &bad),
            Err((Some(AgentForm::Unmerge), _))
        ));
        assert!(matches!(
            parse(agent(), &FormFields::default()),
            Err((None, UiError::Field { .. }))
        ));
    }

    #[test]
    fn merges_are_operator_authored_and_never_self() {
        use crosstalk_spec::ids::OperatorId;
        use crosstalk_spec::interfaces::l8_surface::{Permission, QueryError};
        use crosstalk_spec::observed::agent::MergeAuthor;

        let operator = OperatorId::from_ulid(7);
        let caller = crate::testing::caller_of(operator, &[Permission::Govern]);
        let action =
            merge_action(&caller, AgentId::from_ulid(1), AgentId::from_ulid(2)).expect("valid");
        let OperatorAction::MergeAgents(request) = action else {
            panic!("expected a merge");
        };
        assert_eq!(request.by(), MergeAuthor::Operator(operator));
        assert_eq!(request.source(), AgentId::from_ulid(1));
        assert_eq!(
            merge_action(&caller, AgentId::from_ulid(1), AgentId::from_ulid(1)),
            Err(UiError::Query(QueryError::InvalidInput(
                crosstalk_spec::interfaces::l8_surface::InputError::SelfMerge
            )))
        );
    }
}
