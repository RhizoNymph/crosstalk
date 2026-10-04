//! The set-policy form: a policy and an optional note.

use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l8_surface::PolicyKind;
use topcoat::Result;
use topcoat::view::{View, component, view};

use crate::components::badge::Badge;
use crate::components::error_panel;
use crate::components::form::{BUTTON_PRIMARY, INPUT, LABEL};
use crate::error::UiError;
use crate::pages::common::form::{FormFields, POLICIES, note, policy, policy_code};
use crosstalk_spec::interfaces::l8_surface::OperatorAction;

pub fn parse(
    channel: ChannelId,
    fields: &FormFields,
) -> std::result::Result<OperatorAction, UiError> {
    Ok(OperatorAction::SetPolicy {
        channel,
        policy: policy(fields, "policy")?,
        note: note(fields, "note")?,
    })
}

/// The policy an operator most likely wants next: an unreviewed channel is
/// usually being decided, a decided one usually being reversed.
pub fn suggested(current: PolicyKind) -> PolicyKind {
    match current {
        PolicyKind::Unreviewed | PolicyKind::Unsanctioned => PolicyKind::Sanctioned,
        PolicyKind::Sanctioned => PolicyKind::Unsanctioned,
    }
}

/// The form. `retained` holds a failed submission's fields so nothing typed
/// is lost; `error` is shown above the button.
#[component]
pub async fn policy_form(
    action: String,
    current: PolicyKind,
    retained: Option<FormFields>,
    error: Option<UiError>,
) -> Result<impl View> {
    let selected = retained
        .as_ref()
        .and_then(|f| policy(f, "policy").ok())
        .unwrap_or_else(|| suggested(current));
    let note_text = retained
        .as_ref()
        .and_then(|f| f.text("note"))
        .unwrap_or("")
        .to_owned();
    let options: Vec<_> = POLICIES
        .iter()
        .map(|p| (policy_code(*p), p.label(), *p == selected))
        .collect();
    Ok(view! {
        <form method="post" action=(action) class="space-y-2">
            <input type="hidden" name="action" value="set-policy">
            <div class="flex flex-wrap items-end gap-3">
                <label class="block">
                    <span class=(LABEL)>"Policy"</span>
                    <select name="policy" class=(INPUT)>
                        for (code, label, chosen) in options {
                            <option value=(code) selected=(chosen)>(label)</option>
                        }
                    </select>
                </label>
                <label class="block min-w-64 flex-1">
                    <span class=(LABEL)>"Note (optional)"</span>
                    <input type="text" name="note" value=(note_text) maxlength="2000" class=(format!("{INPUT} w-full")) placeholder="Why this decision">
                </label>
                <button type="submit" class=(BUTTON_PRIMARY)>"Set policy"</button>
            </div>
            if let Some(error) = error {
                error_panel(error: &error)
            }
        </form>
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pages::common::form::invalid;

    #[test]
    fn parses_policy_and_note() {
        let fields = FormFields::from_pairs(&[("policy", "sanctioned"), ("note", " team wiki ")]);
        assert_eq!(
            parse(ChannelId::from_ulid(1), &fields),
            Ok(OperatorAction::SetPolicy {
                channel: ChannelId::from_ulid(1),
                policy: PolicyKind::Sanctioned,
                note: Some("team wiki".into()),
            })
        );
    }

    #[test]
    fn rejects_missing_or_unknown_policy() {
        assert_eq!(
            parse(ChannelId::from_ulid(1), &FormFields::default()),
            Err(invalid("policy", "required"))
        );
        let fields = FormFields::from_pairs(&[("policy", "allow")]);
        assert!(parse(ChannelId::from_ulid(1), &fields).is_err());
    }

    #[test]
    fn suggests_the_likely_next_policy() {
        assert_eq!(suggested(PolicyKind::Unreviewed), PolicyKind::Sanctioned);
        assert_eq!(suggested(PolicyKind::Sanctioned), PolicyKind::Unsanctioned);
    }
}
