//! URLs of entity pages, carrying the view state.

use crosstalk_spec::aggregates::alert::AlertSubject;
use crosstalk_spec::ids::{
    AgentId, AlertId, AlertRuleId, ChannelId, ConversationId, ExchangeId, SpanId, TransmissionId,
};

use crate::components::href;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

pub fn channel_url(id: ChannelId, state: &ViewState) -> String {
    href(&format!("/channels/{}", id.to_ulid()), state, &[])
}

pub fn agent_url(id: AgentId, state: &ViewState) -> String {
    href(&format!("/agents/{}", id.to_ulid()), state, &[])
}

pub fn transmission_url(id: TransmissionId, state: &ViewState) -> String {
    href(&format!("/transmissions/{}", id.to_ulid()), state, &[])
}

/// An agent's conversations.
pub fn agent_conversations_url(id: AgentId, state: &ViewState) -> String {
    href(
        &format!("/agents/{}/conversations", id.to_ulid()),
        state,
        &[],
    )
}

/// A conversation opened at turn `turn` (its first turn when `None`),
/// scrolled to it.
pub fn conversation_url(id: ConversationId, turn: Option<u32>, state: &ViewState) -> String {
    let path = format!("/conversations/{}", id.to_ulid());
    match turn {
        None => href(&path, state, &[]),
        Some(turn) => {
            let at = turn.to_string();
            format!("{}#turn-{turn}", href(&path, state, &[("turn", &at)]))
        }
    }
}

/// The turn an exchange is: a redirect to its conversation.
pub fn exchange_url(id: ExchangeId, state: &ViewState) -> String {
    href(&format!("/exchanges/{}", id.to_ulid()), state, &[])
}

/// The turn holding a span, with the span highlighted: a redirect to its
/// conversation.
pub fn span_url(id: SpanId, state: &ViewState) -> String {
    href(&format!("/spans/{}", id.to_ulid()), state, &[])
}

pub fn alert_url(id: AlertId, state: &ViewState) -> String {
    href(&format!("/alerts/{}", id.to_ulid()), state, &[])
}

pub fn rule_url(id: AlertRuleId, state: &ViewState) -> String {
    href(&format!("/alerts/rules/{}", id.to_ulid()), state, &[])
}

/// An alert subject's page and a label for the link.
pub fn alert_subject(subject: &AlertSubject, state: &ViewState) -> (String, String) {
    match subject {
        AlertSubject::Channel(id) => (
            channel_url(*id, state),
            format!("channel {}", crate::components::short_id(id.to_ulid())),
        ),
        AlertSubject::Agent(id) => (
            agent_url(*id, state),
            format!("agent {}", crate::components::short_id(id.to_ulid())),
        ),
        AlertSubject::Transmission(id) => (
            transmission_url(*id, state),
            format!("transmission {}", crate::components::short_id(id.to_ulid())),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::href::tests::state;

    #[test]
    fn conversation_links_carry_the_view_state_and_the_turn() {
        let state = state();
        let id = ConversationId::from_ulid(9);
        let first = conversation_url(id, None, &state);
        assert!(first.starts_with("/conversations/00000000000000000000000009?from="));
        assert!(!first.contains("turn="));
        let turn = conversation_url(id, Some(37), &state);
        assert!(turn.contains("&turn=37"), "{turn}");
        assert!(turn.ends_with("#turn-37"), "{turn}");
        assert!(
            exchange_url(ExchangeId::from_ulid(3), &state)
                .starts_with("/exchanges/00000000000000000000000003?from=")
        );
        assert!(
            span_url(SpanId::from_ulid(4), &state)
                .starts_with("/spans/00000000000000000000000004?from=")
        );
        assert!(
            agent_conversations_url(AgentId::from_ulid(2), &state)
                .starts_with("/agents/00000000000000000000000002/conversations?from=")
        );
    }

    #[test]
    fn subjects_link_to_their_pages() {
        let (url, label) = alert_subject(
            &AlertSubject::Transmission(TransmissionId::from_ulid(5)),
            &state(),
        );
        assert!(url.starts_with("/transmissions/00000000000000000000000005?from="));
        assert_eq!(label, "transmission …000005");
        let (url, _) = alert_subject(&AlertSubject::Channel(ChannelId::from_ulid(5)), &state());
        assert!(url.starts_with("/channels/"));
        let (url, _) = alert_subject(&AlertSubject::Agent(AgentId::from_ulid(5)), &state());
        assert!(url.starts_with("/agents/"));
    }
}
