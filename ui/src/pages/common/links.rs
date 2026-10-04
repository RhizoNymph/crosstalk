//! URLs of entity pages, carrying the view state.

use crosstalk_spec::aggregates::alert::AlertSubject;
use crosstalk_spec::ids::{AgentId, AlertRuleId, ChannelId, TransmissionId};

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
