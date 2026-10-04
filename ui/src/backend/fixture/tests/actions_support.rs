//! Helpers shared by the action tests.

use crosstalk_spec::ids::{AgentId, AlertId, ChannelId};
use crosstalk_spec::observed::agent::{MergeAuthor, MergeRequest};

use super::super::FixtureBackend;
use super::super::world::ChannelKey;
use super::researcher;
use crate::contract::actions::OperatorAction;
use crate::contract::alerts::AlertState;
use crate::contract::errors::{ConflictKind, QueryError};

pub fn agent(b: &FixtureBackend, key: &str) -> AgentId {
    b.world.scenario.agent(key).expect("agent")
}

pub fn channel(b: &FixtureBackend, key: ChannelKey) -> ChannelId {
    b.world.scenario.channel(key).expect("channel")
}

pub fn merge(b: &FixtureBackend, from: &str, into: &str) -> OperatorAction {
    let request = MergeRequest::new(
        agent(b, from),
        agent(b, into),
        MergeAuthor::Operator(researcher().operator()),
    )
    .expect("request");
    OperatorAction::MergeAgents(request)
}

pub fn conflict(kind: ConflictKind) -> Option<QueryError> {
    Some(QueryError::Conflict(kind))
}

pub async fn audit_len(b: &FixtureBackend) -> usize {
    b.state.read().await.audit.len()
}

pub async fn alert_state(b: &FixtureBackend, id: AlertId) -> AlertState {
    b.state
        .read()
        .await
        .alerts
        .iter()
        .find(|a| a.id == id)
        .expect("alert")
        .state
        .clone()
}

pub async fn find_alert(
    b: &FixtureBackend,
    f: impl Fn(&crate::contract::alerts::Alert) -> bool,
) -> AlertId {
    b.state
        .read()
        .await
        .alerts
        .iter()
        .find(|a| f(a))
        .expect("alert")
        .id
}
