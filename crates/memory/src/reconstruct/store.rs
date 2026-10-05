//! The spec traits on [`MemoryAgents`]. Each method runs one table
//! operation in one critical section, then publishes the events it
//! returned.

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::agents::filter::AgentFilter;
use crosstalk_spec::aggregates::agents::{AgentCluster, AgentName, AgentProfile};
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::ids::{AgentId, MergeId, OperatorId};
use crosstalk_spec::interfaces::l3_reconstruction::agents::{
    ActivityStore, AgentReadError, AgentReads,
};
use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::{
    Advance, AgentLifecycle, AgentLifecycleError, NewAgent,
};
use crosstalk_spec::interfaces::l3_reconstruction::{
    AgentDirectory, ClaimStore, IdentityResolver, Resolution, ResolveError,
};
use crosstalk_spec::observed::agent::{
    AgentLabel, ClaimSet, IdentityEvidence, MergeRecord, MergeRequest, Reversal,
};
use crosstalk_spec::observed::client::HarnessClaim;
use crosstalk_spec::paging::{AgentList, Page, PageRequest};
use crosstalk_spec::support::{Change, NonEmpty, Timestamp};

use super::MemoryAgents;
use super::resolve::resolve_evidence;
use super::table::ReadModelError;
use crate::support::{PageError, lock, page_after};

fn read_model(error: ReadModelError) -> AgentReadError {
    AgentReadError::Store {
        reason: format!("agent table invariant broken: {error:?}"),
    }
}

fn page_error(error: PageError) -> AgentReadError {
    AgentReadError::Store {
        reason: error.to_string(),
    }
}

impl AgentDirectory for MemoryAgents {
    fn canonical(&self, id: AgentId) -> AgentId {
        self.state.read().canonical(id)
    }
}

impl IdentityResolver for MemoryAgents {
    async fn merge(
        &mut self,
        request: MergeRequest,
        at: Timestamp,
    ) -> Result<MergeRecord, ResolveError> {
        let (record, events) = self
            .state
            .write()
            .merge(request, at, || self.next_merge_id())?;
        tracing::debug!(merge = ?record.id(), from = ?record.source(), into = ?record.target(), "agents merged");
        self.outbox.publish(events);
        Ok(record)
    }

    async fn unmerge(
        &mut self,
        merge: MergeId,
        by: OperatorId,
        at: Timestamp,
    ) -> Result<Reversal, ResolveError> {
        let (reversal, events) = self.state.write().unmerge(merge, by, at)?;
        tracing::debug!(merge = ?merge, restored = reversal.restored.len(), "merge reverted");
        self.outbox.publish(events);
        Ok(reversal)
    }

    async fn rename(
        &mut self,
        agent: AgentId,
        label: Option<AgentLabel>,
        by: OperatorId,
    ) -> Result<Change, ResolveError> {
        let (change, events) = self.state.write().rename(agent, label, by)?;
        self.outbox.publish(events);
        Ok(change)
    }

    /// The reference lookup of [`super::resolve`].
    async fn resolve(
        &self,
        evidence: &NonEmpty<IdentityEvidence>,
    ) -> Result<Resolution, ResolveError> {
        Ok(resolve_evidence(&self.state.read(), evidence))
    }
}

impl ClaimStore for MemoryAgents {
    async fn record(
        &mut self,
        agent: AgentId,
        claim: &HarnessClaim,
        at: Timestamp,
    ) -> Result<(), ResolveError> {
        self.state.write().record_claim(agent, claim, at);
        Ok(())
    }

    async fn claims(&self, agent: AgentId) -> Result<ClaimSet, ResolveError> {
        Ok(self.state.read().claims_of(agent))
    }
}

impl ActivityStore for MemoryAgents {
    async fn record(&mut self, agent: AgentId, at: Timestamp) -> Result<(), ResolveError> {
        self.state.write().record_activity(agent, at);
        Ok(())
    }

    async fn last_seen(&self, agent: AgentId) -> Result<Option<Timestamp>, ResolveError> {
        Ok(self.state.read().last_seen_of(agent))
    }
}

impl AgentReads for MemoryAgents {
    async fn list(
        &self,
        filter: &AgentFilter,
        page: &PageRequest<AgentList>,
    ) -> Result<Page<AgentProfile, AgentList>, AgentReadError> {
        let request = serde_json::to_string(filter).map_err(|error| AgentReadError::Store {
            reason: error.to_string(),
        })?;
        let mut cursors = lock(&self.cursors);
        let after = match &page.after {
            None => None,
            Some(cursor) => Some(
                cursors
                    .resolve(cursor, &request)
                    .ok_or(AgentReadError::InvalidCursor)?,
            ),
        };
        let profiles = {
            let table = self.state.read();
            let mut profiles = Vec::new();
            for agent in table.canonical_agents() {
                if after.is_some_and(|after| agent.id >= after) {
                    continue;
                }
                let profile = table.profile(agent).map_err(read_model)?;
                if filter.matches(&profile, |id| table.canonical(id)) {
                    profiles.push(profile);
                }
            }
            profiles
        };
        page_after(&mut cursors, profiles, page.size, request, AgentProfile::id).map_err(page_error)
    }

    async fn cluster(&self, id: AgentId) -> Result<Option<AgentCluster>, AgentReadError> {
        self.state.read().cluster(id).map_err(read_model)
    }

    async fn names(
        &self,
        ids: &IdBatch<AgentId>,
    ) -> Result<BTreeMap<AgentId, AgentName>, AgentReadError> {
        Ok(self.state.read().names(ids.ids()))
    }
}

impl AgentLifecycle for MemoryAgents {
    async fn create(&mut self, agent: NewAgent) -> Result<(), AgentLifecycleError> {
        let events = self.state.write().create(agent)?;
        self.outbox.publish(events);
        Ok(())
    }

    async fn advance(
        &mut self,
        agent: AgentId,
        advance: Advance,
    ) -> Result<(), AgentLifecycleError> {
        let events = self.state.write().advance(agent, advance)?;
        self.outbox.publish(events);
        Ok(())
    }

    async fn attach_evidence(
        &mut self,
        agent: AgentId,
        evidence: IdentityEvidence,
    ) -> Result<(), AgentLifecycleError> {
        let events = self.state.write().attach_evidence(agent, evidence)?;
        self.outbox.publish(events);
        Ok(())
    }
}
