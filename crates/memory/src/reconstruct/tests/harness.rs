//! The model-based harness, run on the reference and on a broken store.

use super::*;

/// `reconstruct.agent-state.legal-transitions`, `reconstruct.agent-merge.
/// target-not-merged`, `reconstruct.agent-merge.record-agreement`,
/// `reconstruct.agent.change-announced` (for the agents a change names) and
/// `surface.agent.rows-canonical`, on random histories: the harness checks
/// the reference against itself, which proves both the harness and these
/// invariants of the reference.
#[test]
fn reference_agrees_with_itself_under_the_harness() {
    let outcome = model::check_agent_store(pipeline_harness(), MemoryAgents::new);
    assert_eq!(outcome, Ok(()));
}

/// The harness catches a store that disagrees with the reference: one
/// that forgets every activity record.
#[test]
fn harness_rejects_a_store_that_drops_activity() {
    let outcome = model::check_agent_store(pipeline_harness(), |ids, outbox| ForgetfulActivity {
        inner: MemoryAgents::new(ids, outbox),
    });
    assert!(
        matches!(outcome, Err(ModelMismatch::Failed { .. })),
        "{outcome:?}"
    );
}

/// [`MemoryAgents`] with `ActivityStore::record` doing nothing.
struct ForgetfulActivity {
    inner: MemoryAgents,
}

impl AgentDirectory for ForgetfulActivity {
    fn canonical(&self, id: AgentId) -> AgentId {
        self.inner.canonical(id)
    }
}

impl IdentityResolver for ForgetfulActivity {
    async fn merge(
        &mut self,
        request: MergeRequest,
        at: Timestamp,
    ) -> Result<crosstalk_spec::observed::agent::MergeRecord, ResolveError> {
        self.inner.merge(request, at).await
    }

    async fn unmerge(
        &mut self,
        merge: MergeId,
        by: OperatorId,
        at: Timestamp,
    ) -> Result<crosstalk_spec::observed::agent::Reversal, ResolveError> {
        self.inner.unmerge(merge, by, at).await
    }

    async fn rename(
        &mut self,
        agent: AgentId,
        label: Option<crosstalk_spec::observed::agent::AgentLabel>,
        by: OperatorId,
    ) -> Result<Change, ResolveError> {
        self.inner.rename(agent, label, by).await
    }

    async fn resolve(
        &self,
        evidence: &NonEmpty<IdentityEvidence>,
    ) -> Result<Resolution, ResolveError> {
        self.inner.resolve(evidence).await
    }
}

impl ClaimStore for ForgetfulActivity {
    async fn record(
        &mut self,
        agent: AgentId,
        claim: &crosstalk_spec::observed::client::HarnessClaim,
        at: Timestamp,
    ) -> Result<(), ResolveError> {
        ClaimStore::record(&mut self.inner, agent, claim, at).await
    }

    async fn claims(&self, agent: AgentId) -> Result<ClaimSet, ResolveError> {
        ClaimStore::claims(&self.inner, agent).await
    }
}

impl ActivityStore for ForgetfulActivity {
    async fn record(&mut self, _agent: AgentId, _at: Timestamp) -> Result<(), ResolveError> {
        Ok(())
    }

    async fn last_seen(&self, agent: AgentId) -> Result<Option<Timestamp>, ResolveError> {
        ActivityStore::last_seen(&self.inner, agent).await
    }
}

impl AgentReads for ForgetfulActivity {
    async fn list(
        &self,
        filter: &AgentFilter,
        page: &PageRequest<crosstalk_spec::paging::AgentList>,
    ) -> Result<
        crosstalk_spec::paging::Page<
            crosstalk_spec::aggregates::agents::AgentProfile,
            crosstalk_spec::paging::AgentList,
        >,
        AgentReadError,
    > {
        self.inner.list(filter, page).await
    }

    async fn cluster(
        &self,
        id: AgentId,
    ) -> Result<Option<crosstalk_spec::aggregates::agents::AgentCluster>, AgentReadError> {
        self.inner.cluster(id).await
    }

    async fn names(
        &self,
        ids: &IdBatch<AgentId>,
    ) -> Result<
        std::collections::BTreeMap<AgentId, crosstalk_spec::aggregates::agents::AgentName>,
        AgentReadError,
    > {
        self.inner.names(ids).await
    }
}

impl AgentLifecycle for ForgetfulActivity {
    async fn create(&mut self, agent: NewAgent) -> Result<(), AgentLifecycleError> {
        self.inner.create(agent).await
    }

    async fn advance(
        &mut self,
        agent: AgentId,
        advance: Advance,
    ) -> Result<(), AgentLifecycleError> {
        self.inner.advance(agent, advance).await
    }

    async fn attach_evidence(
        &mut self,
        agent: AgentId,
        evidence: IdentityEvidence,
    ) -> Result<(), AgentLifecycleError> {
        self.inner.attach_evidence(agent, evidence).await
    }
}

/// The store is `Send + Sync`, so every future its trait methods return is
/// `Send` (`canonical.interface.send-futures`, P0.1).
#[test]
fn the_store_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<MemoryAgents>();
}
