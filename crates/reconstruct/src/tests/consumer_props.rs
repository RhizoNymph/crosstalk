//! Consumer properties on generated traffic: harness callers with
//! sessions, sub-agents and parent ids under a few credentials, delivered
//! through the consumer over the reference agent store.

use std::collections::BTreeMap;

use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l3_reconstruction::agents::AgentReads;
use crosstalk_spec::observed::agent::{Agent, AgentState, IdentityEvidence, Strength};
use crosstalk_spec::observed::client::{ClientContext, CredentialRef, CredentialScheme};
use crosstalk_testkit::build::ExchangeBuilder;
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

use super::rig::Rig;
use super::thread_props::property;
use crate::consumer::Handled;

/// One generated exchange's caller.
#[derive(Debug, Clone, Copy)]
struct Caller {
    credential: u8,
    rotating: bool,
    session: Option<u8>,
    agent: Option<u8>,
    parent: Option<u8>,
}

fn caller() -> impl Strategy<Value = Caller> {
    (
        0u8..2,
        any::<bool>(),
        proptest::option::of(0u8..2),
        proptest::option::of(0u8..4),
        proptest::option::of(0u8..4),
    )
        .prop_map(|(credential, rotating, session, agent, parent)| Caller {
            credential,
            rotating,
            session,
            agent,
            parent,
        })
}

fn client(rig: &mut Rig, base: &[ClientContext], caller: Caller) -> ClientContext {
    let mut client = base[usize::from(caller.credential)].clone();
    if caller.rotating {
        client.credential = Some(CredentialRef {
            scheme: CredentialScheme::OauthAccessToken,
            hash: rig.scene.ids.credential(),
        });
    }
    client.ids.session = caller.session.map(|n| format!("session-{n}"));
    client.ids.agent = caller.agent.map(|n| format!("agent-{n}"));
    client.ids.parent_agent = caller.parent.map(|n| format!("agent-{n}"));
    client
}

/// Every stored agent, by id.
async fn all_agents(rig: &Rig, ids: &[AgentId]) -> Result<BTreeMap<AgentId, Agent>, TestCaseError> {
    let mut agents = BTreeMap::new();
    for id in ids {
        if let Some(cluster) = rig
            .agents
            .cluster(*id)
            .await
            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?
        {
            for agent in std::iter::once(cluster.agent()).chain(cluster.aliases()) {
                agents.insert(agent.id, agent.clone());
            }
        }
    }
    Ok(agents)
}

async fn play(callers: Vec<Caller>) -> Result<(Rig, BTreeMap<AgentId, Agent>), TestCaseError> {
    let mut rig = Rig::new();
    let base: Vec<ClientContext> = (0..2)
        .map(|_| ExchangeBuilder::new(&mut rig.scene.ids).build().meta.client)
        .collect();
    let mut attributed = Vec::new();
    for (n, caller) in callers.into_iter().enumerate() {
        let client = client(&mut rig, &base, caller);
        let user = rig.scene.user(&format!("turn {n}")).await;
        let output = rig.scene.assistant(&format!("reply {n}")).await;
        let at = rig.scene.tick();
        let exchange = ExchangeBuilder::new(&mut rig.scene.ids)
            .started_at(at)
            .client(client)
            .request(vec![user])
            .response(output)
            .build();
        if let Handled::Threaded { agent, .. } = rig
            .deliver(&exchange)
            .await
            .map_err(|e| TestCaseError::fail(format!("{e}")))?
        {
            attributed.push(agent);
        }
    }
    let agents = all_agents(&rig, &attributed).await?;
    Ok((rig, agents))
}

/// `reconstruct.agent.parent-acyclic`: parents the consumer derives never
/// lead back to the agent.
#[test]
fn parent_links_are_acyclic() {
    property(
        64,
        proptest::collection::vec(caller(), 1..16),
        |callers| async move {
            let (_, agents) = play(callers).await?;
            for start in agents.keys() {
                let mut seen = vec![*start];
                let mut at = agents.get(start).and_then(|agent| agent.parent);
                while let Some(parent) = at {
                    prop_assert!(!seen.contains(&parent), "cycle through {:?}", start);
                    seen.push(parent);
                    at = agents.get(&parent).and_then(|agent| agent.parent);
                }
            }
            Ok(())
        },
    );
}

/// `reconstruct.agent-state.established-needs-two-kinds`: every agent the
/// consumer established holds evidence of two variants, one strong.
#[test]
fn established_agents_hold_two_variants_one_strong() {
    property(
        64,
        proptest::collection::vec(caller(), 1..16),
        |callers| async move {
            let (_, agents) = play(callers).await?;
            for agent in agents.values() {
                if let AgentState::Established { .. } = agent.state {
                    let kinds: Vec<std::mem::Discriminant<IdentityEvidence>> =
                        agent.evidence.iter().map(std::mem::discriminant).collect();
                    prop_assert!(kinds.iter().any(|kind| *kind != kinds[0]));
                    prop_assert!(
                        agent
                            .evidence
                            .iter()
                            .any(|item| item.strength() == Strength::Strong)
                    );
                }
            }
            Ok(())
        },
    );
}
