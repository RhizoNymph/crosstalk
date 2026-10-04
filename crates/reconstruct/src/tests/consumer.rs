//! The reconstruct consumer on hand-picked exchanges: agent creation and
//! state, parents, conflicts, attribution, and conversations across
//! merges.

use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l3_reconstruction::agents::AgentReads;
use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::{
    AgentLifecycle, AgentOrigin, NewAgent,
};
use crosstalk_spec::interfaces::l3_reconstruction::IdentityResolver;
use crosstalk_spec::observed::agent::{
    Agent, AgentState, IdentityEvidence, MergeAuthor, MergeRequest,
};
use crosstalk_spec::observed::client::{ClientContext, CredentialRef, CredentialScheme};
use crosstalk_spec::observed::exchange::Exchange;
use crosstalk_spec::support::{NonEmpty, Timestamp};
use crosstalk_testkit::build::ExchangeBuilder;

use super::rig::Rig;
use crate::consumer::Handled;
use crate::evidence::PromptFingerprintEvidence;
use crate::thread::ConversationStore;

/// A completed exchange from `client` with request `[user]`.
async fn exchange(rig: &mut Rig, client: &ClientContext, text: &str) -> Exchange {
    let user = rig.scene.user(text).await;
    let output = rig.scene.assistant(&format!("answer to {text}")).await;
    let at = rig.scene.tick();
    ExchangeBuilder::new(&mut rig.scene.ids)
        .started_at(at)
        .client(client.clone())
        .request(vec![user])
        .response(output)
        .build()
}

/// A Claude Code caller with a fresh API key and no harness ids.
fn caller(rig: &mut Rig) -> ClientContext {
    let mut client = ExchangeBuilder::new(&mut rig.scene.ids).build().meta.client;
    client.ids.session = None;
    client
}

fn agent_of(handled: &Handled) -> AgentId {
    match handled {
        Handled::Threaded { agent, .. } => *agent,
        other => panic!("not attributed: {other:?}"),
    }
}

async fn record(rig: &Rig, id: AgentId) -> Agent {
    let cluster = rig
        .agents
        .cluster(id)
        .await
        .expect("cluster")
        .expect("stored");
    std::iter::once(cluster.agent())
        .chain(cluster.aliases())
        .find(|agent| agent.id == id)
        .cloned()
        .expect("in its cluster")
}

/// `reconstruct.agent-state.traffic-leaves-registered`: a new agent from
/// traffic is provisional.
#[tokio::test]
async fn discovered_agent_starts_provisional() {
    let mut rig = Rig::new();
    let client = caller(&mut rig);
    let first = exchange(&mut rig, &client, "hello").await;
    let agent = agent_of(&rig.deliver(&first).await.expect("handled"));
    assert_eq!(
        record(&rig, agent).await.state,
        AgentState::Provisional {
            first_seen: first.meta.started_at
        }
    );
}

/// `reconstruct.agent-state.traffic-leaves-registered`: a registered
/// agent's first exchange moves it to provisional.
#[tokio::test]
async fn first_exchange_moves_registered_to_provisional() {
    let mut rig = Rig::new();
    let client = caller(&mut rig);
    let hash = client.credential.map(|c| c.hash).expect("credential");
    let registered = rig.scene.ids.agent();
    rig.agents
        .create(NewAgent {
            id: registered,
            evidence: NonEmpty::new(IdentityEvidence::StableCredential(hash)),
            parent: None,
            origin: AgentOrigin::Config {
                at: Timestamp::from_micros(1),
            },
            label: None,
        })
        .await
        .expect("registered");
    let first = exchange(&mut rig, &client, "hello").await;
    let agent = agent_of(&rig.deliver(&first).await.expect("handled"));
    assert_eq!(agent, registered);
    assert_eq!(
        record(&rig, agent).await.state,
        AgentState::Provisional {
            first_seen: first.meta.started_at
        }
    );
    // Seen again with a second kind of evidence: corroborated, established.
    let second = exchange(&mut rig, &client, "again").await;
    rig.deliver(&second).await.expect("handled");
    assert!(matches!(
        record(&rig, agent).await.state,
        AgentState::Established { .. }
    ));
}

/// `reconstruct.agent.parent-derivation`: a harness parent agent id in the
/// same scope names the parent.
#[tokio::test]
async fn parent_from_harness_parent_id_in_scope() {
    let mut rig = Rig::new();
    let mut parent_client = caller(&mut rig);
    parent_client.ids.agent = Some("planner".to_owned());
    let first = exchange(&mut rig, &parent_client, "plan").await;
    let parent = agent_of(&rig.deliver(&first).await.expect("parent"));
    let mut child_client = parent_client.clone();
    child_client.ids.agent = Some("worker".to_owned());
    child_client.ids.parent_agent = Some("planner".to_owned());
    let second = exchange(&mut rig, &child_client, "work").await;
    let child = agent_of(&rig.deliver(&second).await.expect("child"));
    assert_ne!(child, parent);
    assert_eq!(record(&rig, child).await.parent, Some(parent));
}

/// `reconstruct.agent.parent-derivation`: a parent id under another scope
/// names no one.
#[tokio::test]
async fn parent_id_from_other_scope_is_ignored() {
    let mut rig = Rig::new();
    let mut parent_client = caller(&mut rig);
    parent_client.ids.agent = Some("planner".to_owned());
    let first = exchange(&mut rig, &parent_client, "plan").await;
    rig.deliver(&first).await.expect("parent");
    let mut child_client = caller(&mut rig);
    child_client.ids.agent = Some("worker".to_owned());
    child_client.ids.parent_agent = Some("planner".to_owned());
    let second = exchange(&mut rig, &child_client, "work").await;
    let child = agent_of(&rig.deliver(&second).await.expect("child"));
    assert_eq!(record(&rig, child).await.parent, None);
}

/// `reconstruct.agent.parent-derivation`: a harness sub-agent without a
/// parent id is its session's main agent's child.
#[tokio::test]
async fn subagent_parent_is_session_main_agent() {
    let mut rig = Rig::new();
    let mut main_client = caller(&mut rig);
    main_client.ids.session = Some("session-1".to_owned());
    let first = exchange(&mut rig, &main_client, "main").await;
    let main = agent_of(&rig.deliver(&first).await.expect("main"));
    let mut sub_client = main_client.clone();
    sub_client.ids.agent = Some("explorer".to_owned());
    sub_client.class = crosstalk_spec::observed::client::RequestClass::Subagent;
    let second = exchange(&mut rig, &sub_client, "explore").await;
    let sub = agent_of(&rig.deliver(&second).await.expect("sub"));
    assert_ne!(sub, main);
    assert_eq!(record(&rig, sub).await.parent, Some(main));
    // The session id keeps resolving to the main agent only.
    let third = exchange(&mut rig, &main_client, "main again").await;
    assert_eq!(agent_of(&rig.deliver(&third).await.expect("main")), main);
}

/// `reconstruct.agent.parent-derivation`: a main agent has no parent.
#[tokio::test]
async fn main_agent_has_no_parent() {
    let mut rig = Rig::new();
    let mut client = caller(&mut rig);
    client.ids.session = Some("session-1".to_owned());
    let first = exchange(&mut rig, &client, "main").await;
    let main = agent_of(&rig.deliver(&first).await.expect("main"));
    assert_eq!(record(&rig, main).await.parent, None);
}

/// `reconstruct.delta.agent-is-attributed`: a delta names the agent the
/// exchange was attributed to; after a merge its recorded delta still
/// does.
#[tokio::test]
async fn delta_carries_attributed_agent_id() {
    let mut rig = Rig::new();
    let a_client = caller(&mut rig);
    let b_client = caller(&mut rig);
    let first = exchange(&mut rig, &a_client, "from a").await;
    let a = agent_of(&rig.deliver(&first).await.expect("a"));
    let second = exchange(&mut rig, &b_client, "from b").await;
    let b = agent_of(&rig.deliver(&second).await.expect("b"));
    assert_eq!(rig.delta_of(first.meta.id).map(|d| d.agent), Some(a));
    let request = MergeRequest::new(a, b, MergeAuthor::Resolver).expect("two agents");
    rig.agents
        .merge(request, Timestamp::from_micros(9))
        .await
        .expect("merged");
    // Redelivered after the merge: the recorded delta, still naming a.
    match rig.deliver(&first).await.expect("redelivered") {
        Handled::Threaded { outcome, .. } => assert_eq!(outcome.delta().agent, a),
        other => panic!("{other:?}"),
    }
}

/// `reconstruct.agent-merge.records-keep-ids`: merging the agents of two
/// conversations changes neither conversation's agent, and a later
/// exchange of the merged cluster continues the conversation it extends.
#[tokio::test]
async fn merge_leaves_conversation_agent_ids() {
    let mut rig = Rig::new();
    let a_client = caller(&mut rig);
    let b_client = caller(&mut rig);
    let first = exchange(&mut rig, &a_client, "from a").await;
    let (a, a_conversation) = match rig.deliver(&first).await.expect("a") {
        Handled::Threaded { agent, outcome } => (agent, outcome.delta().conversation),
        other => panic!("{other:?}"),
    };
    let second = exchange(&mut rig, &b_client, "from b").await;
    let b = agent_of(&rig.deliver(&second).await.expect("b"));
    let request = MergeRequest::new(a, b, MergeAuthor::Resolver).expect("two agents");
    rig.agents
        .merge(request, Timestamp::from_micros(9))
        .await
        .expect("merged");
    for id in rig.conversations.conversations().await.expect("list") {
        let stored = rig
            .conversations
            .conversation(id)
            .await
            .expect("read")
            .expect("stored");
        assert!(stored.agent == a || stored.agent == b);
        if id == a_conversation {
            assert_eq!(stored.agent, a);
        }
    }
    // b (now the cluster's canonical agent) continues a's conversation.
    let mut request = first.request.clone();
    if let crosstalk_spec::observed::exchange::ExchangeOutcome::Completed { response, .. } =
        &first.outcome
    {
        request.push(*response);
    }
    request.push(rig.scene.user("continued by b").await);
    let output = rig.scene.assistant("ok").await;
    let at = rig.scene.tick();
    let third = ExchangeBuilder::new(&mut rig.scene.ids)
        .started_at(at)
        .client(b_client.clone())
        .request(request)
        .response(output)
        .build();
    match rig.deliver(&third).await.expect("b again") {
        Handled::Threaded { agent, outcome } => {
            assert_eq!(agent, b);
            assert_eq!(outcome.delta().conversation, a_conversation);
        }
        other => panic!("{other:?}"),
    }
    let stored = rig
        .conversations
        .conversation(a_conversation)
        .await
        .expect("read")
        .expect("stored");
    assert_eq!(stored.agent, a);
}

/// Two agents holding the same prompt fingerprint, and an exchange whose
/// most specific evidence is that fingerprint.
async fn weak_conflict(rig: &mut Rig) -> (Exchange, AgentId, AgentId) {
    let mut client = caller(rig);
    client.credential = Some(CredentialRef {
        scheme: CredentialScheme::OauthAccessToken,
        hash: rig.scene.ids.credential(),
    });
    let shared = exchange(rig, &client, "shared prompt").await;
    let opening = vec![rig.scene.messages.message(shared.request[0]).await.expect("body")];
    let fingerprint = PromptFingerprintEvidence::fingerprint(&opening).expect("fingerprint");
    let (x, y) = (rig.scene.ids.agent(), rig.scene.ids.agent());
    for id in [x, y] {
        rig.agents
            .create(NewAgent {
                id,
                evidence: NonEmpty::new(IdentityEvidence::PromptFingerprint(fingerprint)),
                parent: None,
                origin: AgentOrigin::Traffic {
                    first_seen: Timestamp::from_micros(1),
                },
                label: None,
            })
            .await
            .expect("created");
    }
    (shared, x, y)
}

/// `reconstruct.resolve.conflict-not-silently-attributed`: a conflict on
/// weak evidence, or one a veto keeps apart, is left for review: no
/// attribution, no delta, no merge.
#[tokio::test]
async fn conflict_without_merge_is_left_for_review() {
    let mut rig = Rig::new();
    let (shared, x, y) = weak_conflict(&mut rig).await;
    rig.store_events();
    match rig.deliver(&shared).await.expect("handled") {
        Handled::Review { candidates } => {
            assert_eq!(candidates.iter().copied().collect::<Vec<_>>(), vec![x, y]);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(rig.delta_of(shared.meta.id), None);
    assert!(
        !rig.store_events()
            .iter()
            .any(|event| matches!(event, BusEvent::Ingest(IngestEvent::AgentMerged { .. })))
    );
    // A strong conflict an operator vetoed is also left for review.
    let mut rig = Rig::new();
    let client = caller(&mut rig);
    let hash = client.credential.map(|c| c.hash).expect("credential");
    let (p, q) = (rig.scene.ids.agent(), rig.scene.ids.agent());
    for id in [p, q] {
        rig.agents
            .create(NewAgent {
                id,
                evidence: NonEmpty::new(IdentityEvidence::StableCredential(hash)),
                parent: None,
                origin: AgentOrigin::Traffic {
                    first_seen: Timestamp::from_micros(1),
                },
                label: None,
            })
            .await
            .expect("created");
    }
    let operator = rig.scene.ids.operator();
    let record = rig
        .agents
        .merge(
            MergeRequest::new(q, p, MergeAuthor::Operator(operator)).expect("two"),
            Timestamp::from_micros(2),
        )
        .await
        .expect("merged");
    rig.agents
        .unmerge(record.id(), operator, Timestamp::from_micros(3))
        .await
        .expect("unmerged");
    let vetoed = exchange(&mut rig, &client, "vetoed").await;
    assert!(matches!(
        rig.deliver(&vetoed).await.expect("handled"),
        Handled::Review { .. }
    ));
    assert_eq!(rig.delta_of(vetoed.meta.id), None);
}

/// `reconstruct.resolve.conflict-not-silently-attributed`: a conflict on
/// strong evidence is attributed only through the merge that settles it,
/// which publishes `AgentMerged`.
#[tokio::test]
async fn conflict_attributed_only_with_merge() {
    let mut rig = Rig::new();
    let client = caller(&mut rig);
    let hash = client.credential.map(|c| c.hash).expect("credential");
    let (p, q) = (rig.scene.ids.agent(), rig.scene.ids.agent());
    for id in [p, q] {
        rig.agents
            .create(NewAgent {
                id,
                evidence: NonEmpty::new(IdentityEvidence::StableCredential(hash)),
                parent: None,
                origin: AgentOrigin::Traffic {
                    first_seen: Timestamp::from_micros(1),
                },
                label: None,
            })
            .await
            .expect("created");
    }
    rig.store_events();
    let first = exchange(&mut rig, &client, "who am i").await;
    let agent = agent_of(&rig.deliver(&first).await.expect("handled"));
    assert_eq!(agent, p.min(q));
    let merged: Vec<BusEvent> = rig
        .store_events()
        .into_iter()
        .filter(|event| matches!(event, BusEvent::Ingest(IngestEvent::AgentMerged { .. })))
        .collect();
    assert_eq!(merged.len(), 1, "{merged:?}");
    match &merged[0] {
        BusEvent::Ingest(IngestEvent::AgentMerged { from, into, by, .. }) => {
            assert_eq!(*into, agent);
            assert_eq!(*from, p.max(q));
            assert_eq!(*by, MergeAuthor::Resolver);
        }
        other => panic!("{other:?}"),
    }
}

/// An exchange carrying no identity evidence is not attributed.
#[tokio::test]
async fn exchange_without_evidence_is_unattributed() {
    let mut rig = Rig::new();
    let mut client = caller(&mut rig);
    client.credential = None;
    let user = rig.scene.assistant("no user turn, no fingerprint").await;
    let output = rig.scene.assistant("reply").await;
    let at = rig.scene.tick();
    let exchange = ExchangeBuilder::new(&mut rig.scene.ids)
        .started_at(at)
        .client(client)
        .request(vec![user])
        .response(output)
        .build();
    assert_eq!(
        rig.deliver(&exchange).await.expect("handled"),
        Handled::Unattributed
    );
}
