//! Subscription OAuth tokens rotate under one harness session: the harness
//! refreshes its access token directly with the vendor, so the gateway sees
//! token A and later token B under the same `x-claude-code-session-id`.
//! The session, not the token, keeps the agent.

use crosstalk_spec::ids::{AgentId, CredentialHash, MessageHash};
use crosstalk_spec::interfaces::l3_reconstruction::agents::AgentReads;
use crosstalk_spec::observed::agent::IdentityEvidence;
use crosstalk_spec::observed::client::{ClientContext, CredentialRef, CredentialScheme};
use crosstalk_spec::observed::exchange::Exchange;
use crosstalk_testkit::build::ExchangeBuilder;

use super::rig::Rig;
use crate::consumer::Handled;

/// A subscription caller: an OAuth access token with digest `token`, no
/// account, harness session `session`.
fn subscriber(rig: &mut Rig, token: CredentialHash, session: &str) -> ClientContext {
    let mut client = ExchangeBuilder::new(&mut rig.scene.ids).build().meta.client;
    client.account = None;
    client.previous_digests = None;
    client.credential = Some(CredentialRef {
        scheme: CredentialScheme::OauthAccessToken,
        hash: token,
    });
    client.ids.session = Some(session.to_owned());
    client.ids.agent = None;
    client.ids.parent_agent = None;
    client
}

/// A completed exchange from `client` whose request is `history` and then a
/// new user turn `text`. Returns the exchange and the history it leaves.
async fn turn(
    rig: &mut Rig,
    client: &ClientContext,
    history: &[MessageHash],
    text: &str,
) -> (Exchange, Vec<MessageHash>) {
    let mut request = history.to_vec();
    request.push(rig.scene.user(text).await);
    let output = rig.scene.assistant(&format!("answer to {text}")).await;
    let at = rig.scene.tick();
    let exchange = ExchangeBuilder::new(&mut rig.scene.ids)
        .started_at(at)
        .client(client.clone())
        .request(request.clone())
        .response(output)
        .build();
    request.push(output);
    (exchange, request)
}

fn threaded(handled: &Handled) -> AgentId {
    match handled {
        Handled::Threaded { agent, .. } => *agent,
        other => panic!("not attributed: {other:?}"),
    }
}

/// `reconstruct.identity.refresh-keeps-session-agent`: two exchanges with
/// the same harness session id in the same scope, whose rotating
/// credentials differ, resolve to the same agent; the conversation
/// continues across the refresh, and the agent holds both token digests
/// as rotating evidence.
#[tokio::test]
async fn refresh_inside_session_keeps_agent() {
    let mut rig = Rig::new();
    let before = rig.scene.ids.credential();
    let after = rig.scene.ids.credential();
    assert_ne!(before, after);
    let old = subscriber(&mut rig, before, "session-refresh");
    let new = subscriber(&mut rig, after, "session-refresh");

    let (first, history) = turn(&mut rig, &old, &[], "start").await;
    let agent = threaded(&rig.deliver(&first).await.expect("first"));
    let (second, history) = turn(&mut rig, &old, &history, "still token A").await;
    assert_eq!(
        threaded(&rig.deliver(&second).await.expect("second")),
        agent
    );
    let (third, _) = turn(&mut rig, &new, &history, "after the refresh").await;
    assert_eq!(
        threaded(&rig.deliver(&third).await.expect("third")),
        agent,
        "a refreshed token keeps the session's agent"
    );

    let conversation = rig.delta_of(first.meta.id).expect("delta").conversation;
    assert_eq!(
        rig.delta_of(third.meta.id).expect("delta").conversation,
        conversation,
        "the conversation continues across the refresh"
    );
    let cluster = rig
        .agents
        .cluster(agent)
        .await
        .expect("cluster")
        .expect("the agent is stored");
    let evidence: Vec<&IdentityEvidence> = std::iter::once(cluster.agent())
        .chain(cluster.aliases())
        .flat_map(|agent| agent.evidence.iter())
        .collect();
    for hash in [before, after] {
        assert!(
            evidence.contains(&&IdentityEvidence::RotatingCredential(hash)),
            "{hash:?} is not attached"
        );
    }
}

/// Two sessions on one subscription token are two agents, and the shared
/// rotating digest never makes them conflict.
#[tokio::test]
async fn sessions_sharing_a_token_are_separate_agents() {
    let mut rig = Rig::new();
    let token = rig.scene.ids.credential();
    let one = subscriber(&mut rig, token, "terminal-1");
    let two = subscriber(&mut rig, token, "terminal-2");
    let (first, _) = turn(&mut rig, &one, &[], "first terminal").await;
    let a = threaded(&rig.deliver(&first).await.expect("a"));
    let (second, _) = turn(&mut rig, &two, &[], "second terminal").await;
    let b = threaded(&rig.deliver(&second).await.expect("b"));
    assert_ne!(a, b);
    let (third, _) = turn(&mut rig, &one, &[], "first terminal again").await;
    assert_eq!(threaded(&rig.deliver(&third).await.expect("a again")), a);
}
