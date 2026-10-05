//! `reconstruct.delta.excludes-seen-elsewhere`: a delta's `new_inputs`
//! leave out messages the agent's cluster already saw in another
//! conversation within the retention, on both conversation stores.
//!
//! Each scenario is a function over any [`ConversationStore`]; the memory
//! tests run them on [`MemoryConversations`], `pg_*` on
//! [`PgConversations`] (gated on `TEST_DATABASE_URL`).

use std::time::Duration;

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::interfaces::l3_reconstruction::{ThreadOutcome, Threader};
use crosstalk_spec::support::Timestamp;
use crosstalk_testkit::time::T0;

use super::pg::{close, database, truncate};
use super::support::{Clusters, Scene};
use crate::thread::store::outcome_conversation;
use crate::thread::{
    ConversationStore, MemoryConversations, PgConversations, SeenRetention, ThreadConfig,
    ThreadConfigError,
};

fn kind(outcome: &ThreadOutcome) -> &'static str {
    crate::thread::outcome_kind(outcome)
}

/// An episode of two turns: `[opening, a1, t1, a2]`'s history and its
/// messages, threaded for `agent` with `opening` as the first user turn.
struct Episode {
    history: Vec<MessageHash>,
}

async fn episode<S: ConversationStore>(
    scene: &mut Scene,
    threader: &mut super::support::MemoryThreader<S>,
    agent: crosstalk_spec::ids::AgentId,
    tag: &str,
) -> Episode {
    let opening = scene.user(&format!("{tag}: begin the task")).await;
    let a1 = scene.assistant(&format!("{tag}: reading the log")).await;
    let t1 = scene
        .tool(&format!("{tag}-c1"), &format!("{tag}: log line"))
        .await;
    let a2 = scene.assistant(&format!("{tag}: done")).await;
    threader
        .thread(&scene.exchange(vec![opening], a1), agent)
        .await
        .expect("first turn");
    threader
        .thread(&scene.exchange(vec![opening, a1, t1], a2), agent)
        .await
        .expect("second turn");
    Episode {
        history: vec![opening, a1, t1, a2],
    }
}

/// A new conversation whose opening history repeats an earlier
/// conversation's messages yields no new inputs for them; its genuinely
/// new messages still appear, in order, and its output is reported.
async fn replayed_opening_history<S: ConversationStore + Clone>(store: S) {
    let mut scene = Scene::new();
    let mut threader = scene.threader(store);
    let agent = scene.ids.agent();
    let first = episode(&mut scene, &mut threader, agent, "episode one").await;
    // The episode restarts: a new opening turn, the earlier transcript
    // carried as history, then a new message.
    let restart = scene
        .user("episode two: here is what happened before")
        .await;
    let fresh = scene.user("episode two: continue").await;
    let output = scene.assistant("episode two: continuing").await;
    let mut request = vec![restart];
    request.extend(first.history.iter().copied());
    request.push(fresh);
    let outcome = threader
        .thread(&scene.exchange(request, output), agent)
        .await
        .expect("restart");
    let ThreadOutcome::Starts { delta, .. } = &outcome else {
        panic!("expected a new conversation, got {outcome:?}");
    };
    assert_eq!(delta.new_inputs, vec![restart, fresh]);
    assert_eq!(delta.output, Some(output));
}

/// A message replayed later in a conversation (not in its opening) is
/// withheld too, while a new message in the same delta appears.
async fn later_replayed_message<S: ConversationStore + Clone>(store: S) {
    let mut scene = Scene::new();
    let mut threader = scene.threader(store);
    let agent = scene.ids.agent();
    let first = episode(&mut scene, &mut threader, agent, "first").await;
    let opening = scene.user("second: another task").await;
    let a1 = scene.assistant("second: looking").await;
    let outcome = threader
        .thread(&scene.exchange(vec![opening], a1), agent)
        .await
        .expect("second conversation");
    let second = outcome_conversation(&outcome);
    // The harness pastes the first conversation's tool result again, next
    // to a new one.
    let replayed = first.history[2];
    let new_result = scene.tool("second-c1", "second: a new line").await;
    let a2 = scene.assistant("second: compared").await;
    let outcome = threader
        .thread(
            &scene.exchange(vec![opening, a1, replayed, new_result], a2),
            agent,
        )
        .await
        .expect("extension");
    let ThreadOutcome::Extends {
        conversation,
        delta,
    } = &outcome
    else {
        panic!("expected an extension, got {outcome:?}");
    };
    assert_eq!(*conversation, second);
    assert_eq!(delta.new_inputs, vec![new_result]);
}

/// Different agents do not share the set: another agent opening with the
/// same messages gets all of them as new inputs.
async fn agents_do_not_share<S: ConversationStore + Clone>(store: S) {
    let mut scene = Scene::new();
    let mut threader = scene.threader(store);
    let alice = scene.ids.agent();
    let bob = scene.ids.agent();
    let first = episode(&mut scene, &mut threader, alice, "shared").await;
    let restart = scene.user("bob: picking this up").await;
    let output = scene.assistant("bob: on it").await;
    let mut request = vec![restart];
    request.extend(first.history.iter().copied());
    let outcome = threader
        .thread(&scene.exchange(request.clone(), output), bob)
        .await
        .expect("bob");
    assert_eq!(kind(&outcome), "starts");
    assert_eq!(outcome.delta().new_inputs, request);
}

/// A message repeated inside one conversation is not withheld: only
/// another conversation's sighting counts.
async fn same_conversation_repeat_is_new<S: ConversationStore + Clone>(store: S) {
    let mut scene = Scene::new();
    let mut threader = scene.threader(store);
    let agent = scene.ids.agent();
    let u = scene.user("poll the queue").await;
    let a1 = scene.assistant("polling").await;
    let empty = scene.tool("poll", "queue empty").await;
    let a2 = scene.assistant("polling again").await;
    let a3 = scene.assistant("still empty").await;
    threader
        .thread(&scene.exchange(vec![u], a1), agent)
        .await
        .expect("first");
    threader
        .thread(&scene.exchange(vec![u, a1, empty], a2), agent)
        .await
        .expect("second");
    let outcome = threader
        .thread(&scene.exchange(vec![u, a1, empty, a2, empty], a3), agent)
        .await
        .expect("third");
    assert_eq!(kind(&outcome), "extends");
    assert_eq!(outcome.delta().new_inputs, vec![empty]);
}

/// An agent's own earlier output carried into a new conversation's
/// history is withheld like a received message. A byte-identical output
/// is still this exchange's output: outputs are never withheld.
async fn own_output_replayed<S: ConversationStore + Clone>(store: S) {
    let mut scene = Scene::new();
    let mut threader = scene.threader(store);
    let agent = scene.ids.agent();
    let u = scene.user("draft a reply").await;
    let draft = scene.assistant("Dear Bob, the report is attached.").await;
    threader
        .thread(&scene.exchange(vec![u], draft), agent)
        .await
        .expect("first");
    let restart = scene.user("restart: send your last draft again").await;
    let outcome = threader
        .thread(&scene.exchange(vec![restart, draft], draft), agent)
        .await
        .expect("restart");
    assert_eq!(kind(&outcome), "starts");
    assert_eq!(outcome.delta().new_inputs, vec![restart]);
    assert_eq!(outcome.delta().output, Some(draft));
}

/// A merged cluster shares the set: a message one member saw is withheld
/// from another member's new conversation.
async fn cluster_shares<S: ConversationStore + Clone>(store: S) {
    let mut scene = Scene::new();
    let first_agent = scene.ids.agent();
    let second_agent = scene.ids.agent();
    let mut clusters = Clusters::default();
    clusters.join(&[first_agent, second_agent]);
    let mut threader = scene.threader_in(store, clusters);
    let first = episode(&mut scene, &mut threader, first_agent, "merged").await;
    let restart = scene.user("merged: resumed under another key").await;
    let output = scene.assistant("merged: resuming").await;
    let mut request = vec![restart];
    request.extend(first.history.iter().copied());
    let outcome = threader
        .thread(&scene.exchange(request, output), second_agent)
        .await
        .expect("second member");
    assert_eq!(kind(&outcome), "starts");
    assert_eq!(outcome.delta().new_inputs, vec![restart]);
}

/// Sightings older than the retention no longer withhold: the same
/// replay after the window reports the messages again.
async fn retention_bounds_the_set<S: ConversationStore + Clone>(store: S) {
    let mut scene = Scene::new();
    let mut threader = scene.threader(store);
    let agent = scene.ids.agent();
    let first = episode(&mut scene, &mut threader, agent, "old").await;
    // Ten seconds of retention (see the callers); a minute later.
    scene.clock += 60;
    let restart = scene.user("old: long after").await;
    let output = scene.assistant("old: again").await;
    let mut request = vec![restart];
    request.extend(first.history.iter().copied());
    let outcome = threader
        .thread(&scene.exchange(request.clone(), output), agent)
        .await
        .expect("late restart");
    assert_eq!(kind(&outcome), "starts");
    assert_eq!(outcome.delta().new_inputs, request);
}

fn ten_seconds() -> ThreadConfig {
    match SeenRetention::new(Duration::from_secs(10)) {
        Ok(seen_retention) => ThreadConfig { seen_retention },
        Err(error) => panic!("retention refused: {error}"),
    }
}

/// `reconstruct.delta.excludes-seen-elsewhere`.
#[tokio::test]
async fn replayed_opening_history_is_not_new_input() {
    replayed_opening_history(MemoryConversations::new()).await;
}

/// `reconstruct.delta.excludes-seen-elsewhere`.
#[tokio::test]
async fn later_replayed_message_is_not_new_input() {
    later_replayed_message(MemoryConversations::new()).await;
}

/// `reconstruct.delta.excludes-seen-elsewhere`.
#[tokio::test]
async fn different_agents_do_not_share_seen_messages() {
    agents_do_not_share(MemoryConversations::new()).await;
}

/// `reconstruct.delta.excludes-seen-elsewhere`.
#[tokio::test]
async fn repeat_within_a_conversation_is_new_input() {
    same_conversation_repeat_is_new(MemoryConversations::new()).await;
}

/// `reconstruct.delta.excludes-seen-elsewhere`.
#[tokio::test]
async fn replayed_own_output_is_not_new_input() {
    own_output_replayed(MemoryConversations::new()).await;
}

/// `reconstruct.delta.excludes-seen-elsewhere`.
#[tokio::test]
async fn merged_cluster_shares_seen_messages() {
    cluster_shares(MemoryConversations::new()).await;
}

/// `reconstruct.delta.excludes-seen-elsewhere`.
#[tokio::test]
async fn seen_messages_expire_after_retention() {
    retention_bounds_the_set(MemoryConversations::with_config(ten_seconds())).await;
}

/// The default retention, the checks, and JSON decoding through them.
#[test]
fn thread_config_checks_and_decodes() {
    assert_eq!(
        ThreadConfig::default().seen_retention.as_duration(),
        Duration::from_secs(30 * 24 * 60 * 60)
    );
    assert_eq!(
        SeenRetention::new(Duration::ZERO),
        Err(ThreadConfigError::ZeroRetention)
    );
    assert_eq!(
        SeenRetention::new(Duration::from_secs(u64::MAX)),
        Err(ThreadConfigError::RetentionTooLong)
    );
    let decoded: ThreadConfig =
        serde_json::from_str(r#"{"seen_retention_secs": 10}"#).expect("decodes");
    assert_eq!(decoded, ten_seconds());
    let defaulted: ThreadConfig = serde_json::from_str("{}").expect("decodes");
    assert_eq!(defaulted, ThreadConfig::default());
    assert!(serde_json::from_str::<ThreadConfig>(r#"{"seen_retention_secs": 0}"#).is_err());
    assert!(serde_json::from_str::<ThreadConfig>(r#"{"retention": 10}"#).is_err());
    let retention = ten_seconds().seen_retention;
    assert_eq!(
        retention.cutoff(Timestamp::from_micros(25_000_000)),
        Timestamp::from_micros(15_000_000)
    );
    assert_eq!(
        retention.cutoff(Timestamp::from_micros(5)),
        Timestamp::from_micros(0)
    );
}

/// A scenario over the Postgres store.
type PgScenario = fn(PgConversations) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>>;

/// Run each of `scenarios` on an emptied Postgres conversation store (the
/// scenarios mint the same ids).
async fn on_pg(test: &str, config: ThreadConfig, scenarios: &[PgScenario]) {
    let Some(db) = database(test).await else {
        return;
    };
    for scenario in scenarios {
        truncate(db.pool()).await;
        scenario(PgConversations::new(db.pool().clone()).with_config(config)).await;
    }
    close(db).await;
}

/// `reconstruct.delta.excludes-seen-elsewhere`, on Postgres: every
/// scenario above.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_seen_messages_are_withheld() {
    on_pg(
        "pg_seen_messages_are_withheld",
        ThreadConfig::default(),
        &[
            |store| Box::pin(replayed_opening_history(store)),
            |store| Box::pin(later_replayed_message(store)),
            |store| Box::pin(agents_do_not_share(store)),
            |store| Box::pin(same_conversation_repeat_is_new(store)),
            |store| Box::pin(own_output_replayed(store)),
            |store| Box::pin(cluster_shares(store)),
        ],
    )
    .await;
}

/// `reconstruct.delta.excludes-seen-elsewhere`, on Postgres: expiry, and
/// the sweep of agents that stopped calling.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_seen_messages_expire_after_retention() {
    on_pg(
        "pg_seen_messages_expire_after_retention",
        ten_seconds(),
        &[|store| {
            Box::pin(async move {
                retention_bounds_the_set(store.clone()).await;
                // The late restart's sightings are all that is left.
                let later = Timestamp::from_micros(T0.as_micros() + 3_600_000_000);
                let swept = store.forget_seen(later).await.expect("swept");
                assert!(swept > 0, "nothing left to sweep");
                assert_eq!(store.forget_seen(later).await.expect("swept"), 0);
            })
        }],
    )
    .await;
}
