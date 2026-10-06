//! The spec's `ConversationReads` over the conversation stores: turns,
//! the carried-over flag, the list and its filters, successors, branch
//! turns and locate.
//!
//! Scenarios are functions over any store that threads and reads, so the
//! Postgres tests (`pg_conversation_reads`) run the same ones; the
//! properties play generated harness scripts on the in-memory store.

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::ids::{AgentId, ConversationId, ExchangeId, MessageHash};
use crosstalk_spec::interfaces::l3_reconstruction::conversations::{
    ConversationQuery, ConversationReads, ReplayFilter, StoredConversation, StoredTurn,
    ThreadOutcomeKind, TurnIndex, TurnPoint, TurnSlice, TurnWindow,
};
use crosstalk_spec::interfaces::l3_reconstruction::{ThreadOutcome, Threader};
use crosstalk_spec::observed::client::{CorpusId, IngressMode, TrafficSource};
use crosstalk_spec::observed::conversation::{ConversationOrigin, OriginKind};
use crosstalk_spec::observed::exchange::Exchange;
use crosstalk_spec::observed::message::Role;
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_testkit::build::ExchangeBuilder;
use crosstalk_testkit::build::exchange::claude_code_client;
use proptest::test_runner::TestCaseError;

use super::support::{MemoryThreader, Scene};
use super::thread_props::oracle::play;
use super::thread_props::property;
use super::thread_props::script::{Mix, script};
use crate::thread::store::outcome_conversation;
use crate::thread::{ConversationStore, MemoryConversations};

pub(crate) fn window(from: u32, size: u16) -> TurnWindow {
    TurnWindow {
        from: TurnIndex(from),
        size: PageSize::new(size).unwrap_or_else(|error| panic!("page size: {error:?}")),
    }
}

/// Every turn of `id`.
pub(crate) async fn all_turns<S: ConversationReads>(store: &S, id: ConversationId) -> TurnSlice {
    match store.turns(id, &window(0, PageSize::MAX)).await {
        Ok(Some(slice)) => slice,
        other => panic!("turns of {id:?}: {other:?}"),
    }
}

pub(crate) async fn stored<S: ConversationReads>(
    store: &S,
    id: ConversationId,
) -> StoredConversation {
    match store.conversation(id).await {
        Ok(Some(stored)) => stored,
        other => panic!("conversation {id:?}: {other:?}"),
    }
}

/// Every conversation `query` admits, page by page of `size`.
pub(crate) async fn traverse<S: ConversationReads>(
    store: &S,
    query: &ConversationQuery,
    size: u16,
) -> Vec<StoredConversation> {
    let size = PageSize::new(size).unwrap_or_else(|error| panic!("page size: {error:?}"));
    let mut request = PageRequest { size, after: None };
    let mut seen = Vec::new();
    loop {
        let page = match store.list(query, &request).await {
            Ok(page) => page,
            Err(error) => panic!("list: {error:?}"),
        };
        let (items, next) = page.into_parts();
        seen.extend(items);
        match next {
            Some(cursor) => request.after = Some(cursor),
            None => return seen,
        }
    }
}

fn conversation(outcome: &ThreadOutcome) -> ConversationId {
    outcome_conversation(outcome)
}

fn replayed(
    scene: &mut Scene,
    corpus: &str,
    request: Vec<MessageHash>,
    output: MessageHash,
) -> Exchange {
    let at = scene.tick();
    let mut client = claude_code_client(&mut scene.ids);
    client.ingress = IngressMode::Replay {
        corpus: CorpusId(corpus.to_owned()),
    };
    ExchangeBuilder::new(&mut scene.ids)
        .client(client)
        .started_at(at)
        .request(request)
        .response(output)
        .build()
}

/// A conversation of three turns, a mid-conversation system turn in the
/// second, then a compaction carrying one message over, and a fork of the
/// first conversation. Returns (root, compaction, fork) and the threaded
/// exchanges in order with the turn each is.
pub(crate) struct Threaded {
    pub(crate) root: ConversationId,
    pub(crate) compaction: ConversationId,
    pub(crate) fork: ConversationId,
    pub(crate) turns: Vec<(ExchangeId, TurnPoint)>,
    pub(crate) messages: BTreeMap<&'static str, MessageHash>,
    pub(crate) agent: AgentId,
}

pub(crate) async fn thread_scenario<S: ConversationStore>(
    scene: &mut Scene,
    threader: &mut MemoryThreader<S>,
) -> Threaded {
    let agent = scene.ids.agent();
    let s = scene.system("you are a coder").await;
    let u = scene.user("fix the build").await;
    let a1 = scene.assistant("reading the log").await;
    let t1 = scene.tool("c1", "error: missing semicolon").await;
    let reminder = scene.system("reminder: run the tests").await;
    let a2 = scene.assistant("fixed it").await;
    let u3 = scene.user("now run the tests").await;
    let a3 = scene.assistant("tests pass").await;
    let mut turns = Vec::new();
    let mut thread = async |exchange: Exchange| {
        let id = exchange.meta.id;
        let outcome = threader
            .thread(&exchange, agent)
            .await
            .unwrap_or_else(|error| panic!("threaded: {error:?}"));
        (id, outcome)
    };
    let first = scene.exchange(vec![s, u], a1);
    let (e0, o0) = thread(first).await;
    let root = conversation(&o0);
    let second = scene.exchange(vec![s, u, a1, t1, reminder], a2);
    let (e1, o1) = thread(second).await;
    let third = scene.exchange(vec![s, u, a1, t1, reminder, a2, u3], a3);
    let (e2, o2) = thread(third).await;
    assert_eq!(conversation(&o1), root);
    assert_eq!(conversation(&o2), root);
    for (turn, exchange) in [e0, e1, e2].into_iter().enumerate() {
        turns.push((
            exchange,
            TurnPoint {
                conversation: root,
                turn: TurnIndex(turn as u32),
            },
        ));
    }
    // Compaction: the summary, and the first user turn carried over.
    let summary = scene
        .user("This session is being continued from a previous conversation that ran out of context. Summary: the build is fixed.")
        .await;
    let a4 = scene.assistant("continuing from the summary").await;
    let compacted = scene.exchange(vec![s, summary, u], a4);
    let (e3, o3) = thread(compacted).await;
    let ThreadOutcome::Compacts { predecessor, .. } = &o3 else {
        panic!("expected a compaction, got {o3:?}");
    };
    assert_eq!(*predecessor, root);
    let compaction = conversation(&o3);
    turns.push((
        e3,
        TurnPoint {
            conversation: compaction,
            turn: TurnIndex(0),
        },
    ));
    // Fork of the root after its first turn: [s, u, a1, t2].
    let t2 = scene.tool("c1", "error: missing brace").await;
    let a5 = scene.assistant("a different fix").await;
    let forked = scene.exchange(vec![s, u, a1, t2], a5);
    let (e4, o4) = thread(forked).await;
    let ThreadOutcome::Forks {
        parent,
        shared_prefix,
        ..
    } = &o4
    else {
        panic!("expected a fork, got {o4:?}");
    };
    assert_eq!((*parent, *shared_prefix), (root, 2));
    let fork = conversation(&o4);
    turns.push((
        e4,
        TurnPoint {
            conversation: fork,
            turn: TurnIndex(0),
        },
    ));
    let messages = BTreeMap::from([
        ("s", s),
        ("u", u),
        ("a1", a1),
        ("t1", t1),
        ("reminder", reminder),
        ("a2", a2),
        ("u3", u3),
        ("a3", a3),
        ("summary", summary),
        ("a4", a4),
        ("t2", t2),
        ("a5", a5),
    ]);
    Threaded {
        root,
        compaction,
        fork,
        turns,
        messages,
        agent,
    }
}

fn messages(turn: &StoredTurn) -> Vec<MessageHash> {
    turn.entries.iter().map(|entry| entry.message).collect()
}

/// INV-1006, INV-1007 and INV-1009 on one store: each turn's entries are
/// its exchange's transcript entries in ordinal order (new messages in
/// request order across roles, then the output), and the transcript is
/// the turns' entries after the base.
pub(crate) async fn turns_scenario<S: ConversationStore + ConversationReads>(
    store: &S,
    threaded: &Threaded,
) {
    let m = |name: &str| threaded.messages[name];
    let slice = all_turns(store, threaded.root).await;
    assert_eq!(slice.total, 3);
    let turns = &slice.turns;
    assert_eq!(messages(&turns[0]), vec![m("s"), m("u"), m("a1")]);
    assert_eq!(
        messages(&turns[1]),
        vec![m("t1"), m("reminder"), m("a2")],
        "the system turn where the request put it"
    );
    assert_eq!(messages(&turns[2]), vec![m("u3"), m("a3")]);
    let roles: Vec<Role> = turns[1].entries.iter().map(|entry| entry.role).collect();
    assert_eq!(roles, vec![Role::Tool, Role::System, Role::Assistant]);
    for turn in turns {
        let last = turn.entries.last().unwrap_or_else(|| panic!("entries"));
        assert!(last.output, "the output is last");
        assert!(turn.entries.iter().rev().skip(1).all(|entry| !entry.output));
        assert!(
            turn.entries
                .iter()
                .all(|entry| entry.exchange == turn.exchange)
        );
        assert!(turn.entries.iter().all(|entry| !entry.carried_over));
    }
    assert_eq!(
        turns.iter().map(|turn| turn.outcome).collect::<Vec<_>>(),
        vec![
            ThreadOutcomeKind::Starts,
            ThreadOutcomeKind::Extends,
            ThreadOutcomeKind::Extends
        ]
    );
    assert_eq!(
        turns
            .iter()
            .map(|turn| turn.history_end)
            .collect::<Vec<_>>(),
        vec![2, 4, 6]
    );
    let transcript = store
        .transcript(threaded.root)
        .await
        .unwrap_or_else(|error| panic!("transcript: {error:?}"));
    let from_turns: Vec<_> = turns.iter().flat_map(|turn| turn.entries.clone()).collect();
    assert_eq!(from_turns, transcript);
    // A fork's base precedes its turn 0, which holds what it added: the
    // system prompt it reports again (a new conversation's first), the
    // diverging tool result and its output.
    let fork = all_turns(store, threaded.fork).await;
    assert_eq!(fork.total, 1);
    assert_eq!(messages(&fork.turns[0]), vec![m("s"), m("t2"), m("a5")]);
    assert_eq!(fork.turns[0].outcome, ThreadOutcomeKind::Forks);
    let fork_transcript = store
        .transcript(threaded.fork)
        .await
        .unwrap_or_else(|error| panic!("transcript: {error:?}"));
    assert_eq!(fork_transcript.len(), 6);
    assert_eq!(fork.turns[0].entries[0].ordinal, 3);
    assert!(
        fork_transcript[..3]
            .iter()
            .all(|entry| entry.exchange != fork.turns[0].exchange)
    );
}

/// INV-1008: exactly the compaction's turn-0 request messages in the
/// predecessor's history are carried over.
pub(crate) async fn carried_over_scenario<S: ConversationStore + ConversationReads>(
    store: &S,
    threaded: &Threaded,
) {
    let m = |name: &str| threaded.messages[name];
    let slice = all_turns(store, threaded.compaction).await;
    assert_eq!(slice.total, 1);
    let turn = &slice.turns[0];
    assert_eq!(turn.outcome, ThreadOutcomeKind::Compacts);
    let flagged: Vec<(MessageHash, bool)> = turn
        .entries
        .iter()
        .map(|entry| (entry.message, entry.carried_over))
        .collect();
    assert_eq!(
        flagged,
        vec![
            (m("s"), false),
            (m("summary"), false),
            (m("u"), true),
            (m("a4"), false)
        ]
    );
    let fork_transcript = store
        .transcript(threaded.fork)
        .await
        .unwrap_or_else(|error| panic!("transcript: {error:?}"));
    assert!(fork_transcript.iter().all(|entry| !entry.carried_over));
}

/// INV-1003: windows are the index range, clipped to the turns there are.
pub(crate) async fn windows_scenario<S: ConversationReads>(store: &S, threaded: &Threaded) {
    let indexes = |slice: &TurnSlice| {
        slice
            .turns
            .iter()
            .map(|turn| turn.index.0)
            .collect::<Vec<_>>()
    };
    let read = async |from, size| match store.turns(threaded.root, &window(from, size)).await {
        Ok(Some(slice)) => slice,
        other => panic!("window {from}+{size}: {other:?}"),
    };
    let slice = read(0, 2).await;
    assert_eq!((slice.total, indexes(&slice)), (3, vec![0, 1]));
    let slice = read(1, 20).await;
    assert_eq!((slice.total, indexes(&slice)), (3, vec![1, 2]));
    let slice = read(3, 20).await;
    assert_eq!((slice.total, indexes(&slice)), (3, Vec::<u32>::new()));
    let slice = read(40, 1).await;
    assert_eq!((slice.total, indexes(&slice)), (3, Vec::<u32>::new()));
    let unknown = store
        .turns(ConversationId::from_ulid(0xDEAD), &window(0, 20))
        .await;
    assert_eq!(unknown, Ok(None));
}

/// INV-1010, INV-1011 and INV-1022: branch turns, successors, locate.
pub(crate) async fn links_scenario<S: ConversationReads>(store: &S, threaded: &Threaded) {
    // The fork shares 2 messages: the root's turn 0 ends at history 2.
    assert_eq!(
        store.branch_turn(threaded.root, 2).await,
        Ok(Some(TurnIndex(0)))
    );
    assert_eq!(store.branch_turn(threaded.root, 1).await, Ok(None));
    assert_eq!(
        store.branch_turn(threaded.root, 5).await,
        Ok(Some(TurnIndex(1)))
    );
    assert_eq!(
        store.branch_turn(threaded.root, 99).await,
        Ok(Some(TurnIndex(2)))
    );
    assert_eq!(
        store
            .branch_turn(ConversationId::from_ulid(0xDEAD), 2)
            .await,
        Ok(None)
    );
    let successors = store
        .successors(threaded.root)
        .await
        .unwrap_or_else(|error| panic!("successors: {error:?}"));
    let ids: Vec<ConversationId> = successors.iter().map(|s| s.conversation.id).collect();
    assert_eq!(
        ids,
        vec![threaded.compaction, threaded.fork],
        "oldest first"
    );
    assert_eq!(
        store.successors(threaded.fork).await.map(|s| s.len()),
        Ok(0)
    );
    let unthreaded = ExchangeId::from_ulid(0xBEEF);
    let batch = IdBatch::new(
        threaded
            .turns
            .iter()
            .map(|(exchange, _)| *exchange)
            .chain([unthreaded]),
    )
    .unwrap_or_else(|error| panic!("batch: {error:?}"));
    let located = store
        .locate(&batch)
        .await
        .unwrap_or_else(|error| panic!("locate: {error:?}"));
    let want: BTreeMap<ExchangeId, TurnPoint> = threaded.turns.iter().copied().collect();
    let points: BTreeMap<ExchangeId, TurnPoint> = located
        .iter()
        .map(|(exchange, placement)| (*exchange, placement.point()))
        .collect();
    assert_eq!(points, want);
    assert!(
        located
            .values()
            .all(|placement| placement.agent == threaded.agent),
        "the agent each turn was attributed to"
    );
}

/// INV-1016 (store half) and INV-1002: a conversation's source is its first
/// turn's, and the list keeps exactly what the query admits.
pub(crate) async fn filters_scenario<S: ConversationStore + ConversationReads>(
    scene: &mut Scene,
    threader: &mut MemoryThreader<S>,
    store: &S,
) {
    let live_agent = scene.ids.agent();
    let replay_agent = scene.ids.agent();
    let (u, a) = (scene.user("live task").await, scene.assistant("live").await);
    let live = threader
        .thread(&scene.exchange(vec![u], a), live_agent)
        .await
        .unwrap_or_else(|error| panic!("live: {error:?}"));
    let (u2, a2) = (
        scene.user("replayed task").await,
        scene.assistant("replay").await,
    );
    let exchange = replayed(scene, "salt-nlp", vec![u2], a2);
    let salt = threader
        .thread(&exchange, replay_agent)
        .await
        .unwrap_or_else(|error| panic!("replay: {error:?}"));
    let (u3, a3) = (
        scene.user("other corpus").await,
        scene.assistant("dojo").await,
    );
    let exchange = replayed(scene, "agentdojo", vec![u3], a3);
    let dojo = threader
        .thread(&exchange, replay_agent)
        .await
        .unwrap_or_else(|error| panic!("replay: {error:?}"));
    let (live, salt, dojo) = (
        conversation(&live),
        conversation(&salt),
        conversation(&dojo),
    );
    assert_eq!(stored(store, live).await.source, TrafficSource::Live);
    assert_eq!(
        stored(store, salt).await.source,
        TrafficSource::Replay {
            corpus: CorpusId("salt-nlp".into())
        }
    );
    // A live exchange extending the replayed conversation leaves its
    // source as its first turn's.
    let (u4, a4) = (scene.user("more").await, scene.assistant("ok").await);
    threader
        .thread(&scene.exchange(vec![u2, a2, u4], a4), replay_agent)
        .await
        .unwrap_or_else(|error| panic!("extended: {error:?}"));
    let extended = stored(store, salt).await;
    assert_eq!(extended.turns, 2);
    assert!(matches!(extended.source, TrafficSource::Replay { .. }));
    let ids = |rows: Vec<StoredConversation>| -> BTreeSet<ConversationId> {
        rows.into_iter().map(|row| row.conversation.id).collect()
    };
    let only = |agents: &[AgentId], replay: ReplayFilter| ConversationQuery {
        agents: Some(agents.iter().copied().collect()),
        origins: BTreeSet::new(),
        replay,
    };
    let both = [live_agent, replay_agent];
    assert_eq!(
        ids(traverse(store, &only(&both, ReplayFilter::Include), 1).await),
        BTreeSet::from([live, salt, dojo])
    );
    assert_eq!(
        ids(traverse(store, &only(&both, ReplayFilter::Exclude), 1).await),
        BTreeSet::from([live])
    );
    assert_eq!(
        ids(traverse(store, &only(&both, ReplayFilter::Only { corpus: None }), 2).await),
        BTreeSet::from([salt, dojo])
    );
    assert_eq!(
        ids(traverse(
            store,
            &only(
                &both,
                ReplayFilter::Only {
                    corpus: Some(CorpusId("salt-nlp".into()))
                }
            ),
            2
        )
        .await),
        BTreeSet::from([salt])
    );
    assert_eq!(
        ids(traverse(store, &only(&[live_agent], ReplayFilter::Include), 5).await),
        BTreeSet::from([live])
    );
    let roots_only = ConversationQuery {
        agents: Some(both.into_iter().collect()),
        origins: BTreeSet::from([OriginKind::Fork, OriginKind::Compaction]),
        replay: ReplayFilter::Include,
    };
    assert!(traverse(store, &roots_only, 5).await.is_empty());
}

/// INV-1001: a traversal lists every conversation once, newest first, and
/// a cursor presented with another query is refused.
pub(crate) async fn list_order_scenario<S: ConversationReads + ConversationStore>(store: &S) {
    let every = ConversationQuery::default();
    let all = store
        .conversations()
        .await
        .unwrap_or_else(|error| panic!("ids: {error:?}"));
    let mut newest_first = all.clone();
    newest_first.reverse();
    for size in [1, 2, 3, 500] {
        let listed: Vec<ConversationId> = traverse(store, &every, size)
            .await
            .into_iter()
            .map(|row| row.conversation.id)
            .collect();
        assert_eq!(listed, newest_first, "page size {size}");
    }
    let size = PageSize::new(1).unwrap_or_else(|error| panic!("{error:?}"));
    let first = store
        .list(&every, &PageRequest { size, after: None })
        .await
        .unwrap_or_else(|error| panic!("list: {error:?}"));
    let Some(cursor) = first.next().cloned() else {
        assert!(newest_first.len() <= 1, "a page of one with more to follow");
        return;
    };
    let other = ConversationQuery {
        replay: ReplayFilter::Exclude,
        ..ConversationQuery::default()
    };
    let refused = store
        .list(
            &other,
            &PageRequest {
                size,
                after: Some(cursor),
            },
        )
        .await;
    assert_eq!(
        refused.map(|page| page.items().len()),
        Err(crosstalk_spec::interfaces::l3_reconstruction::conversations::ConversationReadError::InvalidCursor)
    );
}

async fn on_memory() -> (Scene, MemoryConversations, Threaded) {
    let mut scene = Scene::new();
    let store = MemoryConversations::new();
    let mut threader = scene.threader(store.clone());
    let threaded = thread_scenario(&mut scene, &mut threader).await;
    (scene, store, threaded)
}

/// INV-1006 `reconstruct.conversation.entries-request-order`.
#[tokio::test]
async fn turn_entries_keep_request_order_across_roles() {
    let (_, store, threaded) = on_memory().await;
    turns_scenario(&store, &threaded).await;
}

/// INV-1007 `surface.conversation.inputs-are-transcript`.
#[tokio::test]
async fn turn_entries_are_the_exchanges_transcript_entries() {
    let (_, store, threaded) = on_memory().await;
    turns_scenario(&store, &threaded).await;
}

/// INV-1008 `reconstruct.conversation.carried-over`.
#[tokio::test]
async fn carried_over_marks_exactly_the_predecessors_messages() {
    let (_, store, threaded) = on_memory().await;
    carried_over_scenario(&store, &threaded).await;
}

/// INV-1003 `surface.conversation.turns-window`.
#[tokio::test]
async fn turn_windows_are_clipped_to_the_turns_there_are() {
    let (_, store, threaded) = on_memory().await;
    windows_scenario(&store, &threaded).await;
}

/// INV-1010 `surface.conversation.origin-resolved` (branch turns).
#[tokio::test]
async fn branch_turn_is_the_last_turn_inside_the_prefix() {
    let (_, store, threaded) = on_memory().await;
    links_scenario(&store, &threaded).await;
}

/// INV-1011 `reconstruct.conversation.successors-complete`.
#[tokio::test]
async fn successors_are_exactly_the_conversations_naming_it() {
    let (_, store, threaded) = on_memory().await;
    links_scenario(&store, &threaded).await;
}

/// INV-1022 `surface.conversation.locate`.
#[tokio::test]
async fn locate_names_each_threaded_exchanges_turn() {
    let (_, store, threaded) = on_memory().await;
    links_scenario(&store, &threaded).await;
}

/// INV-1016 `surface.conversation.traffic-source` (the stored source).
#[tokio::test]
async fn conversation_source_is_its_first_turns() {
    let mut scene = Scene::new();
    let store = MemoryConversations::new();
    let mut threader = scene.threader(store.clone());
    filters_scenario(&mut scene, &mut threader, &store).await;
}

/// INV-1002 `reconstruct.conversation.list-filters`.
#[tokio::test]
async fn list_keeps_exactly_what_the_query_admits() {
    let mut scene = Scene::new();
    let store = MemoryConversations::new();
    let mut threader = scene.threader(store.clone());
    filters_scenario(&mut scene, &mut threader, &store).await;
}

/// INV-1004 `reconstruct.conversation.turn-index-stable`: threading an
/// exchange again adds no turn, and later turns only append.
#[tokio::test]
async fn rethreading_adds_no_turn_and_turns_only_append() {
    let mut scene = Scene::new();
    let store = MemoryConversations::new();
    let mut threader = scene.threader(store.clone());
    let agent = scene.ids.agent();
    let (u, a1, u2, a2) = (
        scene.user("task").await,
        scene.assistant("one").await,
        scene.user("more").await,
        scene.assistant("two").await,
    );
    let first = scene.exchange(vec![u], a1);
    let root = conversation(
        &threader
            .thread(&first, agent)
            .await
            .unwrap_or_else(|e| panic!("{e:?}")),
    );
    let before = all_turns(&store, root).await;
    threader
        .thread(&first, agent)
        .await
        .unwrap_or_else(|error| panic!("again: {error:?}"));
    assert_eq!(
        all_turns(&store, root).await,
        before,
        "a redelivery adds nothing"
    );
    let second = scene.exchange(vec![u, a1, u2], a2);
    threader
        .thread(&second, agent)
        .await
        .unwrap_or_else(|error| panic!("second: {error:?}"));
    let after = all_turns(&store, root).await;
    assert_eq!(after.total, 2);
    assert_eq!(after.turns[0], before.turns[0], "turn 0 unchanged");
    assert_eq!(after.turns[1].exchange, second.meta.id);
    threader
        .thread(&second, agent)
        .await
        .unwrap_or_else(|error| panic!("again: {error:?}"));
    assert_eq!(all_turns(&store, root).await, after);
}

/// INV-1001 `reconstruct.conversation.list-order`, on generated scripts:
/// every page size traverses every conversation once, newest first.
#[test]
fn list_traversal_matches_the_model() {
    property(24, script(Mix::FORKS, 24), |ops| async move {
        let (_, threader, _, _) = play(&ops).await?;
        list_order_scenario(threader.store()).await;
        Ok(())
    });
}

/// INV-1005 `reconstruct.conversation.turns-rebuild-history`, on generated
/// scripts of every mix: the base followed by each turn's non-system
/// entries is the stored history; turns cover the transcript after the
/// base once, in order; each turn's entries are its exchange's, its
/// `history_end` the history so far; every threaded exchange is located
/// at its turn; and carried-over entries sit only on a compaction's turn 0
/// and only on messages its predecessor holds.
#[test]
fn turns_rebuild_the_stored_history() {
    for mix in [Mix::GENERAL, Mix::FORKS, Mix::COMPACTIONS] {
        property(24, script(mix, 24), |ops| async move {
            let (_, threader, _, steps) = play(&ops).await?;
            let store = threader.store();
            rebuilds(store).await?;
            let batch = IdBatch::new(steps.iter().map(|step| step.exchange.meta.id))
                .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
            let located = store
                .locate(&batch)
                .await
                .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
            for (exchange, placement) in located {
                let point = placement.point();
                let slice = all_turns(store, point.conversation).await;
                let turn = slice
                    .turns
                    .get(point.turn.0 as usize)
                    .ok_or_else(|| TestCaseError::fail("located past the turns"))?;
                if turn.exchange != exchange {
                    return Err(TestCaseError::fail(format!(
                        "{exchange:?} located at {point:?}"
                    )));
                }
            }
            Ok(())
        });
    }
}

async fn rebuilds(store: &MemoryConversations) -> Result<(), TestCaseError> {
    let fail = |what: String| Err(TestCaseError::fail(what));
    for id in store
        .conversations()
        .await
        .map_err(|error| TestCaseError::fail(format!("{error:?}")))?
    {
        let conversation = stored(store, id).await;
        let transcript = store
            .transcript(id)
            .await
            .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
        let slice = all_turns(store, id).await;
        if slice.total != conversation.turns || slice.total as usize != slice.turns.len() {
            return fail(format!("{id:?}: turn counts disagree"));
        }
        let base = slice
            .turns
            .first()
            .and_then(|turn| turn.entries.first().map(|entry| entry.ordinal))
            .unwrap_or(transcript.len() as u32);
        let mut history: Vec<MessageHash> = transcript
            .iter()
            .take(base as usize)
            .filter(|entry| entry.history_index.is_some())
            .map(|entry| entry.message)
            .collect();
        if !matches!(
            conversation.conversation.origin,
            ConversationOrigin::Fork { .. }
        ) && !history.is_empty()
        {
            return fail(format!("{id:?}: a base outside a fork"));
        }
        let mut next = base;
        for turn in &slice.turns {
            for entry in &turn.entries {
                if entry.ordinal != next || entry.exchange != turn.exchange {
                    return fail(format!("{id:?}: turn {} entry {entry:?}", turn.index.0));
                }
                next += 1;
                if entry.history_index.is_some() {
                    history.push(entry.message);
                }
                let compaction_turn_zero = turn.index.0 == 0
                    && matches!(
                        conversation.conversation.origin,
                        ConversationOrigin::Compaction { .. }
                    );
                if entry.carried_over {
                    let ConversationOrigin::Compaction { predecessor } =
                        conversation.conversation.origin
                    else {
                        return fail(format!("{id:?}: carried over outside a compaction"));
                    };
                    let held = stored(store, predecessor).await.conversation.messages;
                    if !compaction_turn_zero
                        || entry.output
                        || entry.history_index.is_none()
                        || !held.contains(&entry.message)
                    {
                        return fail(format!("{id:?}: {entry:?} wrongly carried over"));
                    }
                }
            }
            if turn.history_end as usize != history.len() {
                return fail(format!("{id:?}: turn {} history_end", turn.index.0));
            }
        }
        if next as usize != transcript.len() {
            return fail(format!("{id:?}: turns leave entries out"));
        }
        if history != conversation.conversation.messages {
            return fail(format!("{id:?}: turns do not rebuild the history"));
        }
    }
    Ok(())
}
