//! Threading on hand-picked histories: outcomes, origins, failed
//! exchanges, increments, compaction evidence, system turns, and the
//! regression cases the dataset fixtures revealed.

use crosstalk_spec::interfaces::l3_reconstruction::{ThreadOutcome, Threader};
use crosstalk_spec::observed::client::RequestClass;
use crosstalk_spec::observed::conversation::ConversationOrigin;
use crosstalk_spec::observed::exchange::ResponseId;
use crosstalk_spec::observed::message::Role;
use crosstalk_testkit::build::ExchangeBuilder;

use super::support::Scene;
use crate::thread::store::outcome_conversation;
use crate::thread::{ConversationStore, MemoryConversations};

fn conversation(outcome: &ThreadOutcome) -> crosstalk_spec::ids::ConversationId {
    outcome_conversation(outcome)
}

/// `reconstruct.thread.origin-matches-outcome`: a start stores a root.
#[tokio::test]
async fn start_stores_root_origin() {
    let mut scene = Scene::new();
    let store = MemoryConversations::new();
    let mut threader = scene.threader(store.clone());
    let (s, u, a) = (
        scene.system("sys").await,
        scene.user("hello").await,
        scene.assistant("hi").await,
    );
    let agent = scene.ids.agent();
    let exchange = scene.exchange(vec![s, u], a);
    let outcome = threader.thread(&exchange, agent).await.expect("threaded");
    let ThreadOutcome::Starts {
        conversation,
        delta,
    } = &outcome
    else {
        panic!("expected a start, got {outcome:?}");
    };
    assert_eq!(delta.new_inputs, vec![u]);
    assert_eq!(delta.new_system, Some(s));
    assert_eq!(delta.output, Some(a));
    let stored = store
        .conversation(*conversation)
        .await
        .expect("read")
        .expect("stored");
    assert_eq!(stored.origin, ConversationOrigin::Root);
    assert_eq!(stored.agent, agent);
    assert_eq!(stored.messages, vec![u, a]);
}

/// `reconstruct.thread.origin-matches-outcome`: a fork stores its parent
/// and shared prefix.
#[tokio::test]
async fn fork_stores_fork_origin() {
    let mut scene = Scene::new();
    let store = MemoryConversations::new();
    let mut threader = scene.threader(store.clone());
    let agent = scene.ids.agent();
    let (u, a1, t1, a2) = (
        scene.user("task").await,
        scene.assistant("calling a tool").await,
        scene.tool("c1", "result one").await,
        scene.assistant("done").await,
    );
    let first = scene.exchange(vec![u], a1);
    let root = conversation(&threader.thread(&first, agent).await.expect("first"));
    let second = scene.exchange(vec![u, a1, t1], a2);
    threader.thread(&second, agent).await.expect("second");
    // A sub-agent style retry that rewrote the tool result: shares [u, a1].
    let t1b = scene.tool("c1", "result one, rewritten").await;
    let a3 = scene.assistant("done differently").await;
    let fork = scene.exchange(vec![u, a1, t1b], a3);
    let outcome = threader.thread(&fork, agent).await.expect("fork");
    let ThreadOutcome::Forks {
        parent,
        shared_prefix,
        conversation,
        delta,
    } = &outcome
    else {
        panic!("expected a fork, got {outcome:?}");
    };
    assert_eq!(*parent, root);
    assert_eq!(*shared_prefix, 2);
    assert_eq!(delta.new_inputs, vec![t1b]);
    let stored = store
        .conversation(*conversation)
        .await
        .expect("read")
        .expect("stored");
    assert_eq!(
        stored.origin,
        ConversationOrigin::Fork {
            parent: root,
            shared_prefix: 2
        }
    );
    assert_eq!(stored.messages, vec![u, a1, t1b, a3]);
}

/// `reconstruct.thread.origin-matches-outcome`: a compaction stores its
/// predecessor.
#[tokio::test]
async fn compaction_stores_link_to_predecessor() {
    let mut scene = Scene::new();
    let store = MemoryConversations::new();
    let mut threader = scene.threader(store.clone());
    let agent = scene.ids.agent();
    let (s, u, a) = (
        scene.system("sys").await,
        scene.user("long task").await,
        scene.assistant("working").await,
    );
    let first = scene.exchange(vec![s, u], a);
    let before = conversation(&threader.thread(&first, agent).await.expect("first"));
    let summary = scene
        .user("This session is being continued from a previous conversation that ran out of context. Summary: working on the long task.")
        .await;
    let a2 = scene.assistant("continuing").await;
    let compacted = scene.exchange(vec![s, summary], a2);
    let outcome = threader.thread(&compacted, agent).await.expect("compacted");
    let ThreadOutcome::Compacts {
        predecessor,
        conversation,
        delta,
    } = &outcome
    else {
        panic!("expected a compaction, got {outcome:?}");
    };
    assert_eq!(*predecessor, before);
    assert_eq!(delta.new_inputs, vec![summary]);
    assert_eq!(delta.new_system, Some(s));
    let stored = store
        .conversation(*conversation)
        .await
        .expect("read")
        .expect("stored");
    assert_eq!(
        stored.origin,
        ConversationOrigin::Compaction {
            predecessor: before
        }
    );
}

/// `reconstruct.thread.failed-exchanges-threaded`: a failed exchange with a
/// partial response is threaded, its new inputs and partial output in the
/// delta.
#[tokio::test]
async fn failed_exchange_is_threaded() {
    let mut scene = Scene::new();
    let mut threader = scene.threader(MemoryConversations::new());
    let agent = scene.ids.agent();
    let (u, a1, t1, partial) = (
        scene.user("task").await,
        scene.assistant("tool please").await,
        scene.tool("c1", "the tool output").await,
        scene.assistant("half an ans").await,
    );
    let first = scene.exchange(vec![u], a1);
    let root = conversation(&threader.thread(&first, agent).await.expect("first"));
    let failed = scene.failed(vec![u, a1, t1], Some(partial));
    let outcome = threader.thread(&failed, agent).await.expect("failed");
    let ThreadOutcome::Extends {
        conversation,
        delta,
    } = &outcome
    else {
        panic!("expected an extension, got {outcome:?}");
    };
    assert_eq!(*conversation, root);
    assert_eq!(delta.new_inputs, vec![t1]);
    assert_eq!(delta.output, Some(partial));
}

/// `reconstruct.thread.failed-exchanges-threaded`: with no partial
/// response, the delta still carries the new inputs.
#[tokio::test]
async fn failed_exchange_without_partial_response_is_threaded() {
    let mut scene = Scene::new();
    let mut threader = scene.threader(MemoryConversations::new());
    let agent = scene.ids.agent();
    let (u, a1, t1) = (
        scene.user("task").await,
        scene.assistant("tool please").await,
        scene.tool("c1", "the tool output").await,
    );
    let first = scene.exchange(vec![u], a1);
    let root = conversation(&threader.thread(&first, agent).await.expect("first"));
    let failed = scene.failed(vec![u, a1, t1], None);
    let outcome = threader.thread(&failed, agent).await.expect("failed");
    assert_eq!(conversation(&outcome), root);
    assert_eq!(outcome.delta().new_inputs, vec![t1]);
    assert_eq!(outcome.delta().output, None);
    // The retry of the same request extends the stored history, adding
    // nothing new.
    let a2 = scene.assistant("answer").await;
    let retry = scene.exchange(vec![u, a1, t1], a2);
    let outcome = threader.thread(&retry, agent).await.expect("retry");
    assert!(
        matches!(outcome, ThreadOutcome::Extends { .. }),
        "{outcome:?}"
    );
    assert!(outcome.delta().new_inputs.is_empty());
}

/// `reconstruct.thread.unknown-previous-starts`: an increment whose
/// previous response was never seen starts a conversation holding only the
/// increment.
#[tokio::test]
async fn unknown_previous_response_starts_with_increment() {
    let mut scene = Scene::new();
    let mut threader = scene.threader(MemoryConversations::new());
    let agent = scene.ids.agent();
    let (t, u, a) = (
        scene.tool("call_1", "output").await,
        scene.user("and then?").await,
        scene.assistant("then this").await,
    );
    let at = scene.tick();
    let exchange = ExchangeBuilder::new(&mut scene.ids)
        .started_at(at)
        .increment("resp_never_seen", None)
        .request(vec![t, u])
        .response(a)
        .build();
    let outcome = threader.thread(&exchange, agent).await.expect("threaded");
    assert!(
        matches!(outcome, ThreadOutcome::Starts { .. }),
        "{outcome:?}"
    );
    assert_eq!(outcome.delta().new_inputs, vec![t, u]);
}

/// An increment whose previous response is stored continues that
/// conversation.
#[tokio::test]
async fn increment_continues_the_stored_response() {
    let mut scene = Scene::new();
    let mut threader = scene.threader(MemoryConversations::new());
    let agent = scene.ids.agent();
    let (u, a, u2, a2) = (
        scene.user("first").await,
        scene.assistant("first answer").await,
        scene.user("second").await,
        scene.assistant("second answer").await,
    );
    let at = scene.tick();
    let first = ExchangeBuilder::new(&mut scene.ids)
        .started_at(at)
        .request(vec![u])
        .response(a)
        .build();
    let response = match &first.outcome {
        crosstalk_spec::observed::exchange::ExchangeOutcome::Completed {
            response_id: Some(ResponseId(id)),
            ..
        } => id.clone(),
        other => panic!("no response id: {other:?}"),
    };
    let root = conversation(&threader.thread(&first, agent).await.expect("first"));
    let at = scene.tick();
    let next = ExchangeBuilder::new(&mut scene.ids)
        .started_at(at)
        .client(first.meta.client.clone())
        .increment(&response, None)
        .request(vec![u2])
        .response(a2)
        .build();
    let outcome = threader.thread(&next, agent).await.expect("increment");
    assert_eq!(conversation(&outcome), root);
    assert!(
        matches!(outcome, ThreadOutcome::Extends { .. }),
        "{outcome:?}"
    );
    assert_eq!(outcome.delta().new_inputs, vec![u2]);
}

/// `reconstruct.thread.compaction-needs-content-evidence`: the compaction
/// hint on a request with no summary turn is not a compaction.
#[tokio::test]
async fn compaction_hint_without_summary_is_not_compaction() {
    let mut scene = Scene::new();
    let mut threader = scene.threader(MemoryConversations::new());
    let agent = scene.ids.agent();
    let (u, a) = (scene.user("task").await, scene.assistant("ok").await);
    let first = scene.exchange(vec![u], a);
    threader.thread(&first, agent).await.expect("first");
    let (u2, a2) = (
        scene.user("an unrelated new task").await,
        scene.assistant("sure").await,
    );
    let at = scene.tick();
    let hinted = ExchangeBuilder::new(&mut scene.ids)
        .started_at(at)
        .class(RequestClass::Compaction)
        .request(vec![u2])
        .response(a2)
        .build();
    let outcome = threader.thread(&hinted, agent).await.expect("hinted");
    assert!(
        matches!(outcome, ThreadOutcome::Starts { .. }),
        "{outcome:?}"
    );
}

/// `reconstruct.thread.compaction-needs-content-evidence`: with the hint
/// and a summary turn, it is a compaction.
#[tokio::test]
async fn compaction_hint_with_summary_is_compaction() {
    let mut scene = Scene::new();
    let mut threader = scene.threader(MemoryConversations::new());
    let agent = scene.ids.agent();
    let (u, a) = (scene.user("task").await, scene.assistant("ok").await);
    let first = scene.exchange(vec![u], a);
    let before = conversation(&threader.thread(&first, agent).await.expect("first"));
    let (summary, a2) = (
        scene
            .user("Summary of the work so far: the task is half done.")
            .await,
        scene.assistant("resuming").await,
    );
    let at = scene.tick();
    let hinted = ExchangeBuilder::new(&mut scene.ids)
        .started_at(at)
        .class(RequestClass::Compaction)
        .request(vec![summary])
        .response(a2)
        .build();
    let outcome = threader.thread(&hinted, agent).await.expect("hinted");
    match outcome {
        ThreadOutcome::Compacts { predecessor, .. } => assert_eq!(predecessor, before),
        other => panic!("expected a compaction, got {other:?}"),
    }
}

/// A `role: system` turn mid-conversation (Claude Code sends them) is a
/// system message where it stands: out of prefix matching, kept in the
/// transcript at its place, and reported as the delta's `new_system`.
#[tokio::test]
async fn mid_conversation_system_turn_continues_and_is_kept_in_order() {
    let mut scene = Scene::new();
    let store = MemoryConversations::new();
    let mut threader = scene.threader(store.clone());
    let agent = scene.ids.agent();
    let (s, u, a1, t1, reminder, a2) = (
        scene.system("you are an agent").await,
        scene.user("task").await,
        scene.assistant("tool please").await,
        scene.tool("c1", "output").await,
        scene.system("reminder: the todo list changed").await,
        scene.assistant("noted").await,
    );
    let first = scene.exchange(vec![s, u], a1);
    let root = conversation(&threader.thread(&first, agent).await.expect("first"));
    let second = scene.exchange(vec![s, u, a1, t1, reminder], a2);
    let outcome = threader.thread(&second, agent).await.expect("second");
    assert!(
        matches!(outcome, ThreadOutcome::Extends { .. }),
        "{outcome:?}"
    );
    assert_eq!(conversation(&outcome), root);
    assert_eq!(outcome.delta().new_inputs, vec![t1]);
    assert_eq!(outcome.delta().new_system, Some(reminder));
    // The next request keeps the reminder in place: nothing new to report.
    let (u3, a3) = (scene.user("next").await, scene.assistant("ok").await);
    let third = scene.exchange(vec![s, u, a1, t1, reminder, a2, u3], a3);
    let outcome = threader.thread(&third, agent).await.expect("third");
    assert_eq!(conversation(&outcome), root);
    assert_eq!(outcome.delta().new_inputs, vec![u3]);
    assert_eq!(outcome.delta().new_system, None);
    let transcript = store.transcript(root).await.expect("transcript");
    let roles: Vec<(u32, Role)> = transcript
        .iter()
        .map(|entry| (entry.ordinal, entry.role))
        .collect();
    assert_eq!(
        roles,
        vec![
            (0, Role::System),
            (1, Role::User),
            (2, Role::Assistant),
            (3, Role::Tool),
            (4, Role::System),
            (5, Role::Assistant),
            (6, Role::User),
            (7, Role::Assistant),
        ]
    );
    let messages: Vec<_> = transcript.iter().map(|entry| entry.message).collect();
    assert_eq!(messages, vec![s, u, a1, t1, reminder, a2, u3, a3]);
}

/// A changed leading system prompt continues the conversation, is reported
/// once, and is recorded in the transcript where it changed.
#[tokio::test]
async fn changed_system_prompt_is_reported_once_and_recorded() {
    let mut scene = Scene::new();
    let store = MemoryConversations::new();
    let mut threader = scene.threader(store.clone());
    let agent = scene.ids.agent();
    let (s1, s2, u, a1, u2, a2, u3, a3) = (
        scene.system("v1").await,
        scene.system("v2").await,
        scene.user("task").await,
        scene.assistant("one").await,
        scene.user("more").await,
        scene.assistant("two").await,
        scene.user("again").await,
        scene.assistant("three").await,
    );
    let root = conversation(
        &threader
            .thread(&scene.exchange(vec![s1, u], a1), agent)
            .await
            .expect("first"),
    );
    let outcome = threader
        .thread(&scene.exchange(vec![s2, u, a1, u2], a2), agent)
        .await
        .expect("second");
    assert_eq!(conversation(&outcome), root);
    assert_eq!(outcome.delta().new_system, Some(s2));
    let outcome = threader
        .thread(&scene.exchange(vec![s2, u, a1, u2, a2, u3], a3), agent)
        .await
        .expect("third");
    assert_eq!(outcome.delta().new_system, None);
    let messages: Vec<_> = store
        .transcript(root)
        .await
        .expect("transcript")
        .iter()
        .map(|entry| entry.message)
        .collect();
    assert_eq!(messages, vec![s1, u, a1, s2, u2, a2, u3, a3]);
}

/// Regression (lmcache `wildclaw__01_Productivity_Flow_task_1_arxiv_digest`):
/// two re-runs of one task interleave under one session id. Their first
/// user turns differ, so each run is its own root and every later request
/// extends its own run, however the requests interleave.
#[tokio::test]
async fn interleaved_reruns_thread_as_separate_conversations() {
    let mut scene = Scene::new();
    let mut threader = scene.threader(MemoryConversations::new());
    let agent = scene.ids.agent();
    let sa = scene.system("run A prompt").await;
    let sb = scene.system("run B prompt").await;
    let ua = scene.user("[Fri 20:50] task").await;
    let ub = scene.user("[Fri 20:54] task").await;
    let mut history_a = vec![sa, ua];
    let mut history_b = vec![sb, ub];
    let mut root_a = None;
    let mut root_b = None;
    for step in 0..6 {
        let (history, root, tag) = if step % 2 == 0 {
            (&mut history_a, &mut root_a, "a")
        } else {
            (&mut history_b, &mut root_b, "b")
        };
        let output = scene.assistant(&format!("{tag} step {step}")).await;
        let exchange = scene.exchange(history.clone(), output);
        let outcome = threader.thread(&exchange, agent).await.expect("threaded");
        match root {
            None => {
                assert!(
                    matches!(outcome, ThreadOutcome::Starts { .. }),
                    "{outcome:?}"
                );
                *root = Some(conversation(&outcome));
            }
            Some(root) => {
                assert!(
                    matches!(outcome, ThreadOutcome::Extends { .. }),
                    "{outcome:?}"
                );
                assert_eq!(conversation(&outcome), *root);
            }
        }
        let result = scene.tool(&format!("{tag}{step}"), "ok").await;
        history.push(output);
        history.push(result);
    }
    assert_ne!(root_a, root_b);
}

/// Regression (lmcache, the run's last request): the harness rewrote an
/// early tool result in place, so the request shares only the messages
/// before it with the stored run. It forks there, re-reporting every
/// message after the rewrite as new.
#[tokio::test]
async fn rewritten_early_tool_result_forks_at_the_rewrite() {
    let mut scene = Scene::new();
    let mut threader = scene.threader(MemoryConversations::new());
    let agent = scene.ids.agent();
    let u = scene.user("task").await;
    let mut history = vec![u];
    let mut root = None;
    let mut assistants = Vec::new();
    for step in 0..4 {
        let output = scene.assistant(&format!("step {step}")).await;
        let outcome = threader
            .thread(&scene.exchange(history.clone(), output), agent)
            .await
            .expect("threaded");
        root.get_or_insert(conversation(&outcome));
        assistants.push(output);
        history.push(output);
        history.push(
            scene
                .tool(&format!("c{step}"), &format!("result {step}"))
                .await,
        );
    }
    // Rewrite the first tool result (history[2]).
    let rewritten = scene.tool("c0", "result 0 [pruned]").await;
    let mut edited = history.clone();
    edited[2] = rewritten;
    let output = scene.assistant("after pruning").await;
    let outcome = threader
        .thread(&scene.exchange(edited.clone(), output), agent)
        .await
        .expect("edited");
    match &outcome {
        ThreadOutcome::Forks {
            parent,
            shared_prefix,
            delta,
            ..
        } => {
            assert_eq!(Some(*parent), root);
            assert_eq!(*shared_prefix, 2);
            assert_eq!(delta.new_inputs, edited[2..].to_vec());
        }
        other => panic!("expected a fork, got {other:?}"),
    }
}

/// Regression (AI Village Claude Code stream): a session resumed after a
/// restart sends its history again, unchanged, and keeps extending the
/// same conversation; after a compact boundary the next request opens with
/// the summary turn and compacts it.
#[tokio::test]
async fn resumed_session_extends_and_compact_boundary_compacts() {
    let mut scene = Scene::new();
    let mut threader = scene.threader(MemoryConversations::new());
    let agent = scene.ids.agent();
    let s = scene.system("claude code").await;
    let u = scene.user("Check for new events").await;
    let mut history = vec![s, u];
    let mut root = None;
    for step in 0..3 {
        let output = scene.assistant(&format!("tool call {step}")).await;
        let outcome = threader
            .thread(&scene.exchange(history.clone(), output), agent)
            .await
            .expect("threaded");
        assert_eq!(
            *root.get_or_insert(conversation(&outcome)),
            conversation(&outcome)
        );
        history.push(output);
        history.push(scene.tool(&format!("t{step}"), "events").await);
    }
    // Resumed: the same history, plus the prompt the resume adds.
    let resume = scene.user("Continue.").await;
    history.push(resume);
    let output = scene.assistant("continuing after resume").await;
    let outcome = threader
        .thread(&scene.exchange(history.clone(), output), agent)
        .await
        .expect("resumed");
    assert!(
        matches!(outcome, ThreadOutcome::Extends { .. }),
        "{outcome:?}"
    );
    assert_eq!(Some(conversation(&outcome)), root);
    // Compact boundary.
    let summary = scene
        .user("This session is being continued from a previous conversation that ran out of context. The conversation is summarized below: checking events.")
        .await;
    let output = scene.assistant("resuming from the summary").await;
    let outcome = threader
        .thread(&scene.exchange(vec![s, summary], output), agent)
        .await
        .expect("compacted");
    match outcome {
        ThreadOutcome::Compacts { predecessor, .. } => assert_eq!(Some(predecessor), root),
        other => panic!("expected a compaction, got {other:?}"),
    }
}

/// Threading an exchange a second time returns the first outcome.
#[tokio::test]
async fn rethreading_returns_the_recorded_outcome() {
    let mut scene = Scene::new();
    let store = MemoryConversations::new();
    let mut threader = scene.threader(store.clone());
    let agent = scene.ids.agent();
    let (u, a) = (scene.user("task").await, scene.assistant("ok").await);
    let exchange = scene.exchange(vec![u], a);
    let first = threader.thread(&exchange, agent).await.expect("first");
    let again = threader.thread(&exchange, agent).await.expect("again");
    assert_eq!(first, again);
    assert_eq!(store.conversations().await.expect("list").len(), 1);
}
