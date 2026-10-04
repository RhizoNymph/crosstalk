//! Threading properties on generated harness scripts.
//!
//! Every script is played on the in-memory store with the oracle
//! ([`oracle::Oracle::observe`]) checking each step; the tests below differ
//! in the mix of steps they draw and in the metamorphic relations
//! (increments, system changes, compactions) they add. The Postgres store
//! shares the decision, and its model test holds it to the same outcomes.

pub(crate) mod oracle;
pub(crate) mod script;

use crosstalk_spec::interfaces::l3_reconstruction::{ThreadOutcome, Threader};
use crosstalk_spec::observed::message::Role;
use crosstalk_testkit::build::ExchangeBuilder;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};

use oracle::{ResponseIdOf, play};
use script::{Ending, Input, Mix, Op, SystemChange, script};

use crate::thread::store::outcome_conversation;

/// Run `body` on `cases` values of `strategy`, each on a fresh runtime.
pub(crate) fn property<S, F, Fut>(cases: u32, strategy: S, body: F)
where
    S: Strategy,
    S::Value: std::fmt::Debug,
    F: Fn(S::Value) -> Fut,
    Fut: Future<Output = Result<(), TestCaseError>>,
{
    let mut runner = TestRunner::new(Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    });
    let result = runner.run(&strategy, |value| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| TestCaseError::fail(format!("no runtime: {error}")))?;
        runtime.block_on(body(value))
    });
    if let Err(error) = result {
        panic!("{error}");
    }
}

/// Play generated scripts drawn from `mix`, every step checked.
fn scripts(mix: Mix) {
    property(48, script(mix, 28), |ops| async move {
        play(&ops).await.map(|_| ())
    });
}

/// `reconstruct.conversation.history-is-delta-concatenation`.
#[test]
fn conversation_history_is_concatenation_of_deltas() {
    scripts(Mix::GENERAL);
}

/// `reconstruct.delta.names-outcome-conversation`.
#[test]
fn delta_names_outcome_conversation() {
    scripts(Mix::GENERAL);
}

/// `reconstruct.delta.new-inputs-are-request-suffix`.
#[test]
fn new_inputs_are_request_suffix() {
    scripts(Mix::FORKS);
}

/// `reconstruct.delta.new-system-when-changed`: system prompts replaced,
/// removed and appended mid-conversation.
#[test]
fn new_system_only_when_first_or_changed() {
    scripts(Mix::GENERAL);
}

/// `reconstruct.delta.output-is-response`: completed and failed exchanges.
#[test]
fn delta_output_is_exchange_response() {
    scripts(Mix::GENERAL);
}

/// `reconstruct.thread.extends-longest-prefix-match`.
#[test]
fn continuation_extends_longest_matching_conversation() {
    scripts(Mix::FORKS);
}

/// `reconstruct.thread.extends-stored-prefix`.
#[test]
fn extends_requires_stored_prefix() {
    scripts(Mix::GENERAL);
}

/// `reconstruct.thread.fork-or-start`.
#[test]
fn fork_or_start_by_shared_assistant_prefix() {
    scripts(Mix::FORKS);
}

/// `reconstruct.thread.fork-parent-maximal`.
#[test]
fn fork_parent_has_longest_common_prefix() {
    scripts(Mix::FORKS);
}

/// `reconstruct.thread.fork-prefix-bounds`.
#[test]
fn fork_shared_prefix_within_bounds() {
    scripts(Mix::FORKS);
}

/// `reconstruct.thread.fork-prefix-is-lcp`.
#[test]
fn fork_shared_prefix_is_longest_common_prefix() {
    scripts(Mix::FORKS);
}

/// `reconstruct.thread.fork-shares-assistant-message`.
#[test]
fn fork_prefix_contains_assistant_message() {
    scripts(Mix::FORKS);
}

/// `reconstruct.thread.link-targets-exist`.
#[test]
fn fork_and_compaction_links_point_at_stored_conversations() {
    scripts(Mix::GENERAL);
    scripts(Mix::COMPACTIONS);
}

/// `reconstruct.delta.compaction-excludes-carried-over`.
#[test]
fn compaction_new_inputs_exclude_carried_over() {
    scripts(Mix::COMPACTIONS);
}

/// `reconstruct.conversation.compaction-history`.
#[test]
fn compaction_history_is_first_request_then_deltas() {
    scripts(Mix::COMPACTIONS);
}

/// A script, then one continuation of line `line` whose system change is
/// varied.
fn with_final(ops: &[Op], line: u8, input: Input, system: SystemChange) -> Vec<Op> {
    let mut ops = ops.to_vec();
    ops.push(Op::Continue {
        line,
        input,
        system,
        ending: Ending::Completed,
    });
    ops
}

/// The outcome without its delta: variant, conversation and links.
fn shape(outcome: &ThreadOutcome) -> (String, crosstalk_spec::ids::ConversationId, String) {
    let links = match outcome {
        ThreadOutcome::Forks {
            parent,
            shared_prefix,
            ..
        } => format!("{parent:?}/{shared_prefix}"),
        ThreadOutcome::Compacts { predecessor, .. } => format!("{predecessor:?}"),
        ThreadOutcome::Starts { .. } | ThreadOutcome::Extends { .. } => String::new(),
    };
    (
        crate::thread::outcome_kind(outcome).to_owned(),
        outcome_conversation(outcome),
        links,
    )
}

/// `reconstruct.thread.system-change-continues`: the same script, then one
/// continuation sent with its system messages unchanged, or replaced,
/// removed or appended mid-conversation: the same outcome variant,
/// conversation and links.
#[test]
fn system_change_keeps_outcome_and_conversation() {
    let change = prop_oneof![
        (0u8..3).prop_map(SystemChange::Replace),
        Just(SystemChange::Remove),
        (0u8..3).prop_map(SystemChange::Append),
    ];
    let input = prop_oneof![
        Just(Input::Tool),
        (0u8..4).prop_map(Input::User),
        Just(Input::Nothing)
    ];
    property(
        48,
        (script(Mix::GENERAL, 20), any::<u8>(), input, change),
        |(ops, line, input, change)| async move {
            let (_, _, same, _) = play(&with_final(&ops, line, input, SystemChange::Same)).await?;
            let (_, _, changed, _) = play(&with_final(&ops, line, input, change)).await?;
            let (Some(a), Some(b)) = (same.outcomes.last(), changed.outcomes.last()) else {
                return Err(TestCaseError::fail("no final outcome"));
            };
            prop_assert_eq!(shape(a), shape(b));
            Ok(())
        },
    );
}

/// The script played, then an exchange continuing a stored response as an
/// increment (`as_increment`) or as the full history it stands for.
async fn continue_response(
    ops: &[Op],
    pick: usize,
    as_increment: bool,
    same_scope: bool,
) -> Result<Option<ThreadOutcome>, TestCaseError> {
    let (mut harness, mut threader, oracle, steps) = play(ops).await?;
    if oracle.responses.is_empty() {
        return Ok(None);
    }
    let (index, conversation, len) = oracle.responses[pick % oracle.responses.len()];
    let step = &steps[index];
    let response = step
        .exchange
        .outcome_response_id()
        .ok_or_else(|| TestCaseError::fail("picked step has no response id"))?;
    let stored = oracle
        .expected
        .get(&conversation)
        .ok_or_else(|| TestCaseError::fail("no expected history"))?;
    let tool = harness.scene.tool("next", "increment input").await;
    let user = harness.scene.user("and the increment's question").await;
    let output = harness.scene.assistant("increment answer").await;
    let at = harness.scene.tick();
    let client = if same_scope {
        step.exchange.meta.client.clone()
    } else {
        // Another credential: another identity scope.
        ExchangeBuilder::new(&mut harness.scene.ids).build().meta.client
    };
    let builder = ExchangeBuilder::new(&mut harness.scene.ids)
        .started_at(at)
        .client(client)
        .response(output);
    let exchange = if as_increment {
        builder.increment(&response, None).request(vec![tool, user])
    } else {
        let mut full = stored[..len].to_vec();
        full.extend([tool, user]);
        builder.request(full)
    }
    .build();
    let outcome = threader
        .thread(&exchange, step.agent)
        .await
        .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
    Ok(Some(outcome))
}

/// `reconstruct.thread.responses-state-agrees-with-prefix`: an increment
/// continuing a stored response threads as the full history through that
/// response followed by the increment.
#[test]
fn increment_threading_agrees_with_full_history_model() {
    property(
        48,
        (script(Mix::GENERAL, 20), any::<usize>()),
        |(ops, pick)| async move {
            let increment = continue_response(&ops, pick, true, true).await?;
            let full = continue_response(&ops, pick, false, true).await?;
            prop_assert_eq!(increment, full);
            Ok(())
        },
    );
}

/// `reconstruct.thread.previous-response-scoped`: the same increment sent
/// under another identity scope does not find the response, and starts a
/// conversation holding only the increment.
#[test]
fn previous_response_matches_only_in_scope() {
    property(
        48,
        (script(Mix::GENERAL, 20), any::<usize>()),
        |(ops, pick)| async move {
            let Some(outcome) = continue_response(&ops, pick, true, false).await? else {
                return Ok(());
            };
            prop_assert!(matches!(outcome, ThreadOutcome::Starts { .. }), "{outcome:?}");
            prop_assert_eq!(outcome.delta().new_inputs.len(), 2);
            Ok(())
        },
    );
}

/// `reconstruct.thread.compaction-next-turn-extends`: the turn after a
/// compaction extends the compaction's conversation with only its new
/// messages.
#[test]
fn turn_after_compaction_extends_compaction() {
    property(
        48,
        (script(Mix::GENERAL, 16), any::<u8>(), 0u8..4, any::<bool>()),
        |(mut ops, line, keep, tool)| async move {
            ops.push(Op::Compact { line, keep });
            let (harness, _, oracle, _) = play(&ops).await?;
            let compacted_line = (harness.lines.len() - 1) as u8;
            let input = if tool { Input::Tool } else { Input::User(0) };
            ops.push(Op::Continue {
                line: compacted_line,
                input,
                system: SystemChange::Same,
                ending: Ending::Completed,
            });
            let (_, _, after, steps) = play(&ops).await?;
            let Some(ThreadOutcome::Compacts { conversation, .. }) =
                oracle.outcomes.last().cloned()
            else {
                // The compacted request extended a stored history instead
                // (it carried a whole stored history first).
                return Ok(());
            };
            let next = after
                .outcomes
                .last()
                .ok_or_else(|| TestCaseError::fail("no final outcome"))?;
            let ThreadOutcome::Extends {
                conversation: extended,
                delta,
            } = next
            else {
                return Err(TestCaseError::fail(format!("not an extension: {next:?}")));
            };
            prop_assert_eq!(*extended, conversation);
            let last = steps.last().ok_or_else(|| TestCaseError::fail("no step"))?;
            let added: Vec<_> = last
                .request
                .iter()
                .rev()
                .take(1)
                .map(|entry| entry.message)
                .collect();
            prop_assert_eq!(&delta.new_inputs, &added);
            Ok(())
        },
    );
}

/// `reconstruct.delta.chained-compaction-excludes-carried-over`: a
/// compaction of a compaction carrying what the first carried again
/// reports none of it as new.
#[test]
fn chained_compaction_excludes_carried_over() {
    property(
        48,
        (script(Mix::GENERAL, 12), any::<u8>(), 1u8..4, 1u8..4),
        |(mut ops, line, first_keep, second_keep)| async move {
            ops.push(Op::Compact {
                line,
                keep: first_keep,
            });
            let (harness, _, _, _) = play(&ops).await?;
            let compacted = (harness.lines.len() - 1) as u8;
            ops.push(Op::Continue {
                line: compacted,
                input: Input::Tool,
                system: SystemChange::Same,
                ending: Ending::Completed,
            });
            ops.push(Op::Compact {
                line: compacted,
                keep: second_keep + first_keep,
            });
            // The oracle checks every compaction's new inputs against its
            // predecessor's stored history, the chained one included.
            let (_, _, oracle, steps) = play(&ops).await?;
            let (Some(ThreadOutcome::Compacts { delta, .. }), Some(first), Some(last)) = (
                oracle.outcomes.last(),
                steps.get(steps.len() - 3),
                steps.last(),
            ) else {
                return Ok(());
            };
            for carried in first.request.iter().filter(|entry| entry.role != Role::System) {
                if Some(carried.message) != first.summary
                    && last.request.iter().any(|entry| entry.message == carried.message)
                {
                    prop_assert!(
                        !delta.new_inputs.contains(&carried.message),
                        "{:?} carried twice is new again",
                        carried.message
                    );
                }
            }
            Ok(())
        },
    );
}
