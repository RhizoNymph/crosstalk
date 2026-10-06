//! The step's durability protocol on the memory ledger: nothing commits
//! unless the flow side confirmed the inputs, a refused delta retried
//! extracts what it would have, a done delta yields nothing, and expiry
//! forgets by stamp.

use std::time::Duration;

use crosstalk_spec::ids::ExchangeId;
use crosstalk_spec::support::Timestamp;
use proptest::prelude::*;

use super::script::script;
use super::support::{Delta, Harness, Recorder, Refuser, calls, get, post, result};
use crate::consumer::NotDurable;
use crate::extract::ExtractConfig;
use crate::extract::step::{DeltaOutcome, ExtractStepError, MemoryExtractionLedger};

fn memory() -> Harness<MemoryExtractionLedger> {
    Harness::new(MemoryExtractionLedger::new(), ExtractConfig::default())
}

/// A refused delta commits nothing; its retry hands over what one never
/// refused hands over, and leaves the same ledger.
#[tokio::test]
async fn a_refused_delta_commits_nothing_and_its_retry_extracts_the_same() {
    let refused = memory();
    let straight = memory();
    let first = Delta::new(10, 100, vec![], Some(calls(vec![post("call_1")])));
    let second = Delta::new(11, 100, vec![result("call_1", "saved")], None);
    for harness in [&refused, &straight] {
        harness.delta(first.clone()).await;
    }
    let before = refused.step.ledger().state();
    let outcome = refused.run(&second, &mut Refuser).await;
    assert!(matches!(
        outcome,
        Err(ExtractStepError::Flow(NotDurable::Backlogged))
    ));
    assert_eq!(refused.step.ledger().state(), before);
    assert_eq!(
        refused.delta(second.clone()).await,
        straight.delta(second).await
    );
    assert_eq!(
        refused.step.ledger().state(),
        straight.step.ledger().state()
    );
}

/// A redelivered delta whose extraction committed hands nothing over and
/// changes nothing.
#[tokio::test]
async fn a_done_delta_yields_nothing() {
    let harness = memory();
    let delta = Delta::new(
        10,
        100,
        vec![
            calls(vec![get("call_1")]),
            result("call_1", "the relay index"),
        ],
        None,
    );
    assert!(!harness.delta(delta.clone()).await.is_empty());
    let state = harness.step.ledger().state();
    assert!(state.done.contains_key(&ExchangeId::from_ulid(10)));
    let mut flow = Recorder::default();
    let outcome = harness.run(&delta, &mut flow).await;
    assert!(matches!(outcome, Ok(DeltaOutcome::AlreadyDone)));
    assert!(flow.0.is_empty());
    assert_eq!(harness.step.ledger().state(), state);
}

/// Expiry forgets deliveries, contexts and done marks stamped before the
/// horizon and keeps the rest.
#[tokio::test]
async fn expiry_forgets_what_is_older_than_the_horizon() {
    let harness = memory();
    for (exchange, conversation, id) in [(10, 100, "call_1"), (20, 200, "call_2")] {
        harness
            .delta(Delta::new(
                exchange,
                conversation,
                vec![calls(vec![get(id)]), result(id, "the relay index")],
                None,
            ))
            .await;
    }
    let horizon = Timestamp::from_micros(15_000_000);
    let result = harness
        .step
        .expire(Timestamp::from_micros(25_000_000), Duration::from_secs(10))
        .await;
    assert!(result.is_ok());
    let state = harness.step.ledger().state();
    assert_eq!(
        state.done.keys().copied().collect::<Vec<_>>(),
        vec![ExchangeId::from_ulid(20)]
    );
    assert!(state.delivered.values().all(|at| *at >= horizon));
    assert_eq!(state.delivered.len(), 1);
    assert!(state.contexts.values().all(|(_, at)| *at >= horizon));
    assert!(!state.contexts.is_empty());
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap_or_else(|error| panic!("runtime: {error}"))
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, failure_persistence: None, ..ProptestConfig::default() })]

    /// Over generated sequences: every delta refused once and retried
    /// hands over exactly what an unrefused run hands over and ends in the
    /// same ledger; every redelivery of a committed delta hands nothing.
    #[test]
    fn refusals_and_redeliveries_change_nothing(steps in script(12)) {
        runtime().block_on(async {
            let straight = memory();
            let retried = memory();
            for (index, step) in steps.iter().enumerate() {
                let delta = step.delta(10 + u128::try_from(index).unwrap_or(0));
                let refused = retried.run(&delta, &mut Refuser).await;
                prop_assert!(matches!(refused, Err(ExtractStepError::Flow(_))));
                let want = straight.delta(delta.clone()).await;
                prop_assert_eq!(retried.delta(delta.clone()).await, want);
                if step.again {
                    prop_assert!(straight.delta(delta).await.is_empty());
                }
            }
            prop_assert_eq!(retried.step.ledger().state(), straight.step.ledger().state());
            Ok(())
        })?;
    }
}
