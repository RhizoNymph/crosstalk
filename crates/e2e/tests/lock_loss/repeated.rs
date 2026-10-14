//! Five lock losses and resumes in one process: nothing an abandoned
//! pipeline owned (its pool's sessions, its bus's listener and takes, its
//! tasks) outlives it.

use std::time::Duration;

use crosstalk_gateway::live::recovery::{LockState, PipelinePhase};
use crosstalk_memory::support::ManualClock;
use tokio::time::Instant;

use crate::common::gateway::{run, scenario};
use crate::common::{big_stack, migrated};
use crate::harness::{
    Prober, RESUME, alive_tasks, expected, incarnations, serial, sessions_by_name,
    terminate_lock_session, wait_sessions, wait_status,
};

/// How many losses in a row.
const LOSSES: usize = 5;

/// Tasks a settled process may have beyond the baseline: connections the
/// ops listener and the proxy have not reaped yet.
const TASK_SLACK: usize = 8;

/// The harness config's `store.pool.max_connections`.
const POOL_MAX: i64 = 12;

/// After each loss the next incarnation runs, the previous ones' server
/// sessions are gone, the runtime's alive tasks return to the baseline,
/// and the current incarnation stays within its pool's size. The
/// exchanges spread over the losses end as the uninterrupted run's
/// answers.
#[test]
fn repeated_lock_losses_leave_no_pool_listener_or_task_behind() {
    let _serial = serial();
    big_stack(|| async {
        const TEST: &str = "repeated_lock_losses_leave_no_pool_listener_or_task_behind";
        let scenario = scenario();
        let Some(expected) = expected(TEST, &scenario).await else {
            return;
        };
        let Some(db) = migrated(TEST).await else {
            return;
        };
        let run = run(db).await;
        let clock = ManualClock::at(scenario.start);
        let gateway = run.start(&clock).await;
        gateway
            .send(&run.upstream, &scenario.exchanges[..1], 1)
            .await;
        gateway
            .settled(&run.db, "the first exchange's pipeline")
            .await;
        let names = sessions_by_name(&run.db).await;
        let first = incarnations(&names);
        tokio::time::sleep(Duration::from_secs(1)).await;
        let baseline_tasks = alive_tasks();
        eprintln!("baseline: {baseline_tasks} tasks; sessions {names:?}; incarnations {first:?}");
        let prober = Prober::start(gateway.running.proxy_addr().expect("role all proxies"));

        // Exchanges 1, 2 and 3 arrive after losses 1, 3 and 5.
        let arrivals = [Some(1), None, Some(2), None, Some(3)];
        let mut captured = 1;
        for (loss, arrival) in arrivals.iter().enumerate().take(LOSSES) {
            let before = incarnations(&sessions_by_name(&run.db).await);
            let started = Instant::now();
            terminate_lock_session(&run.db).await;
            wait_status(
                &gateway.running,
                "the lock's loss",
                Duration::from_secs(30),
                |status| status.lock == LockState::Lost,
            )
            .await;
            if let Some(index) = arrival {
                captured += 1;
                gateway
                    .send(
                        &run.upstream,
                        &scenario.exchanges[*index..*index + 1],
                        captured,
                    )
                    .await;
            }
            let resumed = wait_status(&gateway.running, "the resume", RESUME, |status| {
                status.lock == LockState::Held && status.phase == PipelinePhase::Running
            })
            .await;
            gateway.settled(&run.db, "the drain after a resume").await;
            let after = wait_sessions(
                &run.db,
                "the abandoned incarnations' sessions ending",
                Duration::from_secs(15),
                |names| {
                    let now = incarnations(names);
                    now.len() == 1 && before.keys().all(|n| !now.contains_key(n))
                },
            )
            .await;
            let current = incarnations(&after);
            let sessions = current.values().copied().max().unwrap_or(0);
            assert!(
                (1..=POOL_MAX).contains(&sessions),
                "loss {loss}: the current incarnation has {sessions} sessions: {after:?}"
            );
            let deadline = Instant::now() + Duration::from_secs(15);
            let tasks = loop {
                let tasks = alive_tasks();
                // The prober's task runs beside the baseline.
                if tasks <= baseline_tasks + 1 + TASK_SLACK || Instant::now() > deadline {
                    break tasks;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            };
            eprintln!(
                "loss {loss}: resumed after {:?}; incarnations {current:?}; {tasks} tasks \
                 (baseline {baseline_tasks}); report {:?}",
                started.elapsed(),
                resumed.report
            );
            assert!(
                tasks <= baseline_tasks + 1 + TASK_SLACK,
                "loss {loss}: {tasks} tasks alive, baseline {baseline_tasks}: something the \
                 abandoned pipeline owned still runs"
            );
        }
        let connects = prober.finish().await;
        assert_eq!(connects.refused, 0, "{connects:?}");
        run.finish(
            gateway,
            &scenario,
            &expected,
            "the run with five lock losses",
        )
        .await;
    });
}
