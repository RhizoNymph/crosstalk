use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::interfaces::l2_transport::{BlobError, BlobStore};
use crosstalk_spec::support::Blake3;

use crate::driver::{CheckFailed, Sim, SimCtx, SimReport};
use crate::node::SuperviseError;
use crate::plan::{StoreFaults, Timed};
use crate::rng::{DurationRange, Probability, Seed};
use crate::store::{FaultyStore, InjectedFault, StoreFaultKind};
use crate::trace::{FaultKind, FaultSite};

fn run(scenario: impl AsyncFnOnce(SimCtx) -> Result<(), CheckFailed>) -> SimReport {
    Sim::run(Seed::new(0), move |ctx| scenario(ctx)).expect("passes")
}

/// A store call that counts its runs.
async fn counted(calls: &AtomicUsize) -> Result<u32, InjectedFault> {
    calls.fetch_add(1, Ordering::Relaxed);
    Ok(7)
}

fn store(ctx: &SimCtx, faults: StoreFaults) -> FaultyStore<()> {
    let node = ctx.node("n");
    ctx.faulty_store((), faults, &node.handle())
}

#[test]
fn fail_before_never_runs_the_call() {
    let faults = StoreFaults {
        fail_before: Probability::ALWAYS,
        ..StoreFaults::none()
    };
    let report = run(async |ctx| {
        let calls = AtomicUsize::new(0);
        let result = store(&ctx, faults).call("op", |f| f, counted(&calls)).await;
        let expected = InjectedFault {
            kind: StoreFaultKind::FailBefore,
            op: "op",
        };
        ctx.check(result == Err(expected), || format!("{result:?}"))?;
        ctx.check(calls.load(Ordering::Relaxed) == 0, || "not run".to_owned())
    });
    let faults: Vec<_> = report.trace.faults().collect();
    assert_eq!(faults.len(), 1);
    assert_eq!(faults[0].kind, FaultKind::StoreFailBefore);
    assert_eq!(faults[0].site, FaultSite::Store { op: "op" });
}

#[test]
fn fail_after_runs_the_call_then_reports_failure() {
    let faults = StoreFaults {
        fail_after: Probability::ALWAYS,
        ..StoreFaults::none()
    };
    let report = run(async |ctx| {
        let calls = AtomicUsize::new(0);
        let result = store(&ctx, faults).call("op", |f| f, counted(&calls)).await;
        ctx.check(
            result
                == Err(InjectedFault {
                    kind: StoreFaultKind::FailAfter,
                    op: "op",
                }),
            || format!("{result:?}"),
        )?;
        ctx.check(calls.load(Ordering::Relaxed) == 1, || "ran once".to_owned())
    });
    assert_eq!(report.trace.count(FaultKind::StoreFailAfter), 1);
}

#[test]
fn fail_after_leaves_a_real_failure_alone() {
    let faults = StoreFaults {
        fail_after: Probability::ALWAYS,
        ..StoreFaults::none()
    };
    let report = run(async |ctx| {
        let real = InjectedFault {
            kind: StoreFaultKind::FailBefore,
            op: "real",
        };
        let result: Result<(), _> = store(&ctx, faults)
            .call("op", |f| f, async { Err(real) })
            .await;
        ctx.check(result == Err(real), || format!("{result:?}"))
    });
    assert_eq!(report.trace.faults().count(), 0);
}

#[test]
fn latency_holds_the_call_back() {
    let faults = StoreFaults {
        latency: Some(Timed::new(
            Probability::ALWAYS,
            DurationRange::exactly(Duration::from_millis(40)),
        )),
        ..StoreFaults::none()
    };
    let report = run(async |ctx| {
        let calls = AtomicUsize::new(0);
        let result = store(&ctx, faults).call("op", |f| f, counted(&calls)).await;
        ctx.check(result == Ok(7), || format!("{result:?}"))?;
        ctx.check(ctx.tracer().elapsed() == Duration::from_millis(40), || {
            "40ms later".to_owned()
        })
    });
    assert_eq!(report.trace.count(FaultKind::StoreLatency), 1);
}

#[test]
fn crash_after_commits_then_takes_the_node_down() {
    let faults = StoreFaults {
        crash_after: Probability::ALWAYS,
        ..StoreFaults::none()
    };
    let report = run(async |ctx| {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut node = ctx.node("n");
        let store = ctx.faulty_store((), faults, &node.handle());
        let outcome = node
            .supervise(0, |_| {
                let store = store.clone();
                let calls = Arc::clone(&calls);
                async move { store.call("op", |f| f, counted(&calls)).await }
            })
            .await;
        ctx.check(
            matches!(outcome, Err(SuperviseError::RestartBudgetExhausted { .. })),
            || format!("{outcome:?}"),
        )?;
        ctx.check(calls.load(Ordering::Relaxed) == 1, || {
            "committed".to_owned()
        })
    });
    assert_eq!(report.trace.count(FaultKind::StoreCrashAfter), 1);
}

#[test]
fn without_faults_calls_pass_through() {
    let report = run(async |ctx| {
        let calls = AtomicUsize::new(0);
        let store = store(&ctx, StoreFaults::none());
        for _ in 0..10 {
            let result = store.call("op", |f| f, counted(&calls)).await;
            ctx.check(result == Ok(7), || format!("{result:?}"))?;
        }
        ctx.check(calls.load(Ordering::Relaxed) == 10, || {
            "ran each".to_owned()
        })
    });
    assert_eq!(report.trace.faults().count(), 0);
}

/// A blob store keyed by a fake digest: the length repeated.
#[derive(Debug, Clone, Default)]
struct ToyBlobs {
    blobs: Arc<Mutex<HashMap<MessageHash, Vec<u8>>>>,
}

impl ToyBlobs {
    fn len(&self) -> usize {
        self.blobs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

impl BlobStore for ToyBlobs {
    async fn put(&self, bytes: &[u8]) -> Result<MessageHash, BlobError> {
        let digest = [u8::try_from(bytes.len()).unwrap_or(u8::MAX); 32];
        let hash = MessageHash::from_digest(Blake3::from_bytes(digest));
        self.blobs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(hash, bytes.to_vec());
        Ok(hash)
    }

    async fn get(&self, hash: MessageHash) -> Result<Option<Vec<u8>>, BlobError> {
        Ok(self
            .blobs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&hash)
            .cloned())
    }
}

#[test]
fn blob_store_failures_read_as_unavailable() {
    let faults = StoreFaults {
        fail_before: Probability::ALWAYS,
        ..StoreFaults::none()
    };
    run(async |ctx| {
        let blobs = ToyBlobs::default();
        let node = ctx.node("n");
        let faulty = ctx.faulty_store(blobs.clone(), faults, &node.handle());
        let result = faulty.put(b"body").await;
        let expected = BlobError::Unavailable {
            reason: "sim: blob.put failed before running".to_owned(),
        };
        ctx.check(result == Err(expected), || format!("{result:?}"))?;
        ctx.check(blobs.len() == 0, || "nothing stored".to_owned())
    });
}

#[test]
fn blob_store_without_faults_round_trips() {
    run(async |ctx| {
        let node = ctx.node("n");
        let faulty = ctx.faulty_store(ToyBlobs::default(), StoreFaults::none(), &node.handle());
        let hash = faulty
            .put(b"body")
            .await
            .map_err(|e| CheckFailed::new(format!("{e:?}")))?;
        let got = faulty.get(hash).await;
        ctx.check(got == Ok(Some(b"body".to_vec())), || format!("{got:?}"))?;
        ctx.check(faulty.inner().len() == 1, || "stored".to_owned())
    });
}
