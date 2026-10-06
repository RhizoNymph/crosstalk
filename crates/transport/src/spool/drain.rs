//! The drainer task: probes the inner bus while spooling, sends the backlog
//! in order while draining, and switches back to `Direct` at the tail.

use std::sync::Arc;

use crosstalk_spec::events::Envelope;
use crosstalk_spec::interfaces::l2_transport::BusError;

use super::log::{ReadFailure, RecordMeta, segment_name};
use super::{DrainTarget, Shared, SpoolState, blocking};
use crate::codec;

/// What one drain step did.
enum Step {
    /// A batch reached the inner bus and the cursor moved past it.
    Sent,
    /// Nothing drainable: the spool is empty, or only records behind a
    /// corruption remain.
    Nothing,
    /// The inner bus is unreachable again.
    Unreachable,
    /// A record does not read back; draining stops before it.
    Corrupted,
    /// Anything else (a disk error, a refused batch); try again later.
    Failed,
    /// A test crash point was reached.
    #[cfg(test)]
    Crash,
}

pub(crate) async fn run<B>(shared: Arc<Shared<B>>)
where
    B: DrainTarget + Send + Sync + 'static,
{
    let probe = shared.config.probe();
    loop {
        let state = shared.state.borrow().clone();
        match state {
            SpoolState::Direct => shared.wake.notified().await,
            SpoolState::Spooling => match shared.inner.probe().await {
                Ok(()) => {
                    let _turn = shared.turn.lock().await;
                    if *shared.state.borrow() == SpoolState::Spooling {
                        shared.set_state(SpoolState::Draining);
                    }
                }
                Err(error) => {
                    tracing::debug!(error = ?error, "inner bus still unreachable");
                    tokio::time::sleep(probe).await;
                }
            },
            SpoolState::Draining | SpoolState::Corrupt { .. } => {
                let corrupt = matches!(state, SpoolState::Corrupt { .. });
                match drain_once(&shared).await {
                    Step::Sent => {}
                    #[cfg(test)]
                    Step::Crash => return,
                    Step::Nothing if !corrupt => match switch_direct(&shared).await {
                        Switch::Done | Switch::Busy => {}
                        Switch::Failed => tokio::time::sleep(probe).await,
                        #[cfg(test)]
                        Switch::Crash => return,
                    },
                    Step::Nothing => {
                        tokio::select! {
                            () = shared.wake.notified() => {}
                            () = tokio::time::sleep(probe) => {}
                        }
                    }
                    Step::Unreachable => {
                        if corrupt {
                            tokio::time::sleep(probe).await;
                        } else {
                            let _turn = shared.turn.lock().await;
                            if *shared.state.borrow() == SpoolState::Draining {
                                shared.set_state(SpoolState::Spooling);
                            }
                        }
                    }
                    Step::Corrupted => {
                        let barrier = shared.lock_log().barrier();
                        if let Some(barrier) = barrier {
                            let _turn = shared.turn.lock().await;
                            shared.set_state(SpoolState::Corrupt {
                                segment: segment_name(barrier.segment),
                                offset: barrier.offset,
                            });
                        }
                    }
                    Step::Failed => tokio::time::sleep(probe).await,
                }
            }
        }
    }
}

/// Decode a spooled payload as the bus decodes a delivery: strictly.
fn decode(payload: &[u8]) -> Result<Envelope, BusError> {
    serde_json::from_slice(payload).map_err(|error| BusError::Decode {
        reason: codec::reason(&error),
    })
}

async fn drain_once<B>(shared: &Arc<Shared<B>>) -> Step
where
    B: DrainTarget + Send + Sync + 'static,
{
    let size = shared.config.drain_batch();
    let read = blocking(&shared.log, move |log| {
        let metas = log.batch(size);
        if metas.is_empty() {
            return Ok(None);
        }
        log.read(&metas).map(|payloads| Some((metas, payloads)))
    })
    .await;
    let (metas, payloads): (Vec<RecordMeta>, Vec<Vec<u8>>) = match read {
        Ok(Ok(Some(batch))) => batch,
        Ok(Ok(None)) => return Step::Nothing,
        Ok(Err(ReadFailure::Corrupt {
            index,
            segment,
            offset,
        })) => {
            let _ = blocking(&shared.log, move |log| {
                log.mark_corrupt(index, segment, offset)
            })
            .await;
            return Step::Corrupted;
        }
        Ok(Err(ReadFailure::Io(error))) | Err(error) => {
            tracing::error!(error = %error, "reading the spool failed");
            return Step::Failed;
        }
    };
    let mut envelopes = Vec::with_capacity(payloads.len());
    for (index, payload) in payloads.iter().enumerate() {
        match decode(payload) {
            Ok(envelope) => envelopes.push(envelope),
            Err(error) => {
                // Checksummed bytes this binary cannot read: an operator
                // decides, as for corruption.
                let meta = metas[index];
                tracing::error!(record = meta.number, error = ?error, "a spooled record does not decode");
                let _ = blocking(&shared.log, move |log| {
                    log.mark_corrupt(index, meta.segment, meta.offset);
                })
                .await;
                return Step::Corrupted;
            }
        }
    }
    let count = envelopes.len();
    let first = metas.first().map(|m| m.number);
    match shared.inner.publish_batch(envelopes).await {
        Ok(()) => {}
        Err(BusError::Disconnected) => return Step::Unreachable,
        Err(error) => {
            tracing::error!(records = count, error = ?error, "the inner bus refused a spooled batch; retrying");
            return Step::Failed;
        }
    }
    #[cfg(test)]
    if shared.crash_at(super::CrashPoint::AfterBatchCommit) {
        return Step::Crash;
    }
    match blocking(&shared.log, move |log| log.advance(count)).await {
        Ok(Ok(())) => {
            tracing::debug!(records = count, first, "spooled batch drained");
            Step::Sent
        }
        Ok(Err(error)) | Err(error) => {
            // The batch is in the inner bus; resending it is harmless.
            tracing::error!(error = %error, "moving the spool cursor failed");
            Step::Failed
        }
    }
}

/// How a switch to `Direct` went.
enum Switch {
    Done,
    /// Records arrived (or a corruption barrier remains): keep draining.
    Busy,
    Failed,
    #[cfg(test)]
    Crash,
}

/// At the tail: under the publish mutex, if the spool is still empty,
/// remove its segments and go `Direct`.
async fn switch_direct<B>(shared: &Arc<Shared<B>>) -> Switch
where
    B: DrainTarget + Send + Sync + 'static,
{
    let _turn = shared.turn.lock().await;
    let (records, barrier) = {
        let log = shared.lock_log();
        (log.records(), log.barrier())
    };
    if let Some(barrier) = barrier {
        shared.set_state(SpoolState::Corrupt {
            segment: segment_name(barrier.segment),
            offset: barrier.offset,
        });
        return Switch::Busy;
    }
    if records > 0 {
        return Switch::Busy;
    }
    #[cfg(test)]
    if shared.crash_at(super::CrashPoint::BeforeDirectSwitch) {
        return Switch::Crash;
    }
    match blocking(&shared.log, |log| log.clear()).await {
        Ok(Ok(())) => {
            shared.set_state(SpoolState::Direct);
            Switch::Done
        }
        Ok(Err(error)) | Err(error) => {
            tracing::error!(error = %error, "removing drained spool segments failed");
            Switch::Failed
        }
    }
}
