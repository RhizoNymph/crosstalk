//! A stage whose store stalled (a statement Postgres cancelled at its
//! `statement_timeout` or `lock_timeout`) releases its delivery without
//! spending an attempt; a failure of the message itself still nacks.

use std::num::NonZeroU32;
use std::time::Duration;

use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{AgentId, EventId};
use crosstalk_spec::interfaces::l2_transport::{
    BusError, Delivery, DeliveryId, RetryPolicy, Subscription,
};
use crosstalk_spec::support::Timestamp;
use crosstalk_store::TimeoutKind;

use super::stage::{STALL_RETRY, Slot, StageError, settle_delivery, stalled};

/// What a stage settled a delivery with.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Settled {
    Ack(DeliveryId),
    Nack(DeliveryId, Duration, String),
    Release(DeliveryId, Duration, String),
}

#[derive(Default)]
struct Recording {
    settled: Vec<Settled>,
}

impl Subscription for Recording {
    async fn next(&mut self) -> Option<Result<Delivery, BusError>> {
        None
    }

    async fn ack(&mut self, id: DeliveryId) -> Result<(), BusError> {
        self.settled.push(Settled::Ack(id));
        Ok(())
    }

    async fn nack(
        &mut self,
        id: DeliveryId,
        retry_after: Duration,
        reason: String,
    ) -> Result<(), BusError> {
        self.settled.push(Settled::Nack(id, retry_after, reason));
        Ok(())
    }

    async fn release(
        &mut self,
        id: DeliveryId,
        retry_after: Duration,
        reason: String,
    ) -> Result<(), BusError> {
        self.settled.push(Settled::Release(id, retry_after, reason));
        Ok(())
    }
}

fn delivery() -> Delivery {
    Delivery {
        id: DeliveryId(7),
        attempt: NonZeroU32::MIN,
        envelope: Envelope {
            id: EventId::from_ulid(7),
            at: Timestamp::from_micros(7),
            event: BusEvent::Changed(Changed::Agent(AgentId::from_ulid(7))),
        },
    }
}

fn policy() -> RetryPolicy {
    RetryPolicy::new(
        NonZeroU32::MIN.saturating_add(4),
        Duration::from_millis(100),
        Duration::from_secs(30),
    )
    .expect("a policy")
}

/// A retry the stage asked for while a store statement was cancelled at
/// its bound is a stall; any other outcome is left as it is.
#[test]
fn a_retry_during_a_timeout_is_a_stall() {
    let retry = || {
        Err(StageError::Retry {
            reason: "store: lock timeout (SQLSTATE 55P03)".to_owned(),
        })
    };
    assert_eq!(
        stalled(retry(), Some(TimeoutKind::Lock)),
        Err(StageError::Stalled {
            kind: TimeoutKind::Lock,
            reason: "store: lock timeout (SQLSTATE 55P03)".to_owned(),
        })
    );
    assert_eq!(stalled(retry(), None), retry());
    assert_eq!(stalled(Ok(()), Some(TimeoutKind::Statement)), Ok(()));
    let reject = Err(StageError::Reject {
        reason: "malformed".to_owned(),
    });
    assert_eq!(
        stalled(reject.clone(), Some(TimeoutKind::Statement)),
        reject,
        "a refusal is the message's, whatever else timed out"
    );
}

/// A stall releases the delivery after [`STALL_RETRY`]; a retry nacks it
/// after the policy's initial backoff; success acks it.
#[tokio::test]
async fn a_stall_releases_and_a_retry_nacks() {
    let mut subscription = Recording::default();
    let delivery = delivery();
    settle_delivery(
        Slot::L4Provenance,
        &mut subscription,
        policy(),
        &delivery,
        Err(StageError::Stalled {
            kind: TimeoutKind::Lock,
            reason: "lock timeout (SQLSTATE 55P03)".to_owned(),
        }),
    )
    .await;
    settle_delivery(
        Slot::L4Provenance,
        &mut subscription,
        policy(),
        &delivery,
        Err(StageError::Retry {
            reason: "the message's bodies are missing".to_owned(),
        }),
    )
    .await;
    settle_delivery(
        Slot::L4Provenance,
        &mut subscription,
        policy(),
        &delivery,
        Ok(()),
    )
    .await;
    assert_eq!(
        subscription.settled,
        vec![
            Settled::Release(
                DeliveryId(7),
                STALL_RETRY,
                "lock timeout (SQLSTATE 55P03)".to_owned()
            ),
            Settled::Nack(
                DeliveryId(7),
                Duration::from_millis(100),
                "the message's bodies are missing".to_owned()
            ),
            Settled::Ack(DeliveryId(7)),
        ]
    );
}

/// The stall backoff keeps a stalled stage from spinning: every round
/// already waits out a bound, and then this.
#[test]
fn the_stall_backoff_is_a_second() {
    assert_eq!(STALL_RETRY, Duration::from_secs(1));
}
