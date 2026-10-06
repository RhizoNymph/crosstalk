//! Bus conformance: the group semantics every [`EventBus`] +
//! [`DeadLetterStore`] of this crate shares, written once over a [`Kit`]
//! and run over [`MpscBus`] (paused clock) and [`PgBus`] (a fresh test
//! database each, real time; skipped without `TEST_DATABASE_URL`).
//!
//! What only one bus has stays in its own tests: `MpscBus`'s backpressure,
//! foreign bytes and seeded delivery order (`crate::tests`, `crate::dst`);
//! `PgBus`'s durability, idempotent publish, restart recovery, prune and
//! stats (`crate::integration`).

mod cases;

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use crosstalk_spec::interfaces::l2_transport::{DeadLetterStore, EventBus};
use crosstalk_spec::support::SystemClock;
use crosstalk_store::TestDb;

use crate::testing::{config, non_zero};
use crate::{BusConfig, MpscBus, PgBus, PgBusConfig};

/// What a case may tune.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Settings {
    pub(crate) ack_timeout: Duration,
    pub(crate) capacity: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            ack_timeout: Duration::from_secs(30),
            capacity: 64,
        }
    }
}

/// A bus under test.
pub(crate) trait Kit: Sync {
    type Bus: EventBus + Send + Sync + 'static;
    type Letters: DeadLetterStore + Send + Sync;

    /// A fresh bus and its dead-letter store.
    fn start(&self, settings: Settings) -> impl Future<Output = (Self::Bus, Self::Letters)> + Send;

    /// How long a case waits to conclude that nothing arrives.
    fn quiet(&self) -> Duration;

    /// The ack timeout a case that times a delivery out runs with: long
    /// enough that a consumer acking at once always beats it, which over a
    /// remote, shared database takes more than in process.
    fn ack_timeout(&self) -> Duration;
}

pub(crate) struct MpscKit;

impl Kit for MpscKit {
    type Bus = MpscBus;
    type Letters = crate::DeadLetters;

    async fn start(&self, settings: Settings) -> (MpscBus, crate::DeadLetters) {
        let bus = MpscBus::start(BusConfig {
            ack_timeout: non_zero(settings.ack_timeout),
            group_capacity: NonZeroUsize::new(settings.capacity).expect("non-zero"),
            ..config()
        })
        .expect("bus starts");
        let letters = bus.dead_letters();
        (bus, letters)
    }

    fn quiet(&self) -> Duration {
        Duration::from_secs(10)
    }

    fn ack_timeout(&self) -> Duration {
        Duration::from_millis(300)
    }
}

pub(crate) struct PgKit {
    db: TestDb,
}

impl PgKit {
    /// A migrated test database, or `None` (a skip) without one.
    pub(crate) async fn new(test: &str) -> Option<Self> {
        let db = TestDb::new_or_skip(test).await.expect("test database")?;
        crate::pg::migrate(db.pool())
            .await
            .expect("transport migrates");
        Some(Self { db })
    }

    pub(crate) async fn close(self) {
        self.db.close().await.expect("drop the test database");
    }
}

impl Kit for PgKit {
    type Bus = PgBus;
    type Letters = crate::PgDeadLetters;

    async fn start(&self, settings: Settings) -> (PgBus, crate::PgDeadLetters) {
        let config = PgBusConfig {
            ack_timeout: non_zero(settings.ack_timeout),
            group_capacity: NonZeroUsize::new(settings.capacity).expect("non-zero"),
            poll: non_zero(Duration::from_millis(50)),
            ..PgBusConfig::default()
        };
        let bus =
            PgBus::new(self.db.pool().clone(), Arc::new(SystemClock), config).expect("bus starts");
        bus.recover_held().await.expect("recovers");
        let letters = bus.dead_letters();
        (bus, letters)
    }

    fn quiet(&self) -> Duration {
        Duration::from_millis(600)
    }

    fn ack_timeout(&self) -> Duration {
        Duration::from_secs(2)
    }
}

/// Each case once over `MpscBus` and once over `PgBus`.
macro_rules! conformance {
    ($($name:ident),* $(,)?) => {
        mod mpsc {
            $(
                #[tokio::test(start_paused = true)]
                async fn $name() {
                    super::cases::$name(&super::MpscKit).await;
                }
            )*
        }
        mod pg {
            $(
                #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
                async fn $name() {
                    let Some(kit) = super::PgKit::new(concat!("conformance::pg::", stringify!($name))).await else {
                        return;
                    };
                    super::cases::$name(&kit).await;
                    kit.close().await;
                }
            )*
        }
    };
}

conformance!(
    each_group_gets_every_envelope_and_members_share_them,
    a_group_keeps_envelopes_while_no_consumer_is_connected,
    a_group_gets_nothing_published_before_it_subscribed,
    a_subscription_yields_only_its_subjects_unchanged,
    ack_of_unheld_delivery_is_unknown,
    nacks_count_attempts_then_dead_letter,
    an_ack_timeout_redelivers_and_refuses_the_late_ack,
    a_dropped_holder_is_redelivered,
    replay_delivers_to_its_group_alone_at_attempt_one,
    subscribe_rejects_another_subject_set_or_policy,
    dead_letters_list_newest_first_with_cursors,
);
