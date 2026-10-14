//! The publish spool in the gateway: the bus a Postgres-mode process
//! publishes on, and the `crosstalk spool` subcommand.
//!
//! ```text
//! publishers (capture, stages, store outboxes)
//!   ─▶ SpoolingBus (crosstalk-transport: fsynced spool on the data volume)
//!        ─▶ Gated<PgBus> ── gate closed: Disconnected (so the spool holds it)
//!                        └─ gate open:   PgBus (transport.events)
//! ```
//!
//! **The gate.** A consumer group that does not exist yet starts at the
//! log's head when it first subscribes: anything published before it
//! would never reach it. On a fresh database no pipeline group exists
//! until recovery subscribes them, while the proxy captures from the first
//! second. So until every pipeline group exists, the inner bus refuses
//! publishes as `Disconnected` and the spool keeps them; once the gate
//! opens, the spool drains them in order under their ids, and every group
//! receives them. The gate opens when this process's own pipeline has
//! subscribed every group ([`GateOpened::GroupsSubscribed`]) or, in a
//! process without the pipeline lock, as soon as it finds every group's
//! row in `transport.groups` ([`GateOpened::GroupsFound`],
//! `gateway::postgres::gate_watch`): another process subscribed them, and
//! a group's row carries its admission base from the moment it exists
//! (INV-1241 `ingress.capture.gate-opens-once-groups-exist`). Everything
//! else is the spool's own contract (`transport.spool.*`): only the gate's
//! or the database's `Disconnected` is spooled, a full spool refuses with
//! `SpoolFull`.
//!
//! [`discard_corrupt`] is `crosstalk spool --discard-corrupt`: offline (it
//! takes the spool's `LOCK`), it drops the corrupt record that stopped the
//! drain and everything after it in its segment.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::interfaces::l2_transport::{BusError, ConsumerGroup, EventBus, RetryPolicy};
use crosstalk_transport::spool::Discarded;
use crosstalk_transport::{DrainTarget, PgBus, PgSubscription, SpoolError, SpoolingBus};

use crate::config::{ConfigError, GatewayConfig, InvalidSpoolSection};

/// Whether the inner bus takes publishes yet. Clones share the gate.
#[derive(Debug, Clone, Default)]
pub struct Gate(Arc<AtomicBool>);

impl Gate {
    /// A closed gate.
    pub fn closed() -> Self {
        Self::default()
    }

    /// Let publishes through, for good: this process's pipeline subscribed
    /// every group.
    pub fn open(&self) {
        self.open_by(GateOpened::GroupsSubscribed);
    }

    /// Let publishes through, for good, logging why. `true` when this call
    /// opened it (it was closed).
    pub fn open_by(&self, by: GateOpened) -> bool {
        let opened = !self.0.swap(true, Ordering::AcqRel);
        if opened {
            tracing::info!(
                opened_by = by.label(),
                "capture gate opened: every pipeline group exists; the spool drains to the bus"
            );
        }
        opened
    }

    pub fn is_open(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Why a [`Gate`] opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateOpened {
    /// This process's pipeline incarnation subscribed every group.
    GroupsSubscribed,
    /// Every pipeline group's row was found in `transport.groups`, created
    /// by whichever process subscribed it.
    GroupsFound,
}

impl GateOpened {
    pub fn label(self) -> &'static str {
        match self {
            Self::GroupsSubscribed => "groups_subscribed",
            Self::GroupsFound => "groups_found",
        }
    }
}

/// `PgBus` behind a [`Gate`]: `Disconnected` while the gate is closed.
#[derive(Debug, Clone)]
pub struct Gated {
    bus: PgBus,
    gate: Gate,
}

impl Gated {
    pub fn new(bus: PgBus, gate: Gate) -> Self {
        Self { bus, gate }
    }

    pub fn bus(&self) -> &PgBus {
        &self.bus
    }

    pub fn gate(&self) -> &Gate {
        &self.gate
    }

    fn check(&self) -> Result<(), BusError> {
        match self.gate.is_open() {
            true => Ok(()),
            false => Err(BusError::Disconnected),
        }
    }
}

impl EventBus for Gated {
    type Subscription = PgSubscription;

    async fn publish(&self, envelope: Envelope) -> Result<(), BusError> {
        self.check()?;
        self.bus.publish(envelope).await
    }

    async fn subscribe(
        &self,
        subjects: &[Subject],
        group: ConsumerGroup,
        retry: RetryPolicy,
    ) -> Result<PgSubscription, BusError> {
        self.bus.subscribe(subjects, group, retry).await
    }
}

impl DrainTarget for Gated {
    async fn probe(&self) -> Result<(), BusError> {
        self.check()?;
        self.bus.probe().await
    }

    async fn publish_batch(&self, envelopes: Vec<Envelope>) -> Result<(), BusError> {
        self.check()?;
        self.bus.publish_batch(envelopes).await
    }
}

/// The bus a Postgres-mode process publishes on.
pub type LiveBus = SpoolingBus<Gated>;

/// Why `crosstalk spool --discard-corrupt` failed.
#[derive(Debug, thiserror::Error)]
pub enum SpoolCommandError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("the spool section: {0}")]
    Section(#[from] InvalidSpoolSection),
    #[error(transparent)]
    Spool(#[from] SpoolError),
}

/// Drop the corruption that stopped the spool's drain: the corrupt record
/// and everything after it in its segment. The gateway must be stopped
/// (the spool's `LOCK`). `None` when there was no corruption.
pub async fn discard_corrupt(
    config: &GatewayConfig,
) -> Result<Option<Discarded>, SpoolCommandError> {
    let spool = config.spool.spool_config(config.data_dir()?)?;
    let discarded = crosstalk_transport::spool::discard_corrupt(&spool).await?;
    match &discarded {
        Some(discarded) => tracing::warn!(
            segment = %discarded.segment,
            offset = discarded.offset,
            bytes = discarded.bytes,
            "corrupt spool records discarded"
        ),
        None => tracing::info!(dir = %spool.dir().display(), "the spool has no corruption"),
    }
    Ok(discarded)
}
