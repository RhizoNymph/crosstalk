//! The consumer groups a Postgres-mode pipeline incarnation subscribes:
//! one list, which [`Live::start_pg`](crate::live::Live::start_pg)
//! subscribes in order and the capture gate's watch
//! (`gateway::postgres::gate_watch`) looks for in `transport.groups`.
//!
//! A group's row is created with its admission base in one statement
//! (`INSERT … admitted_through = <log head>`, `PgBus::subscribe`), and
//! groups are never deleted: once every row of the list exists, every
//! envelope appended to the log from then on is admitted by every group,
//! whoever subscribes it and whenever.

use crosstalk_spec::interfaces::l2_transport::ConsumerGroup;

use crate::live::Slot;
use crate::log::consumer as log_consumer;

/// The stage slots a Postgres-mode incarnation fills and subscribes, in
/// the order it subscribes them. No evidence slot: `PgEvidence` reads the
/// stores.
pub const PG_SLOTS: [Slot; 6] = [
    Slot::L3Reconstruct,
    Slot::L4Provenance,
    Slot::L5Flow,
    Slot::L6Classify,
    Slot::L7Topology,
    Slot::SurfaceRelay,
];

/// Whether the incarnation runs the exchange log, whose group is then one
/// of the pipeline's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExchangeLogGroup {
    Subscribed,
    Absent,
}

impl ExchangeLogGroup {
    /// [`ExchangeLogGroup::Subscribed`] when the incarnation runs an
    /// exchange log.
    pub fn when(runs_log: bool) -> Self {
        match runs_log {
            true => Self::Subscribed,
            false => Self::Absent,
        }
    }
}

/// Every consumer group of one pipeline incarnation: the exchange log's
/// (when it runs one) first, then each of [`PG_SLOTS`]'s.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineGroups {
    log: ExchangeLogGroup,
}

impl PipelineGroups {
    pub fn new(log: ExchangeLogGroup) -> Self {
        Self { log }
    }

    /// Whether the exchange log's group is one of them.
    pub fn log(&self) -> ExchangeLogGroup {
        self.log
    }

    /// The slots whose groups are among them, in subscription order.
    pub fn slots(&self) -> &'static [Slot] {
        &PG_SLOTS
    }

    /// Every group, in subscription order.
    pub fn groups(&self) -> Vec<ConsumerGroup> {
        let log = match self.log {
            ExchangeLogGroup::Subscribed => Some(log_consumer::group()),
            ExchangeLogGroup::Absent => None,
        };
        log.into_iter()
            .chain(PG_SLOTS.iter().map(|slot| slot.group()))
            .collect()
    }

    /// Every group's name, as `transport.groups.name` holds it.
    pub fn names(&self) -> Vec<String> {
        self.groups().into_iter().map(|group| group.0).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn the_exchange_log_group_comes_first_when_it_runs() {
        let names = PipelineGroups::new(ExchangeLogGroup::Subscribed).names();
        assert_eq!(names.first().map(String::as_str), Some(log_consumer::GROUP));
        assert_eq!(names.len(), PG_SLOTS.len() + 1);
    }

    #[test]
    fn without_the_exchange_log_only_the_slots_groups_remain() {
        let names = PipelineGroups::new(ExchangeLogGroup::Absent).names();
        let slots: Vec<String> = PG_SLOTS.iter().map(|slot| slot.group().0).collect();
        assert_eq!(names, slots);
    }

    #[test]
    fn the_groups_are_distinct_and_never_the_evidence_slots() {
        let names = PipelineGroups::new(ExchangeLogGroup::Subscribed).names();
        let distinct: BTreeSet<&String> = names.iter().collect();
        assert_eq!(distinct.len(), names.len(), "{names:?}");
        assert!(!names.contains(&Slot::Evidence.group().0), "{names:?}");
    }

    #[test]
    fn the_slots_are_in_subscription_order() {
        // `Stages::into_plugs` hands the plugs back ordered by slot.
        let mut sorted = PG_SLOTS;
        sorted.sort();
        assert_eq!(sorted, PG_SLOTS);
    }
}
