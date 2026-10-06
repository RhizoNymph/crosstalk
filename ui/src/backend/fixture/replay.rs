//! Replay mode: the data's last stretch plays out in accelerated real time.
//!
//! The replay clock (`Clock::Replay`) hides everything stamped after its
//! present. Every read through `FixtureBackend::read` sees a [`Snapshot`]:
//! the world and state truncated at the clock's present (`World::at`,
//! `State::at`), rebuilt when the present moves past the cached one by a
//! [`STEP`] or an action changes the state. A ticker publishes, every
//! [`TICK`], the watermark and a `Changed` for every alert, channel and
//! agent whose records became visible since the last tick, so pages that
//! watch them refresh. At [`clock::NOW`] the replay stops and the ticker
//! goes quiet.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::ids::TransmissionId;
use crosstalk_spec::support::{Timestamp, Watermark};
use tokio::sync::Mutex;

use super::clock::{self, Clock, SECOND};
use super::live::Feed;
use super::store::State;
use super::world::World;
use super::world::conversations::LazyConversations;

/// How often the ticker publishes.
pub const TICK: Duration = Duration::from_secs(2);

/// How far (in data time) the present moves before a read rebuilds the
/// snapshot.
pub const STEP: u64 = 10 * SECOND;

/// The world and state as they stood at `cutoff`.
#[derive(Debug)]
pub struct Snapshot {
    pub cutoff: Timestamp,
    pub world: World,
    pub state: State,
    pub watermark: Timestamp,
    /// The full world's conversations cut at `cutoff`, made on the
    /// snapshot's first conversation read.
    pub conversations: LazyConversations,
}

impl Snapshot {
    pub fn build(world: &World, state: &State, cutoff: Timestamp) -> Self {
        let world = world.at(cutoff);
        let visible: HashSet<TransmissionId> = world.tx_index.keys().copied().collect();
        let state = state.at(cutoff, |id| visible.contains(&id));
        Self {
            cutoff,
            world,
            state,
            watermark: clock::replay_watermark(cutoff),
            conversations: LazyConversations::default(),
        }
    }
}

/// `at` rounded down to a [`STEP`].
pub fn quantize(at: Timestamp) -> Timestamp {
    Timestamp::from_micros(at.as_micros() - at.as_micros() % STEP)
}

/// The cached snapshot and what becomes visible when.
#[derive(Debug)]
pub struct Replay {
    cache: Mutex<Option<Arc<Snapshot>>>,
    /// Every change a replay reveals, by the time it becomes visible.
    schedule: Arc<Vec<(Timestamp, Changed)>>,
}

impl Replay {
    pub fn new(world: &World, state: &State, from: Timestamp) -> Self {
        let mut schedule: Vec<(Timestamp, Changed)> = Vec::new();
        for alert in &state.alerts {
            schedule.push((alert.raised_at, Changed::Alert(alert.id)));
        }
        for (id, record) in &state.channels {
            schedule.push((record.created, Changed::Channel(*id)));
        }
        for record in &world.transmissions {
            let t = &record.transmission;
            if let Route::Channel(channel) = t.route {
                schedule.push((t.opened_at, Changed::Channel(channel)));
            }
            schedule.push((t.opened_at, Changed::Agent(t.to)));
            if let Some(from) = record.from {
                schedule.push((t.opened_at, Changed::Agent(from)));
            }
        }
        schedule.retain(|(at, _)| *at > from);
        schedule.sort_by_key(|(at, _)| *at);
        Self {
            cache: Mutex::new(None),
            schedule: Arc::new(schedule),
        }
    }

    /// The snapshot at `now` (quantized), rebuilt when stale.
    pub async fn snapshot(&self, world: &World, state: &State, now: Timestamp) -> Arc<Snapshot> {
        let cutoff = quantize(now);
        let mut cache = self.cache.lock().await;
        if let Some(snapshot) = cache.as_ref()
            && snapshot.cutoff == cutoff
        {
            return Arc::clone(snapshot);
        }
        let snapshot = Arc::new(Snapshot::build(world, state, cutoff));
        *cache = Some(Arc::clone(&snapshot));
        snapshot
    }

    /// Drops the cached snapshot: the state changed.
    pub async fn invalidate(&self) {
        *self.cache.lock().await = None;
    }

    /// Spawns the ticker that publishes what the replay reveals.
    pub fn spawn_ticker(&self, clock: Clock, feed: Arc<Feed>) -> tokio::task::JoinHandle<()> {
        let schedule = Arc::clone(&self.schedule);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(TICK);
            let mut last = clock.now();
            loop {
                interval.tick().await;
                let now = clock.now();
                if now <= last {
                    continue;
                }
                let mut changes = changes_between(&schedule, last, now);
                changes.push(Changed::Watermark(Watermark(clock.watermark())));
                tracing::debug!(
                    now = ?now,
                    changes = changes.len(),
                    "replay tick"
                );
                feed.publish(changes).await;
                last = now;
            }
        })
    }
}

/// The changes that became visible in `(after, until]`, each once.
fn changes_between(
    schedule: &[(Timestamp, Changed)],
    after: Timestamp,
    until: Timestamp,
) -> Vec<Changed> {
    let start = schedule.partition_point(|(at, _)| *at <= after);
    let end = schedule.partition_point(|(at, _)| *at <= until);
    let mut seen = HashSet::new();
    schedule
        .get(start..end)
        .unwrap_or(&[])
        .iter()
        .filter(|(_, change)| seen.insert(*change))
        .map(|(_, change)| *change)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::fixture::clock::{HOUR, NOW, ago};

    #[test]
    fn changes_between_reveals_each_change_once_in_its_interval() {
        let a = Changed::Watermark(Watermark(NOW));
        let b = Changed::Watermark(Watermark(ago(HOUR)));
        let schedule = vec![(ago(3 * HOUR), a), (ago(2 * HOUR), a), (ago(HOUR), b)];
        assert_eq!(changes_between(&schedule, ago(4 * HOUR), NOW), vec![a, b]);
        assert_eq!(
            changes_between(&schedule, ago(3 * HOUR), ago(HOUR)),
            vec![a, b]
        );
        assert!(changes_between(&schedule, ago(HOUR), NOW).is_empty());
    }
}
