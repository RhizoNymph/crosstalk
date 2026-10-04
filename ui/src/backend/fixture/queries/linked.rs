//! What every linked view shares (`topology`, `channel_topology`, `series`,
//! `overview`, the edge drill-down, search, the projection fit): the
//! filter's topic version, resolved once per view by [`resolve_version`],
//! and the confirmed transmissions a graph counts, admitted exactly as
//! [`TopologyFilter::admits`] defines ([`Linked::counted`]).
//!
//! A transmission is counted by its confirmation (`Confirmed::at` in the
//! window, `Confirmed::from` as its sender), with both agents resolved
//! through merges and its route's channel through supersession; one whose
//! two agents resolve to one agent is a self-edge and counts nothing.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

use crosstalk_spec::aggregates::filter::{
    AccessSubject, FalseDetections, FilterSubject, TopicVersionSelector, TopologyFilter,
};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::topic_history::TopicVersionHistory;
use crosstalk_spec::aliases::Aliases;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::ids::{AgentId, ChannelId, TopicId};
use crosstalk_spec::interfaces::l7_topology::EdgeQueryError;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::support::{TimeWindow, Timestamp};

use crate::backend::Result;
use crate::backend::fixture::store::State;
use crate::backend::fixture::world::{TxRecord, World, confirmed};

use super::Ctx;

/// The version a linked view is computed under, as the edge store resolves
/// it: the selector resolved against `history` (unknown, fitting, never
/// activated and dropped versions refused), then the filter's topics checked
/// against that version.
pub fn resolve(
    filter: &TopologyFilter,
    history: &TopicVersionHistory,
    retained: impl Fn(TopicModelVersion) -> bool,
    version_of: impl Fn(TopicId) -> Option<TopicModelVersion>,
) -> std::result::Result<TopicModelVersion, EdgeQueryError> {
    let version = filter
        .topic_version
        .resolve(history, retained)
        .map_err(EdgeQueryError::Version)?;
    let topics = filter.topics_outside(version, version_of);
    if topics.is_empty() {
        Ok(version)
    } else {
        Err(EdgeQueryError::TopicsNotInVersion { version, topics })
    }
}

/// [`resolve`] over the world's catalog, mapped to the surface's errors:
/// unknown `NotFound`, never activated `Conflict(TopicVersionNotActivated)`,
/// dropped by retention `VersionNotRetained`, topics outside the version
/// `Conflict(TopicsNotInVersion)`.
pub fn resolve_version(
    world: &World,
    state: &State,
    filter: &TopologyFilter,
) -> Result<TopicModelVersion> {
    resolve(
        filter,
        &state.catalog,
        |version| state.retains(version),
        |topic| {
            world
                .topics
                .topics
                .iter()
                .find(|t| t.id == topic)
                .map(|t| t.version)
        },
    )
    .map_err(QueryError::from)
}

/// The version `selector` resolves to, as a linked view resolves it (no
/// topics to check): what `transmissions_by_id` reads topics under.
pub fn resolve_selector(
    world: &World,
    state: &State,
    selector: TopicVersionSelector,
) -> Result<TopicModelVersion> {
    resolve_version(
        world,
        state,
        &TopologyFilter {
            topic_version: selector,
            ..TopologyFilter::default()
        },
    )
}

/// The version a traversal's cursor pinned on its first page: still
/// readable, or `VersionNotRetained` once retention dropped it. A version
/// the catalog never had came from a cursor the fixture did not issue.
pub fn pinned_version(state: &State, version: TopicModelVersion) -> Result<TopicModelVersion> {
    if state.version_info(version).is_none() {
        return Err(QueryError::InvalidCursor);
    }
    if !state.retains(version) {
        return Err(QueryError::VersionNotRetained { version });
    }
    Ok(version)
}

/// A confirmed transmission as a graph counts it: resolved, in the window,
/// not a self-edge.
#[derive(Debug, Clone)]
pub struct Counted<'a> {
    pub record: &'a TxRecord,
    /// The canonical sender (`Confirmed::from`).
    pub from: AgentId,
    /// The canonical reader.
    pub to: AgentId,
    /// The route with its channel resolved.
    pub route: Route,
    /// `Confirmed::at`.
    pub at: Timestamp,
    pub matched_bytes: NonZeroU64,
    /// Under the view's version; `None` for an outlier.
    pub topic: Option<TopicId>,
    pub false_detection: bool,
}

/// One linked view's window (`None`: all time, as search may ask), filter
/// and resolved version.
pub struct Linked<'a> {
    pub ctx: &'a Ctx<'a>,
    window: Option<TimeWindow>,
    pub filter: &'a TopologyFilter,
    pub version: TopicModelVersion,
}

impl<'a> Linked<'a> {
    /// Resolves the filter's version once, for the whole view.
    pub fn new(ctx: &'a Ctx<'a>, window: TimeWindow, filter: &'a TopologyFilter) -> Result<Self> {
        Self::paged(ctx, Some(window), filter, None)
    }

    /// One page of a traversal: the first page resolves the filter's
    /// version; a later one reads under the version its cursor `pinned`
    /// ([`pinned_version`]), whatever is active now.
    pub fn paged(
        ctx: &'a Ctx<'a>,
        window: Option<TimeWindow>,
        filter: &'a TopologyFilter,
        pinned: Option<TopicModelVersion>,
    ) -> Result<Self> {
        let version = match pinned {
            Some(version) => pinned_version(ctx.state, version)?,
            None => resolve_version(ctx.world, ctx.state, filter)?,
        };
        Ok(Self {
            ctx,
            window,
            filter,
            version,
        })
    }

    /// Whether `at` is in the view's window.
    pub fn in_window(&self, at: Timestamp) -> bool {
        self.window.is_none_or(|window| window.contains(at))
    }

    /// Merges and supersessions as of this read.
    pub fn aliases(&self) -> impl Aliases + Copy + '_ {
        self.ctx.aliases()
    }

    /// `record` as the graph sees it, before the filter: `None` unless it is
    /// confirmed in the window and not a self-edge after resolution.
    fn candidate(&self, record: &'a TxRecord) -> Option<Counted<'a>> {
        self.resolved(record)
            .filter(|counted| counted.from != counted.to)
    }

    /// `record` resolved, when it is confirmed in the window.
    fn resolved(&self, record: &'a TxRecord) -> Option<Counted<'a>> {
        let confirmation = confirmed(&record.transmission.state)?;
        let at = confirmation.at();
        if !self.in_window(at) {
            return None;
        }
        let from = self.ctx.agent(confirmation.from());
        let to = self.ctx.agent(record.transmission.to);
        Some(Counted {
            record,
            from,
            to,
            route: self.ctx.route(&record.transmission.route),
            at,
            matched_bytes: confirmation.matched_bytes(),
            topic: record.topic(self.version),
            false_detection: self.ctx.verdict(record.transmission.id)
                == Some(Verdict::FalseDetection),
        })
    }

    fn admits(&self, counted: &Counted) -> bool {
        let subject = FilterSubject {
            from: counted.from,
            to: counted.to,
            route: &counted.route,
            topic: counted.topic,
            false_detection: counted.false_detection,
        };
        self.filter.admits(&subject, self.aliases())
    }

    /// Every transmission the view counts, oldest record first.
    pub fn counted(&self) -> Vec<Counted<'a>> {
        self.ctx
            .world
            .transmissions
            .iter()
            .filter_map(|record| self.candidate(record))
            .filter(|counted| self.admits(counted))
            .collect()
    }

    /// Every confirmed transmission in the window the filter admits, self-edges
    /// included (a graph drops them; search and projections do not), oldest
    /// record first.
    pub fn admitted(&self) -> Vec<Counted<'a>> {
        self.ctx
            .world
            .transmissions
            .iter()
            .filter_map(|record| self.resolved(record))
            .filter(|counted| self.admits(counted))
            .collect()
    }

    /// Per canonical channel, the topics (under the view's version) of the
    /// channel-routed transmissions confirmed on it in the window that the
    /// filter's `false_detections` keeps: an access's `channel_topics`.
    pub fn channel_topics(&self) -> BTreeMap<ChannelId, Vec<TopicId>> {
        let mut out: BTreeMap<ChannelId, BTreeSet<TopicId>> = BTreeMap::new();
        let exclude = self.filter.false_detections == FalseDetections::Exclude;
        for counted in self
            .ctx
            .world
            .transmissions
            .iter()
            .filter_map(|record| self.candidate(record))
        {
            if exclude && counted.false_detection {
                continue;
            }
            if let (Route::Channel(channel), Some(topic)) = (&counted.route, counted.topic) {
                out.entry(*channel).or_default().insert(topic);
            }
        }
        out.into_iter()
            .map(|(channel, topics)| (channel, topics.into_iter().collect()))
            .collect()
    }

    /// Whether an access by canonical `agent` on canonical `channel` passes,
    /// exactly as [`TopologyFilter::admits_access`] defines.
    pub fn admits_access(
        &self,
        agent: AgentId,
        channel: ChannelId,
        topics: &BTreeMap<ChannelId, Vec<TopicId>>,
    ) -> bool {
        let subject = AccessSubject {
            agent,
            channel,
            channel_topics: topics.get(&channel).map_or(&[], Vec::as_slice),
        };
        self.filter.admits_access(&subject, self.aliases())
    }
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::aggregates::filter::TopicVersionSelector;
    use crosstalk_spec::aggregates::topic_history::{
        CompletedFit, FitRecord, TopicVersionInfo, TopicVersionStatus,
    };
    use crosstalk_spec::interfaces::l8_surface::ConflictKind;

    use super::*;

    const HOUR: u64 = 3_600_000_000;

    fn fit(hours: u64, topics: u32) -> FitRecord {
        let at = Timestamp::from_micros(hours * HOUR);
        FitRecord::Fitted(CompletedFit {
            started_at: at,
            fitted_at: at,
            ready_at: at,
            topics,
        })
    }

    /// v0 active from hour 1, v1 from hour 2, v2 from hour 3.
    fn world_history() -> TopicVersionHistory {
        let at = |hours: u64| Timestamp::from_micros(hours * HOUR);
        TopicVersionHistory::new(vec![
            TopicVersionInfo::new(
                TopicModelVersion(0),
                TopicVersionStatus::Superseded {
                    fit: FitRecord::Unfitted,
                    activated_at: Some(at(1)),
                    by: TopicModelVersion(1),
                    superseded_at: at(2),
                },
            )
            .expect("v0"),
            TopicVersionInfo::new(
                TopicModelVersion(1),
                TopicVersionStatus::Superseded {
                    fit: fit(2, 3),
                    activated_at: Some(at(2)),
                    by: TopicModelVersion(2),
                    superseded_at: at(3),
                },
            )
            .expect("v1"),
            TopicVersionInfo::new(
                TopicModelVersion(2),
                TopicVersionStatus::Active {
                    fit: fit(3, 6),
                    activated_at: at(3),
                },
            )
            .expect("v2"),
        ])
        .expect("history")
    }

    fn filter(selector: TopicVersionSelector, topics: Vec<TopicId>) -> TopologyFilter {
        TopologyFilter {
            topics,
            topic_version: selector,
            ..TopologyFilter::default()
        }
    }

    fn topic(n: u128) -> TopicId {
        TopicId::from_ulid(n)
    }

    /// Topic `n` belongs to version `n / 10`.
    fn version_of(id: TopicId) -> Option<TopicModelVersion> {
        u32::try_from(id.as_ulid() / 10).ok().map(TopicModelVersion)
    }

    fn query(
        filter: &TopologyFilter,
        history: &TopicVersionHistory,
        retained: impl Fn(TopicModelVersion) -> bool,
    ) -> std::result::Result<TopicModelVersion, QueryError> {
        resolve(filter, history, retained, version_of).map_err(QueryError::from)
    }

    #[test]
    fn the_world_history_has_the_newest_version_active() {
        let history = world_history();
        assert_eq!(history.active().version(), TopicModelVersion(2));
        assert_eq!(history.versions().len(), 3);
    }

    #[test]
    fn current_and_activated_versions_resolve() {
        let history = world_history();
        let all = |_| true;
        assert_eq!(
            query(
                &filter(TopicVersionSelector::Current, Vec::new()),
                &history,
                all
            ),
            Ok(TopicModelVersion(2))
        );
        for v in 0..=2 {
            let pinned = TopicVersionSelector::Pinned(TopicModelVersion(v));
            assert_eq!(
                query(&filter(pinned, Vec::new()), &history, all),
                Ok(TopicModelVersion(v))
            );
        }
    }

    #[test]
    fn version_errors_follow_the_spec_mapping() {
        let history = world_history();
        let unknown = TopicVersionSelector::Pinned(TopicModelVersion(9));
        assert_eq!(
            query(&filter(unknown, Vec::new()), &history, |_| true),
            Err(QueryError::NotFound)
        );
        let v1 = TopicVersionSelector::Pinned(TopicModelVersion(1));
        assert_eq!(
            query(&filter(v1, Vec::new()), &history, |v| v
                != TopicModelVersion(1)),
            Err(QueryError::VersionNotRetained {
                version: TopicModelVersion(1)
            })
        );
        // A ready version a newer one overtook was never activated.
        let ready = TopicVersionHistory::new(vec![
            TopicVersionInfo::new(
                TopicModelVersion(0),
                TopicVersionStatus::Active {
                    fit: FitRecord::Unfitted,
                    activated_at: Timestamp::from_micros(HOUR),
                },
            )
            .expect("v0"),
            TopicVersionInfo::new(
                TopicModelVersion(1),
                TopicVersionStatus::Ready {
                    fit: CompletedFit {
                        started_at: Timestamp::from_micros(2 * HOUR),
                        fitted_at: Timestamp::from_micros(2 * HOUR),
                        ready_at: Timestamp::from_micros(2 * HOUR),
                        topics: 4,
                    },
                },
            )
            .expect("v1"),
        ])
        .expect("history");
        assert_eq!(
            query(&filter(v1, Vec::new()), &ready, |_| true),
            Err(QueryError::Conflict(
                ConflictKind::TopicVersionNotActivated {
                    version: TopicModelVersion(1)
                }
            ))
        );
    }

    #[test]
    fn topics_outside_the_resolved_version_conflict() {
        let history = world_history();
        let v2 = TopicVersionSelector::Pinned(TopicModelVersion(2));
        assert_eq!(
            query(&filter(v2, vec![topic(21), topic(22)]), &history, |_| true),
            Ok(TopicModelVersion(2))
        );
        assert_eq!(
            query(
                &filter(v2, vec![topic(11), topic(21), topic(11), topic(99)]),
                &history,
                |_| true
            ),
            Err(QueryError::Conflict(ConflictKind::TopicsNotInVersion {
                version: TopicModelVersion(2),
                topics: vec![topic(11), topic(99)],
            }))
        );
    }
}
