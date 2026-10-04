//! The scope filter of the reads not yet on the spec's linked-view
//! semantics (topic stats, projection samples):
//! empty lists do not restrict, non-empty lists combine with AND, agents
//! match the sender OR the reader after alias resolution, channels match
//! after supersession, topics are read under the scope's version and
//! outliers never match a topic filter. Transmissions are tested by when
//! they were opened, and unconfirmed ones are kept.
//!
//! The version is resolved by the same [`resolve_version`] every graph
//! uses; graphs, series, the edge drill-down and search count through
//! [`super::linked`] instead (confirmed transmissions by `Confirmed::at`,
//! admitted by `TopologyFilter::admits`).

use std::collections::HashSet;

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, ChannelId, TopicId};
use crosstalk_spec::support::TimeWindow;

use crate::backend::Result;
use crate::url::scope::Scope;
use crosstalk_spec::aggregates::filter::FalseDetections;
use crosstalk_spec::derived::flow::verdict::Verdict;

use super::Ctx;
use super::linked::resolve_version;
use crate::backend::fixture::world::TxRecord;

pub struct Filter<'a> {
    pub ctx: &'a Ctx<'a>,
    pub window: TimeWindow,
    pub version: TopicModelVersion,
    agents: HashSet<AgentId>,
    channels: HashSet<ChannelId>,
    routes: HashSet<RouteKind>,
    topics: HashSet<TopicId>,
    exclude_false: bool,
}

impl<'a> Filter<'a> {
    /// Resolves the scope's version as every linked view does
    /// ([`resolve_version`]): unknown `NotFound`, not retained
    /// `VersionNotRetained`, topics outside it `TopicsNotInVersion`.
    pub fn new(ctx: &'a Ctx<'a>, scope: &Scope) -> Result<Self> {
        let version = resolve_version(ctx.world, &scope.topology_filter())?;
        let f = &scope.filter;
        Ok(Self {
            ctx,
            window: scope.window,
            version,
            agents: f.agents.iter().map(|a| ctx.agent(*a)).collect(),
            channels: f.channels.iter().map(|c| ctx.channel(*c)).collect(),
            routes: f.route_kinds.iter().copied().collect(),
            topics: f.topics.iter().copied().collect(),
            exclude_false: f.false_detections == FalseDetections::Exclude,
        })
    }

    /// Whether a transmission is in the scope. Unconfirmed transmissions
    /// match an agent filter by their reader only, and never match a topic
    /// filter.
    pub fn keeps(&self, record: &TxRecord) -> bool {
        let t = &record.transmission;
        if !self.window.contains(t.opened_at) {
            return false;
        }
        if !self.agents.is_empty() {
            let to = self.ctx.agent(t.to);
            let from = record.from.map(|f| self.ctx.agent(f));
            if !self.agents.contains(&to) && !from.is_some_and(|f| self.agents.contains(&f)) {
                return false;
            }
        }
        if !self.channels.is_empty() {
            match &t.route {
                Route::Channel(c) if self.channels.contains(&self.ctx.channel(*c)) => {}
                _ => return false,
            }
        }
        if !self.routes.is_empty() && !self.routes.contains(&RouteKind::from(&t.route)) {
            return false;
        }
        if !self.topics.is_empty()
            && !record
                .topic(self.version)
                .is_some_and(|topic| self.topics.contains(&topic))
        {
            return false;
        }
        if self.exclude_false && self.ctx.verdict(t.id) == Some(Verdict::FalseDetection) {
            return false;
        }
        true
    }
}
