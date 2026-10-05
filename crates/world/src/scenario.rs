//! Named handles into a seeded world, for tests and for the UI's scenario
//! list: which id each role got.
//!
//! Agents are named by the short keys the UI fixture used (`cc0`, `cc0.a`,
//! `pi1`, `al0`; [`Scenario::agent`]). Channels, merges and rules have
//! enums. Ids a store assigns (declared channels, merge records, user
//! rules) are the ones the seeded stores returned.

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::{
    AgentId, AlertRuleId, ChannelId, MergeId, OperatorId, ProjectionId, ResourceId, SinkId,
    TopicId, TransmissionId,
};
use crosstalk_spec::interfaces::l8_surface::SinkKind;

use crate::error::WorldError;

/// Every channel role in the world: the fifteen stored channels
/// ([`ChannelKey::ALL`]) and [`ChannelKey::Scratch`], the channel the
/// world never creates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ChannelKey {
    /// Declared, sanctioned, active: `wiki.corp.internal/eng`.
    InternalWiki,
    /// Declared, sanctioned, active: `git.corp.internal/platform/monorepo`.
    Monorepo,
    /// Declared, sanctioned, active: host `issues.corp.internal`.
    IssueTracker,
    /// Declared, sanctioned, awaiting traffic: `docs.corp.internal/design`.
    DesignDocs,
    /// Declared, sanctioned, unused: `/mnt/shared/releases` on `nfs-01`.
    ReleaseBucket,
    /// Discovered at `notes.corp.internal/team-a/retro`, then promoted by
    /// the researcher with the `/team-a` prefix (sanctioned): declared, same
    /// id, active. Its promotion superseded `OldTeamNotes`.
    TeamNotes,
    /// Discovered, unreviewed, active: a public wiki page agents use to
    /// coordinate (`wiki.example.org/wiki/Agent_Coordination`).
    HijackedWiki,
    /// Discovered, unreviewed, active: the same wiki's talk page.
    WikiTalk,
    /// Discovered, unsanctioned, active: `paste.example.net/raw/q8Zt3LmK`.
    Pastebin,
    /// Discovered, reset to unreviewed, active: the `memory` MCP server's
    /// `create_entities` on `project-atlas`.
    McpMemory,
    /// Discovered, sanctioned, active: `/tmp/agent-handoff/plan.md` on
    /// `devbox-3`.
    SharedFile,
    /// Discovered, unreviewed, dormant: `gist.example.com`.
    Gist,
    /// Discovered, unreviewed, active and listed unconfirmed: an S3
    /// object one agent writes and two others read, with no content match,
    /// so every transmission through it is suspected (or discarded, or
    /// awaiting content).
    S3Handoff,
    /// Discovered at `/home/dev/.codex/handoff.md` on `devbox-7`,
    /// unreviewed, dormant: its only cross-agent traffic is between `al1`
    /// and `cx1`, which an operator later merged, so it is hidden while the
    /// merge stands (and would be listed again by an unmerge).
    SelfNotes,
    /// Discovered at `notes.corp.internal/team-a/standup`, unreviewed,
    /// superseded by `TeamNotes`'s promotion (detection frozen there).
    OldTeamNotes,
    /// Not a channel: the id minted for a channel of the key-value entry
    /// only `cc7` writes and reads ([`Scenario::lone_resource`]). A
    /// resource only one agent uses carries no transmission, so no channel
    /// is ever discovered from it and no store holds this id; the key lets
    /// readers check that.
    Scratch,
}

impl ChannelKey {
    /// The stored channels' roles: every key but [`ChannelKey::Scratch`].
    pub const ALL: [Self; 15] = [
        Self::InternalWiki,
        Self::Monorepo,
        Self::IssueTracker,
        Self::DesignDocs,
        Self::ReleaseBucket,
        Self::TeamNotes,
        Self::HijackedWiki,
        Self::WikiTalk,
        Self::Pastebin,
        Self::McpMemory,
        Self::SharedFile,
        Self::Gist,
        Self::S3Handoff,
        Self::SelfNotes,
        Self::OldTeamNotes,
    ];
}

/// The merges in the world's history, oldest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MergeKey {
    /// `al2` into `al3` by the resolver, six days ago: the first link of a
    /// pi agent's chain.
    PiChainFirst,
    /// `omp3` into `omp1` by the resolver, reverted by the researcher two
    /// days ago, leaving a veto.
    Reverted,
    /// `al0` into `cc0` (atlas-lead) by the resolver: the alias's traffic
    /// to `cc0` becomes a self-edge at read time.
    AtlasAlias,
    /// `al3` into `pi2` by the researcher, which repointed `al2`.
    PiChainSecond,
    /// `al1` into `cx1` by the researcher: two ids of one Codex agent whose
    /// handoff file (`SelfNotes`) then carries traffic within one agent.
    CodexAlias,
}

impl MergeKey {
    pub const ALL: [Self; 5] = [
        Self::PiChainFirst,
        Self::Reverted,
        Self::AtlasAlias,
        Self::PiChainSecond,
        Self::CodexAlias,
    ];
}

/// The user rules, by role. Built-in rules have their fixed ids
/// (`BuiltinRule::id`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RuleKey {
    /// A watched-topic rule on v2: credentials and agent instructions.
    Watch,
    /// A watched-topic rule on v1's "Engineering chatter", stale since v2
    /// was ready (and still enabled).
    Stale,
    /// A semantic query on paste sites, with an agent-subject alert.
    Semantic,
    /// A semantic query on refund escalations, disabled three days ago.
    Refunds,
}

impl RuleKey {
    pub const ALL: [Self; 4] = [Self::Watch, Self::Stale, Self::Semantic, Self::Refunds];
}

/// Which side of a transmission's matches lost its bodies to content
/// retention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BodySide {
    /// The sender's reply holding the originated span.
    Sender,
    /// The message the reader's copy arrived in.
    Reader,
}

/// The seeded projection jobs, one per status the fitter does not leave a
/// new job in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum JobKey {
    /// The first day under v1, fitted and since expired.
    Expired,
    /// Pinned to v0, failed when v2's activation dropped v0.
    Failed,
    /// The last day, fitting since a minute ago.
    Fitting,
    /// The whole week, queued behind it.
    Queued,
}

/// The ids every role of a seeded world got.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Scenario {
    pub(crate) agents: BTreeMap<String, AgentId>,
    pub(crate) channels: BTreeMap<ChannelKey, ChannelId>,
    pub(crate) merges: BTreeMap<MergeKey, MergeId>,
    pub(crate) rules: BTreeMap<RuleKey, AlertRuleId>,
    pub(crate) jobs: BTreeMap<JobKey, ProjectionId>,
    pub(crate) sinks: Vec<(SinkKind, SinkId)>,
    /// Topic ids of each fitted version, ascending.
    pub(crate) topics: BTreeMap<TopicModelVersion, Vec<TopicId>>,
    /// `cc7`'s key-value scratch entry.
    pub(crate) lone_resource: Option<ResourceId>,
    /// Old transmissions whose sender's or reader's bodies content
    /// retention dropped (the seed never stored them), oldest first.
    pub(crate) dropped: Vec<(TransmissionId, BodySide)>,
    /// Agents that send Claude Code's User-Agent on Claude traffic while
    /// running in another harness.
    pub(crate) impersonators: Vec<AgentId>,
    /// Config-registered agents that never sent traffic.
    pub(crate) registered: Vec<AgentId>,
    /// The topic version v1's "Engineering chatter" belongs to, and that
    /// topic.
    pub(crate) unmapped_topic: Option<TopicId>,
}

impl Scenario {
    /// The agent with fixture key `key` (`cc0`, `cc0.a`, `pi1`, `al0`, ...).
    pub fn agent(&self, key: &str) -> Option<AgentId> {
        self.agents.get(key).copied()
    }

    /// Every agent key and its id, by key.
    pub fn agents(&self) -> impl Iterator<Item = (&str, AgentId)> {
        self.agents.iter().map(|(key, id)| (key.as_str(), *id))
    }

    pub fn channel(&self, key: ChannelKey) -> Option<ChannelId> {
        self.channels.get(&key).copied()
    }

    pub fn merge(&self, key: MergeKey) -> Option<MergeId> {
        self.merges.get(&key).copied()
    }

    pub fn rule(&self, key: RuleKey) -> Option<AlertRuleId> {
        self.rules.get(&key).copied()
    }

    pub fn job(&self, key: JobKey) -> Option<ProjectionId> {
        self.jobs.get(&key).copied()
    }

    pub fn sink(&self, kind: SinkKind) -> Option<SinkId> {
        self.sinks
            .iter()
            .find(|(k, _)| *k == kind)
            .map(|(_, id)| *id)
    }

    /// The topics of `version`, ascending; empty for an unfitted version.
    pub fn topics(&self, version: TopicModelVersion) -> &[TopicId] {
        self.topics.get(&version).map_or(&[], Vec::as_slice)
    }

    /// v1's "Engineering chatter": no v2 topic links to it at the remap
    /// threshold, so the rule watching it is stale.
    pub fn unmapped_topic(&self) -> Option<TopicId> {
        self.unmapped_topic
    }

    /// `cc7`'s key-value scratch entry, which only one agent uses.
    pub fn lone_resource(&self) -> Option<ResourceId> {
        self.lone_resource
    }

    /// Transmissions one side of whose bodies content retention dropped.
    pub fn dropped(&self) -> &[(TransmissionId, BodySide)] {
        &self.dropped
    }

    pub fn impersonators(&self) -> &[AgentId] {
        &self.impersonators
    }

    pub fn registered(&self) -> &[AgentId] {
        &self.registered
    }

    /// The researcher (every permission) and the on-call operator.
    pub fn operators(&self) -> [OperatorId; 2] {
        [
            crate::config::OPERATOR_RESEARCHER,
            crate::config::OPERATOR_ONCALL,
        ]
    }

    /// Like [`Scenario::agent`], failing on an unknown key.
    pub fn agent_id(&self, key: &str) -> Result<AgentId, WorldError> {
        self.agent(key)
            .ok_or_else(|| WorldError::missing(format!("agent {key}")))
    }

    /// Like [`Scenario::channel`], failing on an unknown key.
    pub fn channel_id(&self, key: ChannelKey) -> Result<ChannelId, WorldError> {
        self.channel(key)
            .ok_or_else(|| WorldError::missing(format!("channel {key:?}")))
    }
}
