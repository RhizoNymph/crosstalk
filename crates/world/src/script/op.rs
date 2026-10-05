//! One step of the seed script: a store write (or a short sequence of the
//! writes one pipeline component makes for one event), named by what the
//! world means, with keys standing in for the ids a store assigns.

use crosstalk_spec::aggregates::alert::{AlertSubject, BuiltinRule, RuleName, UserRule};
use crosstalk_spec::aggregates::projection::frame::ProjectionFrame;
use crosstalk_spec::aggregates::projection::{FitFailure, ProjectionInfo};
use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::aggregates::watermark::PipelineFrontier;
use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::channel::policy::{PolicyDecision, PolicyKind};
use crosstalk_spec::derived::flow::channel::promotion::Promotion;
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::ids::{
    AgentId, ChannelId, ConfigHash, MessageHash, OperatorId, ProjectionId, ResourceId, SinkId,
    TransmissionId,
};
use crosstalk_spec::interfaces::l2_transport::DeadLetter;
use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::{Advance, NewAgent};
use crosstalk_spec::interfaces::l5_flow::channels::DetectionUpdate;
use crosstalk_spec::interfaces::l6_analysis::corpus::IndexedTransmission;
use crosstalk_spec::interfaces::l6_analysis::lifecycle::StoredAssignment;
use crosstalk_spec::interfaces::l7_topology::EdgeContribution;
use crosstalk_spec::interfaces::l8_surface::SinkError;
use crosstalk_spec::interfaces::l8_surface::audit::ConfigChange;
use crosstalk_spec::observed::agent::{AgentLabel, MergeRequest};
use crosstalk_spec::observed::client::HarnessClaim;
use crosstalk_spec::support::Timestamp;

use crate::scenario::{MergeKey, RuleKey};

/// A planned alert: the n-th alert the plan raises. Its id is the one
/// triage gives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AlertKey(pub u32);

/// The rule a planned alert is raised by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RuleRef {
    Builtin(BuiltinRule),
    User(RuleKey),
}

/// One write, or the writes one component makes for one event.
#[derive(Debug, Clone)]
pub enum Op {
    // Config (L8).
    /// `OperatorStore::load` of the world's access config.
    LoadAccess {
        hash: ConfigHash,
    },
    /// A config change other than the directory's, appended to the audit
    /// log as applied.
    ConfigEntry {
        hash: ConfigHash,
        change: ConfigChange,
    },
    /// `SinkRegistry::record_delivery`.
    Delivery {
        sink: SinkId,
        outcome: Result<Timestamp, SinkError>,
    },
    /// `SearchCorpus::set_model` with the config's embedding model.
    SetModel,

    // Agents (L3).
    CreateAgent(NewAgent),
    Advance {
        agent: AgentId,
        advance: Advance,
    },
    Claim {
        agent: AgentId,
        claim: HarnessClaim,
    },
    Activity {
        agent: AgentId,
    },
    /// `IdentityResolver::merge`; audited when an operator asked.
    Merge {
        key: MergeKey,
        request: MergeRequest,
    },
    /// `IdentityResolver::unmerge` of the merge `key` made; audited.
    Unmerge {
        key: MergeKey,
        by: OperatorId,
    },
    /// `IdentityResolver::rename`; audited.
    Rename {
        agent: AgentId,
        label: AgentLabel,
        by: OperatorId,
    },

    // Channels and transmissions (L5).
    /// `ChannelTraffic::add_resource` at the resource's first sighting,
    /// which must place it on `on` (a declared channel's pattern) or on no
    /// channel.
    AddResource {
        resource: Resource,
        on: Option<ChannelId>,
    },
    /// `ChannelTraffic::record_access`, then `EdgeStore::apply_access`
    /// (L7 buckets accesses by resource).
    Access(Access),
    /// `ChannelTraffic::discover` of `channel` from the stored `resource`
    /// for `transmission`, the first cross-agent transmission through it,
    /// at the time it opened; the registry must create it.
    Discover {
        channel: ChannelId,
        resource: ResourceId,
        transmission: TransmissionId,
    },
    Detection {
        channel: ChannelId,
        update: DetectionUpdate,
    },
    /// `TransmissionStore::save` of the state the correlator reached and,
    /// for a channel transmission past `Detected`,
    /// `ChannelTraffic::record_transmission` of it.
    Save(Box<Transmission>),
    /// `ChannelRegistry::set_policy` of an operator's decision (audited)
    /// and, for a sanction, `AlertTriage::channel_sanctioned`.
    Policy {
        channel: ChannelId,
        decision: PolicyDecision,
    },
    /// `ChannelRegistry::promote` (audited) and, for a sanction,
    /// `AlertTriage::channel_sanctioned`.
    Promote {
        channel: ChannelId,
        promotion: Box<Promotion>,
    },
    /// A policy change the operator lacked the permission for: audited as
    /// `Forbidden`, nothing written.
    ForbiddenPolicy {
        channel: ChannelId,
        policy: PolicyKind,
        note: Option<String>,
        by: OperatorId,
    },
    /// `TransmissionVerdicts::set` (audited); on a new revision,
    /// `EdgeStore::judge`, `SearchCorpus::judge` and
    /// `AlertTriage::transmission_judged`.
    Verdict {
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        by: OperatorId,
        note: Option<String>,
    },

    // Topics, search, edges (L6, L7).
    BeginFit {
        version: TopicModelVersion,
    },
    CompleteFit {
        version: TopicModelVersion,
        topics: Vec<Topic>,
    },
    Assign {
        transmission: TransmissionId,
        version: TopicModelVersion,
        assignment: StoredAssignment,
    },
    /// `TopicVersionReady` for `version` counting `count` transmissions:
    /// `TopicLifecycle::mark_ready`, `EdgeStore::version_ready` and
    /// `AlertRuleMaintenance::topic_version_ready`.
    Ready {
        version: TopicModelVersion,
        count: u64,
    },
    /// `EdgeStore::activate`, then `TopicLifecycle::mark_active`, then
    /// `EdgeStore::drop_version` for each version retention dropped.
    Activate {
        version: TopicModelVersion,
    },
    /// `TopicCatalog::pin` (audited).
    Pin {
        version: TopicModelVersion,
        by: OperatorId,
    },
    Index(Box<IndexedTransmission>),
    Edge(Box<EdgeContribution>),
    /// `EdgeStore::advance_watermark`.
    Watermark(PipelineFrontier),

    // Rules and alerts (L6).
    CreateRule {
        key: RuleKey,
        name: RuleName,
        rule: UserRule,
        sinks: Vec<SinkId>,
        by: OperatorId,
    },
    /// `AlertRuleStore::set_enabled` (audited).
    SetRuleEnabled {
        rule: RuleKey,
        enabled: bool,
        by: OperatorId,
    },
    /// `AlertTriage::triage` of one draft: opens `alert`, or deduplicates
    /// into it while it is active.
    Triage {
        alert: AlertKey,
        rule: RuleRef,
        subject: AlertSubject,
    },
    /// `AlertActions::acknowledge` (audited), skipped once the alert is no
    /// longer active.
    Acknowledge {
        alert: AlertKey,
        by: OperatorId,
    },
    /// `AlertActions::resolve` (audited), skipped once the alert is no
    /// longer active.
    Resolve {
        alert: AlertKey,
        by: OperatorId,
        note: Option<String>,
    },
    /// An acknowledgement of an alert that is no longer active: refused by
    /// the store and audited as rejected.
    RefusedAcknowledge {
        alert: AlertKey,
        by: OperatorId,
    },

    // Projections (L6).
    Enqueue(Box<ProjectionInfo>),
    /// `ProjectionStore::claim`, which must hand out `job`.
    StartFit {
        job: ProjectionId,
    },
    CompleteJob {
        job: ProjectionId,
        frame: Box<ProjectionFrame>,
    },
    FailFit {
        job: ProjectionId,
        failure: FitFailure,
    },
    /// `ProjectionStore::expire` as of the step's time.
    ExpireFrames,

    // Transport (L2).
    /// `BlobStore::put` of one encoded message body.
    Body {
        hash: MessageHash,
        bytes: Vec<u8>,
    },
    DeadLetter(Box<DeadLetter>),
}

/// An op and the time it happens at.
#[derive(Debug, Clone)]
pub struct Step {
    pub at: Timestamp,
    pub op: Op,
}
