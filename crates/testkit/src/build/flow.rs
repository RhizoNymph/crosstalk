//! Resources, accesses and channels.

use crosstalk_spec::derived::flow::access::{Access, AccessOp, Extraction, WriteOutcome};
use crosstalk_spec::derived::flow::channel::detection::{DeclaredDetection, TrafficDetection};
use crosstalk_spec::derived::flow::channel::policy::{Decision, Policy, PolicyAuthor};
use crosstalk_spec::derived::flow::channel::{
    Channel, ChannelOrigin, Declaration, DeclaredHistory, Seed, Supersession,
};
use crosstalk_spec::derived::flow::resource::{Host, Locator, Resource, ResourcePattern};
use crosstalk_spec::ids::{AccessId, AgentId, ChannelId, ExchangeId, ResourceId, SpanId};
use crosstalk_spec::observed::message::{PartRef, ToolName};
use crosstalk_spec::support::Timestamp;

use crate::ids::Ids;
use crate::time::T0;

/// Builds a [`Resource`]. The default is a file under `/workspace/shared`
/// first seen at [`T0`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceBuilder {
    id: ResourceId,
    locator: Locator,
    first_seen: Timestamp,
}

impl ResourceBuilder {
    pub fn new(ids: &mut Ids) -> Self {
        let id = ids.resource();
        Self {
            id,
            locator: Locator::File {
                host: None,
                path: format!("/workspace/shared/{}.md", id.ulid_text().to_lowercase()),
            },
            first_seen: T0,
        }
    }

    pub fn id(&self) -> ResourceId {
        self.id
    }

    pub fn with_id(mut self, id: ResourceId) -> Self {
        self.id = id;
        self
    }

    /// A local file at `path` (absolute, already normalized).
    pub fn file(mut self, path: &str) -> Self {
        self.locator = Locator::File {
            host: None,
            path: path.to_owned(),
        };
        self
    }

    /// A URL, already normalized (lower-case scheme and host, sorted query).
    pub fn url(mut self, scheme: &str, host: &str, path: &str, query: Option<&str>) -> Self {
        self.locator = Locator::Url {
            scheme: scheme.to_owned(),
            host: Host(host.to_owned()),
            path: path.to_owned(),
            query: query.map(str::to_owned),
        };
        self
    }

    pub fn mcp(mut self, server: &str, tool: &str, target: Option<&str>) -> Self {
        self.locator = Locator::Mcp {
            server: server.to_owned(),
            tool: ToolName(tool.to_owned()),
            target: target.map(str::to_owned),
        };
        self
    }

    pub fn locator(mut self, locator: Locator) -> Self {
        self.locator = locator;
        self
    }

    pub fn first_seen(mut self, at: Timestamp) -> Self {
        self.first_seen = at;
        self
    }

    pub fn build(self) -> Resource {
        Resource {
            id: self.id,
            locator: self.locator,
            first_seen: self.first_seen,
        }
    }
}

/// Which operation an [`AccessBuilder`] builds.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Op {
    Write {
        spans: Vec<SpanId>,
        outcome: WriteOutcome,
    },
    Read,
}

/// Builds an [`Access`]. The default is a structured, delivered write at
/// [`T0`] by a fresh agent in a fresh exchange, on a fresh resource; its part is the
/// first part of a fresh message (the tool call for a write, the tool
/// result for a read).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessBuilder {
    id: AccessId,
    agent: AgentId,
    exchange: ExchangeId,
    resource: ResourceId,
    at: Timestamp,
    via: Extraction,
    part: PartRef,
    op: Op,
}

impl AccessBuilder {
    pub fn new(ids: &mut Ids) -> Self {
        Self {
            id: ids.access(),
            agent: ids.agent(),
            exchange: ids.exchange(),
            resource: ids.resource(),
            at: T0,
            via: Extraction::Structured,
            part: PartRef {
                message: ids.message(),
                index: 0,
            },
            op: Op::Write {
                spans: Vec::new(),
                outcome: WriteOutcome::Delivered,
            },
        }
    }

    pub fn id(&self) -> AccessId {
        self.id
    }

    pub fn with_id(mut self, id: AccessId) -> Self {
        self.id = id;
        self
    }

    pub fn by(mut self, agent: AgentId) -> Self {
        self.agent = agent;
        self
    }

    pub fn on(mut self, resource: ResourceId) -> Self {
        self.resource = resource;
        self
    }

    pub fn in_exchange(mut self, exchange: ExchangeId) -> Self {
        self.exchange = exchange;
        self
    }

    pub fn at(mut self, at: Timestamp) -> Self {
        self.at = at;
        self
    }

    pub fn via(mut self, via: Extraction) -> Self {
        self.via = via;
        self
    }

    /// The tool call (write) or tool result (read) part.
    pub fn part(mut self, part: PartRef) -> Self {
        self.part = part;
        self
    }

    /// A delivered write.
    pub fn write(mut self) -> Self {
        self.op = Op::Write {
            spans: Vec::new(),
            outcome: WriteOutcome::Delivered,
        };
        self
    }

    /// A delivered write whose arguments hold `spans`.
    pub fn write_spans(mut self, spans: Vec<SpanId>) -> Self {
        self.op = Op::Write {
            spans,
            outcome: WriteOutcome::Delivered,
        };
        self
    }

    /// A write with `outcome`, keeping the spans of an earlier write call.
    pub fn write_outcome(mut self, outcome: WriteOutcome) -> Self {
        let spans = match self.op {
            Op::Write { spans, .. } => spans,
            Op::Read => Vec::new(),
        };
        self.op = Op::Write { spans, outcome };
        self
    }

    pub fn read(mut self) -> Self {
        self.op = Op::Read;
        self
    }

    pub fn build(self) -> Access {
        let op = match self.op {
            Op::Write { spans, outcome } => AccessOp::Write {
                call: self.part,
                spans,
                outcome,
            },
            Op::Read => AccessOp::Read { result: self.part },
        };
        Access {
            id: self.id,
            agent: self.agent,
            exchange: self.exchange,
            resource: self.resource,
            at: self.at,
            via: self.via,
            op,
        }
    }
}

/// Where a built channel came from.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Origin {
    Discovered,
    Promoted {
        pattern: ResourcePattern,
    },
    Superseded {
        by: ChannelId,
        at: Timestamp,
    },
    Declared {
        pattern: ResourcePattern,
        detection: Declared,
    },
}

/// A channel declared before traffic: its detection.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Declared {
    Awaiting,
    Unused { since: Timestamp },
    InUse,
}

/// Builds a [`Channel`].
///
/// The default is a discovered channel seeded by a fresh resource and
/// access, observed, unreviewed, with no resources beyond its seed. The
/// traffic detection set with [`ChannelBuilder::detection`] applies to every
/// origin that has traffic (discovered, promoted, superseded, declared and
/// in use).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelBuilder {
    id: ChannelId,
    seed: Seed,
    origin: Origin,
    detection: TrafficDetection,
    declared_by: PolicyAuthor,
    declared_at: Timestamp,
    resources: Vec<ResourceId>,
    policy: Policy,
}

impl ChannelBuilder {
    pub fn new(ids: &mut Ids) -> Self {
        let seed = Seed {
            resource: ids.resource(),
            first_access: ids.access(),
        };
        Self {
            id: ids.channel(),
            seed,
            origin: Origin::Discovered,
            detection: TrafficDetection::Observed {
                first_access: seed.first_access,
            },
            declared_by: PolicyAuthor::Config,
            declared_at: T0,
            resources: Vec::new(),
            policy: Policy::Unreviewed(None),
        }
    }

    pub fn id(&self) -> ChannelId {
        self.id
    }

    /// The seed of a discovered, promoted or superseded channel.
    pub fn seed(&self) -> Seed {
        self.seed
    }

    pub fn with_id(mut self, id: ChannelId) -> Self {
        self.id = id;
        self
    }

    pub fn seeded_by(mut self, seed: Seed) -> Self {
        self.seed = seed;
        self
    }

    /// Discovered from traffic (the default).
    pub fn discovered(mut self) -> Self {
        self.origin = Origin::Discovered;
        self
    }

    /// Discovered, then promoted with `pattern`.
    pub fn promoted(mut self, pattern: ResourcePattern) -> Self {
        self.origin = Origin::Promoted { pattern };
        self
    }

    /// Discovered, then superseded by the promoted channel `by` at `at`.
    pub fn superseded_by(mut self, by: ChannelId, at: Timestamp) -> Self {
        self.origin = Origin::Superseded { by, at };
        self
    }

    /// Declared with `pattern` before any traffic, awaiting traffic.
    pub fn declared(mut self, pattern: ResourcePattern) -> Self {
        self.origin = Origin::Declared {
            pattern,
            detection: Declared::Awaiting,
        };
        self
    }

    /// Declared with `pattern` and unused since `since`.
    pub fn declared_unused(mut self, pattern: ResourcePattern, since: Timestamp) -> Self {
        self.origin = Origin::Declared {
            pattern,
            detection: Declared::Unused { since },
        };
        self
    }

    /// Declared with `pattern` and in use, with the traffic detection.
    pub fn declared_in_use(mut self, pattern: ResourcePattern) -> Self {
        self.origin = Origin::Declared {
            pattern,
            detection: Declared::InUse,
        };
        self
    }

    /// Who declared or promoted it, and when (config at [`T0`] by default).
    pub fn declared_by(mut self, by: PolicyAuthor, at: Timestamp) -> Self {
        self.declared_by = by;
        self.declared_at = at;
        self
    }

    /// The traffic detection.
    pub fn detection(mut self, detection: TrafficDetection) -> Self {
        self.detection = detection;
        self
    }

    pub fn resources(mut self, resources: Vec<ResourceId>) -> Self {
        self.resources = resources;
        self
    }

    pub fn with_resource(mut self, resource: ResourceId) -> Self {
        self.resources.push(resource);
        self
    }

    pub fn policy(mut self, policy: Policy) -> Self {
        self.policy = policy;
        self
    }

    /// Sanctioned by config at the declaration time.
    pub fn sanctioned(self) -> Self {
        let decision = self.config_decision();
        self.policy(Policy::Sanctioned(decision))
    }

    /// Unsanctioned by config at the declaration time.
    pub fn unsanctioned(self) -> Self {
        let decision = self.config_decision();
        self.policy(Policy::Unsanctioned(decision))
    }

    fn config_decision(&self) -> Decision {
        Decision {
            by: PolicyAuthor::Config,
            at: self.declared_at,
            note: None,
        }
    }

    pub fn build(self) -> Channel {
        let declaration = |pattern| Declaration {
            pattern,
            by: self.declared_by,
            at: self.declared_at,
        };
        let origin = match self.origin {
            Origin::Discovered => ChannelOrigin::Discovered {
                seed: self.seed,
                detection: self.detection,
            },
            Origin::Promoted { pattern } => ChannelOrigin::Declared {
                declaration: declaration(pattern),
                history: DeclaredHistory::Promoted {
                    from: self.seed,
                    detection: self.detection,
                },
            },
            Origin::Superseded { by, at } => ChannelOrigin::Superseded {
                seed: self.seed,
                detection: self.detection,
                supersession: Supersession { by, at },
            },
            Origin::Declared { pattern, detection } => ChannelOrigin::Declared {
                declaration: declaration(pattern),
                history: DeclaredHistory::BeforeTraffic(match detection {
                    Declared::Awaiting => DeclaredDetection::AwaitingTraffic,
                    Declared::Unused { since } => DeclaredDetection::Unused { since },
                    Declared::InUse => DeclaredDetection::InUse(self.detection),
                }),
            },
        };
        Channel {
            id: self.id,
            origin,
            resources: self.resources,
            policy: self.policy,
        }
    }
}
