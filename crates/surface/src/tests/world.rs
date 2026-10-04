//! The world the surface tests run in: every in-memory reference store,
//! wired as a gateway would wire them, the surface over them, and helpers
//! that seed state through the spec's write traits.

use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::Duration;

use crosstalk_memory::analysis::alerts::{AlertStoreConfig, InMemoryAlertStore};
use crosstalk_memory::analysis::catalog::{CatalogConfig, InMemoryTopicCatalog, RetentionPolicy};
use crosstalk_memory::analysis::fakes::FakeEmbedder;
use crosstalk_memory::analysis::projection::{InMemoryProjectionStore, ProjectionConfig};
use crosstalk_memory::analysis::search::InMemorySearchIndex;
use crosstalk_memory::flow::{MemoryChannels, MemoryVerdicts};
use crosstalk_memory::model::build::{test_model, topic as build_topic, topic_id, unit};
use crosstalk_spec::aggregates::topic::Topic;
use crosstalk_spec::interfaces::l6_analysis::lifecycle::TopicLifecycle;
use crosstalk_memory::reconstruct::MemoryAgents;
use crosstalk_memory::support::{IdSequence, ManualClock, Outbox, drain};
use crosstalk_memory::surface::audit::InMemoryAuditLog;
use crosstalk_memory::surface::operators::InMemoryOperatorStore;
use crosstalk_memory::surface::sinks::{InMemorySinkRegistry, SinkConfig};
use crosstalk_memory::topology::env::Env;
use crosstalk_memory::topology::store::{EdgeStoreConfig, InMemoryEdgeStore};
use crosstalk_spec::aggregates::alert::{AlertDraft, AlertRuleConfig, AlertSubject, BuiltinRule, TriageOutcome};
use crosstalk_spec::aggregates::projection::FrameRetention;
use crosstalk_spec::aggregates::series::{BucketWidth, SeriesGrid, SeriesStep};
use crosstalk_spec::aggregates::watermark::{PipelineFrontier, Watermark};
use crosstalk_spec::derived::flow::access::{Access, AccessKind, AccessOp, Extraction};
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::derived::flow::timing::CorrelationTiming;
use crosstalk_spec::derived::flow::transmission::{Route, Transmission};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::insight::ClassificationCause;
use crosstalk_spec::ids::{
    AccessId, AgentId, AlertId, ChannelId, ConfigHash, ExchangeId, MessageHash, OperatorId,
    SeededRandom, SinkId,
};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::{AgentLifecycle, AgentOrigin, NewAgent};
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelTraffic;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l6_analysis::AlertTriage;
use crosstalk_spec::interfaces::l7_topology::{AccessContribution, EdgeContribution, EdgeStore};
use crosstalk_spec::interfaces::l8_surface::export::{ExportFormat, ExportFormats, ExportLimits, GatewayVersion};
use crosstalk_spec::interfaces::l8_surface::live::{FeedEpoch, LiveConfig};
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, OperatorConfig, OperatorName, OperatorStore, RequestIdentity,
};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, PermissionSet, SinkKind};
use crosstalk_spec::observed::message::PartRef;
use crosstalk_spec::support::{Blake3, NonEmpty, Similarity, TimeWindow, Timestamp};
use crosstalk_testkit::time::T0;
use crosstalk_transport::blob::MemoryBlobStore;
use crosstalk_transport::{BusConfig, DeadLetters, MpscBus};
use tokio::sync::mpsc::UnboundedReceiver;

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::interfaces::l8_surface::audit::{AuditBody, AuditEntry, AuditFilter, AuditLog, OperatorRecord};
use crosstalk_spec::paging::{AuditList, PageRequest};
use crosstalk_testkit::build::{ResourceBuilder, TransmissionBuilder, TransmissionParts};
use crosstalk_testkit::ids::Ids;

use super::fakes::{CountingEmbedder, RecordingBus, TestEvidence};
use crate::export::SpecExportSource;
use crate::live::{FeedHandle, FeedWriter};
use crate::nodes::{NodeCache, NodeFeeder};
use crate::{Surface, SurfaceConfig, SurfaceStores};

/// Every bucket is one minute.
pub const WIDTH: u64 = 60_000_000;

/// The minute `n` minutes after the testkit epoch.
pub fn minute(n: u64) -> Timestamp {
    Timestamp::from_micros(T0.as_micros() + n * WIDTH)
}

/// `[minute(from), minute(to))`.
pub fn minutes(from: u64, to: u64) -> TimeWindow {
    match TimeWindow::new(minute(from), minute(to)) {
        Ok(window) => window,
        Err(_) => panic!("empty window {from}..{to}"),
    }
}

/// A grid of one-bucket steps over `minutes(from, to)`.
pub fn grid_over(from: u64, to: u64) -> SeriesGrid {
    let width = BucketWidth::from_micros(NonZeroU64::new(WIDTH).unwrap_or(NonZeroU64::MIN));
    let step = SeriesStep::new(width, NonZeroU64::new(WIDTH).unwrap_or(NonZeroU64::MIN));
    match step.map(|step| SeriesGrid::new(minutes(from, to), step)) {
        Ok(Ok(grid)) => grid,
        other => panic!("grid: {other:?}"),
    }
}

/// [`grid_over`] the first ten minutes.
pub fn grid() -> SeriesGrid {
    grid_over(0, 10)
}

/// The agent and channel directories every store resolves through: L3's
/// merges and L5's supersessions.
#[derive(Clone)]
pub struct Directory {
    pub agents: MemoryAgents,
    pub channels: MemoryChannels<MemoryAgents>,
}

impl AgentDirectory for Directory {
    fn canonical(&self, id: AgentId) -> AgentId {
        AgentDirectory::canonical(&self.agents, id)
    }
}

impl ChannelDirectory for Directory {
    fn canonical(&self, id: ChannelId) -> ChannelId {
        ChannelDirectory::canonical(&self.channels, id)
    }
}

pub type Edges = InMemoryEdgeStore<Env<InMemoryTopicCatalog, Directory, NodeCache>>;
pub type Nodes = NodeFeeder<MemoryAgents, MemoryChannels<MemoryAgents>>;
pub type Alerts = InMemoryAlertStore<CountingEmbedder, Directory>;
pub type Search = InMemorySearchIndex<Directory>;
pub type Export =
    SpecExportSource<Edges, InMemoryProjectionStore, InMemoryTopicCatalog, CountingEmbedder>;

/// Every store, as handles that share state with the surface's.
#[derive(Clone)]
pub struct World {
    pub agents: MemoryAgents,
    pub channels: MemoryChannels<MemoryAgents>,
    pub transmissions: MemoryVerdicts,
    pub catalog: InMemoryTopicCatalog,
    pub search: Search,
    pub embedder: CountingEmbedder,
    pub projections: InMemoryProjectionStore,
    pub alerts: Alerts,
    pub edges: Edges,
    pub audit: InMemoryAuditLog,
    pub operators: InMemoryOperatorStore,
    pub sinks: InMemorySinkRegistry,
    pub dead_letters: DeadLetters,
    pub bus: RecordingBus,
    pub blobs: MemoryBlobStore,
    pub evidence: TestEvidence,
    pub export: Export,
    pub nodes: NodeCache,
    /// Keeps the dead letters' bus running.
    pub mpsc: MpscBus,
}

impl SurfaceStores for World {
    type Agents = MemoryAgents;
    type Channels = MemoryChannels<MemoryAgents>;
    type Transmissions = MemoryVerdicts;
    type Topics = InMemoryTopicCatalog;
    type Search = Search;
    type Embedder = CountingEmbedder;
    type Projections = InMemoryProjectionStore;
    type Alerts = Alerts;
    type Edges = Edges;
    type Audit = InMemoryAuditLog;
    type Operators = InMemoryOperatorStore;
    type Sinks = InMemorySinkRegistry;
    type DeadLetters = DeadLetters;
    type Bus = RecordingBus;
    type Blobs = MemoryBlobStore;
    type Evidence = TestEvidence;
    type Export = Export;

    fn agents(&self) -> &MemoryAgents {
        &self.agents
    }
    fn channels(&self) -> &MemoryChannels<MemoryAgents> {
        &self.channels
    }
    fn transmissions(&self) -> &MemoryVerdicts {
        &self.transmissions
    }
    fn topics(&self) -> &InMemoryTopicCatalog {
        &self.catalog
    }
    fn search(&self) -> &Search {
        &self.search
    }
    fn embedder(&self) -> &CountingEmbedder {
        &self.embedder
    }
    fn projections(&self) -> &InMemoryProjectionStore {
        &self.projections
    }
    fn alerts(&self) -> &Alerts {
        &self.alerts
    }
    fn edges(&self) -> &Edges {
        &self.edges
    }
    fn audit(&self) -> &InMemoryAuditLog {
        &self.audit
    }
    fn operators(&self) -> &InMemoryOperatorStore {
        &self.operators
    }
    fn sinks(&self) -> &InMemorySinkRegistry {
        &self.sinks
    }
    fn dead_letters(&self) -> &DeadLetters {
        &self.dead_letters
    }
    fn bus(&self) -> &RecordingBus {
        &self.bus
    }
    fn blobs(&self) -> &MemoryBlobStore {
        &self.blobs
    }
    fn evidence(&self) -> &TestEvidence {
        &self.evidence
    }
    fn export_source(&self) -> &Export {
        &self.export
    }
}

/// The operators config defines, one per permission profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Who {
    Admin,
    Viewer,
    Reader,
    Auditor,
    Triager,
    Governor,
    Operator,
}

impl Who {
    pub const ALL: [Self; 7] = [
        Self::Admin,
        Self::Viewer,
        Self::Reader,
        Self::Auditor,
        Self::Triager,
        Self::Governor,
        Self::Operator,
    ];

    pub fn id(self) -> OperatorId {
        OperatorId::from_ulid(0x0B0B_0000 + self as u128)
    }

    pub fn permissions(self) -> PermissionSet {
        use Permission::{Audit, Content, Govern, Operate, Triage, View};
        match self {
            Self::Admin => PermissionSet::ALL,
            Self::Viewer => PermissionSet::of([View]),
            Self::Reader => PermissionSet::of([View, Content]),
            Self::Auditor => PermissionSet::of([Audit]),
            Self::Triager => PermissionSet::of([View, Triage]),
            Self::Governor => PermissionSet::of([View, Govern]),
            Self::Operator => PermissionSet::of([Operate]),
        }
    }
}

/// The surface, its stores, the outbox every store publishes to, the clock
/// and the feed.
pub struct Fixture {
    pub world: World,
    pub surface: Surface<World>,
    pub events: UnboundedReceiver<BusEvent>,
    pub clock: ManualClock,
    pub feed: FeedHandle,
    /// Keeps `world.nodes` current; [`Fixture::relay`] feeds it.
    pub node_feeder: Nodes,
}

pub fn config() -> SurfaceConfig {
    let formats = ExportFormats::new(vec![ExportFormat::Jsonl]);
    let live = LiveConfig::new(
        NonZeroU32::new(8).unwrap_or(NonZeroU32::MIN),
        Duration::from_secs(15),
        Duration::from_secs(600),
    );
    let (Ok(export_formats), Ok(live), Ok(gateway), Ok(threshold)) = (
        formats,
        live,
        GatewayVersion::new("0.1.0-test"),
        Similarity::new(0.8),
    ) else {
        panic!("test config");
    };
    SurfaceConfig {
        export_formats,
        export_limits: ExportLimits::default(),
        gateway,
        default_remap_threshold: threshold,
        frame_retention: FrameRetention::default(),
        live,
    }
}

pub fn sink(n: u8) -> SinkId {
    SinkId::from_ulid(0x5100_0000 + u128::from(n))
}

impl Fixture {
    /// A fresh world at `minute(0)`, every operator of [`Who`] loaded.
    pub async fn new() -> Self {
        Self::with_config(config()).await
    }

    pub async fn with_config(config: SurfaceConfig) -> Self {
        let (outbox, events) = Outbox::channel();
        let clock = ManualClock::at(minute(0));
        let agents = MemoryAgents::new(IdSequence::new(1 << 90), outbox.clone());
        let channels = MemoryChannels::new(agents.clone(), IdSequence::new(2 << 90), outbox.clone());
        let directory = Directory {
            agents: agents.clone(),
            channels: channels.clone(),
        };
        let transmissions = MemoryVerdicts::new(outbox.clone());
        let catalog_config = match (RetentionPolicy::new(3), Similarity::new(0.5)) {
            (Ok(retention), Ok(lineage_floor)) => CatalogConfig {
                retention,
                lineage_floor,
            },
            _ => panic!("catalog config"),
        };
        let Ok(catalog) = InMemoryTopicCatalog::new(catalog_config, T0, outbox.clone()) else {
            panic!("catalog");
        };
        let model = test_model("test");
        let embedder = CountingEmbedder::new(FakeEmbedder::new(model.clone(), 400));
        let search = InMemorySearchIndex::new(catalog.clone(), directory.clone(), model);
        let projections = InMemoryProjectionStore::new(
            ProjectionConfig {
                lease: Duration::from_secs(60),
                frame_retention: config.frame_retention.as_duration(),
            },
            outbox.clone(),
        );
        let sinks = [sink(1), sink(2)];
        let alerts = InMemoryAlertStore::new(
            AlertStoreConfig {
                rules: AlertRuleConfig {
                    default_remap_threshold: config.default_remap_threshold,
                },
                sinks: sinks.iter().copied().collect::<BTreeSet<_>>(),
                builtins: BTreeMap::new(),
            },
            embedder.clone(),
            directory.clone(),
            transmissions.clone(),
            outbox.clone(),
        );
        let nodes = NodeCache::new();
        let Some(timing) = CorrelationTiming::new(
            Duration::from_secs(1),
            Duration::from_secs(30),
            Duration::from_secs(30),
        )
        .ok() else {
            panic!("timing");
        };
        let edges = InMemoryEdgeStore::new(
            EdgeStoreConfig {
                bucket_width: BucketWidth::from_micros(NonZeroU64::new(WIDTH).unwrap_or(NonZeroU64::MIN)),
                timing,
            },
            Env {
                topics: catalog.clone(),
                directory: directory.clone(),
                nodes: nodes.clone(),
            },
            outbox.clone(),
        );
        let audit = InMemoryAuditLog::new();
        let operators = InMemoryOperatorStore::new(audit.clone(), IdSequence::new(3 << 90));
        let sink_registry = InMemorySinkRegistry::new(sinks.iter().enumerate().map(|(n, id)| {
            SinkConfig {
                id: *id,
                kind: SinkKind::Log,
                name: format!("sink {n}"),
            }
        }));
        let Ok(mpsc) = MpscBus::start(BusConfig::default()) else {
            panic!("bus");
        };
        let export = SpecExportSource::new(
            edges.clone(),
            projections.clone(),
            catalog.clone(),
            embedder.clone(),
        );
        let world = World {
            agents,
            channels,
            transmissions,
            catalog,
            search,
            embedder,
            projections,
            alerts,
            edges,
            audit,
            operators,
            sinks: sink_registry,
            dead_letters: mpsc.dead_letters(),
            bus: RecordingBus::default(),
            blobs: MemoryBlobStore::new(),
            evidence: TestEvidence::default(),
            export,
            nodes,
            mpsc,
        };
        let feed = FeedWriter::spawn(config.live, FeedEpoch(7));
        let surface = Surface::new(
            world.clone(),
            Arc::new(clock.clone()),
            config,
            SeededRandom::new(42),
            feed.clone(),
        );
        let node_feeder = NodeFeeder::new(
            world.nodes.clone(),
            world.agents.clone(),
            world.channels.clone(),
        );
        let mut fixture = Self {
            world,
            surface,
            events,
            clock,
            feed,
            node_feeder,
        };
        fixture.load_operators().await;
        fixture
    }

    async fn load_operators(&mut self) {
        let operators = Who::ALL
            .iter()
            .map(|who| {
                let Ok(name) = OperatorName::new(&format!("{who:?}")) else {
                    panic!("operator name");
                };
                OperatorConfig {
                    id: who.id(),
                    name,
                    permissions: who.permissions(),
                }
            })
            .collect();
        let config = AccessConfig::Authenticated(operators);
        let hash = ConfigHash::from_digest(Blake3::of(b"operators"));
        let mut store = self.world.operators.clone();
        if let Err(error) = store.load(&config, hash, minute(0)).await {
            panic!("operators: {error:?}");
        }
    }

    /// The caller of one of the configured operators.
    pub async fn caller(&self, who: Who) -> Caller {
        match self
            .world
            .operators
            .caller(RequestIdentity::Verified(who.id()))
            .await
        {
            Ok(caller) => caller,
            Err(error) => panic!("caller {who:?}: {error:?}"),
        }
    }

    /// Everything the stores published since the last call.
    pub fn published(&mut self) -> Vec<BusEvent> {
        drain(&mut self.events)
    }

    /// Hand everything the stores published since the last call to the
    /// node facts and the live feed, as the gateway's consumers would, and
    /// return it.
    pub async fn relay(&mut self) -> Vec<BusEvent> {
        let events = self.published();
        for event in &events {
            if let Err(error) = self.node_feeder.apply(event).await {
                panic!("node facts: {error}");
            }
            if let BusEvent::Changed(changed) = event
                && let Err(error) = self.feed.append(*changed).await
            {
                panic!("feed: {error}");
            }
        }
        events
    }

    /// The `Changed` notifications among `published`.
    pub fn changes(&mut self) -> Vec<Changed> {
        self.published()
            .into_iter()
            .filter_map(|event| match event {
                BusEvent::Changed(changed) => Some(changed),
                BusEvent::Ingest(_) | BusEvent::Detect(_) | BusEvent::Insight(_) => None,
            })
            .collect()
    }

    // ---- seeding through the spec's write traits -------------------------

    /// Create agent `id` from traffic first seen at `at`.
    pub async fn agent(&self, id: AgentId, at: Timestamp) {
        let agent = NewAgent {
            id,
            evidence: NonEmpty::new(crosstalk_memory::reconstruct::model::evidence(
                u8::try_from(id.as_ulid() % 251).unwrap_or(0),
            )),
            parent: None,
            origin: AgentOrigin::Traffic { first_seen: at },
            label: None,
        };
        if let Err(error) = self.world.agents.clone().create(agent).await {
            panic!("agent {id:?}: {error:?}");
        }
    }

    /// Discover `channel` from `resource`, first written by `writer` at
    /// `at`. Returns the first access.
    pub async fn channel(
        &self,
        channel: ChannelId,
        resource: &Resource,
        writer: AgentId,
        at: Timestamp,
    ) -> Access {
        let first = access(resource, writer, AccessKind::Write, at);
        let mut registry = self.world.channels.clone();
        if let Err(error) = registry.discover(channel, resource.clone(), first.id).await {
            panic!("discover {channel:?}: {error:?}");
        }
        self.record(&first, channel).await;
        first
    }

    /// Record `access` on its resource, counted into `channel`'s buckets.
    pub async fn record(&self, access: &Access, channel: ChannelId) {
        let mut registry = self.world.channels.clone();
        if let Err(error) = registry.record_access(access.clone()).await {
            panic!("access {:?}: {error:?}", access.id);
        }
        let mut edges = self.world.edges.clone();
        let contribution = AccessContribution {
            access: access.id,
            agent: access.agent,
            channel,
            op: access.op.kind(),
            at: access.at,
        };
        if let Err(error) = edges.apply_access(&contribution).await {
            panic!("access bucket {:?}: {error:?}", access.id);
        }
    }

    /// Store `transmission`, and count it into its edge when it is
    /// classified (under its classification's version).
    pub async fn transmission(&self, transmission: &Transmission) {
        let mut store = self.world.transmissions.clone();
        if let Err(error) = store.save(transmission.clone()).await {
            panic!("save {:?}: {error:?}", transmission.id);
        }
        let Some(confirmed) = transmission.state.confirmed() else {
            return;
        };
        if let Route::Channel(channel) = transmission.route {
            let mut registry = self.world.channels.clone();
            if let Err(error) = registry.confirm(channel, transmission.id, confirmed.at()).await {
                panic!("confirm {:?}: {error:?}", transmission.id);
            }
        }
        let classification = match &transmission.state {
            crosstalk_spec::derived::flow::transmission::TransmissionState::Classified {
                classification,
                ..
            }
            | crosstalk_spec::derived::flow::transmission::TransmissionState::Aggregated {
                classification,
                ..
            } => classification.clone(),
            _ => return,
        };
        let contribution = EdgeContribution {
            transmission: transmission.id,
            from: confirmed.from(),
            to: transmission.to,
            route: transmission.route.clone(),
            at: confirmed.at(),
            matched_bytes: confirmed.matched_bytes(),
            classification,
            cause: ClassificationCause::Confirmation,
        };
        if let Err(error) = self.world.edges.clone().apply(&contribution).await {
            panic!("edge {:?}: {error:?}", transmission.id);
        }
    }

    /// Open an alert of `rule` on `subject` raised at `at`.
    pub async fn alert(&self, rule: BuiltinRule, subject: AlertSubject, at: Timestamp) -> AlertId {
        let draft = AlertDraft {
            rule: rule.id(),
            subject,
            raised_at: at,
        };
        match self.world.alerts.clone().triage(draft).await {
            Ok(TriageOutcome::Opened(alert)) => alert.id,
            Ok(TriageOutcome::Deduplicated { into }) => into,
            other => panic!("triage: {other:?}"),
        }
    }

    /// Advance L7's watermark to `at` (a bucket boundary).
    pub async fn watermark(&self, at: Timestamp) -> Watermark {
        let settle = Duration::from_secs(60);
        let settle = u64::try_from(settle.as_micros()).unwrap_or(0);
        let frontier = PipelineFrontier {
            ticked_through: Timestamp::from_micros(at.as_micros() + settle),
            oldest_pending: None,
        };
        let mut edges = self.world.edges.clone();
        if let Err(error) = edges.advance_watermark(frontier).await {
            panic!("watermark: {error:?}");
        }
        match edges.watermark().await {
            Ok(watermark) => watermark,
            Err(error) => panic!("watermark: {error:?}"),
        }
    }
}

/// A small world: three agents, a discovered channel written by the first
/// and read by the second, the classified transmission between them, and an
/// open alert on the channel.
pub struct Scene {
    pub a1: AgentId,
    pub a2: AgentId,
    pub a3: AgentId,
    pub c1: ChannelId,
    pub r1: Resource,
    pub t1: TransmissionParts,
    pub alert: AlertId,
    pub ids: Ids,
}

impl Fixture {
    /// Seed [`Scene`] through the write traits.
    pub async fn scene(&self) -> Scene {
        let mut ids = Ids::seeded(1);
        let (a1, a2, a3) = (ids.agent(), ids.agent(), ids.agent());
        for agent in [a1, a2, a3] {
            self.agent(agent, minute(0)).await;
        }
        let r1 = ResourceBuilder::new(&mut ids)
            .url("https", "wiki.example", "/a", None)
            .first_seen(minute(0))
            .build();
        let c1 = ids.channel();
        self.channel(c1, &r1, a1, minute(0)).await;
        let t1 = match TransmissionBuilder::new(&mut ids)
            .between(a1, a2)
            .channel(c1)
            .accesses(|cross| cross.resource(r1.id))
            .topic(TopicModelVersion(0), None)
            .classified()
            .build_parts()
        {
            Ok(parts) => parts,
            Err(error) => panic!("transmission: {error:?}"),
        };
        self.record(&t1.write, c1).await;
        self.record(&t1.read, c1).await;
        self.transmission(&t1.transmission).await;
        let alert = self
            .alert(BuiltinRule::NewChannel, AlertSubject::Channel(c1), minute(0))
            .await;
        Scene {
            a1,
            a2,
            a3,
            c1,
            r1,
            t1,
            alert,
            ids,
        }
    }

    /// Fit a version whose topics are `topics` (id numbers), started at
    /// `at`, ready one microsecond later; activate it too when `activate`.
    pub async fn fit(&self, at: Timestamp, topics: &[u64], activate: bool) -> TopicModelVersion {
        let mut catalog = self.world.catalog.clone();
        let Ok(version) = catalog.begin_fit(at).await else {
            panic!("begin fit");
        };
        let model = test_model("test");
        let fitted: Vec<Topic> = topics
            .iter()
            .enumerate()
            .map(|(i, n)| {
                let axis = [1.0, 0.0, 0.0];
                let mut values = axis;
                values[i % 3] = 1.0 + i as f32;
                let Some(centroid) = unit(&model, values[0], values[1], values[2]) else {
                    panic!("centroid");
                };
                build_topic(topic_id(*n), version, centroid, at)
            })
            .collect();
        if let Err(error) = catalog.complete_fit(version, fitted, at).await {
            panic!("complete fit: {error:?}");
        }
        let ready = Timestamp::from_micros(at.as_micros() + 1);
        if let Err(error) = catalog.mark_ready(version, ready).await {
            panic!("ready: {error:?}");
        }
        if activate {
            let active = Timestamp::from_micros(at.as_micros() + 2);
            if let Err(error) = catalog.mark_active(version, active).await {
                panic!("active: {error:?}");
            }
        }
        version
    }

    /// Every audit entry, newest first.
    pub async fn audit_entries(&self) -> Vec<AuditEntry> {
        let mut request: PageRequest<AuditList> = super::page(500);
        let mut entries = Vec::new();
        loop {
            let page = match self.world.audit.query(&AuditFilter::default(), &request).await {
                Ok(page) => page,
                Err(error) => panic!("audit: {error:?}"),
            };
            let (items, next) = page.into_parts();
            entries.extend(items);
            match next {
                Some(next) => request.after = Some(next),
                None => return entries,
            }
        }
    }

    /// The operator audit entries, newest first.
    pub async fn operator_entries(&self) -> Vec<(Timestamp, OperatorRecord)> {
        self.audit_entries()
            .await
            .into_iter()
            .filter_map(|entry| match entry.body {
                AuditBody::Operator(record) => Some((entry.at, record)),
                AuditBody::Config(_) | AuditBody::Export(_) => None,
            })
            .collect()
    }
}

/// An access of `kind` to `resource` by `agent` at `at`, its id and
/// exchange derived from those.
pub fn access(resource: &Resource, agent: AgentId, kind: AccessKind, at: Timestamp) -> Access {
    let id = (u128::from(at.as_micros()) << 32)
        ^ (agent.as_ulid() << 8)
        ^ resource.id.as_ulid()
        ^ match kind {
            AccessKind::Write => 1,
            AccessKind::Read => 2,
        };
    let part = PartRef {
        message: MessageHash::from_digest(Blake3::of(&id.to_le_bytes())),
        index: 0,
    };
    Access {
        id: AccessId::from_ulid(id),
        agent,
        exchange: ExchangeId::from_ulid(id),
        resource: resource.id,
        at,
        via: Extraction::Structured,
        op: match kind {
            AccessKind::Write => AccessOp::Write {
                call: part,
                spans: Vec::new(),
            },
            AccessKind::Read => AccessOp::Read { result: part },
        },
    }
}
