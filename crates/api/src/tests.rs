//! The in-process surface end to end: seeded through the write traits,
//! read and acted on through the surface, announced on the live feed.

use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::Duration;

use crosstalk_memory::analysis::catalog::RetentionPolicy;
use crosstalk_memory::model::build::test_model;
use crosstalk_memory::support::ManualClock;
use crosstalk_spec::aggregates::edge::{TopologyFilter, Weighting};
use crosstalk_spec::aggregates::node::GraphNode;
use crosstalk_spec::aggregates::projection::FrameRetention;
use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::derived::flow::channel::confirmation::Confirmation;
use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::derived::flow::timing::CorrelationTiming;
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::{
    AgentLifecycle, AgentOrigin, NewAgent,
};
use crosstalk_spec::interfaces::l5_flow::Discovery;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelTraffic;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l7_topology::{AccessContribution, EdgeStore, NodeFacts};
use crosstalk_spec::interfaces::l8_surface::export::{
    ExportFormat, ExportFormats, ExportLimits, GatewayVersion,
};
use crosstalk_spec::interfaces::l8_surface::live::{
    LiveConfig, LiveFeed, LiveItem, LiveStream, Resume, UiEvent,
};
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, OperatorName, RequestIdentity, TrustedOperator,
};
use crosstalk_spec::interfaces::l8_surface::{
    ActionOutcome, OperatorAction, OperatorActions, QueryApi,
};
use crosstalk_spec::observed::agent::AgentLabel;
use crosstalk_spec::support::{NonEmpty, Similarity, TimeWindow, Timestamp};
use crosstalk_surface::SurfaceConfig;
use crosstalk_testkit::build::{ResourceBuilder, TransmissionBuilder};
use crosstalk_testkit::ids::Ids;
use crosstalk_testkit::time::T0;

use crate::{InProcess, InProcessOptions};

const MINUTE: u64 = 60_000_000;

fn options(clock: ManualClock) -> InProcessOptions {
    let (Ok(formats), Ok(live), Ok(gateway), Ok(threshold), Ok(retention), Ok(floor), Ok(name)) = (
        ExportFormats::new(vec![ExportFormat::Jsonl]),
        LiveConfig::new(
            NonZeroU32::MIN.saturating_add(31),
            Duration::from_secs(15),
            Duration::from_secs(600),
        ),
        GatewayVersion::new("0.1.0-dev"),
        Similarity::new(0.8),
        RetentionPolicy::new(3),
        Similarity::new(0.5),
        OperatorName::new("Developer"),
    ) else {
        panic!("options");
    };
    let Ok(timing) = CorrelationTiming::new(
        Duration::from_secs(1),
        Duration::from_secs(30),
        Duration::from_secs(30),
    ) else {
        panic!("timing");
    };
    InProcessOptions {
        clock: Arc::new(clock),
        seed: 1,
        surface: SurfaceConfig {
            export_formats: formats,
            export_limits: ExportLimits::default(),
            gateway,
            default_remap_threshold: threshold,
            frame_retention: FrameRetention::default(),
            live,
        },
        access: AccessConfig::Trusted(TrustedOperator {
            id: OperatorId::from_ulid(0x0B0B),
            name,
        }),
        bucket_width: BucketWidth::from_micros(NonZeroU64::new(MINUTE).unwrap_or(NonZeroU64::MIN)),
        timing,
        retention,
        lineage_floor: floor,
        embedding_model: test_model("dev"),
        sinks: Vec::new(),
        projection_lease: Duration::from_secs(60),
        projection_fitting: crate::ProjectionFitting::External,
    }
}

/// Seeded through the write traits, the world reads back through the
/// surface; an action is applied, announced on the live feed and reflected
/// in the graph's node facts.
#[tokio::test]
async fn in_process_surface_reads_acts_and_announces() {
    let clock = ManualClock::at(T0);
    let backend = match InProcess::start(options(clock.clone())).await {
        Ok(backend) => backend,
        Err(error) => panic!("start: {error}"),
    };
    let caller = match backend.caller(RequestIdentity::Anonymous).await {
        Ok(caller) => caller,
        Err(error) => panic!("caller: {error:?}"),
    };
    let mut stream = match backend.surface.subscribe(&caller, Resume::Fresh).await {
        Ok(stream) => stream,
        Err(error) => panic!("subscribe: {error:?}"),
    };

    let mut ids = Ids::seeded(3);
    let (agent, reader) = (ids.agent(), ids.agent());
    let mut agents = backend.stores.agents.clone();
    for (id, evidence) in [(agent, 2), (reader, 3)] {
        let created = agents
            .create(NewAgent {
                id,
                evidence: NonEmpty::new(crosstalk_memory::reconstruct::model::evidence(evidence)),
                parent: None,
                origin: AgentOrigin::Traffic { first_seen: T0 },
                label: None,
            })
            .await;
        assert!(created.is_ok(), "{created:?}");
    }
    let resource = ResourceBuilder::new(&mut ids)
        .url("https", "wiki.example", "/a", None)
        .first_seen(T0)
        .build();
    let channel = ids.channel();
    // A write by `agent` that `reader` read: the co-access opens a
    // transmission, which discovers the channel (what the flow consumer
    // does).
    let parts = match TransmissionBuilder::new(&mut ids)
        .between(agent, reader)
        .channel(channel)
        .accesses(|cross| cross.resource(resource.id))
        .awaiting_content()
        .build_parts()
    {
        Ok(parts) => parts,
        Err(error) => panic!("transmission: {error:?}"),
    };
    let mut registry = backend.stores.channels.clone();
    assert_eq!(registry.add_resource(resource.clone()).await, Ok(None));
    let mut edges = backend.stores.edges.clone();
    for access in [&parts.write, &parts.read] {
        assert!(registry.record_access(access.clone()).await.is_ok());
        // What the L7 consumer does on `AccessRecorded`.
        let counted = edges
            .apply_access(&AccessContribution {
                access: access.id,
                agent: access.agent,
                resource: access.resource,
                op: access.op.kind(),
                at: access.at,
            })
            .await;
        assert!(counted.is_ok(), "{counted:?}");
    }
    let transmission = &parts.transmission;
    assert_eq!(
        registry
            .discover(
                channel,
                resource.id,
                transmission.id,
                transmission.opened_at
            )
            .await,
        Ok(Discovery::Created(channel))
    );
    let mut transmissions = backend.stores.transmissions.clone();
    assert!(transmissions.save(transmission.clone()).await.is_ok());
    assert!(registry.record_transmission(transmission).await.is_ok());

    clock.set(Timestamp::from_micros(T0.as_micros() + MINUTE));
    let outcome = backend
        .surface
        .act(
            &caller,
            OperatorAction::SetPolicy {
                channel,
                policy: PolicyKind::Sanctioned,
                note: None,
            },
        )
        .await;
    assert_eq!(outcome, Ok(ActionOutcome::Applied));
    let row = match backend.surface.channel(&caller, channel, None).await {
        Ok(Some(row)) => row,
        other => panic!("channel: {other:?}"),
    };
    assert_eq!(row.value.channel().policy.kind(), PolicyKind::Sanctioned);

    let mut announced = false;
    for _ in 0..8 {
        match tokio::time::timeout(Duration::from_secs(1), stream.next()).await {
            Ok(Ok(LiveItem::Event {
                event: UiEvent::ChannelChanged { id },
                ..
            })) if id == channel => {
                announced = true;
                break;
            }
            Ok(Ok(_)) => {}
            other => panic!("stream: {other:?}"),
        }
    }
    assert!(announced, "the policy change was not announced");

    let label = AgentLabel::new("writer").ok();
    let renamed = backend
        .surface
        .act(
            &caller,
            OperatorAction::RenameAgent {
                agent,
                label: label.clone(),
            },
        )
        .await;
    assert_eq!(renamed, Ok(ActionOutcome::Applied));
    // The relay refreshes the node facts the graphs read.
    for _ in 0..100 {
        if backend
            .stores
            .nodes
            .agent(agent)
            .and_then(|facts| facts.label)
            == label
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let Ok(window) = TimeWindow::new(T0, Timestamp::from_micros(T0.as_micros() + 10 * MINUTE))
    else {
        panic!("window");
    };
    let bipartite = backend
        .surface
        .channel_topology(
            &caller,
            window,
            Weighting::Transmissions,
            &TopologyFilter::default(),
        )
        .await;
    let Ok(bipartite) = bipartite else {
        panic!("channel topology: {bipartite:?}");
    };
    let agent_label = bipartite.value.nodes().iter().find_map(|node| match node {
        GraphNode::Agent(node) if node.id == agent => Some(node.label.clone()),
        _ => None,
    });
    assert_eq!(agent_label, Some(label));
    // Its only transmission awaits content: drawn, marked unconfirmed.
    let confirmation = bipartite.value.nodes().iter().find_map(|node| match node {
        GraphNode::Channel(node) if node.id == channel => Some(node.confirmation),
        _ => None,
    });
    assert_eq!(confirmation, Some(Confirmation::Unconfirmed));
    backend.shutdown().await;
}

mod in_process;
