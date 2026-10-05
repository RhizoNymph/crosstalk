//! The node facts cache over the memory stores: rebuilt from the stores,
//! kept current by events, and read by the edge store's graphs.

use std::time::Duration;

use crosstalk_memory::reconstruct::model::claim;
use crosstalk_spec::aggregates::edge::{TopologyFilter, Weighting};
use crosstalk_spec::aggregates::node::{CanonicalOriginKind, CanonicalStateKind, GraphNode};
use crosstalk_spec::derived::flow::channel::detection::DetectionKind;
use crosstalk_spec::derived::flow::channel::policy::{Policy, PolicyAuthor, PolicyKind};
use crosstalk_spec::derived::flow::resource::{Host, ResourcePattern};
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, EventBus, RetryPolicy};
use crosstalk_spec::interfaces::l3_reconstruction::ClaimStore;
use crosstalk_spec::interfaces::l5_flow::ChannelRegistry;
use crosstalk_spec::interfaces::l7_topology::NodeFacts;
use crosstalk_spec::interfaces::l8_surface::{
    ActionRequest, OperatorAction, OperatorActions, QueryApi,
};
use crosstalk_spec::observed::agent::AgentLabel;
use crosstalk_spec::support::Timestamp;
use crosstalk_testkit::build::ResourceBuilder;
use crosstalk_transport::{BusConfig, MpscBus};

use super::world::{Fixture, Scene, Who, minute, minutes};

fn wiki() -> ResourcePattern {
    ResourcePattern::Host(Host("wiki.example".to_owned()))
}

fn label(text: &str) -> AgentLabel {
    match AgentLabel::new(text) {
        Ok(label) => label,
        Err(error) => panic!("{error:?}"),
    }
}

/// A second channel on the wiki, and a declared one elsewhere.
async fn more_channels(
    fixture: &Fixture,
    scene: &mut Scene,
) -> (
    crosstalk_spec::ids::ChannelId,
    crosstalk_spec::ids::ChannelId,
) {
    let resource = ResourceBuilder::new(&mut scene.ids)
        .url("https", "wiki.example", "/b", None)
        .first_seen(minute(1))
        .build();
    let c2 = scene.ids.channel();
    fixture
        .channel(&mut scene.ids, c2, &resource, scene.a2, scene.a3, minute(1))
        .await;
    let mut registry = fixture.world.channels.clone();
    let declared = match registry
        .declare(
            ResourcePattern::Host(Host("docs.example".to_owned())),
            Policy::Unreviewed(None),
            PolicyAuthor::Config,
            minute(1),
        )
        .await
    {
        Ok(channel) => channel,
        Err(error) => panic!("declare: {error:?}"),
    };
    (c2, declared)
}

/// Rebuilt from the stores, the cache describes every canonical agent with
/// its label, state and cluster claims, and every channel in force with its
/// kinds and summary; a superseded channel is no node.
#[tokio::test]
async fn node_facts_rebuild_from_the_stores() {
    let fixture = Fixture::new().await;
    let mut scene = fixture.scene().await;
    let (c2, declared) = more_channels(&fixture, &mut scene).await;
    let admin = fixture.caller(Who::Admin).await;
    let mut claims = fixture.world.agents.clone();
    for (agent, n) in [(scene.a1, 0), (scene.a3, 1)] {
        if let Err(error) = claims.record(agent, &claim(n), minute(2)).await {
            panic!("claim: {error:?}");
        }
    }
    for action in [
        OperatorAction::RenameAgent {
            agent: scene.a2,
            label: Some(label("reader")),
        },
        OperatorAction::PromoteChannel {
            channel: scene.c1,
            pattern: wiki(),
            policy: PolicyKind::Sanctioned,
            note: None,
        },
    ] {
        assert!(fixture.surface.act(&admin, action).await.is_ok());
    }
    let merged = fixture
        .surface
        .request(
            &admin,
            ActionRequest::MergeAgents {
                from: scene.a3,
                into: scene.a1,
            },
        )
        .await;
    assert!(merged.is_ok());
    let cache = fixture.world.nodes.clone();
    assert!(cache.is_empty());
    if let Err(error) = fixture.node_feeder.rebuild().await {
        panic!("rebuild: {error}");
    }
    let Some(a1) = cache.agent(scene.a1) else {
        panic!("a1 not described");
    };
    assert_eq!(a1.state, CanonicalStateKind::Provisional);
    assert_eq!(a1.claims.entries().len(), 2, "{:?}", a1.claims);
    assert_eq!(
        cache.agent(scene.a2).and_then(|facts| facts.label),
        Some(label("reader"))
    );
    assert_eq!(cache.agent(scene.a3), None, "a merged agent is no node");

    let Some(c1) = cache.channel(scene.c1) else {
        panic!("c1 not described");
    };
    assert_eq!(c1.origin, CanonicalOriginKind::Promoted);
    assert_eq!(c1.policy, PolicyKind::Sanctioned);
    assert_eq!(c1.locator_summary.as_str(), "https://wiki.example/a (+1)");
    assert_eq!(cache.channel(c2), None, "a superseded channel is no node");
    let Some(docs) = cache.channel(declared) else {
        panic!("declared channel not described");
    };
    assert_eq!(docs.origin, CanonicalOriginKind::DeclaredBeforeTraffic);
    assert_eq!(docs.detection, DetectionKind::AwaitingTraffic);
    assert_eq!(docs.locator_summary.as_str(), "docs.example/*");
    assert_eq!(cache.len(), (2, 2));
}

/// Events keep the cache current: renames, merges, unmerges, policy and
/// promotion each re-read what they name.
#[tokio::test]
async fn node_facts_follow_events() {
    let mut fixture = Fixture::new().await;
    let mut scene = fixture.scene().await;
    let (c2, _) = more_channels(&fixture, &mut scene).await;
    fixture.relay().await;
    let cache = fixture.world.nodes.clone();
    assert!(cache.agent(scene.a3).is_some());
    assert_eq!(
        cache.channel(scene.c1).map(|facts| facts.origin),
        Some(CanonicalOriginKind::Discovered)
    );
    let admin = fixture.caller(Who::Admin).await;

    assert!(
        fixture
            .surface
            .act(
                &admin,
                OperatorAction::RenameAgent {
                    agent: scene.a1,
                    label: Some(label("writer")),
                },
            )
            .await
            .is_ok()
    );
    fixture.relay().await;
    assert_eq!(
        cache.agent(scene.a1).and_then(|facts| facts.label),
        Some(label("writer"))
    );

    let merge = match fixture
        .surface
        .request(
            &admin,
            ActionRequest::MergeAgents {
                from: scene.a3,
                into: scene.a1,
            },
        )
        .await
    {
        Ok(crosstalk_spec::interfaces::l8_surface::ActionOutcome::Merged(merge)) => merge,
        other => panic!("merge: {other:?}"),
    };
    fixture.relay().await;
    assert_eq!(cache.agent(scene.a3), None);
    assert!(
        fixture
            .surface
            .act(&admin, OperatorAction::Unmerge { merge })
            .await
            .is_ok()
    );
    fixture.relay().await;
    assert!(cache.agent(scene.a3).is_some());

    assert!(
        fixture
            .surface
            .act(
                &admin,
                OperatorAction::SetPolicy {
                    channel: scene.c1,
                    policy: PolicyKind::Unsanctioned,
                    note: None,
                },
            )
            .await
            .is_ok()
    );
    fixture.relay().await;
    assert_eq!(
        cache.channel(scene.c1).map(|facts| facts.policy),
        Some(PolicyKind::Unsanctioned)
    );

    assert!(
        fixture
            .surface
            .act(
                &admin,
                OperatorAction::PromoteChannel {
                    channel: scene.c1,
                    pattern: wiki(),
                    policy: PolicyKind::Sanctioned,
                    note: None,
                },
            )
            .await
            .is_ok()
    );
    fixture.relay().await;
    assert_eq!(cache.channel(c2), None);
    let Some(c1) = cache.channel(scene.c1) else {
        panic!("c1");
    };
    assert_eq!(c1.origin, CanonicalOriginKind::Promoted);
    assert_eq!(c1.locator_summary.as_str(), "https://wiki.example/a (+1)");
}

/// The surface's graphs describe their nodes from the cache.
#[tokio::test]
async fn graphs_describe_nodes_from_the_cache() {
    let mut fixture = Fixture::new().await;
    let scene = fixture.scene().await;
    let admin = fixture.caller(Who::Admin).await;
    assert!(
        fixture
            .surface
            .act(
                &admin,
                OperatorAction::RenameAgent {
                    agent: scene.a2,
                    label: Some(label("reader")),
                },
            )
            .await
            .is_ok()
    );
    fixture.relay().await;
    let window = minutes(0, 10);
    let Ok(graph) = fixture
        .surface
        .topology(
            &admin,
            window,
            Weighting::Transmissions,
            &TopologyFilter::default(),
        )
        .await
    else {
        panic!("topology");
    };
    let reader = graph.value.nodes().iter().find_map(|node| match node {
        GraphNode::Agent(agent) if agent.id == scene.a2 => Some(agent.label.clone()),
        _ => None,
    });
    assert_eq!(reader, Some(Some(label("reader"))));
    let Ok(bipartite) = fixture
        .surface
        .channel_topology(
            &admin,
            window,
            Weighting::Transmissions,
            &TopologyFilter::default(),
        )
        .await
    else {
        panic!("channel topology");
    };
    let summary = bipartite.value.nodes().iter().find_map(|node| match node {
        GraphNode::Channel(channel) if channel.id == scene.c1 => {
            Some(channel.locator_summary.as_str().to_owned())
        }
        _ => None,
    });
    assert_eq!(summary.as_deref(), Some("https://wiki.example/a"));
}

/// Fed from the bus, the cache applies each event before it is acked.
#[tokio::test]
async fn node_facts_consume_the_bus() {
    let fixture = Fixture::new().await;
    let scene = fixture.scene().await;
    let Ok(bus) = MpscBus::start(BusConfig::default()) else {
        panic!("bus");
    };
    let Ok(retry) = RetryPolicy::new(
        std::num::NonZeroU32::MIN.saturating_add(2),
        Duration::from_millis(10),
        Duration::from_millis(100),
    ) else {
        panic!("retry");
    };
    let subscription = match bus
        .subscribe(
            &[Subject::Changed],
            ConsumerGroup("nodes".to_owned()),
            retry,
        )
        .await
    {
        Ok(subscription) => subscription,
        Err(error) => panic!("subscribe: {error:?}"),
    };
    let consumer = fixture.node_feeder.clone().consume(subscription);
    for (n, changed) in [Changed::Agent(scene.a1), Changed::Channel(scene.c1)]
        .into_iter()
        .enumerate()
    {
        let envelope = Envelope {
            id: EventId::from_ulid(0xE0 + n as u128),
            at: Timestamp::from_micros(1),
            event: BusEvent::Changed(changed),
        };
        if let Err(error) = bus.publish(envelope).await {
            panic!("publish: {error:?}");
        }
    }
    let cache = fixture.world.nodes.clone();
    for _ in 0..100 {
        if cache.len() == (1, 1) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(cache.agent(scene.a1).is_some());
    assert!(cache.channel(scene.c1).is_some());
    consumer.abort();
}
