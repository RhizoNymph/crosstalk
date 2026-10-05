//! What every read refuses, the same way everywhere: unaligned windows,
//! topic versions it cannot read under, cursors issued for another
//! request, and ids it does not know.

use crosstalk_spec::aggregates::edge::{EdgeSelector, TopologyFilter, Weighting};
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::series::SeriesGrouping;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::resource::ResourcePattern;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, AlertId, ChannelId, ProjectionId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::ConsumerGroup;
use crosstalk_spec::interfaces::l8_surface::audit::{AuditAuthor, AuditFilter};
use crosstalk_spec::interfaces::l8_surface::channel_traffic::ChannelTransmissionFilter;
use crosstalk_spec::interfaces::l8_surface::excerpt::ExcerptWindow;
use crosstalk_spec::interfaces::l8_surface::lists::{
    AlertRuleFilter, ChannelFilter, SearchMode, SearchRequest,
};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionSelection;
use crosstalk_spec::interfaces::l8_surface::{InputError, QueryApi, QueryError};
use crosstalk_spec::paging::PageRequest;
use crosstalk_spec::support::NonBlank;

use crate::harness::Harness;
use crate::scenario::named::{hijacked_wiki, merges, promotion};
use crate::support::World;
use crate::support::first;
use crate::support::windows::{grid, points, quiet, unaligned};

fn search(text: &str) -> SearchRequest {
    SearchRequest {
        mode: SearchMode::Text,
        text: NonBlank::new(text).unwrap_or_else(|e| panic!("{e:?}")),
    }
}

/// Graphs, the overview and agent and channel counts refuse a window off
/// bucket boundaries (INV-351).
pub async fn unaligned_windows_are_refused<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let (b, c) = (&w.backend, &w.lead);
    let off = unaligned(w.extent);
    let f = TopologyFilter::default();
    let refused = Some(QueryError::InvalidInput(InputError::UnalignedWindow));
    let wiki = w.id(hijacked_wiki::WIKI);
    let agent = w.id(merges::CANONICAL);
    assert_eq!(
        b.topology(c, off, Weighting::Transmissions, &f).await.err(),
        refused
    );
    assert_eq!(
        b.channel_topology(c, off, Weighting::Transmissions, &f)
            .await
            .err(),
        refused
    );
    assert_eq!(b.overview(c, off, &f).await.err(), refused);
    assert_eq!(
        b.agents(c, &Default::default(), off, &first(5)).await.err(),
        refused
    );
    assert_eq!(b.agent(c, agent, off).await.err(), refused);
    let windowed = ChannelFilter {
        window: Some(off),
        ..ChannelFilter::default()
    };
    assert_eq!(b.channels(c, &windowed, &first(5)).await.err(), refused);
    assert_eq!(b.channel(c, wiki, Some(off)).await.err(), refused);
}

/// A linked view pinned to an unknown version is `NotFound`, and so are
/// the catalog's reads of it (INV-645).
pub async fn unknown_topic_versions_are_not_found<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let (b, c) = (&w.backend, &w.lead);
    let history = b.topic_versions(c).await.expect("history");
    let newest = history
        .versions()
        .iter()
        .map(|v| v.version().0)
        .max()
        .unwrap_or(0);
    let version = TopicModelVersion(newest + 1);
    let f = TopologyFilter {
        topic_version: TopicVersionSelector::Pinned(version),
        ..TopologyFilter::default()
    };
    let unknown = Some(QueryError::NotFound);
    assert_eq!(
        b.topology(c, w.extent, Weighting::Transmissions, &f)
            .await
            .err(),
        unknown
    );
    assert_eq!(
        b.channel_topology(c, w.extent, Weighting::Transmissions, &f)
            .await
            .err(),
        unknown
    );
    assert_eq!(
        b.series(
            c,
            grid(w.bucket, w.extent, points(4)),
            Weighting::Transmissions,
            SeriesGrouping::Total,
            &f
        )
        .await
        .err(),
        unknown
    );
    assert_eq!(b.overview(c, w.extent, &f).await.err(), unknown);
    let edge = EdgeSelector::new(
        w.id(merges::CANONICAL),
        w.id(merges::HOLDER),
        Route::Unobserved,
    )
    .expect("edge");
    assert_eq!(
        b.edge_transmissions(c, &edge, w.extent, &f, &first(5))
            .await
            .err(),
        unknown
    );
    let one = TransmissionSelection::new(vec![w.id(hijacked_wiki::CONFIRMED)]).expect("selection");
    assert_eq!(
        b.transmissions_by_id(c, &one, TopicVersionSelector::Pinned(version), &first(5))
            .await
            .err(),
        unknown
    );
    assert_eq!(
        b.search(c, &search("the"), Some(w.extent), &f, &first(5))
            .await
            .err(),
        unknown
    );
    assert_eq!(
        b.topic_sizes(c, Some(version), Some(w.extent)).await.err(),
        unknown
    );
    assert_eq!(b.topic_sizes(c, Some(version), None).await.err(), unknown);
    assert_eq!(
        b.topics(c, TopicVersionSelector::Pinned(version), &first(5))
            .await
            .err(),
        unknown
    );
    assert_eq!(b.topic_lineage(c, version).await.err(), unknown);
}

/// A version retention dropped is `VersionNotRetained` wherever its
/// buckets or assignments would be read, while its history entry, topics,
/// lineage and all-time sizes stay readable (INV-572, INV-563).
pub async fn dropped_versions_are_not_retained<H: Harness>(h: &H) {
    let w = World::of(h, crate::scenario::named::topics::scenario()).await;
    let (b, c) = (&w.backend, &w.lead);
    let history = b.topic_versions(c).await.expect("history");
    let version = history
        .versions()
        .iter()
        .find(|v| !v.retention().is_retained())
        .expect("a dropped version")
        .version();
    let f = TopologyFilter {
        topic_version: TopicVersionSelector::Pinned(version),
        ..TopologyFilter::default()
    };
    let dropped = Some(QueryError::VersionNotRetained { version });
    assert_eq!(
        b.topology(c, w.extent, Weighting::Transmissions, &f)
            .await
            .err(),
        dropped
    );
    assert_eq!(b.overview(c, w.extent, &f).await.err(), dropped);
    assert_eq!(
        b.series(
            c,
            grid(w.bucket, w.extent, points(4)),
            Weighting::Transmissions,
            SeriesGrouping::Total,
            &f
        )
        .await
        .err(),
        dropped
    );
    assert_eq!(
        b.search(c, &search("the"), Some(w.extent), &f, &first(5))
            .await
            .err(),
        dropped
    );
    assert_eq!(
        b.fit_projection(c, w.extent, &f, crate::tests::projections::params(1, 10))
            .await
            .err(),
        dropped
    );
    assert_eq!(
        b.topic_sizes(c, Some(version), Some(w.extent)).await.err(),
        dropped
    );
    let frozen = b
        .topic_sizes(c, Some(version), None)
        .await
        .expect("all-time sizes");
    assert_eq!(frozen.value.version(), version);
    assert_eq!(frozen.value.window(), None);
    let topics = b
        .topics(c, TopicVersionSelector::Pinned(version), &first(5))
        .await
        .expect("its topics");
    assert_eq!(topics.version, version);
    assert!(
        b.topic_lineage(c, version)
            .await
            .expect("lineage")
            .is_some()
    );
}

/// A cursor is bound to the request it was issued for: another channel,
/// window, group, version or filter refuses it (INV-402).
pub async fn cursors_are_bound_to_their_request<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let (b, c) = (&w.backend, &w.lead);
    let invalid = Some(QueryError::InvalidCursor);

    let notes = w.id(promotion::NOTES);
    let page = b
        .channel_resources(c, notes, w.extent, &first(1))
        .await
        .expect("resources");
    let next = page
        .value
        .page
        .next()
        .cloned()
        .expect("more than one resource");
    let other = PageRequest {
        after: Some(next),
        ..first(1)
    };
    assert_eq!(
        b.channel_resources(c, notes, w.day(), &other).await.err(),
        invalid
    );
    let wiki = w.id(hijacked_wiki::WIKI);
    assert_eq!(
        b.channel_resources(c, wiki, w.extent, &other).await.err(),
        invalid
    );

    let page = b.dead_letters(c, None, &first(1)).await.expect("letters");
    let other = PageRequest {
        after: page.next().cloned(),
        ..first(1)
    };
    let nobody = ConsumerGroup("conformance-nobody".to_owned());
    assert_eq!(
        b.dead_letters(c, Some(&nobody), &other).await.err(),
        invalid
    );

    let history = b.topic_versions(c).await.expect("history");
    let active = history.active().version();
    let older = history
        .versions()
        .iter()
        .map(|v| v.version())
        .find(|v| *v != active)
        .expect("another version");
    let page = b
        .topics(c, TopicVersionSelector::Pinned(active), &first(1))
        .await
        .expect("topics");
    let other = PageRequest {
        after: page.page.next().cloned(),
        ..first(1)
    };
    assert_eq!(
        b.topics(c, TopicVersionSelector::Pinned(older), &other)
            .await
            .err(),
        invalid
    );

    let all = AlertRuleFilter::default();
    let page = b.alert_rules(c, &all, &first(1)).await.expect("rules");
    let other = PageRequest {
        after: page.next().cloned(),
        ..first(1)
    };
    let stale = AlertRuleFilter {
        statuses: Vec::new(),
        stale: Some(true),
    };
    assert_eq!(b.alert_rules(c, &stale, &other).await.err(), invalid);

    let page = b
        .audit(c, &AuditFilter::default(), &first(1))
        .await
        .expect("audit");
    let other = PageRequest {
        after: page.next().cloned(),
        ..first(1)
    };
    let config = AuditFilter {
        by: vec![AuditAuthor::Config],
        ..AuditFilter::default()
    };
    assert_eq!(b.audit(c, &config, &other).await.err(), invalid);

    let page = b
        .channels(c, &ChannelFilter::default(), &first(1))
        .await
        .expect("channels");
    let other = PageRequest {
        after: page.value.next().cloned(),
        ..first(1)
    };
    let windowed = ChannelFilter {
        window: Some(w.day()),
        ..ChannelFilter::default()
    };
    assert_eq!(b.channels(c, &windowed, &other).await.err(), invalid);
}

/// Unknown ids: reads of one thing answer `None`, reads under one thing
/// `NotFound` (INV-702, INV-703).
pub async fn unknown_ids<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let (b, c) = (&w.backend, &w.lead);
    let (agent, channel, alert) = (
        AgentId::from_ulid(1),
        ChannelId::from_ulid(1),
        AlertId::from_ulid(1),
    );
    let (tx, projection) = (TransmissionId::from_ulid(1), ProjectionId::from_ulid(1));
    assert_eq!(b.agent(c, agent, w.extent).await, Ok(None));
    assert_eq!(b.channel(c, channel, None).await, Ok(None));
    assert_eq!(b.policy_history(c, channel).await, Ok(None));
    assert_eq!(b.alert(c, alert).await, Ok(None));
    assert_eq!(b.transmission(c, tx).await, Ok(None));
    assert_eq!(
        b.transmission_evidence(c, tx, ExcerptWindow::DEFAULT).await,
        Ok(None)
    );
    assert_eq!(b.verdicts(c, tx).await, Ok(None));
    let not_found = Some(QueryError::NotFound);
    assert_eq!(
        b.channel_resources(c, channel, w.extent, &first(5))
            .await
            .err(),
        not_found
    );
    assert_eq!(
        b.channel_transmissions(
            c,
            channel,
            &ChannelTransmissionFilter::default(),
            TopicVersionSelector::Current,
            &first(5)
        )
        .await
        .err(),
        not_found
    );
    let pattern = ResourcePattern::Host(crosstalk_spec::derived::flow::resource::Host(
        hijacked_wiki::HOST.to_owned(),
    ));
    assert_eq!(
        b.promotion_preview(c, channel, &pattern).await.err(),
        not_found
    );
    assert_eq!(b.projection_status(c, projection).await.err(), not_found);
    assert_eq!(b.projection(c, projection).await.err(), not_found);
    let one = TransmissionSelection::new(vec![tx]).expect("selection");
    let rows = b
        .transmissions_by_id(c, &one, TopicVersionSelector::Current, &first(5))
        .await
        .expect("rows");
    assert!(rows.page.items().is_empty(), "unknown ids are left out");
    assert!(
        b.topology(
            c,
            quiet(w.bucket),
            Weighting::Transmissions,
            &TopologyFilter::default()
        )
        .await
        .expect("an empty window")
        .value
        .edges()
        .is_empty()
    );
}
