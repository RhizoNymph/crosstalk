//! Channel rows, names, resources and the promotion preview.

use crosstalk_spec::aggregates::edge::{TopologyFilter, Weighting};
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::channel::policy::PolicyKind;
use crosstalk_spec::derived::flow::resource::{Host, ResourcePattern};
use crosstalk_spec::ids::{AgentId, ChannelId};
use crosstalk_spec::interfaces::l8_surface::channels::{
    ChannelActivity, ChannelCounts, ChannelShape, ChannelStanding,
};
use crosstalk_spec::interfaces::l8_surface::lists::{ChannelFilter, OriginFilter};
use crosstalk_spec::interfaces::l8_surface::{
    ConflictKind, InputError, OperatorAction, OperatorActions, QueryApi, QueryError,
};
use crosstalk_spec::paging::{ChannelList, PageRequest, ResourceUseList};
use crosstalk_testkit::build::ResourceBuilder;

use super::page;
use super::world::{Fixture, Scene, Who, access, minute, minutes};

fn wiki() -> ResourcePattern {
    ResourcePattern::Host(Host("wiki.example".to_owned()))
}

/// A second channel, discovered from `/b` on the wiki, written by `a2` at
/// minute 1 and read by `a3` at minute 2.
async fn second_channel(fixture: &Fixture, scene: &mut Scene) -> ChannelId {
    let resource = ResourceBuilder::new(&mut scene.ids)
        .url("https", "wiki.example", "/b", None)
        .first_seen(minute(1))
        .build();
    let channel = scene.ids.channel();
    fixture
        .channel(channel, &resource, scene.a2, minute(1))
        .await;
    let read = access(&resource, scene.a3, AccessKind::Read, minute(2));
    fixture.record(&read, channel).await;
    channel
}

/// INV-668: a promoted channel's resources include the superseded
/// channel's, with canonical writers and readers, each once; asking for the
/// superseded channel answers for the promoted one.
#[tokio::test]
async fn channel_resources_include_superseded() {
    let fixture = Fixture::new().await;
    let mut scene = fixture.scene().await;
    let c2 = second_channel(&fixture, &mut scene).await;
    let caller = fixture.caller(Who::Admin).await;
    let promote = OperatorAction::PromoteChannel {
        channel: scene.c1,
        pattern: wiki(),
        policy: PolicyKind::Sanctioned,
        note: None,
    };
    assert!(fixture.surface.act(&caller, promote).await.is_ok());
    let window = minutes(0, 10);
    for asked in [scene.c1, c2] {
        let mut request: PageRequest<ResourceUseList> = page(1);
        let mut resources = Vec::new();
        loop {
            let Ok(answer) = fixture
                .surface
                .channel_resources(&caller, asked, window, &request)
                .await
            else {
                panic!("resources");
            };
            assert_eq!(answer.value.channel, scene.c1);
            let (items, next) = answer.value.page.into_parts();
            resources.extend(items.into_iter().map(|used| used.resource().id));
            match next {
                Some(next) => request.after = Some(next),
                None => break,
            }
        }
        let mut sorted = resources.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), resources.len());
        assert_eq!(resources.len(), 2);
    }
    assert_eq!(
        fixture
            .surface
            .channel_resources(&caller, ChannelId::from_ulid(0xBAD), window, &page(5))
            .await,
        Err(QueryError::NotFound)
    );
}

/// INV-687 by hand: a row in force counts writers and readers from its
/// resources and transmissions from the graph; a superseded row has its
/// supersession and no counts.
#[tokio::test]
async fn channel_rows_count_resources_and_routed_transmissions() {
    let fixture = Fixture::new().await;
    let mut scene = fixture.scene().await;
    let c2 = second_channel(&fixture, &mut scene).await;
    let caller = fixture.caller(Who::Admin).await;
    let row = match fixture.surface.channel(&caller, scene.c1, None).await {
        Ok(Some(row)) => row,
        other => panic!("row: {other:?}"),
    };
    match row.value.standing() {
        ChannelStanding::InForce(ChannelActivity::Seen { last, counts }) => {
            assert_eq!(
                counts,
                ChannelCounts {
                    writers: 1,
                    readers: 1,
                    transmissions: 1
                }
            );
            assert_eq!(last, scene.t1.read.at);
        }
        other => panic!("standing {other:?}"),
    }
    assert_eq!(row.value.seed().map(|seed| seed.id), Some(scene.r1.id));
    let promote = OperatorAction::PromoteChannel {
        channel: scene.c1,
        pattern: wiki(),
        policy: PolicyKind::Sanctioned,
        note: None,
    };
    assert!(fixture.surface.act(&caller, promote).await.is_ok());
    let Ok(Some(superseded)) = fixture.surface.channel(&caller, c2, None).await else {
        panic!("row");
    };
    let Some(into) = superseded.value.supersession() else {
        panic!("not superseded");
    };
    assert_eq!(into.into(), scene.c1);
    assert_eq!(into.by(), caller.operator());
    assert_eq!(superseded.value.counts(), None);
    let Ok(Some(promoted)) = fixture.surface.channel(&caller, scene.c1, None).await else {
        panic!("row");
    };
    let Some(counts) = promoted.value.counts() else {
        panic!("no counts");
    };
    // a1 and a2 wrote, a2 and a3 read, over both channels.
    assert_eq!((counts.writers, counts.readers), (2, 2));
    // An unaligned window is refused as for topology.
    let unaligned = crosstalk_spec::support::TimeWindow::new(
        crosstalk_spec::support::Timestamp::from_micros(minute(0).as_micros() + 1),
        minute(5),
    );
    let Ok(unaligned) = unaligned else {
        panic!("window");
    };
    assert_eq!(
        fixture
            .surface
            .channel(&caller, scene.c1, Some(unaligned))
            .await,
        Err(QueryError::InvalidInput(InputError::UnalignedWindow))
    );
    assert_eq!(
        fixture
            .surface
            .channel(&caller, ChannelId::from_ulid(0xBAD), None)
            .await,
        Ok(None)
    );
}

/// INV-402: a cursor the surface never issued, and one presented with
/// another filter, are `InvalidCursor`.
#[tokio::test]
async fn forged_cursor_rejected() {
    let fixture = Fixture::new().await;
    let mut scene = fixture.scene().await;
    second_channel(&fixture, &mut scene).await;
    let caller = fixture.caller(Who::Admin).await;
    let Ok(cursor) = crosstalk_spec::paging::Cursor::from_token("forged".to_owned()) else {
        panic!("token");
    };
    let request: PageRequest<ChannelList> = PageRequest {
        size: page::<ChannelList>(1).size,
        after: Some(cursor),
    };
    assert_eq!(
        fixture
            .surface
            .channels(&caller, &ChannelFilter::default(), &request)
            .await,
        Err(QueryError::InvalidCursor)
    );
}

/// INV-402: a cursor issued for one filter is refused with another.
#[tokio::test]
async fn cursor_with_changed_filter_rejected() {
    let fixture = Fixture::new().await;
    let mut scene = fixture.scene().await;
    second_channel(&fixture, &mut scene).await;
    let caller = fixture.caller(Who::Admin).await;
    let first = match fixture
        .surface
        .channels(&caller, &ChannelFilter::default(), &page(1))
        .await
    {
        Ok(first) => first,
        Err(error) => panic!("first page: {error:?}"),
    };
    let Some(next) = first.value.next().cloned() else {
        panic!("one page");
    };
    let other = ChannelFilter {
        origin: OriginFilter::WithSuperseded(Vec::new()),
        ..ChannelFilter::default()
    };
    let request = PageRequest {
        size: page::<ChannelList>(1).size,
        after: Some(next.clone()),
    };
    assert_eq!(
        fixture.surface.channels(&caller, &other, &request).await,
        Err(QueryError::InvalidCursor)
    );
    // With the filter it was issued for, it pages on.
    assert!(
        fixture
            .surface
            .channels(&caller, &ChannelFilter::default(), &request)
            .await
            .is_ok()
    );
}

/// Names follow supersession, keyed by the id asked for.
#[tokio::test]
async fn channel_names_follow_supersession() {
    let fixture = Fixture::new().await;
    let mut scene = fixture.scene().await;
    let c2 = second_channel(&fixture, &mut scene).await;
    let caller = fixture.caller(Who::Admin).await;
    let unknown = ChannelId::from_ulid(0xBAD);
    let Ok(ids) = IdBatch::new([scene.c1, c2, unknown]) else {
        panic!("batch");
    };
    let names = match fixture.surface.channel_names(&caller, &ids).await {
        Ok(names) => names,
        Err(error) => panic!("names: {error:?}"),
    };
    assert_eq!(names.len(), 2);
    assert_eq!(
        names.get(&scene.c1).map(|name| name.shape().clone()),
        Some(ChannelShape::Seed(scene.r1.locator.clone()))
    );
    let promote = OperatorAction::PromoteChannel {
        channel: scene.c1,
        pattern: wiki(),
        policy: PolicyKind::Sanctioned,
        note: None,
    };
    assert!(fixture.surface.act(&caller, promote).await.is_ok());
    let Ok(names) = fixture.surface.channel_names(&caller, &ids).await else {
        panic!("names");
    };
    assert_eq!(names.get(&c2).map(|name| name.id()), Some(scene.c1));
    assert_eq!(
        names.get(&c2).map(|name| name.shape().clone()),
        Some(ChannelShape::Pattern(wiki()))
    );
}

/// The preview of a promotion agrees with the promotion.
#[tokio::test]
async fn promotion_preview_agrees_with_promote() {
    let fixture = Fixture::new().await;
    let mut scene = fixture.scene().await;
    let c2 = second_channel(&fixture, &mut scene).await;
    let viewer = fixture.caller(Who::Viewer).await;
    let Ok(preview) = fixture
        .surface
        .promotion_preview(&viewer, scene.c1, &wiki())
        .await
    else {
        panic!("preview");
    };
    assert_eq!(preview.conflict(), None);
    assert_eq!(preview.superseded_channels().as_slice(), [c2]);
    let governor = fixture.caller(Who::Governor).await;
    let promote = OperatorAction::PromoteChannel {
        channel: scene.c1,
        pattern: wiki(),
        policy: PolicyKind::Sanctioned,
        note: None,
    };
    assert!(fixture.surface.act(&governor, promote).await.is_ok());
    let Ok(again) = fixture
        .surface
        .promotion_preview(&viewer, scene.c1, &wiki())
        .await
    else {
        panic!("preview");
    };
    assert_eq!(
        again.conflict(),
        Some(&ConflictKind::ChannelNotDiscovered { channel: scene.c1 })
    );
    assert_eq!(
        fixture
            .surface
            .promotion_preview(&viewer, ChannelId::from_ulid(0xBAD), &wiki())
            .await,
        Err(QueryError::NotFound)
    );
}

/// The overview counts the graph's activity and the queues as of the read.
#[tokio::test]
async fn overview_counts_activity_and_queues() {
    let fixture = Fixture::new().await;
    let scene = fixture.scene().await;
    let caller = fixture.caller(Who::Viewer).await;
    let Ok(overview) = fixture
        .surface
        .overview(&caller, minutes(0, 10), &TopologyFilter::default())
        .await
    else {
        panic!("overview");
    };
    assert_eq!(overview.value.activity.transmissions, 1);
    assert_eq!(overview.value.activity.active_channels, 1);
    assert_eq!(overview.value.queues.open_alerts, 1);
    assert_eq!(overview.value.queues.unreviewed_channels, 1);
    let Ok(graph) = fixture
        .surface
        .topology(
            &caller,
            minutes(0, 10),
            Weighting::Transmissions,
            &TopologyFilter::default(),
        )
        .await
    else {
        panic!("graph");
    };
    assert_eq!(graph.value.edges().len(), 1);
    let _: AgentId = scene.a1;
}
