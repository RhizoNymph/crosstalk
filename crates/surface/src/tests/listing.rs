//! Cross-agent channel semantics as seen through the surface: listings
//! (confirmed, unconfirmed, declarations, hidden), the channel list's
//! order, merges hiding and unmerges restoring, the overview's queues,
//! alerts about hidden subjects, rows by id, and a channel's transmissions.

use crosstalk_spec::aggregates::alert::{AlertSubject, BuiltinRule};
use crosstalk_spec::aggregates::edge::{TopologyFilter, Weighting};
use crosstalk_spec::aggregates::filter::{TopicVersionSelector, UnconfirmedChannels};
use crosstalk_spec::aggregates::node::GraphNode;
use crosstalk_spec::derived::flow::channel::confirmation::{Confirmation, Listing, ListingKind};
use crosstalk_spec::derived::flow::channel::policy::{Policy, PolicyAuthor, PolicyKind};
use crosstalk_spec::derived::flow::resource::{Host, ResourcePattern};
use crosstalk_spec::ids::{AgentId, AlertId, ChannelId, MergeId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::ChannelRegistry;
use crosstalk_spec::interfaces::l7_topology::NodeFacts;
use crosstalk_spec::interfaces::l8_surface::channel_traffic::{
    ChannelTransmissionFilter, ChannelTransmissionPage,
};
use crosstalk_spec::interfaces::l8_surface::lists::ChannelFilter;
use crosstalk_spec::interfaces::l8_surface::overview::QueueCounts;
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionSelection;
use crosstalk_spec::interfaces::l8_surface::{
    ActionOutcome, ActionRequest, AlertFilter, Caller, OperatorAction, OperatorActions, Permission,
    QueryApi, QueryError,
};
use crosstalk_spec::paging::{ChannelList, ChannelTransmissionList, PageRequest};
use crosstalk_spec::support::TimeWindow;
use crosstalk_testkit::build::{ResourceBuilder, TransmissionBuilder, TransmissionParts};

use super::page;
use super::world::{Fixture, Scene, Who, minute, minutes};

/// A second channel, on `other.example`, discovered at minute 1 by a write
/// by `a2` that `a3` read: its only transmission awaits content, so it is
/// listed unconfirmed.
async fn unconfirmed_channel(
    fixture: &Fixture,
    scene: &mut Scene,
) -> (ChannelId, TransmissionParts) {
    let resource = ResourceBuilder::new(&mut scene.ids)
        .url("https", "other.example", "/x", None)
        .first_seen(minute(1))
        .build();
    let channel = scene.ids.channel();
    let parts = fixture
        .channel(
            &mut scene.ids,
            channel,
            &resource,
            scene.a2,
            scene.a3,
            minute(1),
        )
        .await;
    (channel, parts)
}

/// Every row `filter` lists, in order, by a full traversal.
async fn listed(fixture: &Fixture, caller: &Caller, filter: &ChannelFilter) -> Vec<ChannelId> {
    let mut request: PageRequest<ChannelList> = page(1);
    let mut ids = Vec::new();
    loop {
        let answer = match fixture.surface.channels(caller, filter, &request).await {
            Ok(answer) => answer,
            Err(error) => panic!("channels: {error:?}"),
        };
        let (rows, next) = answer.value.into_parts();
        ids.extend(rows.iter().map(|row| row.channel().id));
        match next {
            Some(next) => request.after = Some(next),
            None => return ids,
        }
    }
}

fn listing_filter(listings: Vec<ListingKind>) -> ChannelFilter {
    ChannelFilter {
        listings,
        ..ChannelFilter::default()
    }
}

async fn listing_of(fixture: &Fixture, caller: &Caller, channel: ChannelId) -> Option<Listing> {
    match fixture.surface.channel(caller, channel, None).await {
        Ok(Some(row)) => row.value.listing(),
        other => panic!("channel {channel:?}: {other:?}"),
    }
}

async fn merge(fixture: &Fixture, from: AgentId, into: AgentId) -> MergeId {
    let admin = fixture.caller(Who::Admin).await;
    match fixture
        .surface
        .request(&admin, ActionRequest::MergeAgents { from, into })
        .await
    {
        Ok(ActionOutcome::Merged(merge)) => merge,
        other => panic!("merge: {other:?}"),
    }
}

async fn unmerge(fixture: &Fixture, merge: MergeId) {
    let admin = fixture.caller(Who::Admin).await;
    let outcome = fixture
        .surface
        .act(&admin, OperatorAction::Unmerge { merge })
        .await;
    assert!(outcome.is_ok(), "unmerge: {outcome:?}");
}

/// Whether the channel-centred graph over `window` draws `channel`.
async fn drawn(fixture: &Fixture, viewer: &Caller, window: TimeWindow, channel: ChannelId) -> bool {
    match fixture
        .surface
        .channel_topology(
            viewer,
            window,
            Weighting::Transmissions,
            &TopologyFilter::default(),
        )
        .await
    {
        Ok(graph) => graph
            .value
            .nodes()
            .iter()
            .any(|node| matches!(node, GraphNode::Channel(node) if node.id == channel)),
        Err(error) => panic!("graph: {error:?}"),
    }
}

async fn queues(fixture: &Fixture, viewer: &Caller, window: TimeWindow) -> QueueCounts {
    match fixture
        .surface
        .overview(viewer, window, &TopologyFilter::default())
        .await
    {
        Ok(overview) => overview.value.queues,
        Err(error) => panic!("overview: {error:?}"),
    }
}

/// The ids of every listed alert, ascending.
async fn shown_alerts(fixture: &Fixture, viewer: &Caller) -> Vec<AlertId> {
    match fixture
        .surface
        .alerts(viewer, &AlertFilter::default(), &page(50))
        .await
    {
        Ok(alerts) => {
            let mut ids: Vec<_> = alerts.items().iter().map(|alert| alert.id).collect();
            ids.sort();
            ids
        }
        Err(error) => panic!("alerts: {error:?}"),
    }
}

async fn rows_by_id(
    fixture: &Fixture,
    viewer: &Caller,
    selection: &TransmissionSelection,
) -> Vec<TransmissionId> {
    match fixture
        .surface
        .transmissions_by_id(viewer, selection, TopicVersionSelector::Current, &page(10))
        .await
    {
        Ok(page) => page.page.items().iter().map(|row| row.id).collect(),
        Err(error) => panic!("rows: {error:?}"),
    }
}

/// INV-857, INV-858: the scene's channel, discovered by a classified
/// transmission, is listed confirmed; a channel whose only cross-agent
/// transmission awaits content is listed unconfirmed; a declaration with no
/// traffic is listed apart. The default filter lists all three, and the
/// listings select each.
#[tokio::test]
async fn unconfirmed_channels_are_listed_and_filterable() {
    let fixture = Fixture::new().await;
    let mut scene = fixture.scene().await;
    let (c2, _) = unconfirmed_channel(&fixture, &mut scene).await;
    let mut registry = fixture.world.channels.clone();
    let declared = match registry
        .declare(
            ResourcePattern::Host(Host("docs.example".to_owned())),
            Policy::Unreviewed(None),
            PolicyAuthor::Config,
            minute(2),
        )
        .await
    {
        Ok(channel) => channel,
        Err(error) => panic!("declare: {error:?}"),
    };
    let viewer = fixture.caller(Who::Viewer).await;
    assert_eq!(
        listing_of(&fixture, &viewer, scene.c1).await,
        Some(Listing::Channel(Confirmation::Confirmed))
    );
    assert_eq!(
        listing_of(&fixture, &viewer, c2).await,
        Some(Listing::Channel(Confirmation::Unconfirmed))
    );
    assert_eq!(
        listing_of(&fixture, &viewer, declared).await,
        Some(Listing::Declaration)
    );
    let all = listed(&fixture, &viewer, &ChannelFilter::default()).await;
    assert_eq!(all.len(), 3);
    for (kinds, expected) in [
        (vec![ListingKind::Unconfirmed], vec![c2]),
        (vec![ListingKind::Confirmed], vec![scene.c1]),
        (vec![ListingKind::Declaration], vec![declared]),
        (
            vec![ListingKind::Confirmed, ListingKind::Declaration],
            vec![declared, scene.c1],
        ),
    ] {
        assert_eq!(
            listed(&fixture, &viewer, &listing_filter(kinds.clone())).await,
            expected,
            "{kinds:?}"
        );
    }
}

/// INV-1035: the list is newest created first, ties by id descending: a
/// declaration by its declaration time, a discovered channel by the opening
/// of the transmission that discovered it.
#[tokio::test]
async fn channels_are_listed_newest_created_first() {
    let fixture = Fixture::new().await;
    let mut scene = fixture.scene().await;
    let (c2, _) = unconfirmed_channel(&fixture, &mut scene).await;
    // Discovered at the same instant as c2, under a larger id.
    let resource = ResourceBuilder::new(&mut scene.ids)
        .url("https", "other.example", "/y", None)
        .first_seen(minute(1))
        .build();
    let c3 = scene.ids.channel();
    fixture
        .channel(&mut scene.ids, c3, &resource, scene.a3, scene.a1, minute(1))
        .await;
    assert!(c3 > c2);
    let mut registry = fixture.world.channels.clone();
    let Ok(declared) = registry
        .declare(
            ResourcePattern::Host(Host("docs.example".to_owned())),
            Policy::Unreviewed(None),
            PolicyAuthor::Config,
            minute(5),
        )
        .await
    else {
        panic!("declare");
    };
    let viewer = fixture.caller(Who::Viewer).await;
    assert_eq!(
        listed(&fixture, &viewer, &ChannelFilter::default()).await,
        vec![declared, c3, c2, scene.c1]
    );
    let Ok(answer) = fixture
        .surface
        .channels(&viewer, &ChannelFilter::default(), &page(10))
        .await
    else {
        panic!("channels");
    };
    let created: Vec<_> = answer
        .value
        .items()
        .iter()
        .map(|row| row.created_at())
        .collect();
    assert_eq!(
        created,
        vec![
            minute(5),
            scene_opened(&fixture, c3).await,
            scene_opened(&fixture, c2).await,
            scene.t1.transmission.opened_at,
        ]
    );
}

/// The opening time of the transmission that discovered `channel`.
async fn scene_opened(fixture: &Fixture, channel: ChannelId) -> crosstalk_spec::support::Timestamp {
    use crosstalk_spec::interfaces::l5_flow::channels::ChannelReads;
    match fixture.world.channels.channel(channel).await {
        Ok(Some(read)) => match read.channel().origin.seed() {
            Some(seed) => seed.opened_at,
            None => panic!("no seed"),
        },
        other => panic!("channel: {other:?}"),
    }
}

/// INV-859: merging the two agents of the scene's only transmission hides
/// its channel from the list, the channel-centred graph and the overview's
/// queues while its row still answers as hidden; the unmerge lists, draws
/// and counts it again.
#[tokio::test]
async fn a_merge_hides_a_channel_and_an_unmerge_lists_it_again() {
    let mut fixture = Fixture::new().await;
    let scene = fixture.scene().await;
    fixture.relay().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let window = minutes(0, 10);
    fixture.watermark(minute(10)).await;
    assert_eq!(
        listed(&fixture, &viewer, &ChannelFilter::default()).await,
        vec![scene.c1]
    );
    assert!(drawn(&fixture, &viewer, window, scene.c1).await);
    let before = queues(&fixture, &viewer, window).await;
    assert_eq!(before.unreviewed_channels, 1);
    assert_eq!(before.open_alerts, 1);

    fixture.clock.set(minute(3));
    let merged = merge(&fixture, scene.a2, scene.a1).await;
    fixture.relay().await;
    assert!(
        listed(&fixture, &viewer, &ChannelFilter::default())
            .await
            .is_empty()
    );
    assert_eq!(
        listing_of(&fixture, &viewer, scene.c1).await,
        Some(Listing::Hidden)
    );
    assert!(
        fixture
            .surface
            .policy_history(&viewer, scene.c1)
            .await
            .is_ok_and(|history| history.is_some())
    );
    assert!(!drawn(&fixture, &viewer, window, scene.c1).await);
    assert_eq!(fixture.world.nodes.channel_of(scene.r1.id), None);
    let hidden = queues(&fixture, &viewer, window).await;
    assert_eq!(hidden.unreviewed_channels, 0);
    assert_eq!(
        hidden.open_alerts, 0,
        "the alert on the hidden channel is not shown"
    );

    fixture.clock.set(minute(4));
    unmerge(&fixture, merged).await;
    fixture.relay().await;
    assert_eq!(
        listed(&fixture, &viewer, &ChannelFilter::default()).await,
        vec![scene.c1]
    );
    assert!(drawn(&fixture, &viewer, window, scene.c1).await);
    assert_eq!(fixture.world.nodes.channel_of(scene.r1.id), Some(scene.c1));
    assert_eq!(queues(&fixture, &viewer, window).await, before);
}

/// INV-864: the overview counts unconfirmed channels under `Include`, and
/// under `Exclude` counts them in no queue and reports no unconfirmed
/// count.
#[tokio::test]
async fn overview_queues_honour_unconfirmed_channels() {
    let fixture = Fixture::new().await;
    let mut scene = fixture.scene().await;
    unconfirmed_channel(&fixture, &mut scene).await;
    let viewer = fixture.caller(Who::Viewer).await;
    let window = minutes(0, 10);
    let include = TopologyFilter::default();
    let exclude = TopologyFilter {
        unconfirmed_channels: UnconfirmedChannels::Exclude,
        ..TopologyFilter::default()
    };
    let Ok(included) = fixture.surface.overview(&viewer, window, &include).await else {
        panic!("overview");
    };
    assert_eq!(included.value.queues.unreviewed_channels, 2);
    assert_eq!(included.value.queues.unconfirmed_channels, Some(1));
    let Ok(excluded) = fixture.surface.overview(&viewer, window, &exclude).await else {
        panic!("overview");
    };
    assert_eq!(excluded.value.queues.unreviewed_channels, 1);
    assert_eq!(excluded.value.queues.unconfirmed_channels, None);
}

/// INV-867: alerts about a transmission whose agents merged into one, or
/// about a hidden channel, are not listed; an alert about an agent always
/// is; an unmerge lists them again.
#[tokio::test]
async fn alerts_about_hidden_subjects_are_not_shown() {
    let fixture = Fixture::new().await;
    let scene = fixture.scene().await;
    let on_transmission = fixture
        .alert(
            BuiltinRule::NewChannel,
            AlertSubject::Transmission(scene.t1.transmission.id),
            minute(1),
        )
        .await;
    let on_agent = fixture
        .alert(
            BuiltinRule::NewChannel,
            AlertSubject::Agent(scene.a3),
            minute(1),
        )
        .await;
    let viewer = fixture.caller(Who::Viewer).await;
    let mut every = vec![scene.alert, on_transmission, on_agent];
    every.sort();
    assert_eq!(shown_alerts(&fixture, &viewer).await, every);
    fixture.clock.set(minute(3));
    let merged = merge(&fixture, scene.a2, scene.a1).await;
    assert_eq!(shown_alerts(&fixture, &viewer).await, vec![on_agent]);
    fixture.clock.set(minute(4));
    unmerge(&fixture, merged).await;
    assert_eq!(shown_alerts(&fixture, &viewer).await, every);
}

/// INV-1036: `transmissions_by_id` leaves out a transmission whose agents
/// merged into one and lists it again after the unmerge.
#[tokio::test]
async fn rows_by_id_skip_transmissions_within_one_agent() {
    let fixture = Fixture::new().await;
    let scene = fixture.scene().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let selection = TransmissionSelection::new(vec![scene.t1.transmission.id])
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        rows_by_id(&fixture, &viewer, &selection).await,
        vec![scene.t1.transmission.id]
    );
    fixture.clock.set(minute(3));
    let merged = merge(&fixture, scene.a2, scene.a1).await;
    assert!(rows_by_id(&fixture, &viewer, &selection).await.is_empty());
    fixture.clock.set(minute(4));
    unmerge(&fixture, merged).await;
    assert_eq!(
        rows_by_id(&fixture, &viewer, &selection).await,
        vec![scene.t1.transmission.id]
    );
}

/// Every row of `channel`'s transmissions under `filter`, a page of `size`
/// at a time.
async fn channel_rows(
    fixture: &Fixture,
    caller: &Caller,
    channel: ChannelId,
    filter: ChannelTransmissionFilter,
    size: u16,
) -> Vec<ChannelTransmissionPage> {
    let mut request: PageRequest<ChannelTransmissionList> = page(size);
    let mut pages = Vec::new();
    loop {
        let answer = match fixture
            .surface
            .channel_transmissions(
                caller,
                channel,
                &filter,
                TopicVersionSelector::Current,
                &request,
            )
            .await
        {
            Ok(answer) => answer,
            Err(error) => panic!("channel transmissions: {error:?}"),
        };
        let next = answer.page.next().cloned();
        pages.push(answer);
        match next {
            Some(next) => request.after = Some(next),
            None => return pages,
        }
    }
}

fn ids_of(pages: &[ChannelTransmissionPage]) -> Vec<TransmissionId> {
    pages
        .iter()
        .flat_map(|page| page.page.items().iter().map(|row| row.summary().id))
        .collect()
}

/// INV-868: a channel's transmissions are its crossing ones, newest opened
/// first, each naming its senders; the confirmation filter selects them; a
/// cursor continues only its own request; a merge leaves out a transmission
/// within one agent.
#[tokio::test]
async fn channel_transmissions_list_crossing_transmissions_newest_first() {
    let fixture = Fixture::new().await;
    let mut scene = fixture.scene().await;
    let later = match TransmissionBuilder::new(&mut scene.ids)
        .between(scene.a3, scene.a2)
        .channel(scene.c1)
        .opened_at(minute(2))
        .accesses(|cross| cross.resource(scene.r1.id))
        .suspected()
        .build_parts()
    {
        Ok(parts) => parts,
        Err(error) => panic!("transmission: {error:?}"),
    };
    fixture.record(&later.write).await;
    fixture.record(&later.read).await;
    fixture.transmission(&later.transmission).await;
    let viewer = fixture.caller(Who::Viewer).await;
    let all = channel_rows(
        &fixture,
        &viewer,
        scene.c1,
        ChannelTransmissionFilter::default(),
        1,
    )
    .await;
    assert_eq!(
        ids_of(&all),
        vec![later.transmission.id, scene.t1.transmission.id]
    );
    assert!(all.iter().all(|page| page.channel == scene.c1));
    let rows: Vec<_> = all
        .iter()
        .flat_map(|page| page.page.items().to_vec())
        .collect();
    assert_eq!(
        rows[0].senders().iter().copied().collect::<Vec<_>>(),
        vec![scene.a3]
    );
    assert_eq!(rows[0].confirmation(), Confirmation::Unconfirmed);
    assert_eq!(
        rows[1].senders().iter().copied().collect::<Vec<_>>(),
        vec![scene.a1]
    );
    assert_eq!(rows[1].confirmation(), Confirmation::Confirmed);
    for (confirmation, expected) in [
        (Confirmation::Unconfirmed, later.transmission.id),
        (Confirmation::Confirmed, scene.t1.transmission.id),
    ] {
        let filter = ChannelTransmissionFilter {
            confirmation: Some(confirmation),
        };
        let pages = channel_rows(&fixture, &viewer, scene.c1, filter, 10).await;
        assert_eq!(ids_of(&pages), vec![expected], "{confirmation:?}");
    }
    // The first page's cursor, presented with another filter.
    let Some(cursor) = all[0].page.next().cloned() else {
        panic!("a second page");
    };
    let other = ChannelTransmissionFilter {
        confirmation: Some(Confirmation::Confirmed),
    };
    let request = PageRequest {
        after: Some(cursor),
        ..page(1)
    };
    assert_eq!(
        fixture
            .surface
            .channel_transmissions(
                &viewer,
                scene.c1,
                &other,
                TopicVersionSelector::Current,
                &request
            )
            .await,
        Err(QueryError::InvalidCursor)
    );
    // An unknown channel.
    assert_eq!(
        fixture
            .surface
            .channel_transmissions(
                &viewer,
                ChannelId::from_ulid(7),
                &ChannelTransmissionFilter::default(),
                TopicVersionSelector::Current,
                &page(10),
            )
            .await,
        Err(QueryError::NotFound)
    );
    // Merging a1 into a2 leaves t1 (a1 to a2) within one agent.
    fixture.clock.set(minute(3));
    merge(&fixture, scene.a1, scene.a2).await;
    let merged = channel_rows(
        &fixture,
        &viewer,
        scene.c1,
        ChannelTransmissionFilter::default(),
        10,
    )
    .await;
    assert_eq!(ids_of(&merged), vec![later.transmission.id]);
}

/// A superseded channel's transmissions are its superseding channel's,
/// which the page names.
#[tokio::test]
async fn channel_transmissions_of_a_superseded_channel_answer_for_its_successor() {
    let fixture = Fixture::new().await;
    let mut scene = fixture.scene().await;
    let resource = ResourceBuilder::new(&mut scene.ids)
        .url("https", "wiki.example", "/b", None)
        .first_seen(minute(1))
        .build();
    let c2 = scene.ids.channel();
    let discovered = fixture
        .channel(&mut scene.ids, c2, &resource, scene.a2, scene.a3, minute(1))
        .await;
    let admin = fixture.caller(Who::Admin).await;
    let promote = OperatorAction::PromoteChannel {
        channel: scene.c1,
        pattern: super::pattern(),
        policy: PolicyKind::Sanctioned,
        note: None,
    };
    assert!(fixture.surface.act(&admin, promote).await.is_ok());
    let pages = channel_rows(
        &fixture,
        &admin,
        c2,
        ChannelTransmissionFilter::default(),
        10,
    )
    .await;
    assert!(pages.iter().all(|page| page.channel == scene.c1));
    assert_eq!(
        ids_of(&pages),
        vec![discovered.transmission.id, scene.t1.transmission.id]
    );
}

/// INV-869: without View, `channel_transmissions` is `Forbidden` before it
/// reads anything (an unknown channel would otherwise be `NotFound`).
#[tokio::test]
async fn channel_transmissions_need_view() {
    let fixture = Fixture::new().await;
    let auditor = fixture.caller(Who::Auditor).await;
    assert_eq!(
        fixture
            .surface
            .channel_transmissions(
                &auditor,
                ChannelId::from_ulid(7),
                &ChannelTransmissionFilter::default(),
                TopicVersionSelector::Current,
                &page(10),
            )
            .await,
        Err(QueryError::Forbidden {
            missing: Permission::View
        })
    );
}
