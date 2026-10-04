//! Channel reads through the spec's types: rows and their counts,
//! resources, names and policy history, and the team-notes promotion in
//! the world's past. Policy and promotion actions are in `promotion`.

use crosstalk_spec::aggregates::edge::{TopologyFilter, Weighting};
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::channel::detection::TrafficDetection;
use crosstalk_spec::derived::flow::channel::policy::{Policy, PolicyAuthor, PolicyKind};
use crosstalk_spec::derived::flow::channel::{ChannelOrigin, DeclaredHistory};
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l8_surface::channels::{
    ChannelActivity, ChannelCounts, ChannelRow, ChannelShape, ChannelStanding,
};
use crosstalk_spec::interfaces::l8_surface::lists::{ChannelFilter, OriginFilter};
use crosstalk_spec::interfaces::l8_surface::{InputError, Permission, QueryError};
use crosstalk_spec::paging::PageRequest;
use crosstalk_spec::support::{TimeWindow, Timestamp};

use super::super::FixtureBackend;
use super::super::clock::{BUCKET, START, plus};
use super::super::world::{ChannelKey, OPERATOR_RESEARCHER};
use super::{caller, collect, day, first, researcher, shared, week};
use crosstalk_spec::interfaces::l8_surface::QueryApi;

use super::actions_support::channel;

/// Every row `filter` lists, page by page.
pub async fn rows(b: &FixtureBackend, filter: &ChannelFilter) -> Vec<ChannelRow> {
    let c = researcher();
    collect(4, async |p| {
        b.channels(&c, filter, &p).await.map(|listed| listed.value)
    })
    .await
}

pub async fn row(b: &FixtureBackend, id: ChannelId, window: Option<TimeWindow>) -> ChannelRow {
    b.channel(&researcher(), id, window)
        .await
        .expect("read")
        .expect("channel")
        .value
}

#[tokio::test]
async fn rows_list_channels_in_force_by_default_newest_first() {
    let b = shared();
    let old = channel(b, ChannelKey::OldTeamNotes);
    let notes = channel(b, ChannelKey::TeamNotes);
    let listed = rows(b, &ChannelFilter::default()).await;
    assert_eq!(listed.len(), 14, "the superseded channel is hidden");
    assert!(listed.iter().all(|r| r.supersession().is_none()));
    let created: Vec<Timestamp> = {
        let state = b.state.read().await;
        listed
            .iter()
            .map(|r| state.channels[&r.channel().id].created)
            .collect()
    };
    assert!(created.windows(2).all(|w| w[0] >= w[1]), "newest first");
    let with = ChannelFilter {
        origin: OriginFilter::WithSuperseded(Vec::new()),
        ..ChannelFilter::default()
    };
    assert_eq!(rows(b, &with).await.len(), 15);
    let only = ChannelFilter {
        origin: OriginFilter::Superseded,
        ..ChannelFilter::default()
    };
    let superseded = rows(b, &only).await;
    assert_eq!(
        superseded
            .iter()
            .map(|r| r.channel().id)
            .collect::<Vec<_>>(),
        [old]
    );
    let standing = superseded[0].supersession().expect("superseded");
    assert_eq!(
        (standing.into(), standing.by()),
        (notes, OPERATOR_RESEARCHER)
    );
    assert_eq!(superseded[0].counts(), None, "no counts of its own");
    assert_eq!(superseded[0].last_activity(), None);
    // Origin kinds.
    let kinds = |kinds| ChannelFilter {
        origin: OriginFilter::InForce(kinds),
        ..ChannelFilter::default()
    };
    use crosstalk_spec::aggregates::node::CanonicalOriginKind as O;
    let promoted = rows(b, &kinds(vec![O::Promoted])).await;
    assert_eq!(
        promoted.iter().map(|r| r.channel().id).collect::<Vec<_>>(),
        [notes]
    );
    assert_eq!(
        rows(b, &kinds(vec![O::DeclaredBeforeTraffic])).await.len(),
        5
    );
    assert_eq!(rows(b, &kinds(vec![O::Discovered])).await.len(), 8);
}

#[tokio::test]
async fn a_superseded_id_answers_with_its_own_record() {
    let b = shared();
    let (old, notes) = (
        channel(b, ChannelKey::OldTeamNotes),
        channel(b, ChannelKey::TeamNotes),
    );
    let own = row(b, old, Some(week().window)).await;
    assert_eq!(own.channel().id, old);
    assert_eq!(own.supersession().map(|s| s.into()), Some(notes));
    assert!(own.seed().is_some());
    let unknown = b
        .channel(&researcher(), ChannelId::from_ulid(1), None)
        .await
        .expect("read");
    assert_eq!(unknown, None);
    let design = row(b, channel(b, ChannelKey::DesignDocs), None).await;
    assert_eq!(
        design.standing(),
        ChannelStanding::InForce(ChannelActivity::Never),
        "declared and never used"
    );
}

#[tokio::test]
async fn row_counts_are_the_resources_tally_and_the_graphs_routed_counts() {
    let b = shared();
    let c = researcher();
    for scope in [day(), week()] {
        let window = scope.window;
        let filter = ChannelFilter {
            window: Some(window),
            ..ChannelFilter::default()
        };
        let graph = b
            .topology(
                &c,
                window,
                Weighting::Transmissions,
                &TopologyFilter::default(),
            )
            .await
            .expect("topology")
            .value;
        let routed = ChannelCounts::routed(&graph);
        let listed = rows(b, &filter).await;
        for listed_row in &listed {
            let id = listed_row.channel().id;
            let uses = collect(3, async |p| {
                b.channel_resources(&c, id, window, &p)
                    .await
                    .map(|page| page.value.page)
            })
            .await;
            let expected = ChannelCounts::tally(&uses, routed.get(&id).copied().unwrap_or(0));
            match listed_row.standing() {
                ChannelStanding::InForce(ChannelActivity::Seen { counts, .. }) => {
                    assert_eq!(counts, expected, "{id:?}");
                }
                ChannelStanding::InForce(ChannelActivity::Never) => {
                    assert_eq!(expected, ChannelCounts::default(), "{id:?}");
                }
                ChannelStanding::Superseded(_) => panic!("listed by default"),
            }
        }
        // The overview's active channels are the rows with traffic.
        let overview = b
            .overview(&c, window, &TopologyFilter::default())
            .await
            .expect("overview")
            .value;
        let active = listed
            .iter()
            .filter(|r| r.counts().is_some_and(|counts| counts.transmissions > 0))
            .count();
        assert_eq!(overview.activity.active_channels, active as u64);
    }
}

#[tokio::test]
async fn the_window_counts_but_never_filters() {
    let b = shared();
    let wiki = channel(b, ChannelKey::HijackedWiki);
    let ids = |listed: Vec<ChannelRow>| {
        listed
            .into_iter()
            .map(|r| r.channel().id)
            .collect::<Vec<_>>()
    };
    let windowed = |window| ChannelFilter {
        window,
        ..ChannelFilter::default()
    };
    let first_bucket =
        TimeWindow::new(START, plus(START, BUCKET.as_micros().get())).expect("bucket");
    assert_eq!(
        ids(rows(b, &windowed(None)).await),
        ids(rows(b, &windowed(Some(first_bucket))).await)
    );
    let all = row(b, wiki, None).await;
    let recent = row(b, wiki, Some(day().window)).await;
    let quiet = row(b, wiki, Some(first_bucket)).await;
    let (all_counts, recent_counts) = (
        all.counts().expect("counts"),
        recent.counts().expect("counts"),
    );
    assert!(
        0 < recent_counts.transmissions && recent_counts.transmissions < all_counts.transmissions
    );
    assert!(recent_counts.writers <= all_counts.writers);
    assert_eq!(quiet.counts(), Some(ChannelCounts::default()));
    assert_eq!(recent.last_activity(), all.last_activity());
    assert_eq!(quiet.last_activity(), all.last_activity());
    // An unaligned window is refused, as for the graph.
    let unaligned =
        TimeWindow::new(Timestamp::from_micros(1), Timestamp::from_micros(2)).expect("window");
    let refused = Some(QueryError::InvalidInput(InputError::UnalignedWindow));
    let c = researcher();
    assert_eq!(
        b.channels(&c, &windowed(Some(unaligned)), &first(10))
            .await
            .err(),
        refused
    );
    assert_eq!(b.channel(&c, wiki, Some(unaligned)).await.err(), refused);
}

#[tokio::test]
async fn resources_page_newest_first_through_the_channel_in_force() {
    let b = shared();
    let c = researcher();
    let (old, notes) = (
        channel(b, ChannelKey::OldTeamNotes),
        channel(b, ChannelKey::TeamNotes),
    );
    let window = week().window;
    let page = b
        .channel_resources(&c, old, window, &first(1))
        .await
        .expect("resources")
        .value;
    assert_eq!(
        page.channel, notes,
        "a superseded channel answers for its channel in force"
    );
    assert_eq!(page.window, window);
    let uses = collect(1, async |p| {
        b.channel_resources(&c, old, window, &p)
            .await
            .map(|page| page.value.page)
    })
    .await;
    let ids: Vec<_> = uses.iter().map(|u| u.resource().id).collect();
    assert!(ids.windows(2).all(|w| w[0] > w[1]), "newest first");
    assert!(uses.iter().any(|u| matches!(&u.resource().locator,
        Locator::Url { path, .. } if path == "/team-a/standup")));
    assert!(uses.iter().any(|u| matches!(&u.resource().locator,
        Locator::Url { path, .. } if path == "/team-a/retro")));
    // A cursor is bound to its channel and window.
    let next = page.page.next().cloned().expect("more than one resource");
    let other = PageRequest {
        after: Some(next),
        ..first(1)
    };
    assert_eq!(
        b.channel_resources(&c, notes, day().window, &other)
            .await
            .err(),
        Some(QueryError::InvalidCursor)
    );
    assert_eq!(
        b.channel_resources(&c, ChannelId::from_ulid(1), window, &first(1))
            .await
            .err(),
        Some(QueryError::NotFound)
    );
    let nobody = caller(&[Permission::Audit]);
    assert_eq!(
        b.channel_resources(&nobody, notes, window, &first(1))
            .await
            .err(),
        Some(QueryError::Forbidden {
            missing: Permission::View
        })
    );
}

#[tokio::test]
async fn names_resolve_supersession_from_one_batch() {
    let b = shared();
    let c = researcher();
    let (old, notes, wiki) = (
        channel(b, ChannelKey::OldTeamNotes),
        channel(b, ChannelKey::TeamNotes),
        channel(b, ChannelKey::HijackedWiki),
    );
    let batch = IdBatch::new([old, notes, wiki, ChannelId::from_ulid(1)]).expect("batch");
    let names = b.channel_names(&c, &batch).await.expect("names");
    assert_eq!(names.len(), 3, "unknown ids are left out");
    assert_eq!(names[&old].id(), notes, "named by the channel in force");
    assert_eq!(names[&old], names[&notes]);
    assert!(matches!(names[&notes].shape(), ChannelShape::Pattern(_)));
    assert!(
        matches!(names[&wiki].shape(), ChannelShape::Seed(Locator::Url { path, .. })
        if path == "/wiki/Agent_Coordination")
    );
}

#[tokio::test]
async fn policy_histories_hold_every_decision() {
    let b = shared();
    let c = researcher();
    let history = async |key| {
        b.policy_history(&c, channel(b, key))
            .await
            .expect("read")
            .expect("history")
    };
    let mcp = history(ChannelKey::McpMemory).await;
    assert_eq!(
        mcp.entries().iter().map(|e| e.kind).collect::<Vec<_>>(),
        [PolicyKind::Sanctioned, PolicyKind::Unreviewed],
        "sanctioned, then reset"
    );
    assert!(matches!(mcp.current(), Policy::Unreviewed(Some(_))));
    let config = history(ChannelKey::InternalWiki).await;
    assert!(
        config
            .entries()
            .iter()
            .all(|e| e.decision.by == PolicyAuthor::Config)
    );
    let notes = history(ChannelKey::TeamNotes).await;
    assert_eq!(
        notes.latest().map(|e| (e.kind, e.decision.by)),
        Some((
            PolicyKind::Sanctioned,
            PolicyAuthor::Operator(OPERATOR_RESEARCHER)
        )),
        "the promotion's decision"
    );
    assert!(history(ChannelKey::HijackedWiki).await.entries().is_empty());
    assert_eq!(
        b.policy_history(&c, ChannelId::from_ulid(1)).await,
        Ok(None)
    );
    // Every channel's policy is its history's current one.
    let state = b.state.read().await;
    for record in state.channels.values() {
        assert_eq!(record.channel().policy, record.history().current());
    }
}

#[tokio::test]
async fn team_notes_were_promoted_and_absorbed_the_standup_page() {
    let b = shared();
    let (old, notes) = (
        channel(b, ChannelKey::OldTeamNotes),
        channel(b, ChannelKey::TeamNotes),
    );
    let promoted = row(b, notes, None).await;
    let ChannelOrigin::Declared {
        declaration,
        history: DeclaredHistory::Promoted { from, .. },
    } = &promoted.channel().origin
    else {
        panic!("team notes are promoted: {:?}", promoted.channel().origin)
    };
    assert_eq!(declaration.by, PolicyAuthor::Operator(OPERATOR_RESEARCHER));
    assert_eq!(Some(from.resource), promoted.seed().map(|s| s.id));
    let absorbed = row(b, old, None).await;
    let ChannelOrigin::Superseded {
        supersession,
        detection,
        ..
    } = &absorbed.channel().origin
    else {
        panic!("the standup page is superseded")
    };
    assert_eq!(supersession.by, notes);
    assert_eq!(supersession.at, declaration.at);
    // Its detection is frozen at the promotion; the promoted channel's
    // follows every later confirmation routed through either
    // ("Detection follows resolution").
    let confirmed_at = |id| {
        let record = b.world.tx(id).expect("transmission");
        super::super::world::confirmed(&record.transmission.state)
            .expect("confirmed")
            .at()
    };
    if let TrafficDetection::Active {
        last_transmission, ..
    } = detection
    {
        assert!(confirmed_at(*last_transmission) <= supersession.at);
    }
    let latest = b
        .world
        .transmissions
        .iter()
        .filter_map(|t| {
            let at = super::super::world::confirmed(&t.transmission.state)?.at();
            let counts = t.transmission.route == Route::Channel(notes)
                || (t.transmission.route == Route::Channel(old) && at > supersession.at);
            counts.then_some((at, t.transmission.id))
        })
        .max_by_key(|(at, _)| *at)
        .map(|(_, id)| id);
    let ChannelOrigin::Declared {
        history:
            DeclaredHistory::Promoted {
                detection:
                    TrafficDetection::Active {
                        last_transmission, ..
                    },
                ..
            },
        ..
    } = &promoted.channel().origin
    else {
        panic!("the promoted channel is active")
    };
    assert_eq!(
        latest.map(confirmed_at),
        Some(confirmed_at(*last_transmission))
    );
}
