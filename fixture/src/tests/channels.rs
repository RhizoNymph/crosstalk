//! Channel reads through the spec's types: rows and their counts,
//! resources, names and policy history, and the team-notes promotion in
//! the world's past. Policy and promotion actions are in `promotion`.

use crosstalk_spec::aggregates::alert::AlertSubject;
use crosstalk_spec::aggregates::edge::{TopologyFilter, Weighting};
use crosstalk_spec::aggregates::filter::{TopicVersionSelector, UnconfirmedChannels};
use crosstalk_spec::aggregates::node::GraphNode;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::channel::confirmation::CrossTraffic;
use crosstalk_spec::derived::flow::channel::confirmation::{Confirmation, Listing, ListingKind};
use crosstalk_spec::derived::flow::channel::detection::TrafficDetection;
use crosstalk_spec::derived::flow::channel::policy::{Policy, PolicyAuthor, PolicyKind};
use crosstalk_spec::derived::flow::channel::{ChannelOrigin, DeclaredHistory};
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l8_surface::channel_traffic::ChannelTransmissionFilter;
use crosstalk_spec::interfaces::l8_surface::channels::{
    ChannelActivity, ChannelCounts, ChannelRow, ChannelShape, ChannelStanding,
};
use crosstalk_spec::interfaces::l8_surface::lists::{ChannelFilter, OriginFilter};
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, OperatorAction, OperatorActions};
use crosstalk_spec::interfaces::l8_surface::{InputError, Permission, QueryError};
use crosstalk_spec::paging::PageRequest;
use crosstalk_spec::support::{TimeWindow, Timestamp};

use super::super::FixtureBackend;
use super::super::clock::{BUCKET, START, plus};
use super::super::world::{ChannelKey, OPERATOR_RESEARCHER};
use super::{caller, collect, day, first, fresh, researcher, shared, week};
use crosstalk_spec::interfaces::l8_surface::QueryApi;

use super::actions_support::{agent, channel};

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
    assert_eq!(
        listed.len(),
        13,
        "the superseded channel and the merged-away one are not listed"
    );
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
    assert_eq!(rows(b, &with).await.len(), 14);
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
    assert_eq!(rows(b, &kinds(vec![O::Discovered])).await.len(), 7);
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
        ChannelStanding::InForce {
            traffic: CrossTraffic::NONE,
            activity: ChannelActivity::Never,
        },
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
                ChannelStanding::InForce {
                    activity: ChannelActivity::Seen { counts, .. },
                    ..
                } => {
                    assert_eq!(counts, expected, "{id:?}");
                }
                ChannelStanding::InForce {
                    activity: ChannelActivity::Never,
                    ..
                } => {
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
    // follows every later cross-agent transmission (opened or confirmed)
    // routed through either ("Detection follows resolution").
    let advanced_at = |id| {
        let record = b.world.tx(id).expect("transmission");
        super::super::world::confirmed(&record.transmission.state)
            .map_or(record.transmission.opened_at, |c| c.at())
    };
    if let TrafficDetection::Active {
        last_transmission, ..
    } = detection
    {
        assert!(advanced_at(*last_transmission) <= supersession.at);
    }
    let latest = b
        .world
        .transmissions
        .iter()
        .filter(|t| {
            super::super::world::confirmed(&t.transmission.state).is_some()
                || !super::super::world::co_accesses(&t.transmission.state).is_empty()
        })
        .filter_map(|t| {
            let at = advanced_at(t.transmission.id);
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
        latest.map(advanced_at),
        Some(advanced_at(*last_transmission))
    );
}

// What counts as a channel.

/// The merge of `al1` into `cx1`: what hides the self-notes channel.
async fn self_merge(b: &FixtureBackend) -> crosstalk_spec::ids::MergeId {
    let (al1, cx1) = (agent(b, "al1"), agent(b, "cx1"));
    b.state
        .read()
        .await
        .identity
        .merges()
        .iter()
        .find(|m| m.source() == al1 && m.target() == cx1)
        .expect("al1 was merged into cx1")
        .id()
}

#[tokio::test]
async fn a_resource_only_one_agent_uses_is_no_channel() {
    let b = shared();
    let lone = b.world.scenario.lone_resource;
    let resource = b.world.resource(lone).expect("recorded");
    assert!(matches!(
        &resource.locator,
        Locator::Opaque { key, .. } if key == "scratch/notes"
    ));
    let cc7 = agent(b, "cc7");
    let accesses: Vec<_> = b
        .world
        .accesses
        .iter()
        .filter(|a| a.resource == lone)
        .collect();
    assert!(!accesses.is_empty(), "its accesses are still recorded");
    assert!(accesses.iter().all(|a| a.agent == cc7));
    let listed = rows(
        b,
        &ChannelFilter {
            origin: OriginFilter::WithSuperseded(Vec::new()),
            ..ChannelFilter::default()
        },
    )
    .await;
    assert!(
        listed
            .iter()
            .all(|r| r.seed().is_none_or(|seed| seed.id != lone)),
        "no channel is seeded by it"
    );
    let state = b.state.read().await;
    assert!(
        state.channels.values().all(|r| r
            .channel()
            .origin
            .seed()
            .is_none_or(|s| s.resource != lone)),
        "no stored channel holds it"
    );
}

#[tokio::test]
async fn an_unconfirmed_channel_lists_its_suspected_transmissions() {
    let b = shared();
    let c = researcher();
    let s3 = channel(b, ChannelKey::S3Handoff);
    let s3_row = row(b, s3, None).await;
    assert_eq!(
        s3_row.listing(),
        Some(Listing::Channel(Confirmation::Unconfirmed))
    );
    let page = async |confirmation| {
        let filter = ChannelTransmissionFilter { confirmation };
        collect(25, async |p| {
            b.channel_transmissions(&c, s3, &filter, TopicVersionSelector::Current, &p)
                .await
                .map(|page| page.page)
        })
        .await
    };
    let suspected = page(Some(Confirmation::Unconfirmed)).await;
    assert!(!suspected.is_empty());
    let traffic = s3_row.traffic().expect("in force");
    assert_eq!(suspected.len() as u64, traffic.unconfirmed);
    for listed in &suspected {
        assert_eq!(listed.confirmation(), Confirmation::Unconfirmed);
        assert!(
            listed.senders().iter().all(|s| *s != listed.summary().to),
            "a sender is never the reader"
        );
        assert_eq!(listed.summary().route, Route::Channel(s3));
    }
    let opened: Vec<_> = suspected.iter().map(|r| r.summary().opened_at).collect();
    assert!(opened.windows(2).all(|w| w[0] >= w[1]), "newest first");
    assert!(page(Some(Confirmation::Confirmed)).await.is_empty());
    assert_eq!(page(None).await.len(), suspected.len());
    let wiki = channel(b, ChannelKey::HijackedWiki);
    let confirmed = b
        .channel_transmissions(
            &c,
            wiki,
            &ChannelTransmissionFilter {
                confirmation: Some(Confirmation::Confirmed),
            },
            TopicVersionSelector::Current,
            &first(10),
        )
        .await
        .expect("wiki");
    assert!(!confirmed.page.items().is_empty());
    assert_eq!(
        b.channel_transmissions(
            &c,
            ChannelId::from_ulid(1),
            &ChannelTransmissionFilter::default(),
            TopicVersionSelector::Current,
            &first(10),
        )
        .await
        .err(),
        Some(QueryError::NotFound)
    );
}

#[tokio::test]
async fn channel_transmissions_need_view() {
    let b = shared();
    let s3 = channel(b, ChannelKey::S3Handoff);
    assert_eq!(
        b.channel_transmissions(
            &caller(&[Permission::Content]),
            s3,
            &ChannelTransmissionFilter::default(),
            TopicVersionSelector::Current,
            &first(10),
        )
        .await
        .err(),
        Some(QueryError::Forbidden {
            missing: Permission::View
        })
    );
}

#[tokio::test]
async fn listings_split_channels_declarations_and_unconfirmed_ones() {
    let b = shared();
    let ids = |listed: Vec<ChannelRow>| {
        let mut ids: Vec<ChannelId> = listed.into_iter().map(|r| r.channel().id).collect();
        ids.sort();
        ids
    };
    let listing = |listings| ChannelFilter {
        listings,
        ..ChannelFilter::default()
    };
    let mut declared = vec![
        channel(b, ChannelKey::DesignDocs),
        channel(b, ChannelKey::ReleaseBucket),
    ];
    declared.sort();
    assert_eq!(
        ids(rows(b, &listing(vec![ListingKind::Declaration])).await),
        declared
    );
    assert_eq!(
        ids(rows(b, &listing(vec![ListingKind::Unconfirmed])).await),
        vec![channel(b, ChannelKey::S3Handoff)]
    );
    let confirmed = rows(b, &listing(vec![ListingKind::Confirmed])).await;
    assert_eq!(confirmed.len(), 10);
    assert!(
        confirmed
            .iter()
            .all(|r| r.confirmation() == Some(Confirmation::Confirmed))
    );
}

#[tokio::test]
async fn overview_queues_honour_confirmed_only() {
    let b = shared();
    let c = researcher();
    let window = week().window;
    let include = b
        .overview(&c, window, &TopologyFilter::default())
        .await
        .expect("overview")
        .value
        .queues;
    assert_eq!(include.unconfirmed_channels, Some(1), "the S3 handoff");
    let exclude = b
        .overview(
            &c,
            window,
            &TopologyFilter {
                unconfirmed_channels: UnconfirmedChannels::Exclude,
                ..TopologyFilter::default()
            },
        )
        .await
        .expect("overview")
        .value
        .queues;
    assert_eq!(exclude.unconfirmed_channels, None);
    assert_eq!(
        exclude.unreviewed_channels + 1,
        include.unreviewed_channels,
        "the unreviewed S3 handoff leaves the review queue"
    );
    assert_eq!(exclude.open_alerts, include.open_alerts);
}

#[tokio::test]
async fn alerts_on_a_hidden_channel_are_not_listed() {
    let b = fresh();
    let c = researcher();
    let notes = channel(&b, ChannelKey::SelfNotes);
    let about = AlertFilter {
        states: Vec::new(),
        channel: Some(notes),
    };
    let listed = b.alerts(&c, &about, &first(20)).await.expect("alerts");
    assert!(listed.items().is_empty(), "hidden with its channel");
    let stored = b
        .state
        .read()
        .await
        .alerts
        .iter()
        .filter(|a| a.subject == AlertSubject::Channel(notes))
        .count();
    assert_eq!(stored, 1, "the NewChannel alert is kept");
    let merge = self_merge(&b).await;
    let outcome = b.act(&c, OperatorAction::Unmerge { merge }).await;
    assert!(outcome.is_ok(), "{outcome:?}");
    let listed = b.alerts(&c, &about, &first(20)).await.expect("alerts");
    assert_eq!(listed.items().len(), 1, "an unmerge shows it again");
}

#[tokio::test]
async fn a_merge_hides_the_channel_its_agents_shared_and_an_unmerge_restores_it() {
    let b = fresh();
    let c = researcher();
    let notes = channel(&b, ChannelKey::SelfNotes);
    let window = week().window;
    let drawn = async |b: &FixtureBackend| {
        b.channel_topology(
            &c,
            window,
            Weighting::Transmissions,
            &TopologyFilter::default(),
        )
        .await
        .expect("graph")
        .value
        .nodes()
        .iter()
        .any(|n| matches!(n, GraphNode::Channel(ch) if ch.id == notes))
    };
    // Hidden: its only cross-agent traffic is al1 ↔ cx1, merged.
    let hidden = row(&b, notes, None).await;
    assert_eq!(hidden.listing(), Some(Listing::Hidden));
    assert!(hidden.traffic().is_some_and(|t| t.confirmation().is_none()));
    let listed = rows(&b, &ChannelFilter::default()).await;
    assert!(listed.iter().all(|r| r.channel().id != notes));
    assert!(!drawn(&b).await);
    let history = b
        .policy_history(&c, notes)
        .await
        .expect("history")
        .expect("kept");
    let before = b
        .overview(&c, window, &TopologyFilter::default())
        .await
        .expect("overview")
        .value
        .queues;
    // The merge's traffic counts nowhere.
    let graph = b
        .topology(
            &c,
            window,
            Weighting::Transmissions,
            &TopologyFilter::default(),
        )
        .await
        .expect("graph")
        .value;
    assert!(graph.edges.iter().all(|e| e.route != Route::Channel(notes)));
    // Unmerging lists, draws and counts it again.
    let merge = self_merge(&b).await;
    let outcome = b.act(&c, OperatorAction::Unmerge { merge }).await;
    assert!(outcome.is_ok(), "{outcome:?}");
    let back = row(&b, notes, None).await;
    assert_eq!(
        back.listing(),
        Some(Listing::Channel(Confirmation::Confirmed))
    );
    let listed = rows(&b, &ChannelFilter::default()).await;
    assert!(listed.iter().any(|r| r.channel().id == notes));
    assert!(drawn(&b).await);
    let after = b
        .overview(&c, window, &TopologyFilter::default())
        .await
        .expect("overview")
        .value
        .queues;
    assert_eq!(
        after.unreviewed_channels,
        before.unreviewed_channels + 1,
        "the unreviewed channel joins the review queue"
    );
    assert_eq!(
        b.policy_history(&c, notes).await.expect("history"),
        Some(history),
        "its record and policy history were kept"
    );
}
