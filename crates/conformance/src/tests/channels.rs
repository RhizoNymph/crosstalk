//! What counts as a channel, and its reads: rows whose listing follows
//! their cross-agent traffic, filters by listing and origin, counts that
//! are the resources' tally and the graph's, a channel's cross-agent
//! transmissions, names, policy histories, and channels a merge hides
//! until an unmerge.

use std::collections::{BTreeSet, HashSet};

use crosstalk_spec::aggregates::edge::{TopologyFilter, Weighting};
use crosstalk_spec::aggregates::filter::{TopicVersionSelector, UnconfirmedChannels};
use crosstalk_spec::aggregates::node::{CanonicalOriginKind, GraphNode};
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::channel::ChannelOrigin;
use crosstalk_spec::derived::flow::channel::confirmation::{
    Confirmation, CrossTraffic, Listing, ListingKind,
};
use crosstalk_spec::derived::flow::channel::policy::{PolicyAuthor, PolicyKind};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{ChannelId, MergeId};
use crosstalk_spec::interfaces::l8_surface::channel_traffic::{
    ChannelTransmission, ChannelTransmissionFilter,
};
use crosstalk_spec::interfaces::l8_surface::channels::{
    ChannelActivity, ChannelCounts, ChannelRow, ChannelShape, ChannelStanding,
};
use crosstalk_spec::interfaces::l8_surface::lists::{ChannelFilter, OriginFilter};
use crosstalk_spec::interfaces::l8_surface::{
    ActionOutcome, AlertFilter, OperatorAction, OperatorActions, Permission, QueryApi, QueryError,
};

use crate::harness::Harness;
use crate::scenario::named::{declared, hidden_channel, hijacked_wiki, promotion, suspected};
use crate::support::reads::{alerts, channel_row, channel_rows, counted, graph};
use crate::support::windows::quiet;
use crate::support::{World, collect, first};

fn ids(rows: &[ChannelRow]) -> Vec<ChannelId> {
    rows.iter().map(|r| r.channel().id).collect()
}

fn origins(kinds: Vec<CanonicalOriginKind>) -> ChannelFilter {
    ChannelFilter {
        origin: OriginFilter::InForce(kinds),
        ..ChannelFilter::default()
    }
}

/// The default list holds every channel in force (confirmed, unconfirmed,
/// declarations) and no hidden or superseded one, newest created first
/// (INV-1035); superseded
/// channels are listed only when asked for, by origin alone, with no
/// counts of their own (INV-858, INV-691).
pub async fn the_default_list_is_every_channel_in_force<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let listed = channel_rows(&w.backend, &w.lead, &ChannelFilter::default()).await;
    let listed_ids = ids(&listed);
    let distinct: HashSet<_> = listed_ids.iter().collect();
    assert_eq!(distinct.len(), listed_ids.len(), "each channel once");
    assert!(
        listed
            .windows(2)
            .all(|p| p[0].created_at() >= p[1].created_at()),
        "newest created first (INV-1035)"
    );
    assert!(listed.iter().all(|r| r.supersession().is_none()));
    assert!(listed.iter().all(|r| r.listing() != Some(Listing::Hidden)));
    for present in [
        w.id(hijacked_wiki::WIKI),
        w.id(suspected::S3),
        w.id(declared::UNUSED),
        w.id(declared::IN_USE),
        w.id(promotion::NOTES),
    ] {
        assert!(listed_ids.contains(&present), "{present:?} is listed");
    }
    for absent in [w.id(hidden_channel::SELF_NOTES), w.id(promotion::OLD)] {
        assert!(!listed_ids.contains(&absent), "{absent:?} is not listed");
    }
    let with = channel_rows(
        &w.backend,
        &w.lead,
        &ChannelFilter {
            origin: OriginFilter::WithSuperseded(Vec::new()),
            ..ChannelFilter::default()
        },
    )
    .await;
    let only = channel_rows(
        &w.backend,
        &w.lead,
        &ChannelFilter {
            origin: OriginFilter::Superseded,
            ..ChannelFilter::default()
        },
    )
    .await;
    let mut both: BTreeSet<_> = listed_ids.iter().copied().collect();
    both.extend(ids(&only));
    assert_eq!(ids(&with).into_iter().collect::<BTreeSet<_>>(), both);
    assert!(ids(&only).contains(&w.id(promotion::OLD)));
    for row in &only {
        let standing = row.supersession().expect("superseded");
        assert_eq!(row.counts(), None, "no counts of its own");
        assert_eq!(row.last_activity(), None);
        assert_eq!(row.listing(), None);
        let by = channel_row(&w.backend, &w.lead, standing.into(), None).await;
        assert!(
            by.supersession().is_none(),
            "it resolves to a channel in force"
        );
    }
    let promoted = channel_rows(
        &w.backend,
        &w.lead,
        &origins(vec![CanonicalOriginKind::Promoted]),
    )
    .await;
    assert!(ids(&promoted).contains(&w.id(promotion::NOTES)));
    let declared_rows = channel_rows(
        &w.backend,
        &w.lead,
        &origins(vec![CanonicalOriginKind::DeclaredBeforeTraffic]),
    )
    .await;
    assert!(ids(&declared_rows).contains(&w.id(declared::UNUSED)));
    assert!(ids(&declared_rows).contains(&w.id(declared::IN_USE)));
    let discovered = channel_rows(
        &w.backend,
        &w.lead,
        &origins(vec![CanonicalOriginKind::Discovered]),
    )
    .await;
    assert!(ids(&discovered).contains(&w.id(hijacked_wiki::WIKI)));
    assert!(
        discovered
            .iter()
            .all(|r| matches!(r.channel().origin, ChannelOrigin::Discovered { .. }))
    );
}

/// Every row's listing and confirmation follow from its origin and its
/// cross-agent traffic (INV-857).
pub async fn listings_follow_cross_agent_traffic<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let mut rows = channel_rows(&w.backend, &w.lead, &ChannelFilter::default()).await;
    rows.push(channel_row(&w.backend, &w.lead, w.id(hidden_channel::SELF_NOTES), None).await);
    for row in &rows {
        let traffic = row.traffic().expect("in force");
        let declared = matches!(row.channel().origin, ChannelOrigin::Declared { .. });
        let expected = if traffic.confirmed > 0 {
            Listing::Channel(Confirmation::Confirmed)
        } else if traffic.unconfirmed > 0 {
            Listing::Channel(Confirmation::Unconfirmed)
        } else if declared {
            Listing::Declaration
        } else {
            Listing::Hidden
        };
        assert_eq!(row.listing(), Some(expected), "{:?}", row.channel().id);
        assert_eq!(row.confirmation(), expected.confirmation());
    }
}

/// Listing kinds filter exactly: declarations, unconfirmed and confirmed
/// channels partition the default list (INV-858).
pub async fn listings_split_channels_declarations_and_unconfirmed_ones<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let of = async |kind| {
        channel_rows(
            &w.backend,
            &w.lead,
            &ChannelFilter {
                listings: vec![kind],
                ..ChannelFilter::default()
            },
        )
        .await
    };
    let declarations = of(ListingKind::Declaration).await;
    let unconfirmed = of(ListingKind::Unconfirmed).await;
    let confirmed = of(ListingKind::Confirmed).await;
    assert!(
        declarations
            .iter()
            .all(|r| r.listing() == Some(Listing::Declaration))
    );
    assert!(
        unconfirmed
            .iter()
            .all(|r| r.confirmation() == Some(Confirmation::Unconfirmed))
    );
    assert!(
        confirmed
            .iter()
            .all(|r| r.confirmation() == Some(Confirmation::Confirmed))
    );
    assert!(ids(&declarations).contains(&w.id(declared::UNUSED)));
    assert!(ids(&unconfirmed).contains(&w.id(suspected::S3)));
    assert!(ids(&confirmed).contains(&w.id(hijacked_wiki::WIKI)));
    let mut parts: Vec<_> = ids(&declarations);
    parts.extend(ids(&unconfirmed));
    parts.extend(ids(&confirmed));
    parts.sort_unstable();
    let mut all = ids(&channel_rows(&w.backend, &w.lead, &ChannelFilter::default()).await);
    all.sort_unstable();
    assert_eq!(parts, all);
}

/// A declaration nobody used is in force, with no cross-agent traffic and
/// no activity (INV-238).
pub async fn a_declaration_without_traffic_is_never_active<H: Harness>(h: &H) {
    let w = World::of(h, declared::scenario()).await;
    let row = channel_row(&w.backend, &w.lead, w.id(declared::UNUSED), None).await;
    assert_eq!(
        row.standing(),
        ChannelStanding::InForce {
            traffic: CrossTraffic::NONE,
            activity: ChannelActivity::Never,
        }
    );
}

/// A row's writers and readers are `ChannelCounts::tally` of a full
/// `channel_resources` traversal for its window, and its transmissions what
/// the default-filter graph routes through it; the overview's active
/// channels are the rows with transmissions (INV-687, INV-743).
pub async fn row_counts_are_the_resources_tally_and_the_graphs_routed_counts<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    for window in [w.day(), w.extent] {
        let filter = ChannelFilter {
            window: Some(window),
            ..ChannelFilter::default()
        };
        let topology = graph(
            &w.backend,
            &w.lead,
            window,
            Weighting::Transmissions,
            &TopologyFilter::default(),
        )
        .await;
        let routed = ChannelCounts::routed(&topology);
        let listed = channel_rows(&w.backend, &w.lead, &filter).await;
        for row in &listed {
            let id = row.channel().id;
            let uses = collect(50, async |p| {
                w.backend
                    .channel_resources(&w.lead, id, window, &p)
                    .await
                    .map(|page| page.value.page)
            })
            .await;
            let expected = ChannelCounts::tally(&uses, routed.get(&id).copied().unwrap_or(0));
            match row.standing() {
                ChannelStanding::InForce {
                    activity: ChannelActivity::Seen { counts, .. },
                    ..
                } => assert_eq!(counts, expected, "{id:?}"),
                ChannelStanding::InForce {
                    activity: ChannelActivity::Never,
                    ..
                } => assert_eq!(expected, ChannelCounts::default(), "{id:?}"),
                ChannelStanding::Superseded(_) => panic!("listed by default"),
            }
        }
        let overview = w
            .backend
            .overview(&w.lead, window, &TopologyFilter::default())
            .await
            .expect("overview")
            .value;
        let active = listed
            .iter()
            .filter(|r| r.counts().is_some_and(|c| c.transmissions > 0))
            .count();
        assert_eq!(overview.activity.active_channels, active as u64);
    }
}

/// The window counts but never filters: the same rows in any window, last
/// activity over all time, and nothing counted before any traffic
/// (INV-692).
pub async fn the_window_counts_but_never_filters<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let windowed = |window| ChannelFilter {
        window,
        ..ChannelFilter::default()
    };
    let before = quiet(w.bucket);
    assert_eq!(
        ids(&channel_rows(&w.backend, &w.lead, &windowed(None)).await),
        ids(&channel_rows(&w.backend, &w.lead, &windowed(Some(before))).await)
    );
    let wiki = w.id(hijacked_wiki::WIKI);
    let all = channel_row(&w.backend, &w.lead, wiki, None).await;
    let recent = channel_row(&w.backend, &w.lead, wiki, Some(w.day())).await;
    let empty = channel_row(&w.backend, &w.lead, wiki, Some(before)).await;
    let (all_counts, recent_counts) = (
        all.counts().expect("counts"),
        recent.counts().expect("counts"),
    );
    assert!(recent_counts.transmissions <= all_counts.transmissions);
    assert!(recent_counts.writers <= all_counts.writers);
    assert!(recent_counts.readers <= all_counts.readers);
    assert_eq!(empty.counts(), Some(ChannelCounts::default()));
    assert_eq!(recent.last_activity(), all.last_activity());
    assert_eq!(empty.last_activity(), all.last_activity());
}

/// A superseded channel's resources are its channel in force's, newest
/// resource first, with canonical writers and readers (INV-668).
pub async fn resources_page_through_the_channel_in_force<H: Harness>(h: &H) {
    let w = World::of(h, promotion::scenario()).await;
    let (old, notes) = (w.id(promotion::OLD), w.id(promotion::NOTES));
    let page = w
        .backend
        .channel_resources(&w.lead, old, w.extent, &first(1))
        .await
        .expect("resources")
        .value;
    assert_eq!(page.channel, notes, "answered for the channel in force");
    assert_eq!(page.window, w.extent);
    let uses = collect(1, async |p| {
        w.backend
            .channel_resources(&w.lead, old, w.extent, &p)
            .await
            .map(|page| page.value.page)
    })
    .await;
    let resources: Vec<_> = uses.iter().map(|u| u.resource().id).collect();
    assert!(resources.windows(2).all(|p| p[0] > p[1]), "newest first");
    assert!(resources.contains(&w.id(promotion::STANDUP)));
    assert!(resources.contains(&w.id(promotion::RETRO)));
}

/// Names come from one batch, a superseded id named by its channel in
/// force, unknown ids left out; a declared channel is named by its
/// pattern, a discovered one by its seed (INV-689).
pub async fn names_resolve_supersession_from_one_batch<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let (old, notes, wiki) = (
        w.id(promotion::OLD),
        w.id(promotion::NOTES),
        w.id(hijacked_wiki::WIKI),
    );
    let batch = IdBatch::new([old, notes, wiki, ChannelId::from_ulid(1)]).expect("batch");
    let names = w
        .backend
        .channel_names(&w.lead, &batch)
        .await
        .expect("names");
    assert_eq!(names.len(), 3, "unknown ids are left out");
    assert_eq!(names[&old].id(), notes);
    assert_eq!(names[&old], names[&notes]);
    assert_eq!(
        names[&notes].shape(),
        &ChannelShape::Pattern(promotion::pattern())
    );
    let page = w
        .scenario
        .resource(hijacked_wiki::PAGE)
        .expect("the page")
        .locator
        .clone();
    assert_eq!(names[&wiki].shape(), &ChannelShape::Seed(page));
}

/// A policy history holds every recorded decision, config's by config;
/// the channel's policy is its history's current one; a discovered channel
/// nobody reviewed has none.
pub async fn policy_histories_hold_every_decision<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let history = async |id| {
        w.backend
            .policy_history(&w.lead, id)
            .await
            .expect("read")
            .expect("history")
    };
    assert!(
        history(w.id(hijacked_wiki::WIKI))
            .await
            .entries()
            .is_empty()
    );
    let config = history(w.id(declared::IN_USE)).await;
    assert!(!config.entries().is_empty());
    assert!(
        config
            .entries()
            .iter()
            .all(|e| e.decision.by == PolicyAuthor::Config)
    );
    let notes = history(w.id(promotion::NOTES)).await;
    assert!(matches!(
        notes.latest().map(|e| (e.kind, e.decision.by)),
        Some((PolicyKind::Sanctioned, PolicyAuthor::Operator(_)))
    ));
    for row in channel_rows(&w.backend, &w.lead, &ChannelFilter::default()).await {
        let recorded = history(row.channel().id).await;
        assert_eq!(
            row.channel().policy,
            recorded.current(),
            "{:?}",
            row.channel().id
        );
    }
}

/// Every transmission of `channel` `filter` keeps, under the current
/// version.
async fn channel_transmissions<H: Harness>(
    w: &World<'_, H>,
    channel: ChannelId,
    confirmation: Option<Confirmation>,
) -> Vec<ChannelTransmission> {
    let filter = ChannelTransmissionFilter { confirmation };
    collect(25, async |p| {
        w.backend
            .channel_transmissions(&w.lead, channel, &filter, TopicVersionSelector::Current, &p)
            .await
            .map(|page| page.page)
    })
    .await
}

/// An unconfirmed channel lists its suspected transmissions for review:
/// as many as its unconfirmed traffic, routed through it, newest opened
/// first, each naming senders other than its reader (INV-868).
pub async fn an_unconfirmed_channel_lists_its_suspected_transmissions<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let s3 = w.id(suspected::S3);
    let row = channel_row(&w.backend, &w.lead, s3, None).await;
    assert_eq!(
        row.listing(),
        Some(Listing::Channel(Confirmation::Unconfirmed))
    );
    let listed = channel_transmissions(&w, s3, Some(Confirmation::Unconfirmed)).await;
    assert_eq!(
        listed.len() as u64,
        row.traffic().expect("in force").unconfirmed
    );
    assert!(
        listed
            .iter()
            .any(|t| t.summary().id == w.id(suspected::SUSPECTED))
    );
    for t in &listed {
        assert_eq!(t.confirmation(), Confirmation::Unconfirmed);
        assert!(
            t.senders().iter().all(|s| *s != t.summary().to),
            "a sender is never the reader"
        );
        assert_eq!(t.summary().route, Route::Channel(s3));
    }
    let opened: Vec<_> = listed.iter().map(|t| t.summary().opened_at).collect();
    assert!(opened.windows(2).all(|p| p[0] >= p[1]), "newest first");
    assert!(
        channel_transmissions(&w, s3, Some(Confirmation::Confirmed))
            .await
            .is_empty()
    );
    assert_eq!(
        channel_transmissions(&w, s3, None).await.len(),
        listed.len()
    );
    let wiki = w.id(hijacked_wiki::WIKI);
    let confirmed = channel_transmissions(&w, wiki, Some(Confirmation::Confirmed)).await;
    assert_eq!(
        confirmed.len() as u64,
        channel_row(&w.backend, &w.lead, wiki, None)
            .await
            .traffic()
            .expect("in force")
            .confirmed
    );
}

/// A transmission whose agents merged into one is not a channel
/// transmission: the hidden channel lists none (INV-868).
pub async fn channel_transmissions_are_cross_agent_only<H: Harness>(h: &H) {
    let w = World::of(h, hidden_channel::scenario()).await;
    let listed = channel_transmissions(&w, w.id(hidden_channel::SELF_NOTES), None).await;
    assert!(
        listed
            .iter()
            .all(|t| t.summary().id != w.id(hidden_channel::BETWEEN))
    );
    assert!(
        listed.is_empty(),
        "every transmission through it is within one agent"
    );
}

/// A channel's transmissions need View (INV-869).
pub async fn channel_transmissions_need_view<H: Harness>(h: &H) {
    let w = World::of(h, suspected::scenario()).await;
    assert_eq!(
        w.backend
            .channel_transmissions(
                &w.caller(&[Permission::Content]),
                w.id(suspected::S3),
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

/// "Confirmed only" changes no transmission view: the graph, the
/// overview's activity and the transmissions they count are the same
/// (INV-863).
pub async fn confirmed_only_changes_no_transmission_view<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let exclude = TopologyFilter {
        unconfirmed_channels: UnconfirmedChannels::Exclude,
        ..TopologyFilter::default()
    };
    let include = TopologyFilter::default();
    for weighting in [Weighting::Transmissions, Weighting::MatchedBytes] {
        assert_eq!(
            graph(&w.backend, &w.lead, w.extent, weighting, &include).await,
            graph(&w.backend, &w.lead, w.extent, weighting, &exclude).await
        );
    }
    let activity = async |f: &TopologyFilter| {
        w.backend
            .overview(&w.lead, w.extent, f)
            .await
            .expect("overview")
            .value
            .activity
    };
    assert_eq!(activity(&include).await, activity(&exclude).await);
    assert_eq!(
        counted(&w.backend, &w.lead, w.day(), &include).await,
        counted(&w.backend, &w.lead, w.day(), &exclude).await
    );
}

/// The standing merge that hides the scenario's channel.
fn hiding_merge<H: Harness>(w: &World<'_, H>) -> MergeId {
    w.id(hidden_channel::MERGE)
}

async fn drawn<H: Harness>(w: &World<'_, H>, channel: ChannelId) -> bool {
    w.backend
        .channel_topology(
            &w.lead,
            w.extent,
            Weighting::Transmissions,
            &TopologyFilter::default(),
        )
        .await
        .expect("graph")
        .value
        .nodes()
        .iter()
        .any(|n| matches!(n, GraphNode::Channel(c) if c.id == channel))
}

/// A discovered channel whose traffic a merge left within one agent is
/// hidden from lists, the channel graph and counts while its row and
/// policy history still answer; unmerging lists, draws and counts it
/// again, and merging again restores exactly the earlier lists (INV-859).
pub async fn a_merge_hides_the_channel_and_an_unmerge_restores_it<H: Harness>(h: &H) {
    let w = World::of(h, hidden_channel::scenario()).await;
    let notes = w.id(hidden_channel::SELF_NOTES);
    let hidden = channel_row(&w.backend, &w.lead, notes, None).await;
    assert_eq!(hidden.listing(), Some(Listing::Hidden));
    assert!(hidden.traffic().is_some_and(|t| t.confirmation().is_none()));
    let listed = ids(&channel_rows(&w.backend, &w.lead, &ChannelFilter::default()).await);
    assert!(!listed.contains(&notes));
    assert!(!drawn(&w, notes).await);
    let history = w
        .backend
        .policy_history(&w.lead, notes)
        .await
        .expect("history");
    assert!(history.is_some(), "its history is kept");
    let queues = async || {
        w.backend
            .overview(&w.lead, w.extent, &TopologyFilter::default())
            .await
            .expect("overview")
            .value
            .queues
    };
    let before = queues().await;
    let topology = graph(
        &w.backend,
        &w.lead,
        w.extent,
        Weighting::Transmissions,
        &TopologyFilter::default(),
    )
    .await;
    assert!(
        topology
            .edges()
            .iter()
            .all(|e| e.route != Route::Channel(notes))
    );

    let outcome = w
        .backend
        .act(
            &w.lead,
            OperatorAction::Unmerge {
                merge: hiding_merge(&w),
            },
        )
        .await;
    assert_eq!(outcome, Ok(ActionOutcome::Applied));
    let back = channel_row(&w.backend, &w.lead, notes, None).await;
    assert!(
        matches!(back.listing(), Some(Listing::Channel(_))),
        "{:?}",
        back.listing()
    );
    let relisted = ids(&channel_rows(&w.backend, &w.lead, &ChannelFilter::default()).await);
    assert!(relisted.contains(&notes));
    assert!(drawn(&w, notes).await);
    let after = queues().await;
    let unreviewed = u64::from(back.channel().policy.kind() == PolicyKind::Unreviewed);
    assert_eq!(
        after.unreviewed_channels,
        before.unreviewed_channels + unreviewed
    );
    assert_eq!(
        w.backend
            .policy_history(&w.lead, notes)
            .await
            .expect("history"),
        history
    );

    let again = OperatorAction::merge_agents(
        &w.lead,
        w.id(hidden_channel::OTHER_ID),
        w.id(hidden_channel::OWNER),
    )
    .expect("two agents");
    assert!(matches!(
        w.backend.act(&w.lead, again).await,
        Ok(ActionOutcome::Merged(_))
    ));
    assert_eq!(
        ids(&channel_rows(&w.backend, &w.lead, &ChannelFilter::default()).await),
        listed
    );
    assert_eq!(
        channel_row(&w.backend, &w.lead, notes, None)
            .await
            .listing(),
        Some(Listing::Hidden)
    );
}

/// Alerts about a hidden channel are not listed, though stored: an unmerge
/// shows them again, and `alert` reads one by id either way (INV-867).
pub async fn alerts_on_a_hidden_channel_are_not_listed<H: Harness>(h: &H) {
    let w = World::of(h, hidden_channel::scenario()).await;
    let notes = w.id(hidden_channel::SELF_NOTES);
    let about = AlertFilter {
        states: Vec::new(),
        channel: Some(notes),
    };
    assert!(
        alerts(&w.backend, &w.lead, &about).await.is_empty(),
        "hidden with its channel"
    );
    w.backend
        .act(
            &w.lead,
            OperatorAction::Unmerge {
                merge: hiding_merge(&w),
            },
        )
        .await
        .expect("unmerge");
    let shown = alerts(&w.backend, &w.lead, &about).await;
    assert!(
        !shown.is_empty(),
        "discovery raised an alert about it (INV-854)"
    );
    let again = OperatorAction::merge_agents(
        &w.lead,
        w.id(hidden_channel::OTHER_ID),
        w.id(hidden_channel::OWNER),
    )
    .expect("two agents");
    w.backend.act(&w.lead, again).await.expect("merge");
    assert!(alerts(&w.backend, &w.lead, &about).await.is_empty());
    for alert in &shown {
        let read = w.backend.alert(&w.lead, alert.id).await.expect("read");
        assert_eq!(read.map(|a| a.id), Some(alert.id), "still readable by id");
    }
}

/// Every channel the scenarios discover has a new-channel alert, and no
/// channel lists a resource of a channel it does not hold (INV-854,
/// INV-852).
pub async fn discovered_channels_raised_an_alert_and_hold_their_resources<H: Harness>(h: &H) {
    use crosstalk_spec::aggregates::alert::{AlertSubject, BuiltinRule};
    let w = World::everything(h).await;
    for channel in [w.id(hijacked_wiki::WIKI), w.id(suspected::S3)] {
        let raised = alerts(
            &w.backend,
            &w.lead,
            &AlertFilter {
                states: Vec::new(),
                channel: Some(channel),
            },
        )
        .await;
        assert!(
            raised
                .iter()
                .any(|a| a.subject == AlertSubject::Channel(channel)
                    && a.rule == BuiltinRule::NewChannel.id()),
            "{channel:?} raised NewChannel"
        );
    }
    let mut owners: std::collections::HashMap<_, ChannelId> = std::collections::HashMap::new();
    for row in channel_rows(&w.backend, &w.lead, &ChannelFilter::default()).await {
        let id = row.channel().id;
        let uses = collect(50, async |p| {
            w.backend
                .channel_resources(&w.lead, id, w.extent, &p)
                .await
                .map(|page| page.value.page)
        })
        .await;
        let resources: HashSet<_> = uses.iter().map(|u| u.resource().id).collect();
        for resource in resources {
            if let Some(other) = owners.insert(resource, id) {
                panic!("{resource:?} is on {other:?} and {id:?}");
            }
        }
    }
}

/// Rows by id leave out a transmission whose agents resolve to one, and
/// an unmerge lists it again (INV-1036).
pub async fn rows_by_id_leave_out_transmissions_within_one_agent<H: Harness>(h: &H) {
    let w = World::of(h, hidden_channel::scenario()).await;
    let between = w.id(hidden_channel::BETWEEN);
    assert!(
        crate::support::reads::row(&w.backend, &w.lead, between)
            .await
            .is_none(),
        "within one agent while the merge stands"
    );
    w.backend
        .act(
            &w.lead,
            OperatorAction::Unmerge {
                merge: hiding_merge(&w),
            },
        )
        .await
        .expect("unmerge");
    let listed = crate::support::reads::row(&w.backend, &w.lead, between).await;
    assert_eq!(
        listed.map(|r| r.id),
        Some(between),
        "listed again after the unmerge"
    );
}
