//! List, detail and content reads: pagination, permissions, topics,
//! projections, list filters and determinism. Transmission reads are in
//! `transmissions`.

use std::collections::HashSet;

use crosstalk_spec::aggregates::edge::Weighting;
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::interfaces::l8_surface::excerpt::ExcerptWindow;
use crosstalk_spec::interfaces::l8_surface::lists::SearchMode;
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionSelection;
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, AlertStateKind, Permission};

use super::super::clock::{DAY, ago};
use super::super::queries::{self, Ctx};
use super::super::world::ChannelKey;
use super::{caller, collect, day, first, graph_of, researcher, shared, week, window};
use crosstalk_spec::aggregates::node::CanonicalOriginKind;
use crosstalk_spec::interfaces::l8_surface::QueryApi;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::interfaces::l8_surface::audit::{
    AuditAuthor, AuditBody, AuditFilter, AuditSubject,
};
use crosstalk_spec::interfaces::l8_surface::lists::{ChannelFilter, OriginFilter};
use crosstalk_spec::observed::agent::AgentState;

use super::reads_support::*;

#[tokio::test]
async fn pagination_covers_every_item_exactly_once() {
    let b = shared();
    let c = researcher();
    let scope = week();
    let everything = all_transmissions(&scope).await;
    let every_id = TransmissionSelection::new(
        b.world
            .transmissions
            .iter()
            .map(|t| t.transmission.id)
            .collect(),
    )
    .expect("selection");
    let v2 = TopicVersionSelector::Pinned(TopicModelVersion(2));
    let paged = collect(97, async |p| {
        b.transmissions_by_id(&c, &every_id, v2, &p)
            .await
            .map(|rows| rows.page)
    })
    .await;
    assert_eq!(paged, everything);
    assert!(
        paged.windows(2).all(|w| w[0].id > w[1].id),
        "newest id first"
    );
    let ids: HashSet<_> = paged.iter().map(|t| t.id).collect();
    assert_eq!(ids.len(), paged.len());

    let agents = collect(7, async |p| {
        b.agents(&c, &Default::default(), scope.window, &p)
            .await
            .map(|rows| rows.value)
    })
    .await;
    assert_eq!(agents.len(), 40);
    assert!(
        agents
            .windows(2)
            .all(|w| w[0].profile.id() > w[1].profile.id()),
        "newest agent first"
    );
    let alerts = collect(50, async |p| {
        b.alerts(&c, &AlertFilter::default(), &p).await
    })
    .await;
    let shown = {
        let state = b.state.read().await;
        let ctx = Ctx::new(&b.world, &state);
        state
            .alerts
            .iter()
            .filter(|alert| queries::alerts::shown(&ctx, alert))
            .count()
    };
    assert_eq!(alerts.len(), shown);
    let audit = collect(33, async |p| b.audit(&c, &AuditFilter::default(), &p).await).await;
    assert_eq!(audit.len(), b.state.read().await.audit.entries().len());
    let filter = ChannelFilter {
        origin: OriginFilter::WithSuperseded(Vec::new()),
        ..Default::default()
    };
    let channels = collect(4, async |p| {
        b.channels(&c, &filter, &p).await.map(|rows| rows.value)
    })
    .await;
    assert_eq!(
        channels.len(),
        14,
        "every stored channel but the hidden one"
    );
    let request = search("the", SearchMode::Text);
    let hits = collect(400, async |p| search_in(b, &c, &request, &scope, &p).await).await;
    let unique: HashSet<_> = hits.iter().map(|h| h.transmission).collect();
    assert_eq!(unique.len(), hits.len());
    assert!(
        hits.windows(2)
            .all(|w| w[0].score.get() >= w[1].score.get())
    );
    let letters = collect(1, async |p| b.dead_letters(&c, None, &p).await).await;
    assert_eq!(letters.len(), b.state.read().await.dead_letters.len());
}

#[tokio::test]
async fn content_needs_the_content_permission() {
    let b = shared();
    let view = caller(&[Permission::View]);
    let forbidden = Some(QueryError::Forbidden {
        missing: Permission::Content,
    });
    let tx = b
        .world
        .transmissions
        .iter()
        .find(|t| t.is_confirmed())
        .expect("confirmed")
        .transmission
        .id;
    assert_eq!(b.transmission(&view, tx).await.err(), forbidden);
    assert_eq!(
        b.transmission_evidence(&view, tx, ExcerptWindow::DEFAULT)
            .await
            .err(),
        forbidden
    );
    assert_eq!(
        search_in(
            b,
            &view,
            &search("deploy", SearchMode::Text),
            &week(),
            &first(5)
        )
        .await
        .err(),
        forbidden
    );
    assert_eq!(
        b.topics(&view, TopicVersionSelector::Current, &first(5))
            .await
            .err(),
        forbidden
    );
    let scope = week();
    assert_eq!(
        b.fit_projection(&view, scope.window, &scope.topology_filter(), params(1, 5))
            .await
            .err(),
        forbidden
    );
    let any = crosstalk_spec::ids::ProjectionId::from_ulid(5);
    assert_eq!(b.projection_status(&view, any).await.err(), forbidden);
    assert_eq!(b.projections(&view, &first(5)).await.err(), forbidden);
    assert_eq!(b.projection(&view, any).await.err(), forbidden);
    // The topic history, sizes and lineage hold no content.
    assert!(b.topic_versions(&view).await.is_ok());
    assert!(
        b.topic_sizes(&view, None, Some(week().window))
            .await
            .is_ok()
    );
    assert!(b.topic_lineage(&view, TopicModelVersion(1)).await.is_ok());
    // Structure is still visible.
    assert!(
        graph_of(b, &view, &week(), Weighting::Transmissions)
            .await
            .is_ok()
    );
    let one = TransmissionSelection::new(vec![tx]).expect("selection");
    assert!(
        b.transmissions_by_id(&view, &one, TopicVersionSelector::Current, &first(5))
            .await
            .is_ok()
    );
    assert!(b.verdicts(&view, tx).await.is_ok(), "verdicts need View");
    assert_eq!(
        b.dead_letters(&view, None, &first(5)).await.err(),
        Some(QueryError::Forbidden {
            missing: Permission::Operate
        })
    );
    let content_only = caller(&[Permission::Content]);
    assert_eq!(
        graph_of(b, &content_only, &week(), Weighting::Transmissions)
            .await
            .err(),
        Some(QueryError::Forbidden {
            missing: Permission::View
        })
    );
    assert!(b.transmission(&content_only, tx).await.is_ok());
    assert_eq!(
        b.transmissions_by_id(
            &content_only,
            &one,
            TopicVersionSelector::Current,
            &first(5)
        )
        .await
        .err(),
        Some(QueryError::Forbidden {
            missing: Permission::View
        })
    );
}

#[tokio::test]
async fn channel_list_filters() {
    let b = shared();
    let c = researcher();
    let list = async |filter: ChannelFilter| {
        collect(50, async |p| {
            b.channels(&c, &filter, &p).await.map(|rows| rows.value)
        })
        .await
    };
    let declared = ChannelFilter {
        origin: OriginFilter::InForce(vec![
            CanonicalOriginKind::DeclaredBeforeTraffic,
            CanonicalOriginKind::Promoted,
        ]),
        ..Default::default()
    };
    assert_eq!(list(declared).await.len(), 6);
    let visible = list(ChannelFilter::default()).await;
    assert_eq!(
        visible.len(),
        13,
        "the superseded channel and the merged-away one are not listed by default"
    );
    let old = channel(ChannelKey::OldTeamNotes);
    assert!(visible.iter().all(|r| r.channel().id != old));
    let unreviewed = ChannelFilter {
        policies: vec![crosstalk_spec::interfaces::l8_surface::PolicyKind::Unreviewed],
        ..Default::default()
    };
    let queue = list(unreviewed).await;
    assert!(
        queue
            .iter()
            .any(|r| r.channel().id == channel(ChannelKey::HijackedWiki))
    );
    let wiki = b
        .channel(&c, channel(ChannelKey::HijackedWiki), None)
        .await
        .expect("ok")
        .expect("wiki")
        .value;
    let counts = wiki.counts().expect("in force and active");
    assert!(
        wiki.seed().is_some()
            && counts.writers > 0
            && counts.readers > 0
            && counts.transmissions > 0
    );
    // The promoted channel holds the superseded channel's resources.
    let notes = b
        .channel_resources(
            &c,
            channel(ChannelKey::TeamNotes),
            week().window,
            &first(50),
        )
        .await
        .expect("resources")
        .value
        .page;
    assert!(notes.items().iter().any(|u| matches!(&u.resource().locator,
        crosstalk_spec::derived::flow::resource::Locator::Url { path, .. } if path == "/team-a/standup")));
}

#[tokio::test]
async fn agent_detail_resolves_aliases() {
    let b = shared();
    let c = researcher();
    let read = async |key: &str| {
        b.agent(&c, agent(key), week().window)
            .await
            .expect("ok")
            .expect("agent")
            .value
            .cluster
    };
    let detail = read("al0").await;
    assert_eq!(detail.agent().id, agent("cc0"));
    assert_eq!(
        detail.lookup(),
        crosstalk_spec::aggregates::agents::AgentLookup::Redirected { from: agent("al0") }
    );
    assert!(detail.aliases().iter().any(|a| a.id == agent("al0")));
    assert!(detail.children().contains(&agent("cc0.a")));
    assert!(!detail.merges().is_empty());
    let pi2 = read("al2").await;
    assert_eq!(pi2.agent().id, agent("pi2"));
    assert_eq!(
        pi2.aliases().len(),
        2,
        "both chained aliases resolve to pi2"
    );
    let omp3 = read("omp3").await;
    assert!(!omp3.vetoes().is_empty());
    assert!(matches!(omp3.agent().state, AgentState::Provisional { .. }));
    assert!(omp3.merges().iter().any(|m| m.reverted().is_some()));
    let impersonator = read("pi0").await;
    assert!(impersonator.profile().claims().entries().len() >= 2);
}

#[tokio::test]
async fn alert_filter_by_state_and_channel() {
    let b = shared();
    let c = researcher();
    let open = AlertFilter {
        states: vec![AlertStateKind::Open],
        channel: None,
    };
    let rows = collect(100, async |p| b.alerts(&c, &open, &p).await).await;
    assert!(!rows.is_empty());
    assert!(
        rows.iter()
            .all(|a| a.state == crosstalk_spec::aggregates::alert::AlertState::Open)
    );
    let wiki = channel(ChannelKey::HijackedWiki);
    let about = AlertFilter {
        states: Vec::new(),
        channel: Some(wiki),
    };
    let rows = collect(100, async |p| b.alerts(&c, &about, &p).await).await;
    assert!(
        rows.iter()
            .any(|a| a.subject == crosstalk_spec::aggregates::alert::AlertSubject::Channel(wiki))
    );
    assert!(rows.iter().any(|a| matches!(
        a.subject,
        crosstalk_spec::aggregates::alert::AlertSubject::Transmission(_)
    )));
    // The superseded channel's alerts show under the declared channel.
    let notes = AlertFilter {
        states: Vec::new(),
        channel: Some(channel(ChannelKey::TeamNotes)),
    };
    let rows = collect(100, async |p| b.alerts(&c, &notes, &p).await).await;
    let old = channel(ChannelKey::OldTeamNotes);
    assert!(
        rows.iter()
            .any(|a| a.subject == crosstalk_spec::aggregates::alert::AlertSubject::Channel(old))
    );
}

#[tokio::test]
async fn audit_filter_by_operator_subject_and_window() {
    let b = shared();
    let c = researcher();
    let oncall = super::super::world::OPERATOR_ONCALL;
    let mine = AuditFilter {
        by: vec![AuditAuthor::Operator(oncall)],
        ..Default::default()
    };
    let rows = collect(100, async |p| b.audit(&c, &mine, &p).await).await;
    assert!(!rows.is_empty());
    assert!(rows.iter().all(|e| e.by() == AuditAuthor::Operator(oncall)));
    let pastebin = channel(ChannelKey::Pastebin);
    let about = AuditFilter {
        subject: Some(AuditSubject::Channel(pastebin)),
        ..Default::default()
    };
    let rows = collect(100, async |p| b.audit(&c, &about, &p).await).await;
    assert!(rows.iter().any(|e| matches!(&e.body,
        AuditBody::Operator(record) if matches!(record.action(),
            crosstalk_spec::interfaces::l8_surface::OperatorAction::SetPolicy { channel, .. } if *channel == pastebin))));
    assert!(
        rows.iter()
            .all(|e| e.subjects().contains(&AuditSubject::Channel(pastebin))),
        "the filter matches the entry's subjects exactly"
    );
    let recent = AuditFilter {
        window: Some(window(ago(DAY))),
        ..Default::default()
    };
    let rows = collect(100, async |p| b.audit(&c, &recent, &p).await).await;
    assert!(rows.iter().all(|e| e.at >= ago(DAY)));
}

#[tokio::test]
async fn same_seed_same_answers() {
    let (a, b) = (super::fresh(), super::fresh());
    let c = researcher();
    assert_eq!(
        graph_of(&a, &c, &day(), Weighting::MatchedBytes).await,
        graph_of(&b, &c, &day(), Weighting::MatchedBytes).await
    );
    assert_eq!(
        search_in(
            &a,
            &c,
            &search("agents", SearchMode::Hybrid),
            &week(),
            &first(50)
        )
        .await,
        search_in(
            &b,
            &c,
            &search("agents", SearchMode::Hybrid),
            &week(),
            &first(50)
        )
        .await
    );
    assert_eq!(
        a.channel_topology(
            &c,
            week().window,
            Weighting::Transmissions,
            &week().topology_filter()
        )
        .await,
        b.channel_topology(
            &c,
            week().window,
            Weighting::Transmissions,
            &week().topology_filter()
        )
        .await
    );
    assert_eq!(
        a.agents(&c, &Default::default(), week().window, &first(100))
            .await,
        b.agents(&c, &Default::default(), week().window, &first(100))
            .await
    );
}

#[tokio::test]
async fn agent_names_resolve_aliases_in_one_call() {
    use crosstalk_spec::batch::IdBatch;
    use crosstalk_spec::ids::AgentId;

    let b = shared();
    let c = researcher();
    let (alias, plain) = (agent("al0"), agent("cc1"));
    let unknown = AgentId::from_ulid(1);
    let batch = IdBatch::new([alias, plain, unknown]).expect("batch");
    let names = b.agent_names(&c, &batch).await.expect("names");
    assert_eq!(names.len(), 2, "unknown ids are left out");
    let canonical = b
        .agent(&c, alias, week().window)
        .await
        .expect("read")
        .expect("agent")
        .value;
    let profile = canonical.cluster.profile();
    assert_eq!(names[&alias].id, profile.id());
    assert_ne!(
        names[&alias].id, alias,
        "an alias is named by its canonical agent"
    );
    assert_eq!(names[&alias].label.as_ref(), profile.label());
    assert_eq!(names[&plain].id, plain);

    let nobody = caller(&[Permission::Audit]);
    assert_eq!(
        b.agent_names(&nobody, &IdBatch::new([plain]).expect("batch"))
            .await
            .err(),
        Some(QueryError::Forbidden {
            missing: Permission::View
        })
    );
}

#[tokio::test]
async fn one_alert_reads_by_id() {
    use crosstalk_spec::ids::AlertId;

    let b = shared();
    let c = researcher();
    let listed = b
        .alerts(&c, &AlertFilter::default(), &first(1))
        .await
        .expect("alerts")
        .into_parts()
        .0
        .remove(0);
    assert_eq!(
        b.alert(&c, listed.id).await.expect("read"),
        Some(listed.clone())
    );
    assert_eq!(b.alert(&c, AlertId::from_ulid(1)).await, Ok(None));
    assert_eq!(
        b.alert(&caller(&[Permission::Audit]), listed.id)
            .await
            .err(),
        Some(QueryError::Forbidden {
            missing: Permission::View
        })
    );
}

#[tokio::test]
async fn agents_filter_by_state_claims_text_and_parent() {
    use crosstalk_spec::aggregates::agents::AgentRow;
    use crosstalk_spec::aggregates::agents::filter::{AgentFilter, AgentText};
    use crosstalk_spec::aggregates::node::CanonicalStateKind;
    use crosstalk_spec::observed::client::HarnessFamily;

    let b = shared();
    let c = researcher();
    let list = async |filter: AgentFilter| -> Vec<AgentRow> {
        b.agents(&c, &filter, week().window, &first(BIG))
            .await
            .expect("agents")
            .value
            .into_parts()
            .0
    };
    let all = list(AgentFilter::default()).await;
    let registered = list(AgentFilter {
        states: vec![CanonicalStateKind::Registered],
        ..AgentFilter::default()
    })
    .await;
    assert_eq!(registered.len(), 3, "three config-registered agents");
    assert!(registered.iter().all(|a| {
        a.profile.state_kind() == CanonicalStateKind::Registered
            && a.profile.last_seen().is_none()
            && a.traffic == Default::default()
    }));
    let claude = list(AgentFilter {
        claimed: vec![HarnessFamily::ClaudeCode],
        ..AgentFilter::default()
    })
    .await;
    assert!(!claude.is_empty() && claude.len() < all.len());
    assert!(claude.iter().all(|a| {
        a.profile
            .claims()
            .entries()
            .iter()
            .any(|s| s.claim.family == HarnessFamily::ClaudeCode)
    }));
    let scraper = list(AgentFilter {
        text: AgentText::new("PI-SCRAPER").ok(),
        ..AgentFilter::default()
    })
    .await;
    assert_eq!(scraper.len(), 1);
    assert_eq!(
        scraper[0].profile.label().map(|l| l.as_str()),
        Some("pi-scraper")
    );
    // An alias's id still finds the agent it was merged into.
    let by_alias = list(AgentFilter {
        text: AgentText::new(&agent("al0").ulid_text()).ok(),
        ..AgentFilter::default()
    })
    .await;
    assert_eq!(
        by_alias.iter().map(|a| a.profile.id()).collect::<Vec<_>>(),
        [agent("cc0")]
    );
    let parent = all
        .iter()
        .find_map(|a| a.profile.parent())
        .expect("some agent has a parent");
    let children = list(AgentFilter {
        parents: vec![parent],
        ..AgentFilter::default()
    })
    .await;
    let detail = b
        .agent(&c, parent, week().window)
        .await
        .expect("read")
        .expect("agent")
        .value;
    let mut listed: Vec<_> = children.iter().map(|a| a.profile.id()).collect();
    listed.sort_unstable();
    assert_eq!(
        listed,
        detail.cluster.children(),
        "one level of the tree, the detail's children"
    );
}
