//! List, detail and content reads: pagination, permissions, selectors,
//! evidence, search, topics, projections and determinism.

use std::collections::HashSet;

use crosstalk_spec::aggregates::edge::Weighting;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, AlertStateKind, Permission};

use super::super::clock::{DAY, ago};
use super::super::world::ChannelKey;
use super::{caller, collect, day, first, researcher, shared, week, window};
use crate::contract::agents::AgentState;
use crate::backend::Backend;
use crate::contract::channels::{ChannelListFilter, OriginKind};
use crate::contract::errors::QueryError;
use crate::contract::graph::{TransmissionSelector, TransmissionStateKind};
use crate::contract::research::{AuditFilter, AuditSubject};
use crate::contract::scope::Scope;
use crate::contract::search::SearchMode;

use super::reads_support::*;

#[tokio::test]
async fn pagination_covers_every_item_exactly_once() {
    let b = shared();
    let c = researcher();
    let scope = week();
    let everything = all_transmissions(&scope).await;
    let paged = collect(97, async |p| {
        b.transmissions(&c, &scope, &TransmissionSelector::All, &p)
            .await
    })
    .await;
    assert_eq!(paged, everything);
    assert!(
        paged.windows(2).all(|w| w[0].opened_at >= w[1].opened_at),
        "newest first"
    );
    let ids: HashSet<_> = paged.iter().map(|t| t.id).collect();
    assert_eq!(ids.len(), paged.len());

    let agents = collect(7, async |p| b.agents(&c, &p).await).await;
    assert_eq!(agents.len(), 40);
    let alerts = collect(50, async |p| {
        b.alerts(&c, &AlertFilter::default(), &p).await
    })
    .await;
    assert_eq!(alerts.len(), b.state.read().await.alerts.len());
    let audit = collect(33, async |p| b.audit(&c, &AuditFilter::default(), &p).await).await;
    assert_eq!(audit.len(), b.state.read().await.audit.len());
    let filter = ChannelListFilter {
        include_superseded: true,
        ..Default::default()
    };
    let channels = collect(4, async |p| b.channels(&c, &filter, &p).await).await;
    assert_eq!(channels.len(), 15);
    let request = search("the", SearchMode::Text);
    let hits = collect(400, async |p| b.search(&c, &request, &scope, &p).await).await;
    let unique: HashSet<_> = hits.iter().map(|h| h.transmission).collect();
    assert_eq!(unique.len(), hits.len());
    assert!(
        hits.windows(2)
            .all(|w| w[0].score.get() >= w[1].score.get())
    );
    let letters = collect(1, async |p| b.dead_letters(&c, &p).await).await;
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
        b.search(
            &view,
            &search("deploy", SearchMode::Text),
            &week(),
            &first(5)
        )
        .await
        .err(),
        forbidden
    );
    assert_eq!(b.topic_versions(&view).await.err(), forbidden);
    assert_eq!(b.topics(&view, TopicModelVersion(2)).await.err(), forbidden);
    assert_eq!(b.topic_stats(&view, &week(), n(3)).await.err(), forbidden);
    assert_eq!(
        b.topic_remap(&view, TopicModelVersion(1)).await.err(),
        forbidden
    );
    assert_eq!(
        b.fit_projection(&view, &week(), params(1, 5)).await.err(),
        forbidden
    );
    // Structure is still visible.
    assert!(
        b.topology(&view, &week(), Weighting::Transmissions)
            .await
            .is_ok()
    );
    assert!(
        b.transmissions(&view, &week(), &TransmissionSelector::All, &first(5))
            .await
            .is_ok()
    );
    assert_eq!(
        b.dead_letters(&view, &first(5)).await.err(),
        Some(QueryError::Forbidden {
            missing: Permission::Operate
        })
    );
    let content_only = caller(&[Permission::Content]);
    assert_eq!(
        b.topology(&content_only, &week(), Weighting::Transmissions)
            .await
            .err(),
        Some(QueryError::Forbidden {
            missing: Permission::View
        })
    );
    assert!(b.transmission(&content_only, tx).await.is_ok());
}

#[tokio::test]
async fn edge_and_id_selectors() {
    let b = shared();
    let c = researcher();
    let scope = week();
    let view = b
        .topology(&c, &scope, Weighting::Transmissions)
        .await
        .expect("topology");
    let edge = view
        .graph()
        .edges
        .iter()
        .max_by_key(|e| e.stats.transmissions)
        .expect("edge");
    let selector = TransmissionSelector::Edge {
        from: edge.from,
        to: edge.to,
        route: edge.route.clone(),
    };
    let rows = b
        .transmissions(&c, &scope, &selector, &first(BIG))
        .await
        .expect("edge rows")
        .items;
    assert_eq!(rows.len() as u64, edge.stats.transmissions.get());
    assert!(
        rows.iter()
            .all(|t| t.from == Some(edge.from) && t.to == edge.to && t.route == edge.route)
    );
    let ids: Vec<_> = rows.iter().take(5).map(|t| t.id).collect();
    let picked = b
        .transmissions(
            &c,
            &scope,
            &TransmissionSelector::Ids(ids.clone()),
            &first(BIG),
        )
        .await
        .expect("ids")
        .items;
    let got: HashSet<_> = picked.iter().map(|t| t.id).collect();
    assert_eq!(got, ids.into_iter().collect());
}

#[tokio::test]
async fn evidence_carries_excerpts_accesses_and_verdicts() {
    let b = shared();
    let c = researcher();
    let record = b
        .world
        .transmissions
        .iter()
        .find(|t| t.is_confirmed() && matches!(t.transmission.route, Route::Channel(_)))
        .expect("channel transmission");
    let evidence = b
        .transmission(&c, record.transmission.id)
        .await
        .expect("ok")
        .expect("found");
    assert!(!evidence.matches.is_empty());
    for m in &evidence.matches {
        assert!(!m.origin.matched().is_empty() && !m.read.matched().is_empty());
    }
    assert_eq!(evidence.accesses.len(), 2, "the write and the read");
    let judged = b
        .state
        .read()
        .await
        .verdicts
        .first()
        .expect("verdict")
        .transmission;
    let judged = b
        .transmission(&c, judged)
        .await
        .expect("ok")
        .expect("found");
    assert!(!judged.verdicts.is_empty());
    let unknown = crosstalk_spec::ids::TransmissionId::from_ulid(1);
    assert_eq!(b.transmission(&c, unknown).await, Ok(None));
}

#[tokio::test]
async fn text_search_is_a_case_insensitive_substring() {
    let b = shared();
    let c = researcher();
    let lower = collect(100, async |p| {
        b.search(&c, &search("rollback", SearchMode::Text), &week(), &p)
            .await
    })
    .await;
    let upper = collect(100, async |p| {
        b.search(&c, &search("ROLLBACK", SearchMode::Text), &week(), &p)
            .await
    })
    .await;
    assert!(!lower.is_empty());
    assert_eq!(lower, upper);
    for hit in &lower {
        let record = b.world.tx(hit.transmission).expect("record");
        let found = record.texts.iter().any(|t| {
            [&t.origin, &t.read].iter().any(|e| {
                format!("{}{}{}", e.before(), e.matched(), e.after())
                    .to_lowercase()
                    .contains("rollback")
            })
        });
        assert!(found);
        assert!(
            hit.snippet.to_lowercase().contains("rollback"),
            "{}",
            hit.snippet
        );
    }
    let semantic = b
        .search(
            &c,
            &search("api key token", SearchMode::Semantic),
            &week(),
            &first(10),
        )
        .await
        .expect("semantic")
        .items;
    assert!(!semantic.is_empty());
    let top = b.world.tx(semantic[0].transmission).expect("record");
    assert_eq!(top.theme, super::super::text::Theme::Credentials);
}

#[tokio::test]
async fn topic_stats_count_confirmed_transmissions_per_topic() {
    let b = shared();
    let c = researcher();
    let confirmed = all_transmissions(&week())
        .await
        .into_iter()
        .filter(|t| {
            matches!(
                t.state,
                TransmissionStateKind::Confirmed
                    | TransmissionStateKind::Classified
                    | TransmissionStateKind::Aggregated
            )
        })
        .count() as u64;
    let stats = b.topic_stats(&c, &week(), n(7)).await.expect("stats");
    assert_eq!(stats.len(), 11, "ten topics and the outliers");
    assert_eq!(
        stats.iter().map(|s| s.transmissions).sum::<u64>(),
        confirmed
    );
    for s in &stats {
        assert_eq!(s.trend.len(), 7);
        assert_eq!(s.trend.iter().sum::<u64>(), s.transmissions);
    }
    let v0 = b
        .topic_stats(
            &c,
            &Scope {
                topic_version: TopicModelVersion(0),
                ..week()
            },
            n(7),
        )
        .await
        .expect("v0");
    assert_eq!(v0.len(), 1);
    assert_eq!(v0[0].topic, None);
    assert_eq!(v0[0].transmissions, confirmed);
}

#[tokio::test]
async fn projections_are_deterministic_stored_and_sampled() {
    let c = researcher();
    let a = super::fresh();
    let b = super::fresh();
    let id_a = a
        .fit_projection(&c, &week(), params(42, 300))
        .await
        .expect("fit");
    let again = a
        .fit_projection(&c, &week(), params(42, 300))
        .await
        .expect("fit");
    assert_eq!(id_a, again, "the same request reuses the stored projection");
    let id_b = b
        .fit_projection(&c, &week(), params(42, 300))
        .await
        .expect("fit");
    let (pa, pb) = (
        a.projection(&c, id_a).await.expect("a"),
        b.projection(&c, id_b).await.expect("b"),
    );
    assert_eq!(pa.xs(), pb.xs());
    assert_eq!(pa.transmissions(), pb.transmissions());
    assert_eq!(pa.len(), 300);
    let other = a
        .fit_projection(&c, &week(), params(43, 300))
        .await
        .expect("fit");
    let po = a.projection(&c, other).await.expect("other");
    assert_ne!(pa.transmissions(), po.transmissions());
    assert!(pa.categories().iter().any(|p| p.topic.is_some()));
    assert!(pa.categories().iter().any(|p| p.channel.is_some()));
    let missing = crate::contract::ProjectionId::from_ulid(5);
    assert_eq!(
        a.projection(&c, missing).await.err(),
        Some(QueryError::NotFound)
    );
}

#[tokio::test]
async fn channel_list_filters() {
    let b = shared();
    let c = researcher();
    let declared = ChannelListFilter {
        origins: vec![OriginKind::Declared],
        ..Default::default()
    };
    let rows = collect(50, async |p| b.channels(&c, &declared, &p).await).await;
    assert_eq!(rows.len(), 6);
    let visible = collect(50, async |p| {
        b.channels(&c, &ChannelListFilter::default(), &p).await
    })
    .await;
    assert_eq!(
        visible.len(),
        14,
        "the superseded channel is hidden by default"
    );
    let old = channel(ChannelKey::OldTeamNotes);
    assert!(visible.iter().all(|r| r.channel.id != old));
    let unreviewed = ChannelListFilter {
        policies: vec![crosstalk_spec::interfaces::l8_surface::PolicyKind::Unreviewed],
        ..Default::default()
    };
    let queue = collect(50, async |p| b.channels(&c, &unreviewed, &p).await).await;
    assert!(
        queue
            .iter()
            .any(|r| r.channel.id == channel(ChannelKey::HijackedWiki))
    );
    let wiki = b
        .channel(&c, channel(ChannelKey::HijackedWiki))
        .await
        .expect("ok")
        .expect("wiki");
    assert!(wiki.seed.is_some() && wiki.writers > 0 && wiki.readers > 0 && wiki.transmissions > 0);
    // The promoted channel holds the superseded channel's resources.
    let notes = b
        .channel_resources(&c, channel(ChannelKey::TeamNotes), week().window)
        .await
        .expect("resources");
    assert!(notes.iter().any(|u| matches!(&u.resource.locator,
        crosstalk_spec::derived::flow::resource::Locator::Url { path, .. } if path == "/team-a/standup")));
}

#[tokio::test]
async fn agent_detail_resolves_aliases() {
    let b = shared();
    let c = researcher();
    let detail = b.agent(&c, agent("al0")).await.expect("ok").expect("agent");
    assert_eq!(detail.agent.id, agent("cc0"));
    assert!(detail.aliases.iter().any(|a| a.id == agent("al0")));
    assert!(detail.children.contains(&agent("cc0.a")));
    assert!(!detail.merges.is_empty());
    let pi2 = b.agent(&c, agent("al2")).await.expect("ok").expect("agent");
    assert_eq!(pi2.agent.id, agent("pi2"));
    assert_eq!(pi2.aliases.len(), 2, "both chained aliases resolve to pi2");
    let omp3 = b
        .agent(&c, agent("omp3"))
        .await
        .expect("ok")
        .expect("agent");
    assert!(!omp3.vetoes.is_empty());
    assert!(matches!(omp3.agent.state, AgentState::Provisional { .. }));
    let impersonator = b.agent(&c, agent("pi0")).await.expect("ok").expect("agent");
    assert!(impersonator.summary.claims.len() >= 2);
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
            .all(|a| a.state == crate::contract::alerts::AlertState::Open)
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
        operators: vec![oncall],
        ..Default::default()
    };
    let rows = collect(100, async |p| b.audit(&c, &mine, &p).await).await;
    assert!(!rows.is_empty());
    assert!(
        rows.iter()
            .all(|e| e.by == crate::contract::research::Actor::Operator(oncall))
    );
    let pastebin = channel(ChannelKey::Pastebin);
    let about = AuditFilter {
        subject: Some(AuditSubject::Channel(pastebin)),
        ..Default::default()
    };
    let rows = collect(100, async |p| b.audit(&c, &about, &p).await).await;
    assert!(rows.iter().any(|e| matches!(&e.action,
        crate::contract::research::AuditedAction::Operator(crate::contract::actions::OperatorAction::SetPolicy { channel, .. }) if *channel == pastebin)));
    let recent = AuditFilter {
        window: Some(window(ago(DAY))),
        ..Default::default()
    };
    let rows = collect(100, async |p| b.audit(&c, &recent, &p).await).await;
    assert!(rows.iter().all(|e| e.at >= ago(DAY)));
}

#[tokio::test]
async fn detection_quality_counts_each_confirmed_transmission() {
    let b = shared();
    let rows = b
        .detection_quality(&researcher(), week().window)
        .await
        .expect("quality");
    assert!(rows.iter().any(|r| r.genuine > 0));
    assert!(rows.iter().any(|r| r.false_detection > 0));
    let kinds: HashSet<_> = rows.iter().map(|r| format!("{:?}", r.match_kind)).collect();
    assert_eq!(kinds.len(), 4);
}

#[tokio::test]
async fn same_seed_same_answers() {
    let (a, b) = (super::fresh(), super::fresh());
    let c = researcher();
    assert_eq!(
        a.topology(&c, &day(), Weighting::MatchedBytes).await,
        b.topology(&c, &day(), Weighting::MatchedBytes).await
    );
    assert_eq!(
        a.search(
            &c,
            &search("agents", SearchMode::Hybrid),
            &week(),
            &first(50)
        )
        .await,
        b.search(
            &c,
            &search("agents", SearchMode::Hybrid),
            &week(),
            &first(50)
        )
        .await
    );
    assert_eq!(
        a.channel_topology(&c, &week(), Weighting::Transmissions)
            .await,
        b.channel_topology(&c, &week(), Weighting::Transmissions)
            .await
    );
    assert_eq!(
        a.agents(&c, &first(100)).await,
        b.agents(&c, &first(100)).await
    );
}

#[tokio::test]
async fn names_resolve_aliases_and_supersession_in_one_call() {
    use crosstalk_spec::ids::{AgentId, ChannelId};

    use crate::contract::graph::ChannelShape;

    let b = shared();
    let c = researcher();
    let (alias, plain) = (agent("al0"), agent("cc1"));
    let unknown = AgentId::from_ulid(1);
    let names = b
        .agent_names(&c, &[alias, plain, unknown])
        .await
        .expect("names");
    assert_eq!(names.len(), 2, "unknown ids are left out");
    let canonical = b.agent(&c, alias).await.expect("read").expect("agent");
    assert_eq!(names[&alias].id, canonical.summary.id);
    assert_ne!(names[&alias].id, alias, "an alias is named by its canonical agent");
    assert_eq!(names[&alias].label, canonical.summary.label);
    assert_eq!(names[&plain].id, plain);

    let (old, declared) = (
        channel(ChannelKey::OldTeamNotes),
        channel(ChannelKey::TeamNotes),
    );
    let names = b
        .channel_names(&c, &[old, ChannelId::from_ulid(1)])
        .await
        .expect("names");
    assert_eq!(names.len(), 1);
    assert_eq!(names[&old].id, declared);
    assert!(matches!(names[&old].shape, ChannelShape::Pattern(_)));

    let nobody = caller(&[]);
    assert_eq!(
        b.agent_names(&nobody, &[plain]).await.err(),
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
        .items
        .remove(0);
    assert_eq!(
        b.alert(&c, listed.id).await.expect("read"),
        Some(listed.clone())
    );
    assert_eq!(b.alert(&c, AlertId::from_ulid(1)).await, Ok(None));
    assert_eq!(
        b.alert(&caller(&[]), listed.id).await.err(),
        Some(QueryError::Forbidden {
            missing: Permission::View
        })
    );
}
