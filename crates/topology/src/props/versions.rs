//! Properties of topic versions: activation, retention and reading one
//! version.
//!
//! A case loads a scene under version 0, then plays re-fits: each fits a
//! version with a few topics in the catalog, re-classifies a subset of the
//! transmissions under it (each into a topic or as an outlier), makes it
//! ready with that subset's size, activates it in the store and then in
//! the catalog, and drops what the catalog's retention (the three most
//! recent versions) drops.

use std::collections::BTreeMap;

use crosstalk_memory::model::build::{test_model, topic, ts, unit};
use crosstalk_memory::model::topology::catalog_ready;
use crosstalk_spec::aggregates::edge::{EdgeSelector, TopologyFilter, Weighting};
use crosstalk_spec::aggregates::filter::{FilterSubject, TopicVersionSelector, VersionUnavailable};
use crosstalk_spec::aggregates::series::{SeriesGrid, SeriesGrouping, SeriesStep};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::events::insight::ClassificationCause;
use crosstalk_spec::ids::{AgentId, TopicId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l6_analysis::lifecycle::{CatalogActivation, TopicLifecycle};
use crosstalk_spec::interfaces::l7_topology::{Activation, EdgeError, EdgeQueryError, EdgeStore};
use crosstalk_spec::paging::{EdgeTransmissionList, PageRequest, PageSize};
use crosstalk_spec::support::TimeWindow;
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

use super::{Scene, check, contribution, ensure, listed, load, scene, window};
use crate::env::EnvAliases;
use crate::store::fold_route_key;
use crate::tests::support::World;

/// One re-fit: how many topics, and per transmission whether it is
/// re-classified and into which topic (`None`: an outlier).
#[derive(Debug, Clone)]
struct Refit {
    topics: u8,
    picks: Vec<Option<Option<u8>>>,
}

fn refit() -> impl Strategy<Value = Refit> {
    (
        1u8..4,
        prop::collection::vec(prop::option::weighted(0.8, prop::option::of(0u8..3)), 14),
    )
        .prop_map(|(topics, picks)| Refit { topics, picks })
}

type Plan = (Scene, Vec<Refit>);

fn plan() -> impl Strategy<Value = Plan> {
    (scene(), prop::collection::vec(refit(), 1..5))
}

/// What a played version holds: each re-classified transmission's index
/// and topic.
type Classified = BTreeMap<usize, Option<TopicId>>;

/// Per-version row counts: contributions and edge buckets.
async fn rows(world: &World) -> BTreeMap<i64, (i64, i64)> {
    let counted: Vec<(i64, i64, i64)> = sqlx::query_as(
        "SELECT v.version, \
         (SELECT count(*) FROM topology.contributions c WHERE c.version = v.version), \
         (SELECT count(*) FROM topology.edge_buckets b WHERE b.version = v.version) \
         FROM topology.versions v",
    )
    .fetch_all(world.store.pool())
    .await
    .expect("row counts");
    counted.into_iter().map(|(v, c, b)| (v, (c, b))).collect()
}

fn fail(what: impl std::fmt::Debug) -> TestCaseError {
    TestCaseError::fail(format!("{what:?}"))
}

/// Fit, classify and ready one version; returns it and what it holds.
async fn fit(
    world: &mut World,
    scene: &Scene,
    plan: &Refit,
    at: u64,
) -> Result<(TopicModelVersion, Classified), TestCaseError> {
    let model = test_model("props");
    let base = 1000 * at;
    let made: Vec<_> = (0..plan.topics)
        .filter_map(|k| {
            Some(topic(
                TopicId::from_ulid(u128::from(base) + u128::from(k)),
                TopicModelVersion(0),
                unit(&model, 1.0, f32::from(k), 0.0)?,
                ts(at),
            ))
        })
        .collect();
    let ids: Vec<TopicId> = made.iter().map(|one| one.id).collect();
    let version = catalog_ready(&mut world.catalog, made, ts(at))
        .await
        .map_err(fail)?;
    let mut classified = Classified::new();
    for (index, one) in scene.sent.iter().enumerate() {
        let Some(Some(pick)) = plan.picks.get(index) else {
            continue;
        };
        let topic = pick.and_then(|k| ids.get(usize::from(k) % ids.len().max(1)).copied());
        let refit = crosstalk_spec::interfaces::l7_topology::EdgeContribution {
            cause: ClassificationCause::Refit,
            ..contribution(index, *one, version, topic)
        };
        match world.store.apply(&refit).await {
            Ok(_) => {
                classified.insert(index, topic);
            }
            Err(EdgeError::SelfEdge) => {}
            Err(error) => return Err(fail(error)),
        }
    }
    let processed = plan
        .picks
        .iter()
        .take(scene.sent.len())
        .filter(|pick| matches!(pick, Some(Some(_))))
        .count();
    world
        .store
        .version_ready(version, processed as u64)
        .await
        .map_err(fail)?;
    Ok((version, classified))
}

/// Activate `version` in the store, then the catalog; drop what retention
/// drops. Returns the dropped versions.
async fn activate(
    world: &mut World,
    version: TopicModelVersion,
    at: u64,
) -> Result<Vec<TopicModelVersion>, TestCaseError> {
    let switched = world.store.activate(version).await.map_err(fail)?;
    ensure(matches!(switched, Activation::Switched { .. }), || {
        format!("{switched:?}")
    })?;
    let catalog = world
        .catalog
        .mark_active(version, ts(at))
        .await
        .map_err(fail)?;
    let CatalogActivation::Switched { dropped, .. } = catalog else {
        return Err(TestCaseError::fail("the catalog ignored the activation"));
    };
    Ok(dropped)
}

/// A fold edge's sort key.
type FoldKey = (AgentId, AgentId, (u8, String));

/// The fold of `version`'s contributions in `window` under `filter`.
fn fold(
    world: &World,
    scene: &Scene,
    held: &Classified,
    window: TimeWindow,
    filter: &TopologyFilter,
) -> Vec<(AgentId, AgentId, Route, u64, u64)> {
    let env = world.store.env();
    let aliases = EnvAliases(env);
    let mut sums: BTreeMap<FoldKey, (Route, u64, u64)> = BTreeMap::new();
    for (index, topic) in held {
        let one = scene.sent[*index];
        if !window.contains(ts(one.at)) {
            continue;
        }
        let from = AgentDirectory::canonical(
            &world.directory,
            crosstalk_memory::model::build::agent(one.from),
        );
        let to = AgentDirectory::canonical(
            &world.directory,
            crosstalk_memory::model::build::agent(one.to),
        );
        let routed = super::route(one.route).resolved(aliases);
        let subject = FilterSubject {
            from,
            to,
            route: &routed,
            topic: *topic,
            false_detection: scene.false_detections.contains(index),
        };
        if filter.admits(&subject, aliases) {
            let entry =
                sums.entry((from, to, fold_route_key(&routed)))
                    .or_insert((routed.clone(), 0, 0));
            entry.1 += 1;
            entry.2 += one.bytes;
        }
    }
    sums.into_iter()
        .map(|((from, to, _), (route, n, bytes))| (from, to, route, n, bytes))
        .collect()
}

const ALL: fn() -> TimeWindow = || window(0, 200);

/// topology.retention.activate-drops-nothing
#[test]
fn activate_keeps_every_version() {
    check(
        "activate_keeps_every_version",
        plan(),
        async |world: &mut World, (scene, refits): &Plan| {
            load(world, scene).await?;
            for (n, plan) in refits.iter().enumerate() {
                let at = 100 * (n as u64 + 1);
                let (version, _) = fit(world, scene, plan, at).await?;
                let before = rows(world).await;
                let switched = world.store.activate(version).await.map_err(fail)?;
                ensure(matches!(switched, Activation::Switched { .. }), || {
                    format!("{switched:?}")
                })?;
                let after = rows(world).await;
                ensure(before == after, || format!("{before:?} -> {after:?}"))?;
                world
                    .catalog
                    .mark_active(version, ts(at + 50))
                    .await
                    .map_err(fail)?;
            }
            Ok(())
        },
    );
}

/// topology.retention.drop-removes-all
#[test]
fn dropped_version_is_gone() {
    check(
        "dropped_version_is_gone",
        plan(),
        async |world: &mut World, (scene, refits): &Plan| {
            load(world, scene).await?;
            for (n, plan) in refits.iter().enumerate() {
                let at = 100 * (n as u64 + 1);
                let (version, _) = fit(world, scene, plan, at).await?;
                for gone in activate(world, version, at + 50).await? {
                    world.store.drop_version(gone).await.map_err(fail)?;
                    let left = rows(world).await;
                    ensure(
                        left.get(&i64::from(gone.0))
                            .is_none_or(|counts| *counts == (0, 0)),
                        || format!("{gone:?} keeps rows: {left:?}"),
                    )?;
                    let applied = world
                        .store
                        .apply(&contribution(0, scene.sent[0], gone, None))
                        .await;
                    ensure(
                        applied == Err(EdgeError::VersionNotRetained { version: gone }),
                        || format!("{applied:?}"),
                    )?;
                    let pinned = TopologyFilter {
                        topic_version: TopicVersionSelector::Pinned(gone),
                        ..TopologyFilter::default()
                    };
                    let not_retained = Some(EdgeQueryError::Version(
                        VersionUnavailable::NotRetained(gone),
                    ));
                    let graph = world
                        .store
                        .graph(ALL(), Weighting::Transmissions, &pinned)
                        .await
                        .err();
                    ensure(graph == not_retained, || format!("graph: {graph:?}"))?;
                    let step = SeriesStep::new(
                        world.store.bucket_width(),
                        std::num::NonZeroU64::MIN.saturating_add(9),
                    )
                    .map_err(fail)?;
                    let grid = SeriesGrid::new(ALL(), step).map_err(fail)?;
                    let series = world
                        .store
                        .series(
                            grid,
                            Weighting::Transmissions,
                            SeriesGrouping::Total,
                            &pinned,
                        )
                        .await
                        .err();
                    ensure(series == not_retained, || format!("series: {series:?}"))?;
                    let edge = EdgeSelector::new(
                        crosstalk_memory::model::build::agent(0),
                        crosstalk_memory::model::build::agent(1),
                        Route::Unobserved,
                    )
                    .map_err(fail)?;
                    let page = PageRequest::<EdgeTransmissionList> {
                        size: PageSize::new(5).map_err(fail)?,
                        after: None,
                    };
                    let drill = world
                        .store
                        .transmissions(&edge, ALL(), &pinned, &page)
                        .await
                        .err();
                    ensure(drill == not_retained, || {
                        format!("transmissions: {drill:?}")
                    })?;
                }
            }
            Ok(())
        },
    );
}

/// topology.version.keeps-current-and-pending
#[test]
fn retention_spares_active_previous_and_pending_versions() {
    check(
        "retention_spares_active_previous_and_pending_versions",
        (plan(), refit()),
        async |world: &mut World, ((scene, refits), pending): &(Plan, Refit)| {
            load(world, scene).await?;
            let mut previous = TopicModelVersion(0);
            for (n, plan) in refits.iter().enumerate() {
                let at = 100 * (n as u64 + 1);
                let (version, _) = fit(world, scene, plan, at).await?;
                // A newer version, ready but not active, while retention runs.
                let (newer, _) = fit(world, scene, pending, at + 10).await?;
                let before = rows(world).await;
                for gone in activate(world, version, at + 50).await? {
                    ensure(
                        gone != version && gone != previous && gone < version,
                        || {
                            format!(
                                "retention dropped {gone:?} (active {version:?}, previous {previous:?})"
                            )
                        },
                    )?;
                    world.store.drop_version(gone).await.map_err(fail)?;
                }
                let after = rows(world).await;
                for kept in [version, previous, newer] {
                    let key = i64::from(kept.0);
                    ensure(before.get(&key) == after.get(&key), || {
                        format!("{kept:?} lost rows")
                    })?;
                }
                // The pending version is never activated here: drop its
                // readiness by superseding it with the next fit.
                previous = version;
            }
            Ok(())
        },
    );
}

/// topology.version.single-version
#[test]
fn graph_reads_one_topic_version() {
    check(
        "graph_reads_one_topic_version",
        plan(),
        async |world: &mut World, (scene, refits): &Plan| {
            load(world, scene).await?;
            let mut held: BTreeMap<TopicModelVersion, Classified> = BTreeMap::new();
            held.insert(
                TopicModelVersion(0),
                scene
                    .sent
                    .iter()
                    .enumerate()
                    .filter(|(_, one)| one.from != one.to)
                    .map(|(index, _)| (index, None))
                    .collect(),
            );
            for (n, plan) in refits.iter().enumerate() {
                let at = 100 * (n as u64 + 1);
                let (version, classified) = fit(world, scene, plan, at).await?;
                held.insert(version, classified);
                for gone in activate(world, version, at + 50).await? {
                    world.store.drop_version(gone).await.map_err(fail)?;
                    held.remove(&gone);
                }
                for (version, classified) in &held {
                    let pinned = TopologyFilter {
                        topic_version: TopicVersionSelector::Pinned(*version),
                        ..TopologyFilter::default()
                    };
                    let graph = world
                        .store
                        .graph(ALL(), Weighting::Transmissions, &pinned)
                        .await
                        .map_err(fail)?
                        .value;
                    ensure(graph.topic_version() == *version, || {
                        "another version".to_owned()
                    })?;
                    let mut got = listed(&graph);
                    got.sort_by_key(|edge| (edge.0, edge.1, fold_route_key(&edge.2)));
                    let expected = fold(world, scene, classified, ALL(), &pinned);
                    ensure(got == expected, || {
                        format!("{version:?}: {got:?} vs {expected:?}")
                    })?;
                }
            }
            Ok(())
        },
    );
}

/// topology.filter.topic-membership
#[test]
fn topic_filter_counts_only_listed_topics() {
    check(
        "topic_filter_counts_only_listed_topics",
        (scene(), refit(), prop::collection::vec(0u8..3, 1..3)),
        async |world: &mut World, (scene, plan, listed_topics): &(Scene, Refit, Vec<u8>)| {
            load(world, scene).await?;
            let (version, classified) = fit(world, scene, plan, 100).await?;
            activate(world, version, 150).await?;
            let ids: Vec<TopicId> = (0..plan.topics)
                .map(|k| TopicId::from_ulid(100_000 + u128::from(k)))
                .collect();
            let filter = TopologyFilter {
                topics: listed_topics
                    .iter()
                    .map(|k| ids[usize::from(*k) % ids.len()])
                    .collect(),
                ..TopologyFilter::default()
            };
            let graph = world
                .store
                .graph(ALL(), Weighting::Transmissions, &filter)
                .await
                .map_err(fail)?
                .value;
            let mut got = listed(&graph);
            got.sort_by_key(|edge| (edge.0, edge.1, fold_route_key(&edge.2)));
            let expected = fold(world, scene, &classified, ALL(), &filter);
            ensure(got == expected, || format!("{got:?} vs {expected:?}"))?;
            // Every counted transmission has a listed topic: an outlier
            // never counts.
            let counted: u64 = got.iter().map(|edge| edge.3).sum();
            let listed_count = classified
                .iter()
                .filter(|(index, topic)| {
                    let one = scene.sent[**index];
                    let from = AgentDirectory::canonical(
                        &world.directory,
                        crosstalk_memory::model::build::agent(one.from),
                    );
                    let to = AgentDirectory::canonical(
                        &world.directory,
                        crosstalk_memory::model::build::agent(one.to),
                    );
                    from != to && topic.is_some_and(|topic| filter.topics.contains(&topic))
                })
                .count() as u64;
            ensure(counted == listed_count, || {
                format!("{counted} counted, {listed_count} listed")
            })
        },
    );
}
