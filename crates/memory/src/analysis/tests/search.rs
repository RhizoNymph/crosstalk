//! `InMemorySearchIndex` and `InMemoryProjectionSource`.

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::filter::{
    FalseDetections, TopicVersionSelector, TopologyFilter, VersionUnavailable,
};
use crosstalk_spec::aggregates::projection::{
    FitFailure, ProjectionLimit, ProjectionParams, ProjectionSpec,
};
use crosstalk_spec::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crosstalk_spec::aggregates::watermark::Watermark;
use crosstalk_spec::derived::flow::transmission::{DelegationDirection, Route};
use crosstalk_spec::derived::flow::verdict::{Observed, Verdict, VerdictRevision};
use crosstalk_spec::interfaces::l6_analysis::{
    ProjectionSource, SampleError, SearchError, SearchIndex, SearchQuery,
};
use crosstalk_spec::paging::{PageRequest, PageSize, SearchList};
use crosstalk_spec::support::NonBlank;

use super::support::{at, fit_active, fit_ready, model};
use crate::analysis::aliases::StaticDirectory;
use crate::analysis::catalog::{InMemoryTopicCatalog, StoredAssignment};
use crate::analysis::search::{
    FixedWatermark, InMemoryProjectionSource, InMemorySearchIndex, IndexedTransmission, sample_key,
    text_score,
};
use crate::analysis::support::ManualClock;
use crate::model::build::{
    agent, catalog, channel, non_zero, test_model, topic_id, transmission, ts, unit, window,
};

struct World {
    catalog: InMemoryTopicCatalog,
    directory: StaticDirectory,
    index: InMemorySearchIndex<StaticDirectory>,
}

fn world() -> World {
    let catalog = catalog(3, 0.5, ManualClock::at(ts(0))).unwrap();
    let directory = StaticDirectory::new();
    let index = InMemorySearchIndex::new(catalog.clone(), directory.clone(), model());
    World {
        catalog,
        directory,
        index,
    }
}

fn doc(
    n: u64,
    from: u64,
    to: u64,
    route: Route,
    at_micros: u64,
    text: &str,
    direction: [f32; 3],
) -> IndexedTransmission {
    IndexedTransmission {
        transmission: transmission(n),
        from: agent(from),
        to: agent(to),
        route,
        confirmed_at: ts(at_micros),
        text: text.to_owned(),
        embedding: unit(&model(), direction[0], direction[1], direction[2]),
    }
}

fn text(query: &str) -> SearchQuery {
    SearchQuery::Text(NonBlank::new(query).unwrap())
}

fn semantic(direction: [f32; 3]) -> SearchQuery {
    SearchQuery::Semantic(unit(&model(), direction[0], direction[1], direction[2]).unwrap())
}

fn first_page(size: u16) -> PageRequest<SearchList> {
    PageRequest {
        size: PageSize::new(size).unwrap(),
        after: None,
    }
}

/// Every hit of a full traversal, in order.
async fn traverse(
    index: &InMemorySearchIndex<StaticDirectory>,
    query: &SearchQuery,
    window: Option<crosstalk_spec::support::TimeWindow>,
    filter: &TopologyFilter,
    size: u16,
) -> Result<Vec<crosstalk_spec::interfaces::l6_analysis::SearchHit>, SearchError> {
    let mut hits = Vec::new();
    let mut request = first_page(size);
    loop {
        let results = index.query(query, window, filter, &request).await?;
        let (items, next) = results.page.into_parts();
        assert!(items.len() <= usize::from(size));
        hits.extend(items);
        match next {
            Some(cursor) => request.after = Some(cursor),
            None => return Ok(hits),
        }
    }
}

fn ids(hits: &[crosstalk_spec::interfaces::l6_analysis::SearchHit]) -> Vec<u64> {
    hits.iter()
        .map(|hit| u64::try_from(hit.transmission.as_ulid() - (1u128 << 100)).unwrap())
        .collect()
}

#[test]
fn text_score_is_the_fraction_of_query_terms_present() {
    assert_eq!(text_score("wiki page", "the Wiki has a page"), 1.0);
    assert_eq!(text_score("wiki page", "the wiki"), 0.5);
    assert_eq!(text_score("wiki", "nothing here"), 0.0);
    assert_eq!(text_score("--", "anything"), 0.0);
}

#[tokio::test]
async fn text_search_ranks_by_score_then_id() {
    let world = world();
    world.index.index(doc(
        1,
        1,
        2,
        Route::Unobserved,
        10,
        "deploy the wiki",
        [1.0, 0.0, 0.0],
    ));
    world
        .index
        .index(doc(2, 1, 2, Route::Unobserved, 11, "wiki", [1.0, 0.0, 0.0]));
    world.index.index(doc(
        3,
        1,
        2,
        Route::Unobserved,
        12,
        "deploy wiki now",
        [1.0, 0.0, 0.0],
    ));
    world.index.index(doc(
        4,
        1,
        2,
        Route::Unobserved,
        13,
        "unrelated",
        [1.0, 0.0, 0.0],
    ));
    let hits = traverse(
        &world.index,
        &text("wiki deploy"),
        None,
        &TopologyFilter::default(),
        1,
    )
    .await
    .unwrap();
    // Full matches (3, 1) by descending id, then the half match (2).
    assert_eq!(ids(&hits), vec![3, 1, 2]);
}

#[tokio::test]
async fn hybrid_score_is_mean_of_text_and_cosine() {
    let world = world();
    world
        .index
        .index(doc(1, 1, 2, Route::Unobserved, 10, "wiki", [1.0, 0.0, 0.0]));
    world.index.index(doc(
        2,
        1,
        2,
        Route::Unobserved,
        10,
        "other",
        [0.0, 1.0, 0.0],
    ));
    let query = SearchQuery::Hybrid {
        text: NonBlank::new("wiki").unwrap(),
        embedding: unit(&model(), 1.0, 0.0, 0.0).unwrap(),
    };
    let results = world
        .index
        .query(&query, None, &TopologyFilter::default(), &first_page(10))
        .await
        .unwrap();
    let scores: Vec<f32> = results
        .page
        .items()
        .iter()
        .map(|hit| hit.score.get())
        .collect();
    assert_eq!(ids(results.page.items()), vec![1, 2]);
    assert_eq!(scores, vec![1.0, 0.0]);
}

#[tokio::test]
async fn semantic_search_rejects_query_of_other_model() {
    // analysis.embedding.same-model-only
    let world = world();
    world
        .index
        .index(doc(1, 1, 2, Route::Unobserved, 10, "wiki", [1.0, 0.0, 0.0]));
    let other: EmbeddingModel = test_model("other");
    let query = SearchQuery::Semantic(unit(&other, 1.0, 0.0, 0.0).unwrap());
    let result = world
        .index
        .query(&query, None, &TopologyFilter::default(), &first_page(10))
        .await;
    assert_eq!(
        result,
        Err(SearchError::WrongModel {
            index: model(),
            query: other
        })
    );
}

#[tokio::test]
async fn search_window_boundaries_are_half_open() {
    // analysis.search.within-window
    let world = world();
    for (n, at_micros) in [(1, 99), (2, 100), (3, 150), (4, 200)] {
        world.index.index(doc(
            n,
            1,
            2,
            Route::Unobserved,
            at_micros,
            "wiki",
            [1.0, 0.0, 0.0],
        ));
    }
    let hits = traverse(
        &world.index,
        &text("wiki"),
        window(100, 200),
        &TopologyFilter::default(),
        10,
    )
    .await
    .unwrap();
    assert_eq!(ids(&hits), vec![3, 2]);
}

#[tokio::test]
async fn hybrid_fusion_truncates_to_limit() {
    // analysis.search.within-limit
    let world = world();
    for n in 1..=7 {
        world
            .index
            .index(doc(n, 1, 2, Route::Unobserved, 10, "wiki", [1.0, 0.0, 0.0]));
    }
    let results = world
        .index
        .query(
            &semantic([1.0, 0.0, 0.0]),
            None,
            &TopologyFilter::default(),
            &first_page(3),
        )
        .await
        .unwrap();
    assert_eq!(results.page.items().len(), 3);
    assert!(results.page.next().is_some());
    let all = traverse(
        &world.index,
        &semantic([1.0, 0.0, 0.0]),
        None,
        &TopologyFilter::default(),
        3,
    )
    .await
    .unwrap();
    assert_eq!(ids(&all), vec![7, 6, 5, 4, 3, 2, 1]);
}

#[tokio::test]
async fn search_filter_field_cases() {
    // analysis.search.honours-filter
    let world = world();
    let v1 = fit_active(
        &world.catalog,
        1,
        &[(1, [1.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0])],
    );
    world.index.index(doc(
        1,
        1,
        2,
        Route::Channel(channel(1)),
        10,
        "wiki",
        [1.0, 0.0, 0.0],
    ));
    world.index.index(doc(
        2,
        3,
        4,
        Route::Channel(channel(2)),
        10,
        "wiki",
        [1.0, 0.0, 0.0],
    ));
    world.index.index(doc(
        3,
        5,
        6,
        Route::Delegation(DelegationDirection::ParentToChild),
        10,
        "wiki",
        [1.0, 0.0, 0.0],
    ));
    world
        .index
        .index(doc(4, 1, 6, Route::Unobserved, 10, "wiki", [1.0, 0.0, 0.0]));
    let assign = |n: u64, topic: Option<u64>| {
        world
            .catalog
            .assign(
                transmission(n),
                v1,
                StoredAssignment {
                    topic: topic.map(topic_id),
                    confirmed_at: ts(10),
                    matched_bytes: non_zero(1),
                },
            )
            .unwrap();
    };
    assign(1, Some(1));
    assign(2, Some(2));
    assign(3, Some(1));
    assign(4, None);
    // Agent 9 was merged into agent 1; channel 2 was superseded by
    // channel 1.
    world.directory.merge(agent(9), agent(1)).unwrap();
    world.directory.supersede(channel(2), channel(1)).unwrap();
    let query = text("wiki");
    let run = |filter: TopologyFilter| {
        let index = world.index.clone();
        let query = query.clone();
        async move { ids(&traverse(&index, &query, None, &filter, 10).await.unwrap()) }
    };
    // A listed merged agent selects its canonical agent.
    assert_eq!(
        run(TopologyFilter {
            agents: vec![agent(9)],
            ..TopologyFilter::default()
        })
        .await,
        vec![4, 1]
    );
    // The superseded channel's traffic counts on its superseder.
    assert_eq!(
        run(TopologyFilter {
            channels: vec![channel(1)],
            ..TopologyFilter::default()
        })
        .await,
        vec![2, 1]
    );
    assert_eq!(
        run(TopologyFilter {
            route_kinds: vec![RouteKind::Delegation, RouteKind::Unobserved],
            ..TopologyFilter::default()
        })
        .await,
        vec![4, 3]
    );
    // Outliers never match a topic list.
    assert_eq!(
        run(TopologyFilter {
            topics: vec![topic_id(1)],
            ..TopologyFilter::default()
        })
        .await,
        vec![3, 1]
    );
    assert_eq!(
        world.index.judge(
            transmission(1),
            Some(Verdict::FalseDetection),
            VerdictRevision::FIRST
        ),
        Observed::Newer
    );
    assert_eq!(
        run(TopologyFilter {
            false_detections: FalseDetections::Exclude,
            ..TopologyFilter::default()
        })
        .await,
        vec![4, 3, 2]
    );
    assert_eq!(run(TopologyFilter::default()).await, vec![4, 3, 2, 1]);
}

#[tokio::test]
async fn filtered_search_rejects_topics_outside_the_version() {
    let world = world();
    let v1 = fit_active(&world.catalog, 1, &[(1, [1.0, 0.0, 0.0])]);
    let v2 = fit_ready(&world.catalog, 10, &[(2, [1.0, 0.0, 0.0])]);
    let filter = TopologyFilter {
        topics: vec![topic_id(2), topic_id(1), topic_id(2)],
        ..TopologyFilter::default()
    };
    let result = world
        .index
        .query(&text("wiki"), None, &filter, &first_page(5))
        .await;
    assert_eq!(
        result,
        Err(SearchError::TopicsNotInVersion {
            version: v1,
            topics: vec![topic_id(2)]
        })
    );
    let pinned = TopologyFilter {
        topic_version: TopicVersionSelector::Pinned(v2),
        ..TopologyFilter::default()
    };
    assert_eq!(
        world
            .index
            .query(&text("wiki"), None, &pinned, &first_page(5))
            .await,
        Err(SearchError::Version(VersionUnavailable::NotActivated(v2)))
    );
}

#[tokio::test]
async fn search_pages_keep_version_across_activation() {
    // analysis.search.cursor-pins-version, in one thread
    let world = world();
    let v1 = fit_active(&world.catalog, 1, &[(1, [1.0, 0.0, 0.0])]);
    for n in 1..=4 {
        world
            .index
            .index(doc(n, 1, 2, Route::Unobserved, 10, "wiki", [1.0, 0.0, 0.0]));
        world
            .catalog
            .assign(
                transmission(n),
                v1,
                StoredAssignment {
                    topic: Some(topic_id(1)),
                    confirmed_at: ts(10),
                    matched_bytes: non_zero(1),
                },
            )
            .unwrap();
    }
    let filter = TopologyFilter {
        topics: vec![topic_id(1)],
        ..TopologyFilter::default()
    };
    let first = world
        .index
        .query(&text("wiki"), None, &filter, &first_page(2))
        .await
        .unwrap();
    assert_eq!(first.topic_version, v1);
    // A new version becomes active mid-traversal.
    fit_active(&world.catalog, 20, &[(2, [1.0, 0.0, 0.0])]);
    let next = PageRequest {
        size: PageSize::new(2).unwrap(),
        after: first.page.next().cloned(),
    };
    let second = world
        .index
        .query(&text("wiki"), None, &filter, &next)
        .await
        .unwrap();
    assert_eq!(second.topic_version, v1);
    assert_eq!(ids(second.page.items()), vec![2, 1]);
    // Once v1 is dropped, the next page names it rather than calling the
    // cursor invalid.
    let first = world
        .index
        .query(&text("wiki"), None, &filter, &first_page(2))
        .await;
    assert!(first.is_err(), "topic 1 is not in the new active version");
    let again = world
        .index
        .query(
            &text("wiki"),
            None,
            &TopologyFilter {
                topic_version: TopicVersionSelector::Pinned(v1),
                topics: vec![topic_id(1)],
                ..TopologyFilter::default()
            },
            &first_page(2),
        )
        .await
        .unwrap();
    fit_active(&world.catalog, 30, &[(3, [1.0, 0.0, 0.0])]);
    fit_active(&world.catalog, 40, &[(4, [1.0, 0.0, 0.0])]);
    assert!(!world.catalog.retains(v1));
    let after_drop = world
        .index
        .query(
            &text("wiki"),
            None,
            &TopologyFilter {
                topic_version: TopicVersionSelector::Pinned(v1),
                topics: vec![topic_id(1)],
                ..TopologyFilter::default()
            },
            &PageRequest {
                size: PageSize::new(2).unwrap(),
                after: again.page.next().cloned(),
            },
        )
        .await;
    assert_eq!(
        after_drop,
        Err(SearchError::Version(VersionUnavailable::NotRetained(v1)))
    );
}

#[tokio::test]
async fn search_scores_unchanged_by_concurrent_indexing() {
    // analysis.search.score-stable and the keyset traversal: a document
    // indexed mid-traversal with a key after the cursor appears once, and
    // nothing repeats.
    let world = world();
    for n in [1, 2, 3, 4] {
        world
            .index
            .index(doc(n, 1, 2, Route::Unobserved, 10, "wiki", [1.0, 0.0, 0.0]));
    }
    let query = semantic([1.0, 0.0, 0.0]);
    let filter = TopologyFilter::default();
    let first = world
        .index
        .query(&query, None, &filter, &first_page(2))
        .await
        .unwrap();
    let first_scores: Vec<f32> = first
        .page
        .items()
        .iter()
        .map(|hit| hit.score.get())
        .collect();
    world
        .index
        .index(doc(0, 1, 2, Route::Unobserved, 10, "wiki", [1.0, 0.0, 0.0]));
    world
        .index
        .index(doc(9, 1, 2, Route::Unobserved, 10, "wiki", [1.0, 0.0, 0.0]));
    world.index.remove(transmission(2));
    let next = PageRequest {
        size: PageSize::new(10).unwrap(),
        after: first.page.next().cloned(),
    };
    let second = world
        .index
        .query(&query, None, &filter, &next)
        .await
        .unwrap();
    assert_eq!(ids(first.page.items()), vec![4, 3]);
    assert_eq!(ids(second.page.items()), vec![1, 0]);
    let again = world
        .index
        .query(&query, None, &filter, &first_page(10))
        .await
        .unwrap();
    let rescored: Vec<f32> = again
        .page
        .items()
        .iter()
        .filter(|hit| [transmission(3), transmission(4)].contains(&hit.transmission))
        .map(|hit| hit.score.get())
        .collect();
    assert_eq!(rescored, first_scores);
}

#[tokio::test]
async fn cursor_with_changed_request_is_rejected() {
    let world = world();
    for n in 1..=3 {
        world
            .index
            .index(doc(n, 1, 2, Route::Unobserved, 10, "wiki", [1.0, 0.0, 0.0]));
    }
    let first = world
        .index
        .query(
            &text("wiki"),
            None,
            &TopologyFilter::default(),
            &first_page(1),
        )
        .await
        .unwrap();
    let next = PageRequest {
        size: PageSize::new(1).unwrap(),
        after: first.page.next().cloned(),
    };
    assert_eq!(
        world
            .index
            .query(
                &text("wiki"),
                window(0, 100),
                &TopologyFilter::default(),
                &next
            )
            .await,
        Err(SearchError::InvalidCursor)
    );
    let other = InMemorySearchIndex::new(world.catalog.clone(), world.directory.clone(), model());
    assert_eq!(
        other
            .query(&text("wiki"), None, &TopologyFilter::default(), &next)
            .await,
        Err(SearchError::InvalidCursor)
    );
}

#[test]
fn judge_keeps_the_newest_revision() {
    let world = world();
    let second = VerdictRevision::FIRST.next().unwrap();
    assert_eq!(
        world
            .index
            .judge(transmission(1), Some(Verdict::Genuine), second),
        Observed::Newer
    );
    assert_eq!(
        world.index.judge(
            transmission(1),
            Some(Verdict::FalseDetection),
            VerdictRevision::FIRST
        ),
        Observed::Stale
    );
}

fn spec(
    window_: crosstalk_spec::support::TimeWindow,
    filter: TopologyFilter,
    version: TopicModelVersion,
    limit: u32,
    seed: u64,
) -> ProjectionSpec {
    let params = ProjectionParams::new(ProjectionLimit::new(limit).unwrap(), 2, 100, seed).unwrap();
    ProjectionSpec::new(window_, filter, version, params, model())
}

fn source(
    world: &World,
    watermark: u64,
) -> InMemoryProjectionSource<StaticDirectory, FixedWatermark> {
    InMemoryProjectionSource::new(
        world.index.clone(),
        FixedWatermark(Watermark(ts(watermark))),
    )
}

#[tokio::test]
async fn prop_sample_is_bottom_k_by_seeded_key() {
    // analysis.projection.sample-selection, on a fixed set
    let world = world();
    for n in 1..=20 {
        world.index.index(doc(
            n,
            1,
            2,
            Route::Unobserved,
            10 + n,
            "wiki",
            [1.0, 0.0, 0.0],
        ));
    }
    let seed = 42;
    let sample = source(&world, 5)
        .sample(&spec(
            window(0, 1_000).unwrap(),
            TopologyFilter::default(),
            TopicModelVersion(0),
            5,
            seed,
        ))
        .await
        .unwrap();
    assert_eq!(sample.matching, 20);
    assert_eq!(sample.watermark, Watermark(ts(5)));
    let mut expected: Vec<_> = (1..=20).map(transmission).collect();
    expected.sort_by_key(|id| (sample_key(seed, *id), *id));
    expected.truncate(5);
    let got: Vec<_> = sample.rows.iter().map(|row| row.transmission).collect();
    assert_eq!(got, expected);
}

#[tokio::test]
async fn sampled_point_resolves_merged_agents() {
    // analysis.projection.point-facts and honours-filter
    let world = world();
    world.index.index(doc(
        1,
        1,
        2,
        Route::Channel(channel(1)),
        10,
        "wiki",
        [1.0, 0.0, 0.0],
    ));
    world
        .index
        .index(doc(2, 3, 2, Route::Unobserved, 20, "wiki", [1.0, 0.0, 0.0]));
    world.index.index(IndexedTransmission {
        embedding: None,
        ..doc(3, 1, 2, Route::Unobserved, 20, "wiki", [1.0, 0.0, 0.0])
    });
    world.directory.merge(agent(1), agent(7)).unwrap();
    let filter = TopologyFilter {
        agents: vec![agent(1)],
        ..TopologyFilter::default()
    };
    let sample = source(&world, 0)
        .sample(&spec(
            window(0, 100).unwrap(),
            filter,
            TopicModelVersion(0),
            10,
            1,
        ))
        .await
        .unwrap();
    // Transmission 3 has no embedding from the model; 2 is not admitted.
    assert_eq!(sample.matching, 1);
    let row = &sample.rows[0];
    assert_eq!(row.transmission, transmission(1));
    assert_eq!(row.from, agent(7));
    assert_eq!(row.to, agent(2));
    assert_eq!(row.route.kind(), RouteKind::Channel);
    assert_eq!(row.topic, None);
    assert_eq!(row.confirmed_at, at(10));
}

#[tokio::test]
async fn sample_fails_for_dropped_version_and_dropped_model() {
    let world = world();
    let v1 = fit_active(&world.catalog, 1, &[(1, [1.0, 0.0, 0.0])]);
    fit_active(&world.catalog, 10, &[(2, [1.0, 0.0, 0.0])]);
    fit_active(&world.catalog, 20, &[(3, [1.0, 0.0, 0.0])]);
    fit_active(&world.catalog, 30, &[(4, [1.0, 0.0, 0.0])]);
    assert!(!world.catalog.retains(v1));
    let sampled = source(&world, 0)
        .sample(&spec(
            window(0, 100).unwrap(),
            TopologyFilter::default(),
            v1,
            10,
            1,
        ))
        .await;
    assert_eq!(
        sampled,
        Err(SampleError::Failed(FitFailure::VersionNotRetained {
            version: v1
        }))
    );
    world.index.drop_model(&model());
    let sampled = source(&world, 0)
        .sample(&spec(
            window(0, 100).unwrap(),
            TopologyFilter::default(),
            TopicModelVersion(4),
            10,
            1,
        ))
        .await;
    assert_eq!(
        sampled,
        Err(SampleError::Failed(FitFailure::EmbeddingModelUnavailable {
            model: model()
        }))
    );
}
