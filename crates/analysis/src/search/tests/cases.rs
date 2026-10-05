//! Focused search cases, one or more per invariant: hits exist
//! (`analysis.search.hit-exists`), pages hold at most their size
//! (`within-limit`), windows are half-open (`within-window`), the filter is
//! honoured field by field (`honours-filter`) and applied before the page
//! is cut (`filter-before-limit`), and the scores the SQL computes.

use std::collections::BTreeSet;
use std::num::NonZeroU16;

use crosstalk_memory::analysis::aliases::StaticDirectory;
use crosstalk_memory::analysis::search::ManualWatermark;
use crosstalk_memory::model::build::{agent, catalog, channel, transmission, ts, unit, window};
use crosstalk_memory::support::Outbox;
use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::filter::{FalseDetections, TopologyFilter};
use crosstalk_spec::aggregates::topic::{Embedding, EmbeddingModel};
use crosstalk_spec::aggregates::watermark::Watermark;
use crosstalk_spec::derived::flow::transmission::{DelegationDirection, Route};
use crosstalk_spec::derived::flow::verdict::{Verdict, VerdictRevision};
use crosstalk_spec::ids::TransmissionId;
use crosstalk_spec::interfaces::l6_analysis::corpus::{IndexedTransmission, SearchCorpus};
use crosstalk_spec::interfaces::l6_analysis::{SearchError, SearchIndex, SearchQuery};
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::{NonBlank, TimeWindow};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use sqlx::PgPool;

use super::Subject;
use crate::pg::testing::{database, truncate};
use crate::search::similarity;

fn model() -> EmbeddingModel {
    EmbeddingModel {
        name: "cases".to_owned(),
        dimension: NonZeroU16::new(3).unwrap_or(NonZeroU16::MIN),
    }
}

fn vector(x: f32, y: f32, z: f32) -> Embedding {
    unit(&model(), x, y, z).unwrap_or_else(|| panic!("a unit vector"))
}

fn text(words: &str) -> NonBlank {
    NonBlank::new(words).unwrap_or_else(|_| panic!("blank text"))
}

fn size(n: u16) -> PageSize {
    PageSize::new(n).unwrap_or_else(|_| panic!("page size {n}"))
}

async fn subject(pool: PgPool) -> (Subject, StaticDirectory) {
    let directory = StaticDirectory::new();
    let catalog = catalog(2, 0.5, Outbox::none()).unwrap_or_else(|| panic!("a catalog"));
    let subject = Subject::new(
        pool,
        model(),
        directory.clone(),
        ManualWatermark::new(Watermark(ts(0))),
        catalog,
    )
    .await;
    (subject, directory)
}

/// A document from agent `from` to `to` on `route`, confirmed at `at`.
fn document(
    n: u64,
    from: u64,
    to: u64,
    route: Route,
    at: u64,
    body: &str,
    embedding: Option<Embedding>,
) -> IndexedTransmission {
    IndexedTransmission {
        transmission: transmission(n),
        from: agent(from),
        to: agent(to),
        route,
        confirmed_at: ts(at),
        text: body.to_owned(),
        embedding,
    }
}

async fn index(subject: &mut Subject, documents: Vec<IndexedTransmission>) {
    for document in documents {
        if let Err(error) = subject.index(document).await {
            panic!("indexing: {error:?}");
        }
    }
}

/// Every page of a traversal: (transmission, score) per page.
async fn traverse(
    subject: &Subject,
    query: &SearchQuery,
    window: Option<TimeWindow>,
    filter: &TopologyFilter,
    page_size: PageSize,
) -> Result<Vec<Vec<(TransmissionId, f32)>>, SearchError> {
    let mut request = PageRequest {
        size: page_size,
        after: None,
    };
    let mut pages = Vec::new();
    loop {
        let (items, next) = subject
            .query(query, window, filter, &request)
            .await?
            .page
            .into_parts();
        pages.push(
            items
                .iter()
                .map(|hit| (hit.transmission, hit.score.get()))
                .collect(),
        );
        match next {
            Some(cursor) => request.after = Some(cursor),
            None => return Ok(pages),
        }
    }
}

fn flat(pages: &[Vec<(TransmissionId, f32)>]) -> Vec<(TransmissionId, f32)> {
    pages.iter().flatten().copied().collect()
}

/// Ten documents mentioning "wiki", each with an embedding.
fn wiki_corpus() -> Vec<IndexedTransmission> {
    (0..10)
        .map(|n| {
            let body = if n % 2 == 0 {
                "wiki page"
            } else {
                "wiki deploy token"
            };
            let x = f32::from(u8::try_from(n).unwrap_or(0)) - 4.0;
            document(
                n,
                1,
                2,
                Route::Channel(channel(1)),
                10 * n,
                body,
                Some(vector(x, 1.0, 0.5)),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn search_hits_resolve_to_confirmed_transmissions() {
    let Some(db) = database("search_hits_resolve_to_confirmed_transmissions").await else {
        return;
    };
    let (mut subject, _) = subject(db.pool().clone()).await;
    index(&mut subject, wiki_corpus()).await;
    for removed in [3, 4] {
        if let Err(error) = subject.remove(transmission(removed)).await {
            panic!("removing: {error:?}");
        }
    }
    let indexed: BTreeSet<TransmissionId> = (0..10)
        .filter(|n| ![3, 4].contains(n))
        .map(transmission)
        .collect();
    for query in [
        SearchQuery::Text(text("wiki")),
        SearchQuery::Semantic(vector(1.0, 0.0, 0.0)),
        SearchQuery::Hybrid {
            text: text("deploy"),
            embedding: vector(0.0, 1.0, 0.0),
        },
    ] {
        let hits = flat(
            &traverse(&subject, &query, None, &TopologyFilter::default(), size(4))
                .await
                .unwrap_or_else(|error| panic!("{error:?}")),
        );
        let found: BTreeSet<TransmissionId> = hits.iter().map(|(id, _)| *id).collect();
        assert_eq!(found.len(), hits.len(), "a hit listed twice: {hits:?}");
        assert_eq!(found, indexed, "{query:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn hybrid_fusion_truncates_to_limit() {
    let Some(db) = database("hybrid_fusion_truncates_to_limit").await else {
        return;
    };
    let (mut subject, _) = subject(db.pool().clone()).await;
    index(&mut subject, wiki_corpus()).await;
    let query = SearchQuery::Hybrid {
        text: text("wiki deploy"),
        embedding: vector(1.0, 1.0, 0.0),
    };
    for page_size in [1, 2, 3, 7] {
        let pages = traverse(
            &subject,
            &query,
            None,
            &TopologyFilter::default(),
            size(page_size),
        )
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
        for page in &pages {
            assert!(page.len() <= usize::from(page_size), "{page:?}");
        }
        assert_eq!(flat(&pages).len(), 10);
        // Every page but the last is full.
        for page in &pages[..pages.len() - 1] {
            assert_eq!(page.len(), usize::from(page_size));
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_hybrid_search_respects_limit() {
    let Some(db) = database("pg_hybrid_search_respects_limit").await else {
        return;
    };
    let (mut subject, _) = subject(db.pool().clone()).await;
    index(&mut subject, wiki_corpus()).await;
    let filter = TopologyFilter::default();
    let query = SearchQuery::Hybrid {
        text: text("token"),
        embedding: vector(0.0, 0.0, 1.0),
    };
    let first = subject
        .query(
            &query,
            None,
            &filter,
            &PageRequest {
                size: size(3),
                after: None,
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(first.page.items().len(), 3);
    assert!(first.page.next().is_some());
    let all = traverse(&subject, &query, None, &filter, size(3))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        all.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![3, 3, 3, 1]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn search_window_boundaries_are_half_open() {
    let Some(db) = database("search_window_boundaries_are_half_open").await else {
        return;
    };
    let (mut subject, _) = subject(db.pool().clone()).await;
    index(&mut subject, wiki_corpus()).await;
    // Confirmed at 0, 10, ..., 90: [20, 50) holds 20, 30 and 40.
    let window = window(20, 50);
    for query in [
        SearchQuery::Text(text("wiki")),
        SearchQuery::Semantic(vector(1.0, 0.0, 0.0)),
    ] {
        let hits: BTreeSet<TransmissionId> = flat(
            &traverse(
                &subject,
                &query,
                window,
                &TopologyFilter::default(),
                size(2),
            )
            .await
            .unwrap_or_else(|error| panic!("{error:?}")),
        )
        .into_iter()
        .map(|(id, _)| id)
        .collect();
        assert_eq!(
            hits,
            BTreeSet::from([transmission(2), transmission(3), transmission(4)])
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn pg_hybrid_search_respects_window() {
    let Some(db) = database("pg_hybrid_search_respects_window").await else {
        return;
    };
    let (mut subject, _) = subject(db.pool().clone()).await;
    index(&mut subject, wiki_corpus()).await;
    let query = SearchQuery::Hybrid {
        text: text("page"),
        embedding: vector(1.0, 0.0, 0.0),
    };
    for (start, end) in [(0, 1), (0, 90), (45, 46), (89, 91), (5, 95)] {
        let window = window(start, end);
        let hits = flat(
            &traverse(
                &subject,
                &query,
                window,
                &TopologyFilter::default(),
                size(3),
            )
            .await
            .unwrap_or_else(|error| panic!("{error:?}")),
        );
        let expected: BTreeSet<TransmissionId> = (0..10u64)
            .filter(|n| (start..end).contains(&(10 * n)))
            .map(transmission)
            .collect();
        let found: BTreeSet<TransmissionId> = hits.into_iter().map(|(id, _)| id).collect();
        assert_eq!(found, expected, "window [{start}, {end})");
    }
}

/// Text scores are the fraction of the query's terms a document holds;
/// semantic scores are the reference's cosine, bit for bit; hybrid scores
/// their mean; ties rank by descending id.
#[tokio::test(flavor = "multi_thread")]
async fn scores_follow_the_documented_formulas() {
    let Some(db) = database("scores_follow_the_documented_formulas").await else {
        return;
    };
    let (mut subject, _) = subject(db.pool().clone()).await;
    let a = vector(0.3, -0.7, 0.2);
    let b = vector(-0.4, 0.1, 0.9);
    index(
        &mut subject,
        vec![
            document(
                1,
                1,
                2,
                Route::Unobserved,
                1,
                "Wiki deploy, TOKEN!",
                Some(a.clone()),
            ),
            document(2, 1, 2, Route::Unobserved, 2, "the wiki", Some(b.clone())),
            document(3, 1, 2, Route::Unobserved, 3, "nothing here", None),
        ],
    )
    .await;
    let filter = TopologyFilter::default();
    let text_hits = flat(
        &traverse(
            &subject,
            &SearchQuery::Text(text("wiki token build")),
            None,
            &filter,
            size(5),
        )
        .await
        .unwrap_or_else(|error| panic!("{error:?}")),
    );
    assert_eq!(
        text_hits,
        vec![(transmission(1), 2.0 / 3.0), (transmission(2), 1.0 / 3.0)]
    );
    let q = vector(0.5, 0.5, -0.1);
    let semantic = flat(
        &traverse(
            &subject,
            &SearchQuery::Semantic(q.clone()),
            None,
            &filter,
            size(5),
        )
        .await
        .unwrap_or_else(|error| panic!("{error:?}")),
    );
    let expected = |embedding: &Embedding| {
        similarity(&q, embedding)
            .unwrap_or_else(|| panic!("same model"))
            .get()
    };
    let mut wanted = vec![
        (transmission(1), expected(&a)),
        (transmission(2), expected(&b)),
    ];
    wanted.sort_by(|x, y| y.1.total_cmp(&x.1).then(y.0.cmp(&x.0)));
    assert_eq!(semantic, wanted);
    let hybrid = flat(
        &traverse(
            &subject,
            &SearchQuery::Hybrid {
                text: text("wiki"),
                embedding: q.clone(),
            },
            None,
            &filter,
            size(5),
        )
        .await
        .unwrap_or_else(|error| panic!("{error:?}")),
    );
    for (id, score) in hybrid {
        let cosine = if id == transmission(1) {
            expected(&a)
        } else {
            expected(&b)
        };
        assert_eq!(score, f32::midpoint(1.0, cosine), "{id:?}");
    }
    // Equal scores rank by descending id.
    index(
        &mut subject,
        vec![
            document(7, 1, 2, Route::Unobserved, 4, "tie", None),
            document(8, 1, 2, Route::Unobserved, 5, "tie", None),
        ],
    )
    .await;
    let ties = flat(
        &traverse(
            &subject,
            &SearchQuery::Text(text("tie")),
            None,
            &filter,
            size(1),
        )
        .await
        .unwrap_or_else(|error| panic!("{error:?}")),
    );
    assert_eq!(ties, vec![(transmission(8), 1.0), (transmission(7), 1.0)]);
}

/// A query embedded with another model than the index's is refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_query_of_another_model_is_wrong_model() {
    let Some(db) = database("a_query_of_another_model_is_wrong_model").await else {
        return;
    };
    let (mut subject, _) = subject(db.pool().clone()).await;
    let other = EmbeddingModel {
        name: "other".to_owned(),
        dimension: NonZeroU16::new(3).unwrap_or(NonZeroU16::MIN),
    };
    let query = SearchQuery::Semantic(
        unit(&other, 1.0, 0.0, 0.0).unwrap_or_else(|| panic!("a unit vector")),
    );
    let page = PageRequest {
        size: size(2),
        after: None,
    };
    let refused = subject
        .query(&query, None, &TopologyFilter::default(), &page)
        .await;
    assert_eq!(
        refused.err(),
        Some(SearchError::WrongModel {
            index: model(),
            query: other.clone(),
        })
    );
    if let Err(error) = subject.set_model(other).await {
        panic!("{error:?}");
    }
    assert!(
        subject
            .query(&query, None, &TopologyFilter::default(), &page)
            .await
            .is_ok()
    );
}

/// Each filter field, on its own, keeps exactly the documents
/// `TopologyFilter::admits` admits: agents (resolved through merges),
/// channels (through supersession), route kinds, false detections, and
/// never a transmission within one agent.
#[tokio::test(flavor = "multi_thread")]
async fn search_filter_field_cases() {
    let Some(db) = database("search_filter_field_cases").await else {
        return;
    };
    let (mut subject, directory) = subject(db.pool().clone()).await;
    index(
        &mut subject,
        vec![
            document(1, 1, 2, Route::Channel(channel(1)), 1, "wiki", None),
            document(2, 2, 3, Route::Channel(channel(2)), 2, "wiki", None),
            document(
                3,
                3,
                4,
                Route::Delegation(DelegationDirection::ParentToChild),
                3,
                "wiki",
                None,
            ),
            document(4, 4, 5, Route::Unobserved, 4, "wiki", None),
            document(5, 5, 6, Route::Channel(channel(3)), 5, "wiki", None),
        ],
    )
    .await;
    let judged = subject
        .judge(
            transmission(4),
            Some(Verdict::FalseDetection),
            VerdictRevision::FIRST,
        )
        .await;
    assert!(judged.is_ok());
    let found = |subject: Subject, filter: TopologyFilter| async move {
        flat(
            &traverse(
                &subject,
                &SearchQuery::Text(text("wiki")),
                None,
                &filter,
                size(2),
            )
            .await
            .unwrap_or_else(|error| panic!("{error:?}")),
        )
        .into_iter()
        .map(|(id, _)| id)
        .collect::<BTreeSet<_>>()
    };
    let ids = |ns: &[u64]| {
        ns.iter()
            .copied()
            .map(transmission)
            .collect::<BTreeSet<_>>()
    };
    let agents = TopologyFilter {
        agents: vec![agent(3)],
        ..TopologyFilter::default()
    };
    assert_eq!(found(subject.clone(), agents.clone()).await, ids(&[2, 3]));
    let channels = TopologyFilter {
        channels: vec![channel(1)],
        ..TopologyFilter::default()
    };
    assert_eq!(found(subject.clone(), channels.clone()).await, ids(&[1]));
    let kinds = TopologyFilter {
        route_kinds: vec![RouteKind::Delegation, RouteKind::Unobserved],
        ..TopologyFilter::default()
    };
    assert_eq!(found(subject.clone(), kinds).await, ids(&[3, 4]));
    let genuine = TopologyFilter {
        false_detections: FalseDetections::Exclude,
        ..TopologyFilter::default()
    };
    assert_eq!(found(subject.clone(), genuine).await, ids(&[1, 2, 3, 5]));
    // Merging agent 3 into 7: listing 7 selects what 3 sent and read; and
    // superseding channel 2 by 1: listing 1 selects both channels.
    assert!(directory.merge(agent(3), agent(7)).is_ok());
    let merged = TopologyFilter {
        agents: vec![agent(7)],
        ..TopologyFilter::default()
    };
    assert_eq!(found(subject.clone(), merged).await, ids(&[2, 3]));
    assert!(directory.supersede(channel(2), channel(1)).is_ok());
    assert_eq!(found(subject.clone(), channels).await, ids(&[1, 2]));
    // Merging 4 into 5: transmission 4 (from 4 to 5) is now within one
    // agent, and no filter admits it.
    assert!(directory.merge(agent(4), agent(5)).is_ok());
    assert_eq!(
        found(subject.clone(), TopologyFilter::default()).await,
        ids(&[1, 2, 3, 5])
    );
}

/// Every hit of a filtered traversal is one the filter admits, with
/// sender and reader resolved at query time.
#[tokio::test(flavor = "multi_thread")]
async fn pg_hybrid_search_honours_filter() {
    let Some(db) = database("pg_hybrid_search_honours_filter").await else {
        return;
    };
    let (mut subject, directory) = subject(db.pool().clone()).await;
    let documents: Vec<IndexedTransmission> = (0..12u64)
        .map(|n| {
            let route = if n % 3 == 0 {
                Route::Unobserved
            } else {
                Route::Channel(channel(n % 3))
            };
            let x = f32::from(u8::try_from(n).unwrap_or(0));
            document(
                n,
                n % 4,
                n % 4 + 1,
                route,
                n,
                "wiki build",
                Some(vector(x, 1.0, 1.0)),
            )
        })
        .collect();
    index(&mut subject, documents.clone()).await;
    assert!(directory.merge(agent(1), agent(2)).is_ok());
    let filter = TopologyFilter {
        agents: vec![agent(2)],
        channels: vec![channel(1)],
        ..TopologyFilter::default()
    };
    let query = SearchQuery::Hybrid {
        text: text("wiki"),
        embedding: vector(1.0, 0.0, 0.0),
    };
    let hits = flat(
        &traverse(&subject, &query, None, &filter, size(2))
            .await
            .unwrap_or_else(|error| panic!("{error:?}")),
    );
    let canonical = |n: u64| {
        let id = agent(n);
        crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory::canonical(&directory, id)
    };
    let expected: BTreeSet<TransmissionId> = documents
        .iter()
        .map(|d| d.transmission)
        .filter(|id| {
            let n = (0..12u64).find(|n| transmission(*n) == *id).unwrap_or(0);
            let (from, to) = (canonical(n % 4), canonical(n % 4 + 1));
            from != to && (from == agent(2) || to == agent(2)) && n % 3 == 1
        })
        .collect();
    let found: BTreeSet<TransmissionId> = hits.iter().map(|(id, _)| *id).collect();
    assert_eq!(found, expected);
}

/// `analysis.search.filter-before-limit`: a filtered traversal's hits are
/// the unfiltered traversal's, in the same order, less the ones the filter
/// does not admit, whatever the page size.
#[tokio::test(flavor = "multi_thread")]
async fn prop_filtered_search_is_filtered_ranking() {
    let Some(db) = database("prop_filtered_search_is_filtered_ranking").await else {
        return;
    };
    let pool = db.pool().clone();
    let handle = tokio::runtime::Handle::current();
    let cases = (
        prop::collection::vec(
            (0u64..5, 0u64..5, 0u8..4, -2i8..3, -2i8..3, prop::bool::ANY),
            1..14,
        ),
        prop::collection::vec(0u64..5, 0..2),
        prop::collection::vec(0u64..3, 0..2),
        prop::bool::ANY,
        1u16..5,
        -2i8..3,
    );
    let mut runner = TestRunner::new(Config {
        cases: 12,
        failure_persistence: None,
        ..Config::default()
    });
    let result = tokio::task::block_in_place(|| {
        runner.run(&cases, |(docs, agents, channels, exclude, page, qx)| {
            handle.block_on(async {
                if let Err(error) = truncate(&pool).await {
                    return Err(TestCaseError::fail(format!("truncate: {error}")));
                }
                let (mut subject, _) = subject(pool.clone()).await;
                for (n, (from, to, route, x, y, false_detection)) in docs.iter().enumerate() {
                    let n = u64::try_from(n).unwrap_or(0);
                    let route = match route {
                        3 => Route::Unobserved,
                        k => Route::Channel(channel(u64::from(*k))),
                    };
                    let embedding = vector(f32::from(*x), f32::from(*y), 1.0);
                    index(
                        &mut subject,
                        vec![document(n, *from, *to, route, n, "wiki", Some(embedding))],
                    )
                    .await;
                    if *false_detection {
                        let judged = subject
                            .judge(
                                transmission(n),
                                Some(Verdict::FalseDetection),
                                VerdictRevision::FIRST,
                            )
                            .await;
                        prop_assert!(judged.is_ok());
                    }
                }
                let query = SearchQuery::Semantic(vector(f32::from(qx), 1.0, 0.5));
                let filter = TopologyFilter {
                    agents: agents.iter().copied().map(agent).collect(),
                    channels: channels.iter().copied().map(channel).collect(),
                    false_detections: if exclude {
                        FalseDetections::Exclude
                    } else {
                        FalseDetections::Include
                    },
                    ..TopologyFilter::default()
                };
                let all = flat(
                    &traverse(
                        &subject,
                        &query,
                        None,
                        &TopologyFilter::default(),
                        size(page),
                    )
                    .await
                    .map_err(|error| TestCaseError::fail(format!("{error:?}")))?,
                );
                let filtered = flat(
                    &traverse(&subject, &query, None, &filter, size(page))
                        .await
                        .map_err(|error| TestCaseError::fail(format!("{error:?}")))?,
                );
                let admitted: Vec<(TransmissionId, f32)> = all
                    .into_iter()
                    .filter(|(id, _)| {
                        let n = docs
                            .iter()
                            .enumerate()
                            .find(|(k, _)| transmission(u64::try_from(*k).unwrap_or(0)) == *id)
                            .map(|(_, doc)| *doc);
                        n.is_some_and(|(from, to, route, _, _, false_detection)| {
                            let agent_ok =
                                agents.is_empty() || agents.contains(&from) || agents.contains(&to);
                            let channel_ok = channels.is_empty()
                                || (route != 3 && channels.contains(&u64::from(route)));
                            let verdict_ok = !exclude || !false_detection;
                            from != to && agent_ok && channel_ok && verdict_ok
                        })
                    })
                    .collect();
                prop_assert_eq!(filtered, admitted);
                Ok(())
            })
        })
    });
    if let Err(error) = result {
        panic!("{error}");
    }
}
