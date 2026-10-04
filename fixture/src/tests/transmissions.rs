//! Transmission reads: the rows behind an edge and a selection, the
//! evidence behind one transmission (excerpts from stored bodies, dropped
//! bodies, accesses), verdict logs, search and detection quality.

use std::collections::HashSet;

use crosstalk_spec::aggregates::edge::{EdgeSelector, Weighting};
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::quality::{DetectionQuality, MatchClass, QualityMatch};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::interfaces::l8_surface::excerpt::{ExcerptWindow, Excerpted};
use crosstalk_spec::interfaces::l8_surface::lists::SearchMode;
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionSelection;

use super::{collect, first, graph_of, researcher, shared, week};
use crosstalk_spec::interfaces::l8_surface::QueryApi;

use super::reads_support::*;

#[tokio::test]
async fn edge_and_id_selectors() {
    let b = shared();
    let c = researcher();
    let scope = week();
    let view = graph_of(b, &c, &scope, Weighting::Transmissions)
        .await
        .expect("topology");
    let edge = view
        .value
        .edges
        .iter()
        .max_by_key(|e| e.stats.transmissions)
        .expect("edge");
    let selector = EdgeSelector::new(edge.from, edge.to, edge.route.clone()).expect("edge");
    let filter = scope.topology_filter();
    let listed = b
        .edge_transmissions(&c, &selector, scope.window, &filter, &first(BIG))
        .await
        .expect("edge rows");
    assert_eq!(listed.watermark, super::super::queries::graph::watermark());
    assert_eq!(listed.value.topic_version, view.value.topic_version);
    let rows = listed.value.page.into_parts().0;
    // Exactly what the edge counts: as many rows, the same bytes.
    assert_eq!(rows.len() as u64, edge.stats.transmissions.get());
    assert_eq!(
        rows.iter().map(|r| r.matched_bytes.get()).sum::<u64>(),
        edge.stats.matched_bytes.get()
    );
    assert!(
        rows.windows(2)
            .all(|w| w[0].confirmed_at >= w[1].confirmed_at),
        "newest confirmation first"
    );
    // Paged in small steps, the same rows.
    let paged = collect(7, async |p| {
        b.edge_transmissions(&c, &selector, scope.window, &filter, &p)
            .await
            .map(|w| w.value.page)
    })
    .await;
    assert_eq!(paged, rows);
    let ids: Vec<_> = rows.iter().take(5).map(|r| r.transmission).collect();
    let selection = TransmissionSelection::new(ids.clone()).expect("selection");
    let picked = b
        .transmissions_by_id(&c, &selection, TopicVersionSelector::Current, &first(BIG))
        .await
        .expect("ids");
    assert_eq!(picked.topic_version, TopicModelVersion(2));
    let picked = picked.page.into_parts().0;
    let got: HashSet<_> = picked.iter().map(|t| t.id).collect();
    assert_eq!(got, ids.into_iter().collect());
    assert!(picked.iter().all(|t| {
        t.state.delivery().map(|d| d.from) == Some(edge.from)
            && t.to == edge.to
            && t.route == edge.route
    }));
    // An edge whose ends resolve to one agent counts nothing.
    let alias = agent("al0");
    let canonical = b.state.read().await.identity.canonical(alias);
    assert_ne!(alias, canonical);
    let self_edge = EdgeSelector::new(alias, canonical, Route::Unobserved).expect("two ids");
    assert!(
        b.edge_transmissions(&c, &self_edge, scope.window, &filter, &first(5))
            .await
            .expect("rows")
            .value
            .page
            .items()
            .is_empty()
    );
    // Unknown ids are left out; an empty selection is refused before the call.
    let unknown =
        TransmissionSelection::new(vec![crosstalk_spec::ids::TransmissionId::from_ulid(1)])
            .expect("selection");
    assert!(
        b.transmissions_by_id(&c, &unknown, TopicVersionSelector::Current, &first(5))
            .await
            .expect("rows")
            .page
            .items()
            .is_empty()
    );
    assert_eq!(
        TransmissionSelection::new(Vec::new()).map_err(QueryError::from),
        Err(QueryError::InvalidInput(
            crosstalk_spec::interfaces::l8_surface::InputError::EmptySelection
        ))
    );
}

#[tokio::test]
async fn evidence_carries_excerpts_accesses_and_verdicts() {
    let b = shared();
    let c = researcher();
    let record = b
        .world
        .transmissions
        .iter()
        .rev()
        .find(|t| t.is_confirmed() && matches!(t.transmission.route, Route::Channel(_)))
        .expect("channel transmission");
    let evidence = b
        .transmission_evidence(&c, record.transmission.id, ExcerptWindow::DEFAULT)
        .await
        .expect("ok")
        .expect("found");
    assert_eq!(evidence.transmission(), &record.transmission);
    assert!(!evidence.matches().is_empty());
    for m in evidence.matches() {
        for side in [m.origin(), m.read()] {
            let Excerpted::Shown(excerpt) = side else {
                panic!("a recent body is kept: {side:?}");
            };
            assert!(!excerpt.matched().is_empty());
            assert!(excerpt.before().len() <= 256 && excerpt.after().len() <= 256);
        }
        let Excerpted::Shown(read) = m.read() else {
            panic!("shown");
        };
        assert_eq!(
            read.part_len(),
            read.elided_before() + read.text().len() as u64 + read.elided_after(),
            "the excerpt accounts for its whole part"
        );
        assert_eq!(
            read.matched().len() as u32,
            m.content_match().read_at().range.len().get()
        );
    }
    assert_eq!(evidence.accesses().len(), 2, "the write and the read");
    assert_eq!(
        b.transmission(&c, record.transmission.id).await,
        Ok(Some(record.transmission.clone()))
    );
    let judged = *b
        .state
        .read()
        .await
        .verdicts
        .keys()
        .next()
        .expect("verdict");
    let log = b.verdicts(&c, judged).await.expect("ok").expect("found");
    assert!(!log.records().is_empty());
    assert_eq!(log.transmission(), judged);
    let unjudged = {
        let state = b.state.read().await;
        b.world
            .transmissions
            .iter()
            .map(|t| t.transmission.id)
            .find(|id| !state.verdicts.contains_key(id))
            .expect("an unjudged transmission")
    };
    let empty = b.verdicts(&c, unjudged).await.expect("ok").expect("found");
    assert!(empty.records().is_empty());
    let unknown = crosstalk_spec::ids::TransmissionId::from_ulid(1);
    assert_eq!(
        b.transmission_evidence(&c, unknown, ExcerptWindow::DEFAULT)
            .await,
        Ok(None)
    );
    assert_eq!(b.verdicts(&c, unknown).await, Ok(None));
    assert_eq!(b.transmission(&c, unknown).await, Ok(None));
}

#[tokio::test]
async fn dropped_bodies_are_reported_on_their_side_only() {
    use super::super::world::BodySide;

    let b = shared();
    let c = researcher();
    let dropped = &b.world.scenario.dropped;
    assert!(dropped.len() >= 4);
    assert!(dropped.iter().any(|(_, side)| *side == BodySide::Sender));
    assert!(dropped.iter().any(|(_, side)| *side == BodySide::Reader));
    for (id, side) in dropped {
        let evidence = b
            .transmission_evidence(&c, *id, ExcerptWindow::DEFAULT)
            .await
            .expect("still readable")
            .expect("found");
        assert!(!evidence.matches().is_empty());
        for m in evidence.matches() {
            let (gone, kept) = match side {
                BodySide::Sender => (m.origin(), m.read()),
                BodySide::Reader => (m.read(), m.origin()),
            };
            assert!(matches!(gone, Excerpted::BodyDropped { .. }), "{gone:?}");
            assert!(matches!(kept, Excerpted::Shown(_)), "{kept:?}");
        }
    }
    // The rest of the world keeps its bodies.
    let kept = b
        .world
        .transmissions
        .iter()
        .rev()
        .find(|t| t.is_confirmed())
        .expect("confirmed");
    let evidence = b
        .transmission_evidence(&c, kept.transmission.id, ExcerptWindow::MATCH_ONLY)
        .await
        .expect("ok")
        .expect("found");
    for m in evidence.matches() {
        let Excerpted::Shown(origin) = m.origin() else {
            panic!("kept");
        };
        assert!(origin.before().is_empty() && origin.after().is_empty());
    }
}

#[tokio::test]
async fn text_search_is_a_case_insensitive_substring() {
    let b = shared();
    let c = researcher();
    let lower = collect(100, async |p| {
        search_in(b, &c, &search("rollback", SearchMode::Text), &week(), &p).await
    })
    .await;
    let upper = collect(100, async |p| {
        search_in(b, &c, &search("ROLLBACK", SearchMode::Text), &week(), &p).await
    })
    .await;
    assert!(!lower.is_empty());
    assert_eq!(lower, upper);
    // Without a window, every confirmation counts: a superset, same scores.
    let request = search("rollback", SearchMode::Text);
    let unbounded = collect(100, async |p| {
        b.search(&c, &request, None, &week().topology_filter(), &p)
            .await
            .map(|r| r.page)
    })
    .await;
    assert!(lower.iter().all(|hit| unbounded.contains(hit)));
    for hit in &lower {
        let record = b.world.tx(hit.transmission).expect("record");
        let found = record.texts.iter().any(|t| {
            [&t.origin, &t.read]
                .iter()
                .any(|text| text.to_lowercase().contains("rollback"))
        });
        assert!(found);
        assert!(
            hit.snippet.to_lowercase().contains("rollback"),
            "{}",
            hit.snippet
        );
    }
    let semantic = search_in(
        b,
        &c,
        &search("api key token", SearchMode::Semantic),
        &week(),
        &first(10),
    )
    .await
    .expect("semantic")
    .into_parts()
    .0;
    assert!(!semantic.is_empty());
    let top = b.world.tx(semantic[0].transmission).expect("record");
    assert_eq!(top.theme, super::super::text::Theme::Credentials);
}

#[tokio::test]
async fn detection_quality_counts_each_judgeable_transmission() {
    let b = shared();
    let quality = b
        .detection_quality(&researcher(), week().window)
        .await
        .expect("quality");
    let rows = quality.rows();
    assert!(rows.iter().any(|r| r.genuine > 0));
    assert!(rows.iter().any(|r| r.false_detection > 0));
    let kinds: HashSet<_> = rows.iter().map(|r| r.match_kind).collect();
    for class in [
        MatchClass::Exact,
        MatchClass::Normalized,
        MatchClass::Decoded,
        MatchClass::Semantic,
    ] {
        assert!(kinds.contains(&QualityMatch::Content(class)), "{class:?}");
    }
    assert!(kinds.contains(&QualityMatch::Suspected));
    assert!(kinds.contains(&QualityMatch::Discarded));
    // Exactly the reference tally over the stored transmissions.
    let state = b.state.read().await;
    let ctx = super::super::queries::Ctx::new(&b.world, &state);
    let tally = DetectionQuality::tally(
        week().window,
        b.world
            .transmissions
            .iter()
            .map(|t| (&t.transmission, ctx.verdict(t.transmission.id))),
    );
    assert_eq!(quality, tally);
    let judgeable = b
        .world
        .transmissions
        .iter()
        .filter(|t| t.transmission.state.judgeable().is_ok())
        .count() as u64;
    assert_eq!(rows.iter().map(|r| r.total()).sum::<u64>(), judgeable);
}
