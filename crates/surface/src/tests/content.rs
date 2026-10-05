//! Transmission rows by id, search, and the evidence behind a transmission.

use std::num::NonZeroU32;

use crosstalk_memory::analysis::fakes::FakeEmbedder;
use crosstalk_memory::model::build::test_model;
use crosstalk_spec::aggregates::edge::TopologyFilter;
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::transmission::{
    Confirmed, Route, Transmission, TransmissionState,
};
use crosstalk_spec::derived::provenance::span::{Span, SpanState};
use crosstalk_spec::ids::{ExchangeId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::BlobStore;
use crosstalk_spec::interfaces::l6_analysis::corpus::{IndexedTransmission, SearchCorpus};
use crosstalk_spec::interfaces::l8_surface::excerpt::{ExcerptWindow, Excerpted};
use crosstalk_spec::interfaces::l8_surface::lists::{SearchMode, SearchRequest};
use crosstalk_spec::interfaces::l8_surface::summary::{
    SummaryState, TopicUnder, TransmissionSelection,
};
use crosstalk_spec::interfaces::l8_surface::{ConflictKind, QueryApi, QueryError};
use crosstalk_spec::observed::message::{MessageBody, PartRef, encoding};
use crosstalk_spec::paging::{Cursor, PageRequest, SearchList, TransmissionList};
use crosstalk_spec::support::{NonBlank, NonEmpty};
use crosstalk_testkit::build::message::{assistant_text, user_text};
use crosstalk_testkit::build::{ContentMatchBuilder, TransmissionBuilder};

use super::page;
use super::world::{Fixture, Who, minute};

/// INV-703: a traversal lists one row per stored transmission of the
/// selection, newest id first, under one version; unknown ids are left
/// out; the surface's cursors refuse another selection and a forgery.
#[tokio::test]
async fn rows_by_id_follow_the_selection() {
    let fixture = Fixture::new().await;
    let mut scene = fixture.scene().await;
    let caller = fixture.caller(Who::Viewer).await;
    let suspected = match TransmissionBuilder::new(&mut scene.ids)
        .between(scene.a2, scene.a3)
        .suspected()
        .build()
    {
        Ok(transmission) => transmission,
        Err(error) => panic!("{error:?}"),
    };
    let detected = match TransmissionBuilder::new(&mut scene.ids).detected().build() {
        Ok(transmission) => transmission,
        Err(error) => panic!("{error:?}"),
    };
    fixture.transmission(&suspected).await;
    fixture.transmission(&detected).await;
    let unknown = TransmissionId::from_ulid(1);
    let ids = vec![scene.t1.transmission.id, suspected.id, detected.id, unknown];
    let Ok(selection) = TransmissionSelection::new(ids) else {
        panic!("selection");
    };
    let mut request: PageRequest<TransmissionList> = page(1);
    let mut rows = Vec::new();
    let mut versions = Vec::new();
    let mut pages = 0;
    loop {
        let Ok(page) = fixture
            .surface
            .transmissions_by_id(&caller, &selection, TopicVersionSelector::Current, &request)
            .await
        else {
            panic!("rows");
        };
        pages += 1;
        versions.push(page.topic_version);
        let (items, next) = page.page.into_parts();
        rows.extend(items);
        match next {
            Some(next) => request.after = Some(next),
            None => break,
        }
    }
    assert_eq!(pages, 3);
    let listed: Vec<TransmissionId> = rows.iter().map(|row| row.id).collect();
    let mut expected = vec![scene.t1.transmission.id, suspected.id, detected.id];
    expected.sort_by(|a, b| b.cmp(a));
    assert_eq!(listed, expected);
    assert!(
        versions
            .iter()
            .all(|version| *version == TopicModelVersion(0))
    );
    let Some(t1) = rows.iter().find(|row| row.id == scene.t1.transmission.id) else {
        panic!("t1");
    };
    match &t1.state {
        SummaryState::Classified {
            delivery,
            topic,
            verdict,
        } => {
            assert_eq!(delivery.from, scene.a1);
            assert_eq!(*topic, TopicUnder::Outlier);
            assert_eq!(*verdict, None);
        }
        other => panic!("state {other:?}"),
    }
    assert_eq!(t1.route, Route::Channel(scene.c1));

    // A cursor of another selection, and one never issued.
    let Ok(first) = fixture
        .surface
        .transmissions_by_id(&caller, &selection, TopicVersionSelector::Current, &page(1))
        .await
    else {
        panic!("rows");
    };
    let Some(next) = first.page.next().cloned() else {
        panic!("no next");
    };
    let Ok(other) = TransmissionSelection::new(vec![suspected.id]) else {
        panic!("selection");
    };
    let resumed = PageRequest {
        size: page::<TransmissionList>(1).size,
        after: Some(next),
    };
    assert_eq!(
        fixture
            .surface
            .transmissions_by_id(&caller, &other, TopicVersionSelector::Current, &resumed)
            .await,
        Err(QueryError::InvalidCursor)
    );
    let Ok(forged) = Cursor::from_token("00_00".to_owned()) else {
        panic!("token");
    };
    let forged = PageRequest {
        size: page::<TransmissionList>(1).size,
        after: Some(forged),
    };
    assert_eq!(
        fixture
            .surface
            .transmissions_by_id(&caller, &selection, TopicVersionSelector::Current, &forged)
            .await,
        Err(QueryError::InvalidCursor)
    );
    // An unknown pinned version is refused as for a linked view.
    assert_eq!(
        fixture
            .surface
            .transmissions_by_id(
                &caller,
                &selection,
                TopicVersionSelector::Pinned(TopicModelVersion(9)),
                &page(1)
            )
            .await,
        Err(QueryError::NotFound)
    );
}

async fn index_texts(fixture: &Fixture, texts: &[&str]) {
    let embedder = FakeEmbedder::new(test_model("test"), 400);
    let mut corpus = fixture.world.search.clone();
    for (n, text) in texts.iter().enumerate() {
        let Ok(embedding) = embedder.embed_one(text) else {
            panic!("embed");
        };
        let document = IndexedTransmission {
            transmission: TransmissionId::from_ulid(0x5E00 + n as u128),
            from: crosstalk_spec::ids::AgentId::from_ulid(0xA1),
            to: crosstalk_spec::ids::AgentId::from_ulid(0xA2),
            route: Route::Unobserved,
            confirmed_at: minute(1),
            text: (*text).to_owned(),
            embedding: Some(embedding),
        };
        if let Err(error) = corpus.index(document).await {
            panic!("index: {error:?}");
        }
    }
}

fn search(mode: SearchMode, text: &str) -> SearchRequest {
    let Ok(text) = NonBlank::new(text) else {
        panic!("text");
    };
    SearchRequest { mode, text }
}

/// INV-642: a page after the embedding model changed is
/// `Conflict(EmbeddingModelChanged)`, with no hits.
#[tokio::test]
async fn search_page_after_model_change_conflicts() {
    let fixture = Fixture::new().await;
    index_texts(&fixture, &["deploy keys here", "deploy keys there", "keys"]).await;
    let caller = fixture.caller(Who::Reader).await;
    let request = search(SearchMode::Hybrid, "deploy keys");
    let filter = TopologyFilter::default();
    let Ok(first) = fixture
        .surface
        .search(&caller, &request, None, &filter, &page::<SearchList>(1))
        .await
    else {
        panic!("first page");
    };
    let Some(next) = first.page.next().cloned() else {
        panic!("one page");
    };
    let resumed = PageRequest {
        size: page::<SearchList>(1).size,
        after: Some(next),
    };
    // The same model pages on.
    assert!(
        fixture
            .surface
            .search(&caller, &request, None, &filter, &resumed)
            .await
            .is_ok()
    );
    fixture.world.embedder.switch(test_model("other"));
    assert_eq!(
        fixture
            .surface
            .search(&caller, &request, None, &filter, &resumed)
            .await,
        Err(QueryError::Conflict(ConflictKind::EmbeddingModelChanged))
    );
    // A first page under the new model meets the index's old one.
    assert_eq!(
        fixture
            .surface
            .search(&caller, &request, None, &filter, &page(1))
            .await,
        Err(QueryError::Conflict(ConflictKind::EmbeddingModelChanged))
    );
}

/// INV-643: semantic and hybrid searches embed the text once; text
/// searches never do.
#[tokio::test]
async fn search_modes_embed_only_when_needed() {
    let fixture = Fixture::new().await;
    index_texts(&fixture, &["deploy keys"]).await;
    let caller = fixture.caller(Who::Reader).await;
    let filter = TopologyFilter::default();
    for (mode, embeds) in [
        (SearchMode::Text, 0),
        (SearchMode::Semantic, 1),
        (SearchMode::Hybrid, 1),
    ] {
        let before = fixture.world.embedder.embedded();
        let result = fixture
            .surface
            .search(
                &caller,
                &search(mode, "deploy keys"),
                None,
                &filter,
                &page(5),
            )
            .await;
        assert!(result.is_ok(), "{mode:?}: {result:?}");
        assert_eq!(
            fixture.world.embedder.embedded() - before,
            embeds,
            "{mode:?}"
        );
    }
}

/// The evidence cuts both sides of each match from the stored bodies, and
/// reports a dropped body as `BodyDropped`.
#[tokio::test]
async fn evidence_quotes_both_sides_and_reports_dropped_bodies() {
    let fixture = Fixture::new().await;
    let mut scene = fixture.scene().await;
    let caller = fixture.caller(Who::Reader).await;
    let written: MessageBody = assistant_text("please rotate the deploy keys tonight");
    let read: MessageBody = user_text("note: please rotate the deploy keys tonight, thanks");
    let Ok(written_hash) = fixture.world.blobs.put(&encoding::encode(&written)).await else {
        panic!("put");
    };
    let read_hash = encoding::hash(&read);
    let span = scene.ids.span();
    fixture.world.evidence.span(Span {
        id: span,
        location: crosstalk_spec::derived::provenance::span::SpanLocation {
            part: PartRef {
                message: written_hash,
                index: 0,
            },
            range: match crosstalk_spec::support::ByteRange::new(7, 28) {
                Ok(range) => range,
                Err(error) => panic!("{error:?}"),
            },
        },
        agent: scene.a1,
        exchange: ExchangeId::from_ulid(0xE0),
        state: SpanState::Originated,
    });
    let Some(matched) = NonZeroU32::new(21) else {
        panic!("bytes");
    };
    let content = match ContentMatchBuilder::new(&mut scene.ids)
        .from(scene.a1)
        .to(scene.a2)
        .origin(span)
        .part(PartRef {
            message: read_hash,
            index: 0,
        })
        .range(13, matched)
        .matched(matched)
        .build()
    {
        Ok(content) => content,
        Err(error) => panic!("{error:?}"),
    };
    let Ok(confirmed) = Confirmed::new(NonEmpty::new(content), vec![scene.t1.co_access], minute(1))
    else {
        panic!("confirmed");
    };
    let transmission = Transmission {
        id: scene.ids.transmission(),
        to: scene.a2,
        route: Route::Channel(scene.c1),
        opened_at: minute(1),
        state: TransmissionState::Confirmed(confirmed),
    };
    let mut store = fixture.world.transmissions.clone();
    use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
    assert!(store.save(transmission.clone()).await.is_ok());
    fixture.world.evidence.access(scene.t1.write.clone());
    fixture.world.evidence.access(scene.t1.read.clone());
    fixture.world.evidence.resource(scene.r1.clone());

    let Ok(Some(evidence)) = fixture
        .surface
        .transmission_evidence(&caller, transmission.id, ExcerptWindow::MATCH_ONLY)
        .await
    else {
        panic!("evidence");
    };
    assert_eq!(evidence.matches().len(), 1);
    match evidence.matches()[0].origin() {
        Excerpted::Shown(excerpt) => assert_eq!(excerpt.matched(), "rotate the deploy key"),
        other => panic!("origin {other:?}"),
    }
    assert_eq!(
        *evidence.matches()[0].read(),
        Excerpted::BodyDropped { message: read_hash }
    );
    let agents: Vec<_> = evidence
        .accesses()
        .iter()
        .map(|detail| detail.agent())
        .collect();
    assert_eq!(agents, [scene.a1, scene.a2]);

    // A missing span record is a store fault, reported as `Store`.
    let other_span = TransmissionBuilder::new(&mut scene.ids)
        .between(scene.a1, scene.a2)
        .confirmed()
        .build();
    let Ok(other_span) = other_span else {
        panic!("transmission");
    };
    assert!(store.save(other_span.clone()).await.is_ok());
    assert!(matches!(
        fixture
            .surface
            .transmission_evidence(&caller, other_span.id, ExcerptWindow::DEFAULT)
            .await,
        Err(QueryError::Store { .. })
    ));
    assert_eq!(
        fixture
            .surface
            .transmission_evidence(
                &caller,
                TransmissionId::from_ulid(3),
                ExcerptWindow::DEFAULT
            )
            .await,
        Ok(None)
    );
}

/// INV-1072: a row's topic under a version is the catalog's stored
/// assignment under it, whatever version the transmission's own state was
/// classified under; with no assignment under the version, its stored
/// classification decides. Rows by id and a channel's transmissions agree.
#[tokio::test]
async fn row_topics_are_the_catalogs_assignments_under_the_version() {
    use crosstalk_memory::model::build::topic_id;
    use crosstalk_spec::interfaces::l6_analysis::lifecycle::{StoredAssignment, TopicLifecycle};
    use crosstalk_spec::interfaces::l8_surface::channel_traffic::ChannelTransmissionFilter;
    use crosstalk_spec::paging::ChannelTransmissionList;

    let fixture = Fixture::new().await;
    let scene = fixture.scene().await;
    let caller = fixture.caller(Who::Viewer).await;
    let t1 = &scene.t1.transmission;
    let Some(confirmed) = t1.state.confirmed() else {
        panic!("t1 is confirmed");
    };
    // A re-fit assigns t1 under v1 without reclassifying its stored state
    // (classified under v0).
    let v1 = fixture.fit(minute(30), &[7], true).await;
    let mut catalog = fixture.world.catalog.clone();
    let assigned = catalog
        .assign(
            t1.id,
            v1,
            StoredAssignment {
                topic: Some(topic_id(7)),
                confirmed_at: confirmed.at(),
                matched_bytes: confirmed.matched_bytes(),
                from: confirmed.from(),
                to: t1.to,
            },
        )
        .await;
    assert!(assigned.is_ok(), "{assigned:?}");
    let Ok(selection) = TransmissionSelection::new(vec![t1.id]) else {
        panic!("selection");
    };
    let topic_of = |page: crosstalk_spec::interfaces::l8_surface::summary::TransmissionPage| {
        page.page.items().first().and_then(|row| row.state.topic())
    };
    for (version, expected) in [
        (v1, TopicUnder::Topic(topic_id(7))),
        (TopicModelVersion(0), TopicUnder::Outlier),
    ] {
        let rows = fixture
            .surface
            .transmissions_by_id(
                &caller,
                &selection,
                TopicVersionSelector::Pinned(version),
                &page(10),
            )
            .await;
        let Ok(rows) = rows else {
            panic!("rows under {version:?}: {rows:?}");
        };
        assert_eq!(topic_of(rows), Some(expected), "rows under {version:?}");
        let listed = fixture
            .surface
            .channel_transmissions(
                &caller,
                scene.c1,
                &ChannelTransmissionFilter::default(),
                TopicVersionSelector::Pinned(version),
                &page::<ChannelTransmissionList>(10),
            )
            .await;
        let Ok(listed) = listed else {
            panic!("channel transmissions under {version:?}: {listed:?}");
        };
        let topic = listed
            .page
            .items()
            .iter()
            .find(|row| row.summary().id == t1.id)
            .and_then(|row| row.summary().state.topic());
        assert_eq!(topic, Some(expected), "channel rows under {version:?}");
    }
}
