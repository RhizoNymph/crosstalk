//! `export`: every dataset streams between a header and a trailer that
//! `verify_export` accepts, re-runs reproduce the rows and digest, the
//! permission and the limits are checked before anything is read, and every
//! call is audited.

use std::num::NonZeroU64;

use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::projection::ProjectionStatusKind;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l8_surface::audit::{AuditBody, AuditFilter, AuditSubject};
use crosstalk_spec::interfaces::l8_surface::excerpt::Excerpted;
use crosstalk_spec::interfaces::l8_surface::export::{
    Export, ExportBasis, ExportDataset, ExportEvent, ExportFormat, ExportHeader, ExportLimits,
    ExportRequest, ExportRow, ExportScope, ExportStep, ExportStream, ExportTrailer, verify_export,
};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionSelection;
use crosstalk_spec::interfaces::l8_surface::{
    Caller, ConflictKind, InputError, Permission, QueryError,
};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use super::super::FixtureBackend;
use super::super::clock::{DAY, MINUTE, NOW, START, WATERMARK, ago, plus};
use super::super::export::ExportRows;
use super::super::export::digest::RowDigest;
use super::{caller, collect, day, first, fresh, researcher, week};
use crate::url::scope::Scope;
use crosstalk_spec::interfaces::l8_surface::QueryApi;

fn scope(scope: &Scope) -> ExportScope {
    ExportScope {
        window: scope.window,
        filter: scope.topology_filter(),
    }
}

fn request(dataset: ExportDataset, content: bool) -> ExportRequest {
    ExportRequest::new(dataset, ExportFormat::Jsonl, content).expect("request")
}

/// Reads an export to its trailer.
async fn drain(export: Export<ExportRows>) -> (ExportHeader, Vec<ExportRow>, ExportTrailer) {
    let Export { header, mut rows } = export;
    let mut out = Vec::new();
    loop {
        match rows.next().await {
            ExportStep::Row(row, rest) => {
                out.push(row);
                rows = rest;
            }
            ExportStep::End(trailer) => return (header, out, trailer),
        }
    }
}

async fn run(
    b: &FixtureBackend,
    c: &Caller,
    request: &ExportRequest,
) -> (ExportHeader, Vec<ExportRow>, ExportTrailer) {
    drain(b.export(c, request).await.expect("export starts")).await
}

/// The export verifies, and its header, rows and trailer agree.
fn verified(header: &ExportHeader, rows: &[ExportRow], trailer: &ExportTrailer) {
    assert_eq!(
        verify_export(header, rows, Some(trailer), RowDigest::new()),
        Ok(())
    );
    assert!(trailer.is_complete());
    assert_eq!(trailer.export(), header.id());
    assert_eq!(header.rows(), trailer.rows());
    assert_eq!(u64::try_from(rows.len()).ok(), Some(header.rows()));
}

#[tokio::test]
async fn a_transmissions_export_verifies_and_its_rows_are_the_surfaces() {
    let b = &fresh();
    let c = researcher();
    let day = day();
    let (header, rows, trailer) = run(
        b,
        &c,
        &request(ExportDataset::Transmissions(scope(&day)), false),
    )
    .await;
    verified(&header, &rows, &trailer);
    assert!(rows.len() > 100, "a day of confirmed transmissions");
    assert_eq!(header.by(), c.operator());
    assert_eq!(header.started_at(), NOW);
    assert_eq!(header.watermark().at(), WATERMARK);
    let ExportBasis::Scoped {
        topic_version,
        settled,
        ..
    } = header.basis()
    else {
        panic!("a scoped basis");
    };
    assert_eq!(*topic_version, TopicModelVersion(2));
    let settled = settled.expect("settled");
    assert_eq!(
        settled.end(),
        WATERMARK,
        "the window is cut at the watermark"
    );

    // Each row is the summary `transmissions_by_id` lists for it.
    let mut ids = Vec::new();
    for row in &rows {
        let ExportRow::Transmission(row) = row else {
            panic!("transmission rows only");
        };
        assert!(settled.contains(row.delivery().confirmed_at));
        assert!(row.content().is_none());
        ids.push(row.summary().id);
    }
    let some: Vec<_> = ids.iter().take(50).copied().collect();
    let selection = TransmissionSelection::new(some.clone()).expect("selection");
    let listed = collect(50, async |page| {
        b.transmissions_by_id(
            &c,
            &selection,
            TopicVersionSelector::Pinned(TopicModelVersion(2)),
            &page,
        )
        .await
        .map(|p| p.page)
    })
    .await;
    for summary in listed {
        let row = rows
            .iter()
            .find_map(|row| match row {
                ExportRow::Transmission(row) if row.summary().id == summary.id => Some(row),
                _ => None,
            })
            .expect("listed row exported");
        assert_eq!(row.summary(), &summary);
    }
}

#[tokio::test]
async fn a_re_run_reproduces_the_rows_count_and_digest() {
    let b = fresh();
    let c = researcher();
    let request = request(ExportDataset::Edges(scope(&day())), true);
    let (first_header, first_rows, first_trailer) = run(&b, &c, &request).await;
    let (second_header, second_rows, second_trailer) = run(&b, &c, &request).await;
    verified(&first_header, &first_rows, &first_trailer);
    assert_ne!(first_header.id(), second_header.id(), "two exports");
    assert_eq!(first_header.basis(), second_header.basis());
    assert_eq!(first_rows, second_rows);
    assert_eq!(first_trailer.digest(), second_trailer.digest());
    assert_eq!(first_trailer.rows(), second_trailer.rows());
}

#[tokio::test]
async fn every_dataset_verifies() {
    let b = fresh();
    let c = researcher();
    let week = week();
    let ready = collect(10, async |page| b.projections(&c, &page).await)
        .await
        .into_iter()
        .find(|info| info.status().kind() == ProjectionStatusKind::Ready);
    let projection = match ready {
        Some(info) => info.id(),
        None => b
            .fit_projection(
                &c,
                week.window,
                &week.topology_filter(),
                super::reads_support::params(3, 400),
            )
            .await
            .expect("fit"),
    };
    let datasets = [
        ExportDataset::Transmissions(scope(&week)),
        ExportDataset::Edges(scope(&day())),
        ExportDataset::Accesses(scope(&day())),
        ExportDataset::Topics(scope(&week)),
        ExportDataset::Projection(projection),
        ExportDataset::Verdicts(week.window),
    ];
    for dataset in datasets {
        let kind = dataset.kind();
        for content in [false, true] {
            let Ok(request) = ExportRequest::new(dataset.clone(), ExportFormat::Jsonl, content)
            else {
                assert!(content && !kind.has_content_columns());
                continue;
            };
            let (header, rows, trailer) = run(&b, &c, &request).await;
            verified(&header, &rows, &trailer);
            assert!(!rows.is_empty(), "{kind:?} has rows");
            assert!(rows.iter().all(|row| row.kind() == kind));
            assert!(rows.iter().all(|row| row.has_content() == content));
        }
    }
}

#[tokio::test]
async fn topic_rows_count_every_topic_of_the_version() {
    let b = &fresh();
    let c = researcher();
    let (header, rows, trailer) =
        run(b, &c, &request(ExportDataset::Topics(scope(&week())), true)).await;
    verified(&header, &rows, &trailer);
    let sizes = b
        .topic_sizes(&c, Some(TopicModelVersion(2)), None)
        .await
        .expect("sizes")
        .value;
    assert_eq!(rows.len(), sizes.topics().len(), "one row per topic");
    for row in &rows {
        let ExportRow::Topic(row) = row else {
            panic!("topic rows");
        };
        assert!(row.content.as_ref().is_some_and(|c| !c.label.is_empty()));
    }
}

#[tokio::test]
async fn content_quotes_mark_bodies_retention_dropped() {
    let b = &fresh();
    let c = researcher();
    let dropped: Vec<_> = b.world.scenario.dropped.iter().map(|(id, _)| *id).collect();
    assert!(!dropped.is_empty());
    let (header, rows, trailer) = run(
        b,
        &c,
        &request(ExportDataset::Transmissions(scope(&week())), true),
    )
    .await;
    verified(&header, &rows, &trailer);
    let mut marked = 0;
    for row in &rows {
        let ExportRow::Transmission(row) = row else {
            panic!("transmission rows");
        };
        let content = row.content().expect("content");
        let gone = content.matches.iter().any(|text| {
            matches!(text.quotes.origin, Excerpted::BodyDropped { .. })
                || matches!(text.quotes.read, Excerpted::BodyDropped { .. })
        });
        assert_eq!(gone, dropped.contains(&row.summary().id));
        marked += usize::from(gone);
        for text in content.matches.iter() {
            for quote in [&text.quotes.origin, &text.quotes.read] {
                if let Excerpted::Shown(excerpt) = quote {
                    assert!(excerpt.before().is_empty(), "no context");
                    assert!(excerpt.after().is_empty(), "no context");
                    assert!(excerpt.matched().len() <= 8192, "capped at 8 KiB");
                }
            }
        }
    }
    assert_eq!(marked, dropped.len());
}

#[tokio::test]
async fn a_window_after_the_watermark_settles_nothing() {
    let b = &fresh();
    let c = researcher();
    let late = TimeWindow::new(WATERMARK, NOW).expect("window");
    let filter = day().topology_filter();
    let scoped = |window| ExportScope {
        window,
        filter: filter.clone(),
    };
    let (header, rows, trailer) = run(
        b,
        &c,
        &request(ExportDataset::Transmissions(scoped(late)), false),
    )
    .await;
    verified(&header, &rows, &trailer);
    assert!(rows.is_empty());
    assert!(matches!(
        header.basis(),
        ExportBasis::Scoped { settled: None, .. }
    ));
    let (_, topics, _) = run(b, &c, &request(ExportDataset::Topics(scoped(late)), false)).await;
    assert!(!topics.is_empty(), "every topic, counted zero");
    assert!(topics.iter().all(|row| matches!(
        row,
        ExportRow::Topic(topic) if topic.transmissions == 0 && topic.matched_bytes == 0
    )));
}

#[tokio::test]
async fn content_and_projections_need_the_content_permission() {
    let b = fresh();
    let viewer = caller(&[Permission::View]);
    let content = request(ExportDataset::Transmissions(scope(&day())), true);
    let projection = request(ExportDataset::Projection(ProjectionId::from_ulid(1)), false);
    for request in [&content, &projection] {
        assert_eq!(
            b.export(&viewer, request).await.err(),
            Some(QueryError::Forbidden {
                missing: Permission::Content
            })
        );
    }
    let triage = caller(&[Permission::Triage]);
    let plain = request(ExportDataset::Verdicts(day().window), false);
    assert_eq!(
        b.export(&triage, &plain).await.err(),
        Some(QueryError::Forbidden {
            missing: Permission::View
        })
    );
    assert!(b.export(&viewer, &plain).await.is_ok(), "View suffices");
}

#[tokio::test]
async fn refusals_before_anything_is_sent() {
    let b = fresh().with_export_limits(ExportLimits::new(NonZeroU64::MIN));
    let c = researcher();
    let day = day();
    assert!(matches!(
        b.export(&c, &request(ExportDataset::Transmissions(scope(&day)), false))
            .await
            .err(),
        Some(QueryError::Conflict(ConflictKind::ExportTooLarge { limit: 1, rows })) if rows > 1
    ));
    let unaligned = TimeWindow::new(plus(ago(DAY), MINUTE), NOW).expect("window");
    for dataset in [
        ExportDataset::Edges(ExportScope {
            window: unaligned,
            filter: day.topology_filter(),
        }),
        ExportDataset::Accesses(ExportScope {
            window: unaligned,
            filter: day.topology_filter(),
        }),
    ] {
        assert_eq!(
            b.export(&c, &request(dataset, false)).await.err(),
            Some(QueryError::InvalidInput(InputError::UnalignedWindow))
        );
    }
    let parquet = ExportRequest::new(
        ExportDataset::Verdicts(day.window),
        ExportFormat::Parquet,
        false,
    )
    .expect("request");
    assert!(matches!(
        b.export(&c, &parquet).await.err(),
        Some(QueryError::Store { reason }) if reason.contains("Parquet")
    ));
    let unknown = request(ExportDataset::Projection(ProjectionId::from_ulid(9)), false);
    assert_eq!(
        b.export(&c, &unknown).await.err(),
        Some(QueryError::NotFound)
    );
    let mut pinned = day.topology_filter();
    pinned.topic_version = TopicVersionSelector::Pinned(TopicModelVersion(0));
    assert_eq!(
        b.export(
            &c,
            &request(
                ExportDataset::Transmissions(ExportScope {
                    window: day.window,
                    filter: pinned,
                }),
                false
            )
        )
        .await
        .err(),
        Some(QueryError::VersionNotRetained {
            version: TopicModelVersion(0)
        })
    );
}

/// The export events in the audit log, in append order.
async fn events(b: &FixtureBackend) -> Vec<ExportEvent> {
    b.state
        .read()
        .await
        .audit
        .entries()
        .iter()
        .filter_map(|entry| match &entry.body {
            AuditBody::Export(record) => Some(record.event().clone()),
            AuditBody::Operator(_) | AuditBody::Config(_) => None,
        })
        .collect()
}

#[tokio::test]
async fn every_export_is_audited() {
    let b = fresh();
    let c = researcher();
    assert!(events(&b).await.is_empty(), "the world holds no exports");

    // Started before the header is returned, Ended with the trailer.
    let request_day = request(ExportDataset::Verdicts(day().window), false);
    let export = b.export(&c, &request_day).await.expect("export");
    let id = export.header.id();
    assert!(matches!(
        events(&b).await.as_slice(),
        [ExportEvent::Started(header)] if header.id() == id
    ));
    let (_, rows, trailer) = drain(export).await;
    assert!(matches!(
        events(&b).await.as_slice(),
        [ExportEvent::Started(_), ExportEvent::Ended(ended)] if *ended == trailer
    ));

    // A refusal is one entry, exactly what `export` returned.
    let viewer = caller(&[Permission::View]);
    let refused = request(ExportDataset::Transmissions(scope(&day())), true);
    let error = b.export(&viewer, &refused).await.expect_err("refused");
    assert!(matches!(
        events(&b).await.last(),
        Some(ExportEvent::Refused(recorded)) if *recorded == error
    ));

    // Dropped before its trailer: abandoned after the rows sent.
    let export = b
        .export(
            &c,
            &request(ExportDataset::Transmissions(scope(&day())), false),
        )
        .await
        .expect("export");
    let abandoned = export.header.id();
    let ExportStep::Row(_, rest) = export.rows.next().await else {
        panic!("a first row");
    };
    let ExportStep::Row(_, rest) = rest.next().await else {
        panic!("a second row");
    };
    drop(rest);
    assert!(matches!(
        events(&b).await.last(),
        Some(ExportEvent::Abandoned { export, rows: 2 }) if *export == abandoned
    ));

    // The log's subject filter finds an export's start and end together.
    let filter = AuditFilter {
        subject: Some(AuditSubject::Export(id)),
        ..AuditFilter::default()
    };
    let found = b.audit(&c, &filter, &first(10)).await.expect("audit");
    assert_eq!(found.items().len(), 2);
    assert!(!rows.is_empty());
}

#[tokio::test]
async fn a_verdict_export_reproduces_the_detection_quality_tally() {
    let b = &fresh();
    let c = researcher();
    let week = week();
    let (header, rows, trailer) =
        run(b, &c, &request(ExportDataset::Verdicts(week.window), false)).await;
    verified(&header, &rows, &trailer);
    let settled = TimeWindow::new(START, WATERMARK).expect("settled");
    let logs: usize = b
        .state
        .read()
        .await
        .verdicts
        .iter()
        .filter(|(id, _)| {
            b.world.tx(**id).is_some_and(|record| {
                settled.contains(record.transmission.opened_at)
                    && record.transmission.state.judgeable().is_ok()
            })
        })
        .map(|(_, log)| log.records().len())
        .sum();
    assert_eq!(rows.len(), logs);
    let _: Timestamp = header.started_at();
}

#[tokio::test]
async fn a_week_of_access_buckets_is_over_the_fixture_limit() {
    let b = &fresh();
    let refused = b
        .export(
            &researcher(),
            &request(ExportDataset::Accesses(scope(&week())), false),
        )
        .await
        .err();
    assert!(matches!(
        refused,
        Some(QueryError::Conflict(ConflictKind::ExportTooLarge { rows, limit }))
            if limit == super::super::export::MAX_ROWS.get() && rows > limit
    ));
}
