//! Exports: refusals before anything is read, the stream between header and
//! trailer, and the audit entries of each call.

use std::num::NonZeroU64;

use crosstalk_spec::aggregates::edge::TopologyFilter;
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l8_surface::audit::AuditBody;
use crosstalk_spec::interfaces::l8_surface::export::{
    ExportDataset, ExportEvent, ExportFormat, ExportLimits, ExportRecord, ExportRequest, ExportRow,
    ExportScope, ExportStep, ExportStream, ExportTrailer, verify_export,
};
use crosstalk_spec::interfaces::l8_surface::{
    ConflictKind, InputError, Permission, QueryApi, QueryError,
};

use super::world::{Fixture, Who, config, minute, minutes};
use crate::export::Blake3RowHasher;

fn accesses(format: ExportFormat) -> ExportRequest {
    let scope = ExportScope {
        window: minutes(0, 10),
        filter: TopologyFilter::default(),
    };
    match ExportRequest::new(ExportDataset::Accesses(scope), format, false) {
        Ok(request) => request,
        Err(error) => panic!("request: {error:?}"),
    }
}

async fn export_records(fixture: &Fixture) -> Vec<ExportRecord> {
    fixture
        .audit_entries()
        .await
        .into_iter()
        .filter_map(|entry| match entry.body {
            AuditBody::Export(record) => Some(record),
            AuditBody::Operator(_) | AuditBody::Config(_) => None,
        })
        .collect()
}

async fn drain<S: ExportStream>(mut stream: S) -> (Vec<ExportRow>, ExportTrailer) {
    let mut rows = Vec::new();
    loop {
        match stream.next().await {
            ExportStep::Row(row, rest) => {
                rows.push(row);
                stream = rest;
            }
            ExportStep::End(trailer) => return (rows, trailer),
        }
    }
}

/// An access export streams its settled rows between a header and a
/// complete trailer that verifies, and is audited as started and ended.
#[tokio::test]
async fn access_export_streams_verifies_and_is_audited() {
    let fixture = Fixture::new().await;
    fixture.scene().await;
    let watermark = fixture.watermark(minute(5)).await;
    let caller = fixture.caller(Who::Viewer).await;
    let request = accesses(ExportFormat::Jsonl);
    let export = match fixture.surface.export(&caller, &request).await {
        Ok(export) => export,
        Err(error) => panic!("export: {error:?}"),
    };
    assert_eq!(export.header.watermark(), watermark);
    assert_eq!(export.header.rows(), 2);
    let header = export.header.clone();
    let (rows, trailer) = drain(export.rows).await;
    assert!(trailer.is_complete(), "{trailer:?}");
    assert_eq!(
        verify_export(&header, rows.iter(), Some(&trailer), Blake3RowHasher::new()),
        Ok(())
    );
    let records = export_records(&fixture).await;
    assert_eq!(records.len(), 2);
    assert!(matches!(records[0].event(), ExportEvent::Ended(ended) if *ended == trailer));
    assert!(matches!(records[1].event(), ExportEvent::Started(started) if **started == header));
}

/// A stream dropped before its trailer is audited as abandoned with the
/// rows it handed out.
#[tokio::test]
async fn abandoned_export_is_audited() {
    let fixture = Fixture::new().await;
    fixture.scene().await;
    fixture.watermark(minute(5)).await;
    let caller = fixture.caller(Who::Viewer).await;
    let export = match fixture
        .surface
        .export(&caller, &accesses(ExportFormat::Jsonl))
        .await
    {
        Ok(export) => export,
        Err(error) => panic!("export: {error:?}"),
    };
    let id = export.header.id();
    match export.rows.next().await {
        ExportStep::Row(_, rest) => drop(rest),
        ExportStep::End(trailer) => panic!("ended at once: {trailer:?}"),
    }
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    let records = export_records(&fixture).await;
    assert!(
        matches!(records[0].event(), ExportEvent::Abandoned { export, rows } if *export == id && *rows == 1),
        "{:?}",
        records[0].event()
    );
}

/// INV-788 (`surface.export.unsupported-format-refused`): a format the
/// gateway does not write is refused after the permission check, and the
/// refusal is audited.
#[tokio::test]
async fn export_refuses_an_unwritten_format_before_reading() {
    let fixture = Fixture::new().await;
    fixture.scene().await;
    assert!(!config().export_formats.offers(ExportFormat::Parquet));
    let viewer = fixture.caller(Who::Viewer).await;
    let refused = QueryError::InvalidInput(InputError::UnsupportedFormat {
        format: ExportFormat::Parquet,
    });
    assert_eq!(
        fixture
            .surface
            .export(&viewer, &accesses(ExportFormat::Parquet))
            .await
            .err(),
        Some(refused.clone())
    );
    let records = export_records(&fixture).await;
    assert_eq!(records.len(), 1);
    assert_eq!(*records[0].event(), ExportEvent::Refused(refused));
    // The permission check comes first.
    let operator = fixture.caller(Who::Operator).await;
    assert_eq!(
        fixture
            .surface
            .export(&operator, &accesses(ExportFormat::Parquet))
            .await
            .err(),
        Some(QueryError::Forbidden {
            missing: Permission::View
        })
    );
}

/// An export needs its permission, and a refusal is audited.
#[tokio::test]
async fn export_without_permission_is_refused_and_audited() {
    let fixture = Fixture::new().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let projection = match ExportRequest::new(
        ExportDataset::Projection(ProjectionId::from_ulid(1)),
        ExportFormat::Jsonl,
        false,
    ) {
        Ok(request) => request,
        Err(error) => panic!("{error:?}"),
    };
    let forbidden = QueryError::Forbidden {
        missing: Permission::Content,
    };
    assert_eq!(
        fixture.surface.export(&viewer, &projection).await.err(),
        Some(forbidden.clone())
    );
    let records = export_records(&fixture).await;
    assert_eq!(*records[0].event(), ExportEvent::Refused(forbidden));
    // With Content, an unknown projection is refused as `projection` is.
    let reader = fixture.caller(Who::Reader).await;
    assert_eq!(
        fixture.surface.export(&reader, &projection).await.err(),
        Some(QueryError::NotFound)
    );
}

/// An export over the row limit is refused before anything streams.
#[tokio::test]
async fn oversized_export_is_refused() {
    let mut config = config();
    config.export_limits = ExportLimits::new(NonZeroU64::MIN);
    let fixture = Fixture::with_config(config).await;
    fixture.scene().await;
    fixture.watermark(minute(5)).await;
    let viewer = fixture.caller(Who::Viewer).await;
    let refused = QueryError::Conflict(ConflictKind::ExportTooLarge { rows: 2, limit: 1 });
    assert_eq!(
        fixture
            .surface
            .export(&viewer, &accesses(ExportFormat::Jsonl))
            .await
            .err(),
        Some(refused.clone())
    );
    let records = export_records(&fixture).await;
    assert_eq!(*records[0].event(), ExportEvent::Refused(refused));
}

/// Nothing settled yet: the export plans no rows and completes empty.
#[tokio::test]
async fn export_before_the_watermark_is_empty() {
    let fixture = Fixture::new().await;
    fixture.scene().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let export = match fixture
        .surface
        .export(&viewer, &accesses(ExportFormat::Jsonl))
        .await
    {
        Ok(export) => export,
        Err(error) => panic!("export: {error:?}"),
    };
    assert_eq!(export.header.rows(), 0);
    let (rows, trailer) = drain(export.rows).await;
    assert!(rows.is_empty());
    assert!(trailer.is_complete());
}
