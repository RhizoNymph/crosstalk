//! Exports over HTTP: the JSONL golden streamed in arbitrary chunks reads
//! back exactly, and a stream ends `Complete` only when the rows received
//! verify against the header and the surface's trailer; every other end is
//! a `Failed` trailer saying why.

use crosstalk_spec::interfaces::l8_surface::QueryApi;
use crosstalk_spec::interfaces::l8_surface::export::{
    ExportEnd, ExportFailure, ExportFormat, ExportHeader, ExportLine, ExportRequest, ExportRow,
    ExportSealer, ExportStep, ExportStream, ExportTrailer, RowHasher, RowRefused, SourceFailure,
    read_jsonl, verify_export,
};
use crosstalk_spec::interfaces::l8_surface::http::export::{
    JSONL, PARQUET, content_disposition, content_type,
};
use crosstalk_spec::interfaces::l8_surface::{ConflictKind, InputError, QueryError};
use crosstalk_spec::support::Blake3;

use super::stub::{Reply, Step, Stub};
use super::{caller, golden, golden_value};
use crate::{Blake3RowHasher, HttpClient, HttpExportRows};

const GOLDEN: &str = "surface_reads/export/export_complete.jsonl";

/// The spec's stand-in for BLAKE3, whose digests the export goldens hold:
/// FNV-1a over what it is fed, in four lanes.
#[derive(Debug, Default)]
struct StandInHasher {
    fed: Vec<u8>,
}

impl RowHasher for StandInHasher {
    fn update(&mut self, bytes: &[u8]) {
        self.fed.extend_from_slice(bytes);
    }

    fn finalize(&self) -> Blake3 {
        let mut out = [0_u8; 32];
        for (chunk, lane) in out.chunks_mut(8).zip(0_u64..) {
            let mut hash: u64 = 0xcbf2_9ce4_8422_2325 ^ lane;
            for byte in &self.fed {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(0x0100_0000_01b3);
            }
            chunk.copy_from_slice(&hash.to_le_bytes());
        }
        Blake3::from_bytes(out)
    }
}

/// The golden export: its header, rows and trailer.
fn golden_export() -> (ExportHeader, Vec<ExportRow>, ExportTrailer) {
    let read = read_jsonl(&golden(GOLDEN)).unwrap_or_else(|error| panic!("{error:?}"));
    let trailer = read.trailer.unwrap_or_else(|| panic!("a trailer"));
    (read.header, read.rows, trailer)
}

fn line(line: &ExportLine) -> String {
    serde_json::to_string(line).unwrap_or_else(|error| panic!("{error}")) + "\n"
}

/// The JSONL body of a header, rows and (optionally) a trailer.
fn body(header: &ExportHeader, rows: &[ExportRow], trailer: Option<&ExportTrailer>) -> String {
    let mut text = line(&ExportLine::Header(Box::new(header.clone())));
    for row in rows {
        text.push_str(&line(&ExportLine::Row(row.clone())));
    }
    if let Some(trailer) = trailer {
        text.push_str(&line(&ExportLine::Trailer(trailer.clone())));
    }
    text
}

/// The trailer a surface sealing `rows` with `H` would send.
fn sealed<H: RowHasher + Default>(header: &ExportHeader, rows: &[ExportRow]) -> ExportTrailer {
    let mut sealer = ExportSealer::new(header, H::default());
    for row in rows {
        sealer.push(row).unwrap_or_else(|error| panic!("{error:?}"));
    }
    sealer.finish()
}

/// The surface's accepted export response: its headers, then `steps`.
fn accepted(header: &ExportHeader, steps: Vec<Step>) -> Reply {
    Reply::stream(200, JSONL, steps)
        .with_header("content-disposition", content_disposition(header))
        .with_header("cache-control", "no-store")
        .with_header("x-content-type-options", "nosniff")
}

/// `text` in chunks of `size` bytes.
fn chunked(text: &str, size: usize) -> Vec<Step> {
    text.as_bytes()
        .chunks(size)
        .map(|chunk| Step::Send(chunk.to_vec()))
        .collect()
}

async fn drain<H: RowHasher + Send>(
    mut rows: HttpExportRows<H>,
) -> (Vec<ExportRow>, ExportTrailer) {
    let mut received = Vec::new();
    loop {
        match rows.next().await {
            ExportStep::Row(row, rest) => {
                received.push(row);
                rows = rest;
            }
            ExportStep::End(trailer) => return (received, trailer),
        }
    }
}

/// Runs the export of the golden header's request against `reply`, with
/// the stand-in hasher.
async fn export_with(reply: Reply) -> (ExportHeader, Vec<ExportRow>, ExportTrailer) {
    let (header, _, _) = golden_export();
    let stub = Stub::always(reply).await;
    let client: HttpClient<StandInHasher> = stub.client().with_row_hasher();
    let export = client
        .export(&caller(), header.request())
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    let (rows, trailer) = drain(export.rows).await;
    (export.header, rows, trailer)
}

fn failed_store(trailer: &ExportTrailer) -> String {
    match trailer.end() {
        ExportEnd::Failed(ExportFailure::Store { reason }) => reason.clone(),
        other => panic!("not a client failure: {other:?}"),
    }
}

/// The golden JSONL export, sent in chunks of every size, reads back as
/// its header, its rows and its `Complete` trailer, which verify.
#[tokio::test]
async fn the_golden_export_reads_back_complete() {
    let text = String::from_utf8(golden(GOLDEN)).unwrap_or_else(|error| panic!("{error}"));
    let (header, rows, trailer) = golden_export();
    for size in [1, 7, 64, text.len()] {
        let mut stub = Stub::always(accepted(&header, chunked(&text, size))).await;
        let client: HttpClient<StandInHasher> = stub.client().with_row_hasher();
        let export = client
            .export(&caller(), header.request())
            .await
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(export.header, header);
        let (received, end) = drain(export.rows).await;
        assert_eq!(received, rows, "chunks of {size}");
        assert_eq!(end, trailer, "chunks of {size}");
        assert!(end.is_complete());
        assert_eq!(
            verify_export(&header, &received, Some(&end), StandInHasher::default()),
            Ok(())
        );
        let request = stub.only_request();
        assert_eq!(request.path, "/exports");
        assert_eq!(request.header("accept"), Some(JSONL));
        let sent: ExportRequest =
            serde_json::from_slice(&request.body).unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(&sent, header.request());
    }
}

/// With the surface's own digest, BLAKE3 under the row context, a complete
/// export verifies with the client's default hasher.
#[tokio::test]
async fn a_blake3_export_verifies_with_the_default_hasher() {
    let (header, rows, _) = golden_export();
    let trailer = sealed::<Blake3RowHasher>(&header, &rows);
    let text = body(&header, &rows, Some(&trailer));
    let stub = Stub::always(accepted(&header, chunked(&text, 50))).await;
    let export = stub
        .client()
        .export(&caller(), header.request())
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    let (received, end) = drain(export.rows).await;
    assert_eq!(received, rows);
    assert_eq!(end, trailer);
    assert!(end.is_complete());
}

/// A failure the surface recorded in its trailer after the `200` is
/// passed on as the surface's own account.
#[tokio::test]
async fn a_failed_trailer_is_passed_on() {
    let (header, rows, _) = golden_export();
    let mut sealer = ExportSealer::new(&header, StandInHasher::default());
    sealer
        .push(&rows[0])
        .unwrap_or_else(|error| panic!("{error:?}"));
    let failed = sealer.fail(SourceFailure::Store {
        reason: "connection reset by peer".to_owned(),
    });
    let text = body(&header, &rows[..1], Some(&failed));
    let (_, received, end) = export_with(accepted(&header, chunked(&text, 30))).await;
    assert_eq!(received, rows[..1].to_vec());
    assert_eq!(end, failed);
}

/// A body cut before its trailer (aborted, ended, or cut inside the
/// trailer line) ends `Failed` with the rows received.
#[tokio::test]
async fn a_cut_export_ends_failed() {
    let (header, rows, trailer) = golden_export();
    let complete = body(&header, &rows, Some(&trailer));
    let without_trailer = body(&header, &rows, None);
    let half_trailer = &complete[..complete.len() - 20];
    let cuts = [
        {
            let mut steps = chunked(&without_trailer, 40);
            steps.push(Step::Abort);
            steps
        },
        chunked(&without_trailer, 40),
        chunked(half_trailer, 40),
    ];
    for steps in cuts {
        let (_, received, end) = export_with(accepted(&header, steps)).await;
        assert_eq!(received, rows);
        assert_eq!(end.rows(), 2);
        assert_eq!(end.export(), header.id());
        assert!(failed_store(&end).contains("cut off"), "{end:?}");
        assert!(verify_export(&header, &received, Some(&end), StandInHasher::default()).is_err());
    }
}

/// A complete trailer that does not match what arrived is not trusted: a
/// digest of other rows, a count other than the rows received, fewer rows
/// than planned, or a trailer of another export.
#[tokio::test]
async fn a_trailer_that_does_not_verify_ends_failed() {
    let (header, rows, trailer) = golden_export();
    let good = body(&header, &rows, Some(&trailer));
    let digest = trailer.digest().digest().to_hex();
    let other_digest = good.replace(&digest, &"0".repeat(64));
    let (_, _, end) = export_with(accepted(&header, chunked(&other_digest, 64))).await;
    assert!(failed_store(&end).contains("digest"), "{end:?}");

    let mut miscounted = serde_json::to_value(ExportLine::Trailer(trailer.clone()))
        .unwrap_or_else(|error| panic!("{error}"));
    miscounted["data"]["rows"] = 3.into();
    let other_count = format!("{}{miscounted}\n", body(&header, &rows, None));
    let (_, _, end) = export_with(accepted(&header, chunked(&other_count, 64))).await;
    assert!(failed_store(&end).contains("counts 3 rows"), "{end:?}");

    let one_row = sealed::<StandInHasher>(&header, &rows[..1]);
    let short = body(&header, &rows[..1], Some(&one_row));
    let (_, received, end) = export_with(accepted(&header, chunked(&short, 64))).await;
    assert_eq!(received.len(), 1);
    assert_eq!(
        end.end(),
        &ExportEnd::Failed(ExportFailure::CountMismatch {
            planned: 2,
            produced: 1
        })
    );

    let other_export = good.replace(
        &format!("\"export\":\"{}\"", header.id().ulid_text()),
        "\"export\":\"01J9Z3N4P5Q6R7S8T9V0W1X2Y3\"",
    );
    assert_ne!(other_export, good);
    let (_, _, end) = export_with(accepted(&header, chunked(&other_export, 64))).await;
    assert!(failed_store(&end).contains("another export"), "{end:?}");
}

/// A row the sealer refuses is not yielded and ends the stream with the
/// refusal; so does anything that is not the framing.
#[tokio::test]
async fn a_bad_line_ends_failed() {
    let (header, rows, trailer) = golden_export();
    let repeated = body(&header, &[rows[0].clone(), rows[0].clone()], Some(&trailer));
    let (_, received, end) = export_with(accepted(&header, chunked(&repeated, 64))).await;
    assert_eq!(received, rows[..1].to_vec());
    assert_eq!(
        end.end(),
        &ExportEnd::Failed(ExportFailure::InvalidRow {
            index: 1,
            refused: RowRefused::OutOfOrder
        })
    );

    let header_line = line(&ExportLine::Header(Box::new(header.clone())));
    let complete = body(&header, &rows, Some(&trailer));
    let cases = [
        (
            format!("{complete}{{\"type\":\"row\"}}\n"),
            "after the trailer",
        ),
        (format!("{header_line}{header_line}"), "a second header"),
        (format!("{header_line}not json\n"), "undecodable"),
    ];
    for (text, why) in cases {
        let (_, _, end) = export_with(accepted(&header, chunked(&text, 64))).await;
        assert!(failed_store(&end).contains(why), "{why}: {end:?}");
    }
}

/// The response must be this request's export: its header names the
/// request, and Content-Disposition names its file.
#[tokio::test]
async fn the_header_must_be_this_requests() {
    let (header, rows, trailer) = golden_export();
    let text = body(&header, &rows, Some(&trailer));
    let other: ExportRequest = golden_value("surface_reads/export/export_request_verdicts.json");
    let stub = Stub::always(accepted(&header, chunked(&text, 64))).await;
    let refused = stub.client().export(&caller(), &other).await.map(|_| ());
    assert!(
        matches!(&refused, Err(QueryError::Store { reason }) if reason.contains("another request")),
        "{refused:?}"
    );

    let unnamed = Reply::stream(200, JSONL, chunked(&text, 64));
    let stub = Stub::always(unnamed).await;
    let refused = stub
        .client()
        .export(&caller(), header.request())
        .await
        .map(|_| ());
    assert!(
        matches!(&refused, Err(QueryError::Store { reason }) if reason.contains("Content-Disposition")),
        "{refused:?}"
    );

    let stub = Stub::always(accepted(&header, Vec::new())).await;
    let refused = stub
        .client()
        .export(&caller(), header.request())
        .await
        .map(|_| ());
    assert!(
        matches!(&refused, Err(QueryError::Store { reason }) if reason.contains("before its header")),
        "{refused:?}"
    );
}

/// A refusal before streaming is the error, with nothing streamed.
#[tokio::test]
async fn a_refused_export_is_its_error() {
    let (header, _, _) = golden_export();
    let too_large = QueryError::Conflict(ConflictKind::ExportTooLarge {
        rows: 2_000_000,
        limit: 1_000_000,
    });
    let stub = Stub::always(Reply::value(409, &too_large)).await;
    let refused = stub
        .client()
        .export(&caller(), header.request())
        .await
        .map(|_| ());
    assert_eq!(refused, Err(too_large));
}

/// The trait reads JSONL only: a Parquet request is refused unsent. The
/// download passes either format on as bytes with its file name.
#[tokio::test]
async fn parquet_is_downloaded_not_decoded() {
    let request: ExportRequest =
        golden_value("surface_reads/export/export_request_edges_parquet.json");
    assert_eq!(request.format(), ExportFormat::Parquet);
    let mut stub = Stub::always(Reply::json(500, "{}")).await;
    let refused = stub.client().export(&caller(), &request).await.map(|_| ());
    assert_eq!(
        refused,
        Err(QueryError::InvalidInput(InputError::UnsupportedFormat {
            format: ExportFormat::Parquet
        }))
    );
    assert!(stub.requests().is_empty(), "nothing was sent");

    let disposition = "attachment; filename=\"crosstalk-edges-01J9Z3K8M4Q7R2T5V6W8X9Y0ZA.parquet\"";
    let file = b"PAR1 row groups ... footer PAR1".to_vec();
    let reply = Reply::stream(
        200,
        PARQUET,
        vec![
            Step::Send(file[..10].to_vec()),
            Step::Send(file[10..].to_vec()),
        ],
    )
    .with_header("content-disposition", disposition);
    let mut stub = Stub::always(reply).await;
    let mut download = stub
        .client()
        .download_export(&request)
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(download.format(), ExportFormat::Parquet);
    assert_eq!(download.content_type(), content_type(ExportFormat::Parquet));
    assert_eq!(download.content_disposition(), disposition);
    let mut bytes = Vec::new();
    while let Some(chunk) = download
        .chunk()
        .await
        .unwrap_or_else(|error| panic!("{error}"))
    {
        bytes.extend_from_slice(&chunk);
    }
    assert_eq!(bytes, file);
    assert_eq!(stub.only_request().header("accept"), Some(PARQUET));
}

/// A download cut short reports the cut.
#[tokio::test]
async fn a_cut_download_is_an_error() {
    let (header, rows, _) = golden_export();
    let text = body(&header, &rows, None);
    let mut steps = chunked(&text, 64);
    steps.push(Step::Abort);
    let stub = Stub::always(accepted(&header, steps)).await;
    let mut download = stub
        .client()
        .download_export(header.request())
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    let mut outcome = Ok(());
    loop {
        match download.chunk().await {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(error) => {
                outcome = Err(error);
                break;
            }
        }
    }
    assert!(outcome.is_err(), "the cut is reported");
}
