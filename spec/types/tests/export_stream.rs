//! The export stream: canonical row encoding and digest, the sealer, the
//! sealed stream and verification of a received export.

use std::future::Future;
use std::num::NonZeroU64;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use crate::aggregates::edge::EdgeSelector;
use crate::aggregates::quality::MatchClass;
use crate::derived::flow::access::AccessKind;
use crate::derived::flow::transmission::{DelegationDirection, DirectCarrier, Route};
use crate::derived::flow::verdict::Verdict;
use crate::ids::{ExportId, TopicId};
use crate::interfaces::l8_surface::export::digest::{encode_route, hash_row};
use crate::interfaces::l8_surface::export::rows::{
    AccessRow, EdgeRow, LabelContent, MatchText, TopicContent, TopicRow, TransmissionContent,
    TransmissionRow,
};
use crate::interfaces::l8_surface::export::{
    ExportDataset, ExportDatasetKind, ExportEnd, ExportFailure, ExportFormat, ExportHeader,
    ExportRequest, ExportRow, ExportSealer, ExportStep, ExportStream, ExportTrailer, Incomplete,
    RowHasher, RowRefused, RowSource, SealedRows, SourceFailure, verify_export,
};
use crate::observed::message::ToolName;
use crate::support::{Blake3, NonEmpty};
use crate::tests::export::{V, header, parts, scope, scoped_basis, window};
use crate::tests::fixtures::{agent, at, channel, transmission};

/// A stand-in for BLAKE3: records what it is fed, and digests it with
/// FNV-1a, enough to tell different inputs apart in these tests.
#[derive(Debug, Default)]
struct TestHasher {
    fed: Vec<u8>,
}

impl RowHasher for TestHasher {
    fn update(&mut self, bytes: &[u8]) {
        self.fed.extend_from_slice(bytes);
    }

    fn finalize(&self) -> Blake3 {
        let mut out = [0_u8; 32];
        for (lane, chunk) in out.chunks_mut(8).enumerate() {
            let mut hash: u64 = 0xcbf2_9ce4_8422_2325 ^ lane as u64;
            for byte in &self.fed {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(0x0100_0000_01b3);
            }
            chunk.copy_from_slice(&hash.to_le_bytes());
        }
        Blake3::from_bytes(out)
    }
}

fn hasher() -> TestHasher {
    TestHasher::default()
}

/// Runs a future that never waits: the test sources are in memory.
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("in-memory sources never wait"),
    }
}

fn nz(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).expect("non-zero fixture count")
}

fn tx_row(n: u128, confirmed_at: u64, content: bool) -> ExportRow {
    ExportRow::Transmission(TransmissionRow {
        id: transmission(n),
        from: agent(1),
        to: agent(2),
        route: Route::Channel(channel(3)),
        opened_at: at(confirmed_at - 1),
        confirmed_at: at(confirmed_at),
        matched_bytes: nz(40),
        strongest: MatchClass::Exact,
        topic: Some(TopicId::from_ulid(5)),
        verdict: Some(Verdict::Genuine),
        content: content.then(|| TransmissionContent {
            topic_label: Some("billing".into()),
            matches: NonEmpty::new(MatchText {
                class: MatchClass::Exact,
                origin: "the invoice total is 42".into(),
                read: "the invoice total is 42".into(),
            }),
        }),
    })
}

fn access_row(agent_n: u128) -> ExportRow {
    ExportRow::Access(AccessRow {
        agent: agent(agent_n),
        channel: channel(3),
        op: AccessKind::Write,
        bucket: window(0, 100),
        accesses: nz(2),
    })
}

/// Rows 1..=n of a transmissions export without content, in key order.
fn tx_rows(n: u128) -> Vec<ExportRow> {
    (1..=n)
        .map(|i| tx_row(i, 100 + u64::try_from(i).expect("small"), false))
        .collect()
}

fn tx_header(rows: u64) -> ExportHeader {
    header(ExportDataset::Transmissions(scope()), false, rows)
}

fn seal(header: &ExportHeader, rows: &[ExportRow]) -> ExportTrailer {
    let mut sealer = ExportSealer::new(header, hasher());
    for row in rows {
        sealer.push(row).expect("valid row");
    }
    sealer.finish()
}

fn digest_of(rows: &[ExportRow]) -> Blake3 {
    let mut hasher = hasher();
    let mut scratch = Vec::new();
    for row in rows {
        hash_row(&mut hasher, row, &mut scratch);
    }
    hasher.finalize()
}

// ── Encoding and digest ────────────────────────────────────────────────────

#[test]
fn encoding_starts_with_the_dataset_tag_and_is_deterministic() {
    for (row, kind) in [
        (tx_row(1, 100, false), ExportDatasetKind::Transmissions),
        (access_row(1), ExportDatasetKind::Accesses),
    ] {
        let mut first = Vec::new();
        let mut second = Vec::new();
        row.encode(&mut first);
        row.clone().encode(&mut second);
        assert_eq!(first.first(), Some(&kind.code()));
        assert_eq!(first, second);
    }
}

#[test]
fn rows_differing_in_one_field_encode_differently() {
    let base = tx_row(1, 100, true);
    let ExportRow::Transmission(row) = base.clone() else {
        panic!("a transmission row");
    };
    let variants = [
        TransmissionRow {
            verdict: None,
            ..row.clone()
        },
        TransmissionRow {
            topic: None,
            ..row.clone()
        },
        TransmissionRow {
            strongest: MatchClass::Semantic,
            ..row.clone()
        },
        TransmissionRow {
            content: None,
            ..row.clone()
        },
        TransmissionRow {
            content: Some(TransmissionContent {
                topic_label: None,
                matches: NonEmpty::new(MatchText {
                    class: MatchClass::Exact,
                    origin: "the invoice total is 42".into(),
                    read: "the invoice total is 42".into(),
                }),
            }),
            ..row.clone()
        },
    ];
    let mut encoded = Vec::new();
    base.encode(&mut encoded);
    for variant in variants {
        let mut other = Vec::new();
        ExportRow::Transmission(variant.clone()).encode(&mut other);
        assert_ne!(encoded, other, "{variant:?}");
    }
}

#[test]
fn strings_are_length_prefixed_so_boundaries_cannot_shift() {
    let topic = |label: &str, term: &str| {
        let mut out = Vec::new();
        ExportRow::Topic(TopicRow {
            topic: TopicId::from_ulid(5),
            transmissions: 1,
            matched_bytes: 1,
            content: Some(TopicContent {
                label: label.into(),
                terms: vec![(term.into(), 0.5)],
            }),
        })
        .encode(&mut out);
        out
    };
    assert_ne!(topic("ab", "c"), topic("a", "bc"));
}

#[test]
fn route_encoding_distinguishes_every_route() {
    let routes = [
        Route::Channel(channel(1)),
        Route::Channel(channel(2)),
        Route::Delegation(DelegationDirection::ParentToChild),
        Route::Delegation(DelegationDirection::ChildToParent),
        Route::Direct(DirectCarrier::UserTurn),
        Route::Direct(DirectCarrier::SystemPrompt),
        Route::Direct(DirectCarrier::ToolResult(ToolName("bash".into()))),
        Route::Direct(DirectCarrier::ToolResult(ToolName("read".into()))),
        Route::Unobserved,
    ];
    let encoded: Vec<Vec<u8>> = routes
        .iter()
        .map(|route| {
            let mut out = Vec::new();
            encode_route(route, &mut out);
            out
        })
        .collect();
    for (i, a) in encoded.iter().enumerate() {
        for b in &encoded[i + 1..] {
            assert_ne!(a, b);
        }
    }
}

#[test]
fn each_row_is_hashed_after_its_length() {
    let row = access_row(1);
    let mut encoded = Vec::new();
    row.encode(&mut encoded);
    let mut hasher = hasher();
    hash_row(&mut hasher, &row, &mut Vec::new());
    let mut expected = u64::try_from(encoded.len())
        .expect("small")
        .to_le_bytes()
        .to_vec();
    expected.extend_from_slice(&encoded);
    assert_eq!(hasher.fed, expected);
}

// ── Sealer ─────────────────────────────────────────────────────────────────

#[test]
fn sealer_completes_when_every_planned_row_is_accepted() {
    let rows = tx_rows(3);
    let trailer = seal(&tx_header(3), &rows);
    assert_eq!(trailer.export(), ExportId::from_ulid(9));
    assert_eq!(trailer.rows(), 3);
    assert_eq!(*trailer.digest().digest(), digest_of(&rows));
    assert!(trailer.is_complete());
}

#[test]
fn an_empty_export_completes_with_no_rows() {
    let trailer = seal(&tx_header(0), &[]);
    assert_eq!(trailer.rows(), 0);
    assert_eq!(*trailer.digest().digest(), digest_of(&[]));
    assert!(trailer.is_complete());
}

#[test]
fn equal_rows_give_equal_digests_whatever_the_format() {
    let rows = tx_rows(3);
    let jsonl = seal(&tx_header(3), &rows);
    let parquet_request = ExportRequest::new(
        ExportDataset::Transmissions(scope()),
        ExportFormat::Parquet,
        false,
    )
    .expect("valid request");
    let parquet =
        ExportHeader::new(parts(parquet_request, scoped_basis(), 3)).expect("valid header");
    assert_eq!(seal(&parquet, &rows), jsonl);
    assert_eq!(seal(&tx_header(3), &rows), jsonl, "a re-run seals the same");
}

#[test]
fn sealer_refuses_a_row_of_another_dataset() {
    let mut sealer = ExportSealer::new(&tx_header(1), hasher());
    assert_eq!(
        sealer.push(&access_row(1)),
        Err(RowRefused::OtherDataset {
            expected: ExportDatasetKind::Transmissions,
            got: ExportDatasetKind::Accesses,
        })
    );
    assert_eq!(sealer.rows(), 0);
}

#[test]
fn sealer_refuses_content_the_request_did_not_ask_for_and_its_absence() {
    let mut plain = ExportSealer::new(&tx_header(1), hasher());
    assert_eq!(
        plain.push(&tx_row(1, 100, true)),
        Err(RowRefused::ContentMismatch { requested: false })
    );
    let with_content = header(ExportDataset::Transmissions(scope()), true, 1);
    let mut sealer = ExportSealer::new(&with_content, hasher());
    assert_eq!(
        sealer.push(&tx_row(1, 100, false)),
        Err(RowRefused::ContentMismatch { requested: true })
    );
}

#[test]
fn sealer_refuses_rows_out_of_order_and_repeats() {
    for second in [tx_row(1, 100, false), tx_row(9, 50, false)] {
        let mut sealer = ExportSealer::new(&tx_header(2), hasher());
        sealer.push(&tx_row(1, 100, false)).expect("first row");
        assert_eq!(sealer.push(&second), Err(RowRefused::OutOfOrder));
    }
    let mut sealer = ExportSealer::new(&tx_header(2), hasher());
    sealer.push(&tx_row(5, 100, false)).expect("first row");
    sealer
        .push(&tx_row(6, 100, false))
        .expect("same time, later id");
}

#[test]
fn edge_rows_order_by_bucket_then_endpoints_route_and_topic() {
    let edge = |start: u64, from: u128, route: Route, topic: Option<u128>| {
        ExportRow::Edge(EdgeRow {
            edge: EdgeSelector::new(agent(from), agent(9), route).expect("not a self-edge"),
            topic: topic.map(TopicId::from_ulid),
            bucket: window(start, start + 100),
            transmissions: nz(1),
            matched_bytes: nz(1),
            content: None,
        })
    };
    let ordered = [
        edge(0, 1, Route::Channel(channel(1)), None),
        edge(0, 1, Route::Channel(channel(1)), Some(4)),
        edge(0, 1, Route::Unobserved, None),
        edge(0, 2, Route::Channel(channel(1)), None),
        edge(100, 1, Route::Channel(channel(1)), None),
    ];
    for pair in ordered.windows(2) {
        assert!(pair[0].key() < pair[1].key(), "{:?}", pair[1]);
    }
    let header = header(ExportDataset::Edges(scope()), false, 5);
    assert!(seal(&header, &ordered).is_complete());
}

#[test]
fn access_rows_put_writes_before_reads() {
    let row = |op| {
        ExportRow::Access(AccessRow {
            agent: agent(1),
            channel: channel(3),
            op,
            bucket: window(0, 100),
            accesses: nz(1),
        })
    };
    assert!(row(AccessKind::Write).key() < row(AccessKind::Read).key());
}

#[test]
fn sealer_refuses_rows_beyond_the_plan() {
    let rows = tx_rows(3);
    let mut sealer = ExportSealer::new(&tx_header(2), hasher());
    sealer.push(&rows[0]).expect("planned");
    sealer.push(&rows[1]).expect("planned");
    assert_eq!(
        sealer.push(&rows[2]),
        Err(RowRefused::BeyondPlan { planned: 2 })
    );
}

#[test]
fn after_a_refusal_every_row_is_refused_and_the_trailer_fails() {
    let rows = tx_rows(3);
    let mut sealer = ExportSealer::new(&tx_header(3), hasher());
    sealer.push(&rows[0]).expect("first row");
    assert!(sealer.push(&access_row(1)).is_err());
    assert_eq!(sealer.push(&rows[1]), Err(RowRefused::AfterRefusal));
    let trailer = sealer.finish();
    assert_eq!(trailer.rows(), 1);
    assert_eq!(*trailer.digest().digest(), digest_of(&rows[..1]));
    assert_eq!(
        *trailer.end(),
        ExportEnd::Failed(ExportFailure::InvalidRow {
            index: 1,
            refused: RowRefused::OtherDataset {
                expected: ExportDatasetKind::Transmissions,
                got: ExportDatasetKind::Accesses,
            },
        })
    );
}

#[test]
fn a_source_that_runs_short_fails_with_the_counts() {
    let rows = tx_rows(2);
    let trailer = seal(&tx_header(3), &rows);
    assert_eq!(trailer.rows(), 2);
    assert_eq!(
        *trailer.end(),
        ExportEnd::Failed(ExportFailure::CountMismatch {
            planned: 3,
            produced: 2
        })
    );
}

#[test]
fn a_source_failure_is_recorded_after_the_rows_sent() {
    let rows = tx_rows(2);
    let mut sealer = ExportSealer::new(&tx_header(5), hasher());
    for row in &rows {
        sealer.push(row).expect("valid row");
    }
    let trailer = sealer.fail(SourceFailure::Store {
        reason: "connection reset".into(),
    });
    assert_eq!(trailer.rows(), 2);
    assert_eq!(*trailer.digest().digest(), digest_of(&rows));
    assert_eq!(
        *trailer.end(),
        ExportEnd::Failed(ExportFailure::Store {
            reason: "connection reset".into()
        })
    );
    let dropped = ExportSealer::new(&tx_header(5), hasher())
        .fail(SourceFailure::VersionNotRetained { version: V });
    assert_eq!(
        *dropped.end(),
        ExportEnd::Failed(ExportFailure::VersionNotRetained { version: V })
    );
}

// ── The sealed stream ──────────────────────────────────────────────────────

/// Yields its rows, then either ends or fails.
struct VecSource {
    rows: std::vec::IntoIter<ExportRow>,
    then: Option<SourceFailure>,
}

impl VecSource {
    fn new(rows: Vec<ExportRow>, then: Option<SourceFailure>) -> Self {
        Self {
            rows: rows.into_iter(),
            then,
        }
    }
}

impl RowSource for VecSource {
    async fn next(&mut self) -> Result<Option<ExportRow>, SourceFailure> {
        match self.rows.next() {
            Some(row) => Ok(Some(row)),
            None => self.then.take().map_or(Ok(None), Err),
        }
    }
}

/// Drains `stream`: the rows it sent, then its trailer.
fn drain<S: ExportStream>(mut stream: S) -> (Vec<ExportRow>, ExportTrailer) {
    let mut sent = Vec::new();
    loop {
        match ready(stream.next()) {
            ExportStep::Row(row, rest) => {
                sent.push(row);
                stream = rest;
            }
            ExportStep::End(trailer) => return (sent, trailer),
        }
    }
}

#[test]
fn a_stream_sends_every_row_then_one_complete_trailer() {
    let header = tx_header(3);
    let rows = tx_rows(3);
    let stream = SealedRows::new(&header, VecSource::new(rows.clone(), None), hasher());
    let (sent, trailer) = drain(stream);
    assert_eq!(sent, rows);
    assert!(trailer.is_complete());
    assert_eq!(
        verify_export(&header, &sent, Some(&trailer), hasher()),
        Ok(())
    );
}

#[test]
fn a_stream_ends_with_the_store_failure_in_its_trailer() {
    let header = tx_header(3);
    let failure = SourceFailure::Store {
        reason: "connection reset".into(),
    };
    let source = VecSource::new(tx_rows(1), Some(failure));
    let (sent, trailer) = drain(SealedRows::new(&header, source, hasher()));
    assert_eq!(sent.len(), 1);
    assert_eq!(trailer.rows(), 1);
    assert_eq!(
        *trailer.end(),
        ExportEnd::Failed(ExportFailure::Store {
            reason: "connection reset".into()
        })
    );
}

#[test]
fn a_stream_never_sends_a_refused_row() {
    let header = tx_header(3);
    let mut rows = tx_rows(1);
    rows.push(access_row(1));
    rows.extend(tx_rows(3).into_iter().skip(1));
    let (sent, trailer) = drain(SealedRows::new(
        &header,
        VecSource::new(rows, None),
        hasher(),
    ));
    assert_eq!(sent, tx_rows(1));
    assert!(matches!(
        trailer.end(),
        ExportEnd::Failed(ExportFailure::InvalidRow { index: 1, .. })
    ));
}

// ── Verification ───────────────────────────────────────────────────────────

#[test]
fn verify_accepts_exactly_the_complete_export() {
    let header = tx_header(3);
    let rows = tx_rows(3);
    let trailer = seal(&header, &rows);
    assert_eq!(
        verify_export(&header, &rows, Some(&trailer), hasher()),
        Ok(())
    );
}

#[test]
fn a_truncated_export_never_verifies() {
    let header = tx_header(3);
    let rows = tx_rows(3);
    let trailer = seal(&header, &rows);
    assert_eq!(
        verify_export(&header, &rows[..2], None, hasher()),
        Err(Incomplete::NoTrailer)
    );
    assert_eq!(
        verify_export(&header, &rows, None, hasher()),
        Err(Incomplete::NoTrailer),
        "every row but no trailer is still cut off"
    );
    assert_eq!(
        verify_export(&header, &rows[..2], Some(&trailer), hasher()),
        Err(Incomplete::RowCount {
            planned: 3,
            sent: 3,
            received: 2
        })
    );
}

#[test]
fn a_failed_export_never_verifies() {
    let header = tx_header(3);
    let rows = tx_rows(2);
    let short = seal(&header, &rows);
    assert!(matches!(
        verify_export(&header, &rows, Some(&short), hasher()),
        Err(Incomplete::Failed(ExportFailure::CountMismatch { .. }))
    ));
}

#[test]
fn verify_rejects_altered_extra_and_misplaced_rows() {
    let header = tx_header(3);
    let rows = tx_rows(3);
    let trailer = seal(&header, &rows);

    let mut altered = rows.clone();
    altered[1] = tx_row(2, 102, false);
    if let ExportRow::Transmission(row) = &mut altered[1] {
        row.verdict = Some(Verdict::FalseDetection);
    }
    assert_eq!(
        verify_export(&header, &altered, Some(&trailer), hasher()),
        Err(Incomplete::DigestMismatch)
    );

    let mut extra = rows.clone();
    extra.push(tx_row(4, 104, false));
    assert_eq!(
        verify_export(&header, &extra, Some(&trailer), hasher()),
        Err(Incomplete::RowCount {
            planned: 3,
            sent: 3,
            received: 4
        })
    );

    let swapped = vec![rows[1].clone(), rows[0].clone(), rows[2].clone()];
    assert_eq!(
        verify_export(&header, &swapped, Some(&trailer), hasher()),
        Err(Incomplete::InvalidRow {
            index: 1,
            refused: RowRefused::OutOfOrder
        })
    );
}

#[test]
fn verify_rejects_another_exports_trailer() {
    let rows = tx_rows(1);
    let header = tx_header(1);
    let mut other_parts = parts(
        ExportRequest::new(
            ExportDataset::Transmissions(scope()),
            ExportFormat::Jsonl,
            false,
        )
        .expect("valid request"),
        scoped_basis(),
        1,
    );
    other_parts.id = ExportId::from_ulid(10);
    let other = ExportHeader::new(other_parts).expect("valid header");
    let trailer = seal(&other, &rows);
    assert_eq!(
        verify_export(&header, &rows, Some(&trailer), hasher()),
        Err(Incomplete::OtherExport)
    );
}

#[test]
fn label_content_is_part_of_the_digest() {
    let edge = |label: Option<&str>| {
        ExportRow::Edge(EdgeRow {
            edge: EdgeSelector::new(agent(1), agent(2), Route::Unobserved).expect("distinct"),
            topic: Some(TopicId::from_ulid(5)),
            bucket: window(0, 100),
            transmissions: nz(1),
            matched_bytes: nz(1),
            content: Some(LabelContent {
                topic_label: label.map(str::to_owned),
            }),
        })
    };
    assert_ne!(
        digest_of(&[edge(Some("billing"))]),
        digest_of(&[edge(None)])
    );
}
