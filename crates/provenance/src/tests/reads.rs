//! The spec's `ProvenanceReads` and `SpanIndex` over L4's records: scan
//! status, output spans of every origin, matches by reader exchange and a
//! span's readers. Each scenario runs on any store; the Postgres store
//! runs the same ones (`integration::reads`).

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::provenance::matching::{Carrier, ContentMatch, MatchKind};
use crosstalk_spec::derived::provenance::span::{RelaySource, Span, SpanLocation, SpanState};
use crosstalk_spec::ids::{AgentId, ExchangeId, MessageHash, SpanId};
use crosstalk_spec::interfaces::l4_provenance::SpanIndex;
use crosstalk_spec::interfaces::l4_provenance::reads::{
    ForwardStatus, ProvenanceReadError, ProvenanceReads, ScanStatus,
};
use crosstalk_spec::observed::message::{PartRef, ToolCallId};
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::{ByteRange, Timestamp};
use crosstalk_testkit::ids::Ids;

use crate::store::{
    ExchangeRecord, MemoryProvenanceStore, ProvenanceStore, ScanCommit, ScannedAs, StoredMatch,
};

fn at(seconds: u64) -> Timestamp {
    Timestamp::from_micros(seconds * 1_000_000)
}

fn located(message: MessageHash, part: u16, start: u32) -> SpanLocation {
    SpanLocation {
        part: PartRef {
            message,
            index: part,
        },
        range: ByteRange::new(start, start + 20).unwrap_or_else(|_| panic!("range")),
    }
}

/// What the scenario committed.
pub(crate) struct World {
    pub(crate) writer: ExchangeId,
    pub(crate) relayer: ExchangeId,
    pub(crate) reader: ExchangeId,
    pub(crate) pending: ExchangeId,
    pub(crate) originated: SpanId,
    pub(crate) common: SpanId,
    pub(crate) forwarded: SpanId,
    pub(crate) relayed: SpanId,
    /// Matches read in the relayer's and the reader's exchanges, in scan
    /// order.
    pub(crate) read_in: BTreeMap<ExchangeId, Vec<ContentMatch>>,
}

fn span(
    id: SpanId,
    agent: AgentId,
    exchange: ExchangeId,
    at: SpanLocation,
    state: SpanState,
) -> Span {
    Span {
        id,
        location: at,
        agent,
        exchange,
        state,
    }
}

fn content(
    origin: SpanId,
    origin_agent: AgentId,
    reader: AgentId,
    exchange: ExchangeId,
    read_at: SpanLocation,
) -> ContentMatch {
    ContentMatch::new(
        origin,
        origin_agent,
        reader,
        exchange,
        read_at,
        Carrier::ToolResult(ToolCallId("call_1".into())),
        MatchKind::Exact,
        NonZeroU32::new(20).unwrap_or(NonZeroU32::MIN),
    )
    .unwrap_or_else(|error| panic!("match: {error:?}"))
}

async fn record<S: ProvenanceStore>(
    store: &mut S,
    id: ExchangeId,
    at: Timestamp,
    output: MessageHash,
) {
    store
        .record_exchange(ExchangeRecord {
            id,
            started_at: at,
            request: Vec::new(),
            output: Some(output),
        })
        .await
        .unwrap_or_else(|error| panic!("recorded: {error:?}"));
}

/// A writer's output with an originated, a common and a forwarded span;
/// a relayer whose output copies the originated span and who read it; a
/// reader who read the originated span twice and the forwarded one; and
/// an exchange recorded but never scanned. The relayer's scan is left for
/// `relayer_scan` so a test can read before it.
pub(crate) async fn scenario<S: ProvenanceStore>(
    store: &mut S,
    ids: &mut Ids,
) -> (World, ScanCommit) {
    let (writer_agent, relayer_agent, reader_agent) = (ids.agent(), ids.agent(), ids.agent());
    let (writer, relayer, reader, pending) = (
        ids.exchange(),
        ids.exchange(),
        ids.exchange(),
        ids.exchange(),
    );
    let (out_w, out_r, out_x, out_p) = (ids.message(), ids.message(), ids.message(), ids.message());
    let fetched = ids.message();
    let (originated, common, forwarded, relayed) = (ids.span(), ids.span(), ids.span(), ids.span());
    record(store, writer, at(10), out_w).await;
    record(store, relayer, at(20), out_r).await;
    record(store, reader, at(30), out_x).await;
    record(store, pending, at(40), out_p).await;
    let committed = store
        .commit_scan(ScanCommit {
            exchange: writer,
            agent: writer_agent,
            at: at(10),
            spans: vec![
                span(
                    originated,
                    writer_agent,
                    writer,
                    located(out_w, 0, 0),
                    SpanState::Originated,
                ),
                span(
                    common,
                    writer_agent,
                    writer,
                    located(out_w, 0, 40),
                    SpanState::Common,
                ),
                span(
                    forwarded,
                    writer_agent,
                    writer,
                    located(out_w, 1, 0),
                    SpanState::Relayed {
                        source: RelaySource::Input(fetched),
                    },
                ),
            ],
            matches: Vec::new(),
            messages: vec![(out_w, ScannedAs::Output)],
            forwarding: true,
        })
        .await;
    assert!(committed.is_ok(), "{committed:?}");
    store
        .mark_indexed(writer, at(11))
        .await
        .unwrap_or_else(|error| panic!("indexed: {error:?}"));
    let read_by_relayer = content(
        originated,
        writer_agent,
        relayer_agent,
        relayer,
        located(ids.message(), 0, 0),
    );
    let first = content(
        originated,
        writer_agent,
        reader_agent,
        reader,
        located(ids.message(), 0, 0),
    );
    let second = content(
        originated,
        writer_agent,
        reader_agent,
        reader,
        located(ids.message(), 1, 0),
    );
    let of_forwarded = content(
        forwarded,
        writer_agent,
        reader_agent,
        reader,
        located(ids.message(), 2, 0),
    );
    let stored = |id, ordinal, at, content| StoredMatch {
        id,
        ordinal,
        at,
        content,
    };
    let committed = store
        .commit_scan(ScanCommit {
            exchange: reader,
            agent: reader_agent,
            at: at(30),
            spans: Vec::new(),
            matches: vec![
                stored(ids.event(), 0, at(30), first.clone()),
                stored(ids.event(), 1, at(30), second.clone()),
                stored(ids.event(), 2, at(30), of_forwarded.clone()),
            ],
            messages: vec![(out_x, ScannedAs::Output)],
            forwarding: true,
        })
        .await;
    assert!(committed.is_ok(), "{committed:?}");
    let relayer_scan = ScanCommit {
        exchange: relayer,
        agent: relayer_agent,
        at: at(20),
        spans: vec![span(
            relayed,
            relayer_agent,
            relayer,
            located(out_r, 0, 0),
            SpanState::Relayed {
                source: RelaySource::Span(originated),
            },
        )],
        matches: vec![stored(ids.event(), 0, at(20), read_by_relayer.clone())],
        messages: vec![(out_r, ScannedAs::Output)],
        forwarding: true,
    };
    let read_in = BTreeMap::from([
        (relayer, vec![read_by_relayer]),
        (reader, vec![first, second, of_forwarded]),
    ]);
    (
        World {
            writer,
            relayer,
            reader,
            pending,
            originated,
            common,
            forwarded,
            relayed,
            read_in,
        },
        relayer_scan,
    )
}

fn batch<T: Ord + Copy>(ids: impl IntoIterator<Item = T>) -> IdBatch<T> {
    IdBatch::new(ids).unwrap_or_else(|error| panic!("batch: {error:?}"))
}

/// INV-1023: before its scan commits, an exchange is `Pending` with no
/// spans and no matches; after, `Scanned` with all of them.
pub(crate) async fn status_follows_the_commit<S: ProvenanceStore + ProvenanceReads>(store: &mut S) {
    let mut ids = Ids::new();
    let (world, relayer_scan) = scenario(store, &mut ids).await;
    let one = batch([world.relayer]);
    assert_eq!(
        store.scan_status(&one).await,
        Ok(BTreeMap::from([(world.relayer, ScanStatus::Pending)]))
    );
    assert_eq!(
        store
            .output_spans(&one)
            .await
            .map(|spans| spans[&world.relayer].len()),
        Ok(0)
    );
    assert_eq!(
        store
            .matches_read_in(&one)
            .await
            .map(|read| read[&world.relayer].len()),
        Ok(0)
    );
    let committed = store.commit_scan(relayer_scan).await;
    assert!(committed.is_ok(), "{committed:?}");
    assert_eq!(
        store.scan_status(&one).await,
        Ok(BTreeMap::from([(
            world.relayer,
            ScanStatus::Scanned { at: at(20) }
        )]))
    );
    assert_eq!(
        store
            .output_spans(&one)
            .await
            .map(|spans| spans[&world.relayer].len()),
        Ok(1)
    );
    assert_eq!(
        store
            .matches_read_in(&one)
            .await
            .map(|read| read[&world.relayer].clone()),
        Ok(world.read_in[&world.relayer].clone())
    );
    let statuses = store
        .scan_status(&batch([
            world.writer,
            world.reader,
            world.pending,
            ids.exchange(),
        ]))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(statuses.len(), 3, "an unknown exchange is absent");
    assert_eq!(statuses[&world.writer], ScanStatus::Indexed { at: at(11) });
    assert_eq!(statuses[&world.reader], ScanStatus::Scanned { at: at(30) });
    assert_eq!(statuses[&world.pending], ScanStatus::Pending);
}

/// INV-1014: every non-common span of an output, in output order, with
/// its current state; relayed spans keep their source.
pub(crate) async fn output_spans_keep_every_origin<
    S: ProvenanceStore + ProvenanceReads + SpanIndex,
>(
    store: &mut S,
) {
    let mut ids = Ids::new();
    let (world, relayer_scan) = scenario(store, &mut ids).await;
    let committed = store.commit_scan(relayer_scan).await;
    assert!(committed.is_ok(), "{committed:?}");
    let unknown = ids.exchange();
    let spans = store
        .output_spans(&batch([
            world.writer,
            world.relayer,
            world.pending,
            unknown,
        ]))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(spans.len(), 3, "an unknown exchange is absent");
    let writer: Vec<(SpanId, SpanState, Option<ForwardStatus>)> = spans[&world.writer]
        .iter()
        .map(|stored| (stored.span.id, stored.span.state.clone(), stored.forward))
        .collect();
    assert_eq!(writer.len(), 2, "the common span is left out");
    assert_eq!(writer[0].0, world.originated);
    assert!(
        matches!(writer[0].1, SpanState::Propagated { indexed_at, .. } if indexed_at == at(11)),
        "{:?}",
        writer[0].1
    );
    assert_eq!(writer[0].2, None);
    assert_eq!(writer[1].0, world.forwarded);
    assert_eq!(writer[1].2, Some(ForwardStatus::Indexed { at: at(11) }));
    let relayer = &spans[&world.relayer];
    assert_eq!(relayer.len(), 1);
    assert_eq!(relayer[0].span.id, world.relayed);
    assert_eq!(
        relayer[0].span.state,
        SpanState::Relayed {
            source: RelaySource::Span(world.originated)
        }
    );
    assert!(spans[&world.pending].is_empty());
    let indexed = SpanIndex::spans(
        store,
        &batch([
            world.originated,
            world.common,
            world.forwarded,
            world.relayed,
        ]),
    )
    .await
    .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        indexed.keys().copied().collect::<Vec<_>>(),
        {
            let mut want = vec![world.originated, world.forwarded];
            want.sort_unstable();
            want
        },
        "SpanIndex records the originated and forwarded spans only"
    );
}

/// INV-1012: the matches read in an exchange, in scan order.
pub(crate) async fn matches_read_in_every_match<S: ProvenanceStore + ProvenanceReads>(
    store: &mut S,
) {
    let mut ids = Ids::new();
    let (world, relayer_scan) = scenario(store, &mut ids).await;
    let committed = store.commit_scan(relayer_scan).await;
    assert!(committed.is_ok(), "{committed:?}");
    let read = store
        .matches_read_in(&batch([
            world.relayer,
            world.reader,
            world.writer,
            ids.exchange(),
        ]))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(read.len(), 3);
    assert_eq!(read[&world.reader], world.read_in[&world.reader]);
    assert_eq!(read[&world.relayer], world.read_in[&world.relayer]);
    assert!(read[&world.writer].is_empty());
}

/// INV-1015: a span's readers newest first, their total, each once over a
/// traversal; a cursor is bound to its span.
pub(crate) async fn readers_newest_first<S: ProvenanceStore + ProvenanceReads>(store: &mut S) {
    let mut ids = Ids::new();
    let (world, relayer_scan) = scenario(store, &mut ids).await;
    let committed = store.commit_scan(relayer_scan).await;
    assert!(committed.is_ok(), "{committed:?}");
    let size = PageSize::new(2).unwrap_or_else(|error| panic!("{error:?}"));
    let first = store
        .readers(world.originated, &PageRequest { size, after: None })
        .await
        .unwrap_or_else(|error| panic!("{error:?}"))
        .unwrap_or_else(|| panic!("a kept span"));
    assert_eq!(first.total, 3);
    let (items, next) = first.page.into_parts();
    assert_eq!(items.len(), 2);
    assert!(
        items
            .iter()
            .all(|content| content.reader_exchange() == world.reader)
    );
    let cursor = next.unwrap_or_else(|| panic!("a third reader"));
    let wrong_span = store
        .readers(
            world.forwarded,
            &PageRequest {
                size,
                after: Some(cursor.clone()),
            },
        )
        .await;
    assert_eq!(
        wrong_span.map(|_| ()),
        Err(ProvenanceReadError::InvalidCursor)
    );
    let second = store
        .readers(
            world.originated,
            &PageRequest {
                size,
                after: Some(cursor),
            },
        )
        .await
        .unwrap_or_else(|error| panic!("{error:?}"))
        .unwrap_or_else(|| panic!("a kept span"));
    let (rest, next) = second.page.into_parts();
    assert_eq!(rest.len(), 1);
    assert_eq!(rest[0].reader_exchange(), world.relayer);
    assert!(next.is_none());
    let mut all = items;
    all.extend(rest);
    let mut want: Vec<ContentMatch> = world.read_in[&world.reader][..2].to_vec();
    want.push(world.read_in[&world.relayer][0].clone());
    let key = |content: &ContentMatch| (content.reader_exchange(), content.read_at());
    let mut got_keys: Vec<_> = all.iter().map(key).collect();
    let mut want_keys: Vec<_> = want.iter().map(key).collect();
    got_keys.sort();
    want_keys.sort();
    assert_eq!(got_keys, want_keys, "every reader once");
    let common = store
        .readers(world.common, &PageRequest { size, after: None })
        .await
        .unwrap_or_else(|error| panic!("{error:?}"))
        .unwrap_or_else(|| panic!("a kept span"));
    assert_eq!(common.total, 0);
    assert_eq!(
        store
            .readers(ids.span(), &PageRequest { size, after: None })
            .await
            .map(|page| page.is_none()),
        Ok(true)
    );
}

/// INV-1023 `provenance.scan.status-after-commit`.
#[tokio::test]
async fn scan_status_follows_the_commit() {
    status_follows_the_commit(&mut MemoryProvenanceStore::new()).await;
}

/// INV-1014 `surface.conversation.output-spans` (L4's half).
#[tokio::test]
async fn output_spans_keep_every_origin_in_output_order() {
    output_spans_keep_every_origin(&mut MemoryProvenanceStore::new()).await;
}

/// INV-1012 `surface.conversation.inbound-are-matches` (L4's half).
#[tokio::test]
async fn matches_read_in_lists_every_match_of_the_reader_exchange() {
    matches_read_in_every_match(&mut MemoryProvenanceStore::new()).await;
}

/// INV-1015 `surface.conversation.read-by` (L4's half).
#[tokio::test]
async fn readers_page_newest_first_with_their_total() {
    readers_newest_first(&mut MemoryProvenanceStore::new()).await;
}
