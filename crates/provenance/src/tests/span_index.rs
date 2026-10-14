//! `SpanIndex::record` on L4's stores (INV-965
//! `provenance.span-index.spans-as-recorded`): a span recorded through
//! the index reads back as first recorded, whether or not a scan
//! committed it. The world seed records each content match's origin span
//! this way, with no exchange or scan behind it, and the evidence page
//! reads the span back to cut the sender's excerpt. Each scenario runs on
//! any store; the Postgres store runs the same ones
//! (`integration::span_index`).

use std::collections::BTreeMap;

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::provenance::span::{OriginatedSpan, Span, SpanLocation, SpanState};
use crosstalk_spec::ids::SpanId;
use crosstalk_spec::interfaces::l4_provenance::{IndexedSpan, SpanIndex};
use crosstalk_spec::observed::message::PartRef;
use crosstalk_spec::support::ByteRange;
use crosstalk_testkit::ids::Ids;

use super::reads::scenario;
use crate::store::{MemoryProvenanceStore, ProvenanceStore};

fn location(ids: &mut Ids, start: u32) -> SpanLocation {
    SpanLocation {
        part: PartRef {
            message: ids.message(),
            index: 0,
        },
        range: ByteRange::new(start, start + 24).unwrap_or_else(|_| panic!("range")),
    }
}

fn originated(id: SpanId, ids: &mut Ids, start: u32) -> OriginatedSpan {
    OriginatedSpan::new(Span {
        id,
        location: location(ids, start),
        agent: ids.agent(),
        exchange: ids.exchange(),
        state: SpanState::Originated,
    })
    .unwrap_or_else(|| panic!("an originated span"))
}

fn batch(ids: impl IntoIterator<Item = SpanId>) -> IdBatch<SpanId> {
    IdBatch::new(ids).unwrap_or_else(|error| panic!("batch: {error:?}"))
}

async fn read<S: SpanIndex>(
    store: &S,
    ids: impl IntoIterator<Item = SpanId>,
) -> BTreeMap<SpanId, IndexedSpan> {
    store
        .spans(&batch(ids))
        .await
        .unwrap_or_else(|error| panic!("spans: {error:?}"))
}

async fn record<S: SpanIndex>(store: &mut S, span: &OriginatedSpan) {
    store
        .record(span)
        .await
        .unwrap_or_else(|error| panic!("record: {error:?}"));
}

/// A span recorded with no scan behind it (its exchange unknown to L4)
/// reads back as recorded; an unknown id stays absent; a second record
/// under the same id changes nothing; the scan records are untouched.
pub(crate) async fn recorded_without_a_scan_reads_back<S: ProvenanceStore + SpanIndex>(
    store: &mut S,
) {
    let mut ids = Ids::new();
    let id = ids.span();
    let first = originated(id, &mut ids, 0);
    record(store, &first).await;
    let unknown = ids.span();
    assert_eq!(
        read(store, [id, unknown]).await,
        BTreeMap::from([(id, IndexedSpan::of(&first))]),
        "a recorded span reads back; an unknown one is absent"
    );
    let again = originated(id, &mut ids, 100);
    record(store, &again).await;
    assert_eq!(
        read(store, [id]).await,
        BTreeMap::from([(id, IndexedSpan::of(&first))]),
        "the first record is kept"
    );
    assert_eq!(
        ProvenanceStore::span(store, id)
            .await
            .map(|span| span.is_none()),
        Ok(true),
        "recording writes no scan record"
    );
}

/// A span a scan committed keeps its committed record when it is
/// recorded again through the index.
pub(crate) async fn committed_then_recorded_keeps_the_commit<S: ProvenanceStore + SpanIndex>(
    store: &mut S,
) {
    let mut ids = Ids::new();
    let (world, _) = scenario(store, &mut ids).await;
    let committed = read(store, [world.originated]).await;
    assert_eq!(committed.len(), 1, "the scan indexed the originated span");
    let later = originated(world.originated, &mut ids, 200);
    record(store, &later).await;
    assert_eq!(
        read(store, [world.originated, world.common]).await,
        committed,
        "the committed record is kept; a common span stays absent"
    );
}

/// INV-965 on the memory store, for a span no scan committed.
#[tokio::test]
async fn memory_recorded_span_reads_back_without_a_scan() {
    recorded_without_a_scan_reads_back(&mut MemoryProvenanceStore::new()).await;
}

/// INV-965 on the memory store, for a span a scan committed first.
#[tokio::test]
async fn memory_committed_span_keeps_its_record() {
    committed_then_recorded_keeps_the_commit(&mut MemoryProvenanceStore::new()).await;
}
