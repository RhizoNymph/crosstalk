//! Where marks point: span points (a span's author, exchange and turn),
//! the transmission holding each content match, and a span's readers.
//! Every read is batched (`IdBatch::MAX` ids or keys a call) and every
//! agent resolved through `AgentDirectory` at the read.

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::aliases::Aliases;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::provenance::matching::ContentMatch;
use crosstalk_spec::ids::{ExchangeId, SpanId, TransmissionId};
use crosstalk_spec::interfaces::l3_reconstruction::conversations::ConversationReads;
use crosstalk_spec::interfaces::l4_provenance::reads::{ProvenanceReads, ReaderPage};
use crosstalk_spec::interfaces::l4_provenance::{IndexedSpan, SpanIndex};
use crosstalk_spec::interfaces::l5_flow::transmissions::{MatchKey, TransmissionStore};
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::interfaces::l8_surface::conversation::turn::{Reader, TransmissionMark};
use crosstalk_spec::interfaces::l8_surface::conversation::{SpanPoint, TurnPoint};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionStateKind;
use crosstalk_spec::paging::{PageRequest, SpanReaderList};

use crate::service::Surface;
use crate::stores::SurfaceStores;

/// The most ids or keys one batched read takes.
const BATCH: usize = 1000;

fn batches<T: Ord + Copy>(ids: &BTreeSet<T>) -> Result<Vec<IdBatch<T>>, QueryError> {
    let ids: Vec<T> = ids.iter().copied().collect();
    ids.chunks(BATCH)
        .map(|chunk| IdBatch::new(chunk.iter().copied()).map_err(QueryError::from))
        .collect()
}

/// A fault in the stored records: a mark naming a record that is not kept.
pub(super) fn missing(what: String) -> QueryError {
    QueryError::Store {
        reason: format!("conversation records disagree: {what}"),
    }
}

/// What the marks of one read point at, read in batches.
#[derive(Debug, Default)]
pub(super) struct Pointers {
    /// Where each span named sits.
    pub(super) points: BTreeMap<SpanId, SpanPoint>,
    /// The turn each exchange named is, when threaded.
    pub(super) turns: BTreeMap<ExchangeId, TurnPoint>,
    /// The transmission holding each match named, when one does.
    pub(super) marks: BTreeMap<MatchKey, TransmissionMark>,
}

impl Pointers {
    /// The span point of `span`; a fault when L4 keeps no record of it.
    pub(super) fn point(&self, span: SpanId) -> Result<SpanPoint, QueryError> {
        self.points
            .get(&span)
            .cloned()
            .ok_or_else(|| missing(format!("span {} has no record", span.ulid_text())))
    }

    /// `content` as a reader of its origin span.
    pub(super) fn reader(&self, content: &ContentMatch, aliases: impl Aliases) -> Reader {
        Reader {
            agent: aliases.agent(content.reader()),
            exchange: content.reader_exchange(),
            turn: self.turns.get(&content.reader_exchange()).copied(),
            read_at: content.read_at(),
            carrier: content.carrier().clone(),
            kind: content.kind().clone(),
            transmission: self.marks.get(&MatchKey::of(content)).cloned(),
        }
    }
}

impl<S: SurfaceStores> Surface<S> {
    /// Where each of `exchanges` sits, the unthreaded ones left out.
    pub(super) async fn locate_all(
        &self,
        exchanges: &BTreeSet<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, TurnPoint>, QueryError> {
        let mut located = BTreeMap::new();
        for batch in batches(exchanges)? {
            located.extend(
                self.stores
                    .conversations()
                    .locate(&batch)
                    .await?
                    .into_iter()
                    .map(|(exchange, placement)| (exchange, placement.point())),
            );
        }
        Ok(located)
    }

    /// L4's records of `spans` (`SpanIndex::spans`); unknown ones absent.
    async fn indexed(
        &self,
        spans: &BTreeSet<SpanId>,
    ) -> Result<BTreeMap<SpanId, IndexedSpan>, QueryError> {
        let mut indexed = BTreeMap::new();
        for batch in batches(spans)? {
            indexed.extend(self.stores.provenance().spans(&batch).await?);
        }
        Ok(indexed)
    }

    /// Where each of `spans` L4 keeps a record of sits, its author
    /// resolved now (`surface.conversation.locate`).
    pub(super) async fn span_points_of(
        &self,
        spans: &BTreeSet<SpanId>,
    ) -> Result<BTreeMap<SpanId, SpanPoint>, QueryError> {
        let indexed = self.indexed(spans).await?;
        let exchanges: BTreeSet<ExchangeId> = indexed.values().map(|span| span.exchange).collect();
        let turns = self.locate_all(&exchanges).await?;
        let aliases = self.aliases();
        Ok(indexed
            .into_iter()
            .map(|(id, span)| {
                (
                    id,
                    SpanPoint {
                        span: id,
                        agent: aliases.agent(span.author),
                        exchange: span.exchange,
                        turn: turns.get(&span.exchange).copied(),
                        location: span.location,
                    },
                )
            })
            .collect())
    }

    /// The transmission holding each of `keys` that one holds, with its
    /// route resolved and its state now
    /// (`surface.conversation.inbound-transmission`).
    pub(super) async fn transmission_marks(
        &self,
        keys: &BTreeSet<MatchKey>,
    ) -> Result<BTreeMap<MatchKey, TransmissionMark>, QueryError> {
        let keys: Vec<MatchKey> = keys.iter().copied().collect();
        let mut held: BTreeMap<MatchKey, TransmissionId> = BTreeMap::new();
        for chunk in keys.chunks(BATCH) {
            let chunk: BTreeSet<MatchKey> = chunk.iter().copied().collect();
            held.extend(self.stores.transmissions().holding(&chunk).await?);
        }
        let ids: BTreeSet<TransmissionId> = held.values().copied().collect();
        let aliases = self.aliases();
        let mut marks = BTreeMap::new();
        for id in ids {
            let transmission = self
                .stores
                .transmissions()
                .transmission(id)
                .await?
                .ok_or_else(|| {
                    missing(format!(
                        "transmission {} holds a match but is not stored",
                        id.ulid_text()
                    ))
                })?;
            marks.insert(
                id,
                TransmissionMark {
                    id,
                    route: transmission.route.resolved(aliases),
                    state: TransmissionStateKind::of(&transmission.state),
                },
            );
        }
        Ok(held
            .into_iter()
            .filter_map(|(key, id)| marks.get(&id).cloned().map(|mark| (key, mark)))
            .collect())
    }

    /// One page of `span`'s readers; `None` for a span L4 does not keep.
    pub(super) async fn reader_page(
        &self,
        span: SpanId,
        page: &PageRequest<SpanReaderList>,
    ) -> Result<Option<ReaderPage>, QueryError> {
        Ok(self.stores.provenance().readers(span, page).await?)
    }

    /// The pointers of `matches` read as readers and inbound marks: their
    /// origin spans (and `extra_spans`), their reader exchanges' turns,
    /// and the transmissions holding them.
    pub(super) async fn pointers(
        &self,
        matches: &[&ContentMatch],
        extra_spans: BTreeSet<SpanId>,
    ) -> Result<Pointers, QueryError> {
        let mut spans = extra_spans;
        let mut exchanges = BTreeSet::new();
        let mut keys = BTreeSet::new();
        for content in matches {
            spans.insert(content.origin());
            exchanges.insert(content.reader_exchange());
            keys.insert(MatchKey::of(content));
        }
        Ok(Pointers {
            points: self.span_points_of(&spans).await?,
            turns: self.locate_all(&exchanges).await?,
            marks: self.transmission_marks(&keys).await?,
        })
    }
}
