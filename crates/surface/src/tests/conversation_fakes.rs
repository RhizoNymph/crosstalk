//! Test doubles for the conversation reads' stores, which no reference
//! store in `crosstalk-memory` covers (their implementations live in the
//! layer crates, which the surface may not depend on): L1's exchanges,
//! L3's conversations and turns, and L4's spans, matches and scan status,
//! each a map a test fills directly. Lists and reader pages are paged by
//! offset; no test traverses past the cursors it checks.

use crosstalk_spec::interfaces::l3_reconstruction::conversations::ExchangePlacement;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::provenance::matching::ContentMatch;
use crosstalk_spec::derived::provenance::span::{Origin, OriginatedSpan};
use crosstalk_spec::ids::{ConversationId, ExchangeId, SpanId};
use crosstalk_spec::interfaces::l1_canonical::exchanges::{
    ExchangeQuery, ExchangeReads, ExchangeStoreError, StoredExchange,
};
use crosstalk_spec::interfaces::l3_reconstruction::conversations::{
    ConversationQuery, ConversationReadError, ConversationReads, StoredConversation, StoredTurn,
    TurnIndex, TurnSlice, TurnWindow,
};
use crosstalk_spec::interfaces::l4_provenance::reads::{
    ProvenanceReadError, ProvenanceReads, ReaderPage, ScanStatus, StoredSpan,
};
use crosstalk_spec::interfaces::l4_provenance::{IndexedSpan, SpanIndex, SpanIndexError};
use crosstalk_spec::paging::{
    ConversationList, Cursor, ExchangeList, Page, PageRequest, SpanReaderList,
};
use crosstalk_spec::support::{NonEmpty, Timestamp};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One page of `items` from `offset`, the next cursor an offset token.
fn offset_page<T: Clone, L>(
    items: &[T],
    offset: usize,
    size: crosstalk_spec::paging::PageSize,
) -> Page<T, L> {
    let limit = usize::from(size.get().get());
    let rest = items.get(offset..).unwrap_or(&[]);
    if rest.len() <= limit {
        return Page::last(size, rest.to_vec()).unwrap_or_else(|error| panic!("{error:?}"));
    }
    let served = NonEmpty::from_vec(rest[..limit].to_vec()).unwrap_or_else(|| panic!("served"));
    let next = Cursor::from_token(format!("o{}", offset + limit))
        .unwrap_or_else(|error| panic!("{error:?}"));
    Page::more(size, served, next).unwrap_or_else(|error| panic!("{error:?}"))
}

fn offset_of<L>(cursor: Option<&Cursor<L>>) -> Option<usize> {
    match cursor {
        None => Some(0),
        Some(cursor) => cursor.token().strip_prefix('o')?.parse().ok(),
    }
}

#[derive(Debug, Default)]
struct Conversations {
    stored: BTreeMap<ConversationId, StoredConversation>,
    turns: BTreeMap<ConversationId, Vec<StoredTurn>>,
}

/// L3's conversations as a test puts them.
#[derive(Debug, Clone, Default)]
pub struct TestConversations {
    inner: Arc<Mutex<Conversations>>,
}

impl TestConversations {
    /// Keep `conversation` with `turns` (their indexes 0, 1, ...).
    pub fn put(&self, conversation: StoredConversation, turns: Vec<StoredTurn>) {
        let mut inner = lock(&self.inner);
        inner.turns.insert(conversation.conversation.id, turns);
        inner
            .stored
            .insert(conversation.conversation.id, conversation);
    }
}

impl ConversationReads for TestConversations {
    async fn list(
        &self,
        query: &ConversationQuery,
        page: &PageRequest<ConversationList>,
    ) -> Result<Page<StoredConversation, ConversationList>, ConversationReadError> {
        let offset = offset_of(page.after.as_ref()).ok_or(ConversationReadError::InvalidCursor)?;
        let inner = lock(&self.inner);
        let admitted: Vec<StoredConversation> = inner
            .stored
            .values()
            .rev()
            .filter(|stored| query.admits(stored))
            .cloned()
            .collect();
        Ok(offset_page(&admitted, offset, page.size))
    }

    async fn conversation(
        &self,
        id: ConversationId,
    ) -> Result<Option<StoredConversation>, ConversationReadError> {
        Ok(lock(&self.inner).stored.get(&id).cloned())
    }

    async fn successors(
        &self,
        id: ConversationId,
    ) -> Result<Vec<StoredConversation>, ConversationReadError> {
        let mut successors: Vec<StoredConversation> = lock(&self.inner)
            .stored
            .values()
            .filter(|stored| stored.conversation.origin.source() == Some(id))
            .cloned()
            .collect();
        successors.sort_by_key(|stored| (stored.started_at, stored.conversation.id));
        Ok(successors)
    }

    async fn turns(
        &self,
        id: ConversationId,
        window: &TurnWindow,
    ) -> Result<Option<TurnSlice>, ConversationReadError> {
        let inner = lock(&self.inner);
        if !inner.stored.contains_key(&id) {
            return Ok(None);
        }
        let turns = inner.turns.get(&id).cloned().unwrap_or_default();
        let total = u32::try_from(turns.len()).unwrap_or(u32::MAX);
        let range = window.range(total);
        Ok(Some(TurnSlice {
            total,
            turns: turns[range.start as usize..range.end as usize].to_vec(),
        }))
    }

    async fn locate(
        &self,
        ids: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, ExchangePlacement>, ConversationReadError> {
        let inner = lock(&self.inner);
        let mut located = BTreeMap::new();
        for (conversation, turns) in &inner.turns {
            for turn in turns {
                if ids.ids().contains(&turn.exchange) {
                    located.insert(
                        turn.exchange,
                        ExchangePlacement {
                            agent: turn.agent,
                            conversation: *conversation,
                            turn: turn.index,
                        },
                    );
                }
            }
        }
        Ok(located)
    }

    async fn branch_turn(
        &self,
        parent: ConversationId,
        shared_prefix: u32,
    ) -> Result<Option<TurnIndex>, ConversationReadError> {
        Ok(lock(&self.inner).turns.get(&parent).and_then(|turns| {
            turns
                .iter()
                .filter(|turn| turn.history_end <= shared_prefix)
                .map(|turn| turn.index)
                .next_back()
        }))
    }
}

/// L1's exchange records as a test puts them.
#[derive(Debug, Clone, Default)]
pub struct TestExchanges {
    inner: Arc<Mutex<BTreeMap<ExchangeId, StoredExchange>>>,
}

impl TestExchanges {
    pub fn put(&self, exchange: StoredExchange) {
        lock(&self.inner).insert(exchange.id(), exchange);
    }
}

impl ExchangeReads for TestExchanges {
    async fn exchanges(
        &self,
        ids: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, StoredExchange>, ExchangeStoreError> {
        let inner = lock(&self.inner);
        Ok(ids
            .ids()
            .iter()
            .filter_map(|id| inner.get(id).map(|stored| (*id, stored.clone())))
            .collect())
    }

    async fn list(
        &self,
        query: &ExchangeQuery,
        page: &PageRequest<ExchangeList>,
    ) -> Result<Page<StoredExchange, ExchangeList>, ExchangeStoreError> {
        let offset = offset_of(page.after.as_ref()).ok_or(ExchangeStoreError::InvalidCursor)?;
        let mut admitted: Vec<StoredExchange> = lock(&self.inner)
            .values()
            .filter(|stored| query.admits(stored))
            .cloned()
            .collect();
        admitted.sort_by_key(|stored| {
            std::cmp::Reverse((stored.exchange.meta.started_at, stored.id()))
        });
        Ok(offset_page(&admitted, offset, page.size))
    }
}

#[derive(Debug, Default)]
struct Provenance {
    /// Each recorded exchange's status, output spans and matches read in
    /// it with the reader exchange's start.
    status: BTreeMap<ExchangeId, ScanStatus>,
    spans: BTreeMap<ExchangeId, Vec<StoredSpan>>,
    matches: BTreeMap<ExchangeId, (Timestamp, Vec<ContentMatch>)>,
}

/// L4's records as a test puts them.
#[derive(Debug, Clone, Default)]
pub struct TestProvenance {
    inner: Arc<Mutex<Provenance>>,
}

impl TestProvenance {
    /// Record `exchange`, started `at`, as `status` with its output's
    /// `spans` (common ones left out) and the `matches` read in it.
    pub fn put(
        &self,
        exchange: ExchangeId,
        at: Timestamp,
        status: ScanStatus,
        spans: Vec<StoredSpan>,
        matches: Vec<ContentMatch>,
    ) {
        let mut inner = lock(&self.inner);
        inner.status.insert(exchange, status);
        inner.spans.insert(exchange, spans);
        inner.matches.insert(exchange, (at, matches));
    }

    fn span(&self, id: SpanId) -> Option<StoredSpan> {
        lock(&self.inner)
            .spans
            .values()
            .flatten()
            .find(|stored| stored.span.id == id)
            .cloned()
    }
}

impl SpanIndex for TestProvenance {
    async fn record(&mut self, _span: &OriginatedSpan) -> Result<(), SpanIndexError> {
        Ok(())
    }

    async fn spans(
        &self,
        ids: &IdBatch<SpanId>,
    ) -> Result<BTreeMap<SpanId, IndexedSpan>, SpanIndexError> {
        Ok(ids
            .ids()
            .iter()
            .filter_map(|id| self.span(*id))
            .filter(|stored| {
                stored.span.state.origin() == Some(Origin::Originated) || stored.forward.is_some()
            })
            .map(|stored| {
                (
                    stored.span.id,
                    IndexedSpan {
                        exchange: stored.span.exchange,
                        author: stored.span.agent,
                        location: stored.span.location,
                    },
                )
            })
            .collect())
    }
}

impl ProvenanceReads for TestProvenance {
    async fn output_spans(
        &self,
        exchanges: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, Vec<StoredSpan>>, ProvenanceReadError> {
        let inner = lock(&self.inner);
        Ok(exchanges
            .ids()
            .iter()
            .filter_map(|id| inner.spans.get(id).map(|spans| (*id, spans.clone())))
            .collect())
    }

    async fn matches_read_in(
        &self,
        exchanges: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, Vec<ContentMatch>>, ProvenanceReadError> {
        let inner = lock(&self.inner);
        Ok(exchanges
            .ids()
            .iter()
            .filter_map(|id| inner.matches.get(id).map(|(_, read)| (*id, read.clone())))
            .collect())
    }

    async fn readers(
        &self,
        span: SpanId,
        page: &PageRequest<SpanReaderList>,
    ) -> Result<Option<ReaderPage>, ProvenanceReadError> {
        let offset = offset_of(page.after.as_ref()).ok_or(ProvenanceReadError::InvalidCursor)?;
        if self.span(span).is_none() {
            return Ok(None);
        }
        let inner = lock(&self.inner);
        let mut readers: Vec<(Timestamp, usize, ContentMatch)> = Vec::new();
        for (at, read) in inner.matches.values() {
            for content in read.iter().filter(|content| content.origin() == span) {
                readers.push((*at, readers.len(), content.clone()));
            }
        }
        readers.sort_by_key(|(at, order, _)| std::cmp::Reverse((*at, *order)));
        let matches: Vec<ContentMatch> =
            readers.into_iter().map(|(_, _, content)| content).collect();
        Ok(Some(ReaderPage {
            total: u32::try_from(matches.len()).unwrap_or(u32::MAX),
            page: offset_page(&matches, offset, page.size),
        }))
    }

    async fn scan_status(
        &self,
        exchanges: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, ScanStatus>, ProvenanceReadError> {
        let inner = lock(&self.inner);
        Ok(exchanges
            .ids()
            .iter()
            .filter_map(|id| inner.status.get(id).map(|status| (*id, *status)))
            .collect())
    }
}
