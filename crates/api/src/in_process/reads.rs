//! The conversation reads' stores an in-process surface reads
//! ([`ConversationStores`]): L1's exchange records, L3's conversations and
//! L4's records. A composer that runs those layers (the gateway's live
//! process) hands its own stores in; [`Unrecorded`], the default, holds
//! none, for a surface whose world records no conversations (the UI's
//! seeded world, the HTTP server's tests).

use crosstalk_spec::interfaces::l3_reconstruction::conversations::ExchangePlacement;
use std::collections::BTreeMap;

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::provenance::matching::ContentMatch;
use crosstalk_spec::derived::provenance::span::OriginatedSpan;
use crosstalk_spec::ids::{ConversationId, ExchangeId, SpanId};
use crosstalk_spec::interfaces::l1_canonical::exchanges::{
    ExchangeQuery, ExchangeReads, ExchangeStoreError, StoredExchange,
};
use crosstalk_spec::interfaces::l3_reconstruction::conversations::{
    ConversationQuery, ConversationReadError, ConversationReads, StoredConversation, TurnIndex,
    TurnSlice, TurnWindow,
};
use crosstalk_spec::interfaces::l4_provenance::reads::{
    ProvenanceReadError, ProvenanceReads, ReaderPage, ScanStatus, StoredSpan,
};
use crosstalk_spec::interfaces::l4_provenance::{IndexedSpan, SpanIndex, SpanIndexError};
use crosstalk_spec::paging::{ConversationList, ExchangeList, Page, PageRequest, SpanReaderList};

/// The three stores behind the conversation reads, as handles that share
/// state with whatever writes them.
pub trait ConversationStores: Clone + Send + Sync + 'static {
    type Exchanges: ExchangeReads + Send + Sync + 'static;
    type Conversations: ConversationReads + Send + Sync + 'static;
    type Provenance: ProvenanceReads + Send + Sync + 'static;

    fn exchanges(&self) -> &Self::Exchanges;
    fn conversations(&self) -> &Self::Conversations;
    fn provenance(&self) -> &Self::Provenance;
}

/// No exchange, conversation, span or match recorded: every list is
/// empty, every lookup absent.
#[derive(Debug, Clone, Copy, Default)]
pub struct Unrecorded;

impl ConversationStores for Unrecorded {
    type Exchanges = Unrecorded;
    type Conversations = Unrecorded;
    type Provenance = Unrecorded;

    fn exchanges(&self) -> &Unrecorded {
        self
    }
    fn conversations(&self) -> &Unrecorded {
        self
    }
    fn provenance(&self) -> &Unrecorded {
        self
    }
}

impl ExchangeReads for Unrecorded {
    async fn exchanges(
        &self,
        _ids: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, StoredExchange>, ExchangeStoreError> {
        Ok(BTreeMap::new())
    }

    async fn list(
        &self,
        _query: &ExchangeQuery,
        page: &PageRequest<ExchangeList>,
    ) -> Result<Page<StoredExchange, ExchangeList>, ExchangeStoreError> {
        if page.after.is_some() {
            return Err(ExchangeStoreError::InvalidCursor);
        }
        Page::last(page.size, Vec::new()).map_err(|error| ExchangeStoreError::Store {
            reason: format!("{error:?}"),
        })
    }
}

impl ConversationReads for Unrecorded {
    async fn list(
        &self,
        _query: &ConversationQuery,
        page: &PageRequest<ConversationList>,
    ) -> Result<Page<StoredConversation, ConversationList>, ConversationReadError> {
        if page.after.is_some() {
            return Err(ConversationReadError::InvalidCursor);
        }
        Page::last(page.size, Vec::new()).map_err(|error| ConversationReadError::Store {
            reason: format!("{error:?}"),
        })
    }

    async fn conversation(
        &self,
        _id: ConversationId,
    ) -> Result<Option<StoredConversation>, ConversationReadError> {
        Ok(None)
    }

    async fn successors(
        &self,
        _id: ConversationId,
    ) -> Result<Vec<StoredConversation>, ConversationReadError> {
        Ok(Vec::new())
    }

    async fn turns(
        &self,
        _id: ConversationId,
        _window: &TurnWindow,
    ) -> Result<Option<TurnSlice>, ConversationReadError> {
        Ok(None)
    }

    async fn locate(
        &self,
        _ids: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, ExchangePlacement>, ConversationReadError> {
        Ok(BTreeMap::new())
    }

    async fn branch_turn(
        &self,
        _parent: ConversationId,
        _shared_prefix: u32,
    ) -> Result<Option<TurnIndex>, ConversationReadError> {
        Ok(None)
    }
}

impl SpanIndex for Unrecorded {
    async fn record(&mut self, _span: &OriginatedSpan) -> Result<(), SpanIndexError> {
        Ok(())
    }

    async fn spans(
        &self,
        _ids: &IdBatch<SpanId>,
    ) -> Result<BTreeMap<SpanId, IndexedSpan>, SpanIndexError> {
        Ok(BTreeMap::new())
    }
}

impl ProvenanceReads for Unrecorded {
    async fn output_spans(
        &self,
        _exchanges: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, Vec<StoredSpan>>, ProvenanceReadError> {
        Ok(BTreeMap::new())
    }

    async fn matches_read_in(
        &self,
        _exchanges: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, Vec<ContentMatch>>, ProvenanceReadError> {
        Ok(BTreeMap::new())
    }

    async fn readers(
        &self,
        _span: SpanId,
        _page: &PageRequest<SpanReaderList>,
    ) -> Result<Option<ReaderPage>, ProvenanceReadError> {
        Ok(None)
    }

    async fn scan_status(
        &self,
        _exchanges: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, ScanStatus>, ProvenanceReadError> {
        Ok(BTreeMap::new())
    }
}
