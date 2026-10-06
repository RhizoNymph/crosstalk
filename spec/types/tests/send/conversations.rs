//! The conversation reads' store traits: L1's exchange store, L3's
//! conversation reads and L4's provenance reads.

use crate::interfaces::l3_reconstruction::conversations::ExchangePlacement;
use std::collections::BTreeMap;

use crate::batch::IdBatch;
use crate::derived::provenance::matching::ContentMatch;
use crate::ids::{ConversationId, ExchangeId, SpanId};
use crate::interfaces::l1_canonical::exchanges::{
    ExchangeQuery, ExchangeReads, ExchangeStore, ExchangeStoreError, StoredExchange,
};
use crate::interfaces::l3_reconstruction::conversations::{
    ConversationQuery, ConversationReadError, ConversationReads, StoredConversation, TurnIndex,
    TurnSlice, TurnWindow,
};
use crate::interfaces::l4_provenance::reads::{
    ProvenanceReadError, ProvenanceReads, ReaderPage, ScanStatus, StoredSpan,
};
use crate::paging::{ConversationList, ExchangeList, Page, PageRequest, SpanReaderList};

use super::{Dummy, arg, assert_send};

impl ExchangeStore for Dummy {
    async fn put(&mut self, _exchange: StoredExchange) -> Result<(), ExchangeStoreError> {
        match *self {}
    }
}

impl ExchangeReads for Dummy {
    async fn exchanges(
        &self,
        _ids: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, StoredExchange>, ExchangeStoreError> {
        match *self {}
    }
    async fn list(
        &self,
        _query: &ExchangeQuery,
        _page: &PageRequest<ExchangeList>,
    ) -> Result<Page<StoredExchange, ExchangeList>, ExchangeStoreError> {
        match *self {}
    }
}

fn exchange_store<T: ExchangeStore + ExchangeReads>(x: &mut T, never: &Dummy) {
    assert_send(x.put(arg(never)));
    assert_send(x.exchanges(arg(never)));
    assert_send(x.list(arg(never), arg(never)));
}

impl ConversationReads for Dummy {
    async fn list(
        &self,
        _query: &ConversationQuery,
        _page: &PageRequest<ConversationList>,
    ) -> Result<Page<StoredConversation, ConversationList>, ConversationReadError> {
        match *self {}
    }
    async fn conversation(
        &self,
        _id: ConversationId,
    ) -> Result<Option<StoredConversation>, ConversationReadError> {
        match *self {}
    }
    async fn successors(
        &self,
        _id: ConversationId,
    ) -> Result<Vec<StoredConversation>, ConversationReadError> {
        match *self {}
    }
    async fn turns(
        &self,
        _id: ConversationId,
        _window: &TurnWindow,
    ) -> Result<Option<TurnSlice>, ConversationReadError> {
        match *self {}
    }
    async fn locate(
        &self,
        _ids: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, ExchangePlacement>, ConversationReadError> {
        match *self {}
    }
    async fn branch_turn(
        &self,
        _parent: ConversationId,
        _shared_prefix: u32,
    ) -> Result<Option<TurnIndex>, ConversationReadError> {
        match *self {}
    }
}

fn conversation_reads<T: ConversationReads>(x: &T, never: &Dummy) {
    assert_send(x.list(arg(never), arg(never)));
    assert_send(x.conversation(arg(never)));
    assert_send(x.successors(arg(never)));
    assert_send(x.turns(arg(never), arg(never)));
    assert_send(x.locate(arg(never)));
    assert_send(x.branch_turn(arg(never), arg(never)));
}

impl ProvenanceReads for Dummy {
    async fn output_spans(
        &self,
        _exchanges: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, Vec<StoredSpan>>, ProvenanceReadError> {
        match *self {}
    }
    async fn matches_read_in(
        &self,
        _exchanges: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, Vec<ContentMatch>>, ProvenanceReadError> {
        match *self {}
    }
    async fn readers(
        &self,
        _span: SpanId,
        _page: &PageRequest<SpanReaderList>,
    ) -> Result<Option<ReaderPage>, ProvenanceReadError> {
        match *self {}
    }
    async fn scan_status(
        &self,
        _exchanges: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, ScanStatus>, ProvenanceReadError> {
        match *self {}
    }
}

fn provenance_reads<T: ProvenanceReads>(x: &T, never: &Dummy) {
    assert_send(x.output_spans(arg(never)));
    assert_send(x.matches_read_in(arg(never)));
    assert_send(x.readers(arg(never), arg(never)));
    assert_send(x.scan_status(arg(never)));
}

#[test]
fn conversation_read_futures_are_send() {
    let _ = exchange_store::<Dummy>;
    let _ = conversation_reads::<Dummy>;
    let _ = provenance_reads::<Dummy>;
}
