//! The search index's write side: what `analyze` indexes, its copy of each
//! transmission's current verdict, and the embedding model queries use.
//!
//! `SearchIndex::query` and `ProjectionSource::sample` read what these
//! writes store. Topics are read from the topic catalog's assignments and
//! agents and channels resolved through the directories at query time, so
//! nothing here is rewritten by a merge, a supersession or a re-fit.

use crate::aggregates::topic::{Embedding, EmbeddingModel};
use crate::derived::flow::transmission::Route;
use crate::derived::flow::verdict::{Observed, Verdict, VerdictRevision};
use crate::ids::{AgentId, TransmissionId};
use crate::support::Timestamp;

/// One confirmed transmission as the index stores it.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexedTransmission {
    pub transmission: TransmissionId,
    /// As attributed; resolved at query time.
    pub from: AgentId,
    pub to: AgentId,
    /// As stored; its channel is resolved at query time.
    pub route: Route,
    /// `Confirmed::at`.
    pub confirmed_at: Timestamp,
    /// The matched content, as the embedder saw it.
    pub text: String,
    /// Its embedding, when one was made.
    pub embedding: Option<Embedding>,
}

/// What `analyze` writes to the search index. None of these publishes
/// anything: search results are read, not announced.
pub trait SearchCorpus {
    /// Add or re-index a transmission: its text and parties replace the
    /// stored ones, and its embedding, if any, replaces the one from the
    /// same model (an embedding of another model is kept).
    fn index(
        &mut self,
        document: IndexedTransmission,
    ) -> impl Future<Output = Result<(), CorpusError>> + Send;

    /// Remove a transmission and every embedding of it. Removing an unknown
    /// transmission changes nothing.
    fn remove(
        &mut self,
        transmission: TransmissionId,
    ) -> impl Future<Output = Result<(), CorpusError>> + Send;

    /// `VerdictSet`: record `transmission`'s verdict at `revision` in the
    /// index's copy (`CurrentVerdict::observe`), whether or not the
    /// transmission is indexed; the first one seen becomes the copy. A
    /// revision not newer than the one held is `Stale` and changes nothing.
    /// `TopologyFilter::false_detections` reads the copy.
    fn judge(
        &mut self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        revision: VerdictRevision,
    ) -> impl Future<Output = Result<Observed, CorpusError>> + Send;

    /// The embedder's model changed: queries must now be embedded with
    /// `model`, and a query embedded with another is `WrongModel`. Vectors
    /// of the old model stay until [`SearchCorpus::drop_model`].
    fn set_model(
        &mut self,
        model: EmbeddingModel,
    ) -> impl Future<Output = Result<(), CorpusError>> + Send;

    /// Delete every vector of `model`. A projection sampling that model
    /// afterwards fails with `EmbeddingModelUnavailable`.
    fn drop_model(
        &mut self,
        model: &EmbeddingModel,
    ) -> impl Future<Output = Result<(), CorpusError>> + Send;
}

/// Why a search index write failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CorpusError {
    Store { reason: String },
}
