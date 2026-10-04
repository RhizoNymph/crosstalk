//! The evidence behind one transmission: both sides of each content match,
//! cut from the stored bodies, and the accesses behind its co-access
//! records ([`TransmissionEvidence::assemble`]).
//!
//! Every record is read first (the assembly's callbacks are synchronous),
//! in the order assembly asks for them: for each content match its origin
//! span, then both bodies; then each access its co-access records name, in
//! order of first mention, with its resource.

use std::collections::VecDeque;

use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{AccessId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::BlobStore;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l8_surface::evidence::{
    AccessDetail, EvidenceError, EvidenceRecord, MatchQuotes, TransmissionEvidence,
};
use crosstalk_spec::interfaces::l8_surface::excerpt::{ExcerptWindow, Excerpted};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, QueryError};
use crosstalk_spec::observed::message::encoding;

use crate::service::{Surface, require};
use crate::stores::{EvidenceRecords, SurfaceStores};

fn record_failed(error: crate::stores::RecordReadError) -> EvidenceError {
    EvidenceError::Store {
        reason: error.to_string(),
    }
}

impl<S: SurfaceStores> Surface<S> {
    pub(crate) async fn transmission_evidence_query(
        &self,
        caller: &Caller,
        id: TransmissionId,
        window: ExcerptWindow,
    ) -> Result<Option<TransmissionEvidence>, QueryError> {
        require(caller, Permission::Content)?;
        let Some(transmission) = self.stores.transmissions().transmission(id).await? else {
            return Ok(None);
        };
        Ok(Some(self.evidence_of(transmission, window).await?))
    }

    /// The evidence of a stored transmission, its excerpts cut with
    /// `window`.
    pub(crate) async fn evidence_of(
        &self,
        transmission: Transmission,
        window: ExcerptWindow,
    ) -> Result<TransmissionEvidence, EvidenceError> {
        let mut quotes = VecDeque::new();
        if let Some(confirmed) = transmission.state.confirmed() {
            for content_match in confirmed.content().iter() {
                let span = self
                    .stores
                    .evidence()
                    .span(content_match.origin())
                    .await
                    .map_err(record_failed)?
                    .ok_or(EvidenceError::Missing(EvidenceRecord::Span(
                        content_match.origin(),
                    )))?;
                quotes.push_back(MatchQuotes {
                    origin: self.excerpt(span.location, window).await?,
                    read: self.excerpt(content_match.read_at(), window).await?,
                });
            }
        }
        let mut named: Vec<AccessId> = Vec::new();
        for record in transmission.state.co_accesses() {
            for id in [record.write(), record.read()] {
                if !named.contains(&id) {
                    named.push(id);
                }
            }
        }
        let aliases = self.aliases();
        let mut details = VecDeque::with_capacity(named.len());
        for id in named {
            let records = self.stores.evidence();
            let access = records
                .access(id)
                .await
                .map_err(record_failed)?
                .ok_or(EvidenceError::Missing(EvidenceRecord::Access(id)))?;
            let resource = records
                .resource(access.resource)
                .await
                .map_err(record_failed)?
                .ok_or(EvidenceError::Missing(EvidenceRecord::Resource(
                    access.resource,
                )))?;
            details.push_back(AccessDetail::new(access, resource, aliases)?);
        }
        TransmissionEvidence::assemble(
            transmission,
            |_| {
                quotes.pop_front().ok_or(EvidenceError::Store {
                    reason: "more content matches than quotes read".to_owned(),
                })
            },
            |_| {
                details.pop_front().ok_or(EvidenceError::Store {
                    reason: "more accesses than details read".to_owned(),
                })
            },
        )
    }

    /// The excerpt at `location`: its body from the blob store, decoded,
    /// cut with `window`; `BodyDropped` when the store no longer holds it.
    async fn excerpt(
        &self,
        location: SpanLocation,
        window: ExcerptWindow,
    ) -> Result<Excerpted, EvidenceError> {
        let hash = location.part.message;
        let body = match self.stores.blobs().get(hash).await? {
            None => None,
            Some(bytes) => {
                let body = encoding::decode(&bytes).map_err(|error| EvidenceError::Store {
                    reason: format!("body {hash:?} does not decode: {error:?}"),
                })?;
                Some(encoding::message(body))
            }
        };
        Ok(Excerpted::of(location, body.as_ref(), window)?)
    }
}
