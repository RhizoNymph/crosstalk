//! The evidence behind one transmission: the text of each content match on
//! both sides, and the accesses behind each co-access record
//! (`QueryApi::transmission_evidence`, Content).
//!
//! **Assembly.** The surface reads the transmission, then for each of its
//! content matches the origin span's location (L4's span records), and
//! both bodies from the blob store, and for each access its co-access
//! records name the access and its resource (L5's records). Verdicts are
//! not part of the evidence; they are `QueryApi::verdicts` (View).
//! [`TransmissionEvidence::assemble`] fixes what is listed from the
//! transmission itself, so the evidence cannot hold a match or an access
//! the transmission does not name, or miss one:
//!
//! - `matches`: one [`MatchEvidence`] per content match of the confirmed
//!   transmission, in stored order; none before it is confirmed. `origin`
//!   is cut around the origin span's location ([`Span::location`]), `read`
//!   around the match's `read_at`, as the text arrived (before decoding).
//! - `accesses`: one [`AccessDetail`] per distinct access the state's
//!   co-access records name ([`TransmissionState::co_accesses`]), in order
//!   of first mention, each record's write before its read.
//!
//! **Stored ids.** The transmission, its matches and its accesses are
//! records and keep the ids they were stored with. The canonical sender,
//! reader and route are the transmission's [`TransmissionSummary`]
//! (`QueryApi::transmissions_by_id`); each access detail adds the canonical
//! agent of its access, the one thing the summary does not name.
//!
//! **Failure.** A dropped body is an outcome of the excerpt
//! ([`Excerpted::BodyDropped`]). Anything else that keeps the evidence from
//! being read is an [`EvidenceError`], which becomes a `QueryError::Store`.
//!
//! [`Span::location`]: crate::derived::provenance::span::Span::location
//! [`TransmissionState::co_accesses`]: crate::derived::flow::transmission::TransmissionState::co_accesses
//! [`TransmissionSummary`]: super::summary::TransmissionSummary

use serde::{Deserialize, Serialize};

use crate::aliases::Aliases;
use crate::derived::flow::access::Access;
use crate::derived::flow::resource::Resource;
use crate::derived::flow::transmission::Transmission;
use crate::derived::provenance::matching::ContentMatch;
use crate::ids::{AccessId, AgentId, ResourceId, SpanId};
use crate::interfaces::l2_transport::BlobError;
use crate::wire::Rejected;

use super::excerpt::{ExcerptError, Excerpted};

/// The two excerpts of one content match, as the surface cuts them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct MatchQuotes {
    /// Around the sender's originated span.
    pub origin: Excerpted,
    /// Around the range the reader read, as it arrived (before decoding).
    pub read: Excerpted,
}

/// One content match of the transmission with the sender's and the
/// reader's text. Built only by [`TransmissionEvidence::assemble`].
/// Decoding cannot rerun it, which reads the stored records.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct MatchEvidence {
    content_match: ContentMatch,
    quotes: MatchQuotes,
}

impl MatchEvidence {
    pub fn content_match(&self) -> &ContentMatch {
        &self.content_match
    }

    pub fn origin(&self) -> &Excerpted {
        &self.quotes.origin
    }

    pub fn read(&self) -> &Excerpted {
        &self.quotes.read
    }
}

/// An access a co-access record names, with its resource and the canonical
/// agent behind it.
///
/// Built only through [`AccessDetail::new`], which checks that the resource
/// is the access's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawAccessDetail")]
pub struct AccessDetail {
    access: Access,
    resource: Resource,
    agent: AgentId,
}

/// Records that do not belong together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidEvidence {
    /// The resource is not the one the access touched.
    ResourceMismatch {
        access: AccessId,
        expected: ResourceId,
        got: ResourceId,
    },
    /// The access looked up is not the one asked for.
    WrongAccess { asked: AccessId, got: AccessId },
}

/// [`AccessDetail`]'s fields, decoded without the check. Decoding goes
/// through [`AccessDetail::new`], with the recorded `agent` as the
/// resolution of the access's agent.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawAccessDetail {
    access: Access,
    resource: Resource,
    agent: AgentId,
}

impl TryFrom<RawAccessDetail> for AccessDetail {
    type Error = Rejected<InvalidEvidence>;

    fn try_from(raw: RawAccessDetail) -> Result<Self, Self::Error> {
        let agent = raw.agent;
        Self::new(raw.access, raw.resource, move |_: AgentId| agent)
            .map_err(|error| Rejected::new("access detail", error))
    }
}

impl AccessDetail {
    /// `agent` is `access.agent` resolved through `aliases`.
    pub fn new(
        access: Access,
        resource: Resource,
        aliases: impl Aliases,
    ) -> Result<Self, InvalidEvidence> {
        if resource.id != access.resource {
            return Err(InvalidEvidence::ResourceMismatch {
                access: access.id,
                expected: access.resource,
                got: resource.id,
            });
        }
        let agent = aliases.agent(access.agent);
        Ok(Self {
            access,
            resource,
            agent,
        })
    }

    /// As stored.
    pub fn access(&self) -> &Access {
        &self.access
    }

    pub fn resource(&self) -> &Resource {
        &self.resource
    }

    /// The canonical agent that read or wrote.
    pub fn agent(&self) -> AgentId {
        self.agent
    }
}

/// Everything the evidence page shows about one transmission, besides its
/// summary and verdicts. Built only by [`TransmissionEvidence::assemble`].
/// A response; decoding cannot rerun `assemble`, which reads the stored
/// records the transmission names.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TransmissionEvidence {
    transmission: Transmission,
    matches: Vec<MatchEvidence>,
    accesses: Vec<AccessDetail>,
}

impl TransmissionEvidence {
    /// The evidence of `transmission`: `quote` is called once per content
    /// match, in stored order, and `detail` once per distinct access its
    /// co-access records name, in order of first mention. A detail for an
    /// access other than the one asked for is `WrongAccess`. The first error
    /// stops assembly.
    pub fn assemble<E: From<InvalidEvidence>>(
        transmission: Transmission,
        mut quote: impl FnMut(&ContentMatch) -> Result<MatchQuotes, E>,
        mut detail: impl FnMut(AccessId) -> Result<AccessDetail, E>,
    ) -> Result<Self, E> {
        let mut matches = Vec::new();
        if let Some(confirmed) = transmission.state.confirmed() {
            for content_match in confirmed.content().iter() {
                matches.push(MatchEvidence {
                    quotes: quote(content_match)?,
                    content_match: content_match.clone(),
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
        let mut accesses = Vec::with_capacity(named.len());
        for asked in named {
            let found = detail(asked)?;
            if found.access.id != asked {
                return Err(InvalidEvidence::WrongAccess {
                    asked,
                    got: found.access.id,
                }
                .into());
            }
            accesses.push(found);
        }
        Ok(Self {
            transmission,
            matches,
            accesses,
        })
    }

    /// As stored.
    pub fn transmission(&self) -> &Transmission {
        &self.transmission
    }

    /// One per content match, in stored order; empty before confirmation.
    pub fn matches(&self) -> &[MatchEvidence] {
        &self.matches
    }

    /// One per distinct access the co-access records name.
    pub fn accesses(&self) -> &[AccessDetail] {
        &self.accesses
    }
}

/// A record the transmission names that is not stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceRecord {
    Span(SpanId),
    Access(AccessId),
    Resource(ResourceId),
}

/// Why the surface could not read a transmission's evidence. A body the
/// blob store no longer holds is not one of these: it is
/// [`Excerpted::BodyDropped`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvidenceError {
    /// The transmission, span, access or resource records could not be
    /// read; a retry may succeed.
    Store {
        reason: String,
    },
    Blob(BlobError),
    /// A record the transmission names is missing. Spans, accesses and
    /// resources are never deleted, so this is a fault.
    Missing(EvidenceRecord),
    /// A stored location does not fit its stored body.
    Excerpt(ExcerptError),
    /// Records that do not belong together.
    Invalid(InvalidEvidence),
}

impl From<BlobError> for EvidenceError {
    fn from(error: BlobError) -> Self {
        Self::Blob(error)
    }
}

impl From<ExcerptError> for EvidenceError {
    fn from(error: ExcerptError) -> Self {
        Self::Excerpt(error)
    }
}

impl From<InvalidEvidence> for EvidenceError {
    fn from(error: InvalidEvidence) -> Self {
        Self::Invalid(error)
    }
}
