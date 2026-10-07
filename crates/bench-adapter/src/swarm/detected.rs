//! The gateway's detections, from a saved L8 export and the evidence of
//! each exported transmission: which transmissions a `from-export` run
//! writes ([`choose`]) and what their rows look up ([`SwarmDirectory`]).
//!
//! - **Export** (`POST /exports`, JSONL; spec `export::framing`): a header,
//!   one `transmission` row per exported transmission (its
//!   `TransmissionSummary`, its state when the export's states are not the
//!   default, the strongest match class of a confirmed one, optional
//!   content), and
//!   a trailer. It is read with the spec's `read_jsonl` and verified with
//!   `verify_export` (the trailer completes it, the counts agree, the BLAKE3
//!   row digest matches), so a cut-off export is refused. Its rows say
//!   which transmissions to write.
//! - **Evidence** (`GET /transmissions/{id}/evidence`, one
//!   `TransmissionEvidence` JSON per line): the stored `Transmission` with
//!   its content matches (reader exchange, read location, carrier, match
//!   kind) and the accesses of its co-access records (agent, exchange,
//!   resource). An export row with no evidence line is logged and not
//!   written.
//!
//! A channel's resources are the locators of the accesses behind its
//! transmissions.
//!
//! **Co-access.** A suspected or discarded transmission has no content
//! match, only co-access records. Its rows come from the accesses its
//! evidence lists ([`AccessDetail`]: the access, its resource and its
//! canonical agent): the write's agent to the read's agent, at the read's
//! exchange, located at the whole tool result the read returned
//! (`AccessOp::Read::result`) and the write's whole tool call. Those
//! parts' texts come from the gateway's blobs.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::BufRead;

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::access::AccessOp;
use crosstalk_spec::derived::flow::resource::{Locator, Resource};
use crosstalk_spec::derived::flow::transmission::{Route, TransmissionState};
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{AccessId, ChannelId, ExchangeId, SpanId, TransmissionId};
use crosstalk_spec::interfaces::l4_provenance::IndexedSpan;
use crosstalk_spec::interfaces::l8_surface::evidence::AccessDetail;
use crosstalk_spec::interfaces::l8_surface::evidence::TransmissionEvidence;
use crosstalk_spec::interfaces::l8_surface::export::digest::{ROW_DIGEST_CONTEXT, RowHasher};
use crosstalk_spec::interfaces::l8_surface::export::framing::{JsonlExport, read_jsonl};
use crosstalk_spec::interfaces::l8_surface::export::rows::ExportRow;
use crosstalk_spec::observed::message::PartRef;
use crosstalk_spec::support::Blake3;

use super::bodies::{Bodies, Cached};
use super::window::outside_reader;
use crate::directory::Directory;
use crate::location::whole_part;

/// The export's row digest: BLAKE3 in key-derivation mode under
/// [`ROW_DIGEST_CONTEXT`], as the gateway computes it.
pub struct Blake3RowHasher(blake3::Hasher);

impl Default for Blake3RowHasher {
    fn default() -> Self {
        Self(blake3::Hasher::new_derive_key(ROW_DIGEST_CONTEXT))
    }
}

impl RowHasher for Blake3RowHasher {
    fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    fn finalize(&self) -> Blake3 {
        Blake3::from_bytes(*self.0.finalize().as_bytes())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DetectedError {
    #[error("the export is not JSONL export framing: {0}")]
    Framing(String),
    #[error("the export does not verify: {0}")]
    Incomplete(String),
    #[error("the export holds {0} rows, not transmissions")]
    NotTransmissions(&'static str),
    #[error("reading evidence line {line}: {source}")]
    EvidenceRead {
        line: usize,
        #[source]
        source: std::io::Error,
    },
    #[error("evidence line {line} is not a transmission's evidence: {source}")]
    EvidenceDecode {
        line: usize,
        #[source]
        source: serde_json::Error,
    },
}

/// A verified transmissions export.
#[derive(Debug, Clone)]
pub struct Exported {
    pub export: JsonlExport,
    /// The exported transmissions, in export order.
    pub transmissions: Vec<TransmissionId>,
}

/// Reads and verifies a JSONL transmissions export.
pub fn read_export(bytes: &[u8]) -> Result<Exported, DetectedError> {
    let export = read_jsonl(bytes).map_err(|error| DetectedError::Framing(format!("{error:?}")))?;
    export
        .verify(Blake3RowHasher::default())
        .map_err(|error| DetectedError::Incomplete(format!("{error:?}")))?;
    let mut transmissions = Vec::with_capacity(export.rows.len());
    for row in &export.rows {
        match row {
            ExportRow::Transmission(row) => transmissions.push(row.summary().id),
            ExportRow::Edge(_) => return Err(DetectedError::NotTransmissions("edge")),
            ExportRow::Access(_) => return Err(DetectedError::NotTransmissions("access")),
            ExportRow::Topic(_) => return Err(DetectedError::NotTransmissions("topic")),
            ExportRow::Point(_) => return Err(DetectedError::NotTransmissions("point")),
            ExportRow::Verdict(_) => return Err(DetectedError::NotTransmissions("verdict")),
        }
    }
    Ok(Exported {
        export,
        transmissions,
    })
}

/// Reads an evidence file: one `TransmissionEvidence` per non-blank line.
pub fn read_evidence<R: BufRead>(input: R) -> Result<Vec<TransmissionEvidence>, DetectedError> {
    let mut out = Vec::new();
    for (at, line) in input.lines().enumerate() {
        let line_no = at + 1;
        let text = line.map_err(|source| DetectedError::EvidenceRead {
            line: line_no,
            source,
        })?;
        if text.trim().is_empty() {
            continue;
        }
        out.push(
            serde_json::from_str(&text).map_err(|source| DetectedError::EvidenceDecode {
                line: line_no,
                source,
            })?,
        );
    }
    Ok(out)
}

/// The gateway's channels with the resources it saw, and the accesses its
/// evidence lists.
#[derive(Debug, Clone, Default)]
pub struct SwarmDirectory {
    channels: BTreeMap<ChannelId, Vec<Locator>>,
    /// The accesses the evidence lists, with their resources.
    accesses: BTreeMap<AccessId, (Access, Resource)>,
    /// The whole text of each part an access names (a read's tool result,
    /// a write's tool call), by its exchange and part.
    parts: HashMap<(ExchangeId, PartRef), SpanLocation>,
}

impl SwarmDirectory {
    /// Learns from `evidence`, reading the parts its accesses name from
    /// `bodies`.
    pub fn learn<B: Bodies>(evidence: &[&TransmissionEvidence], bodies: &mut Cached<B>) -> Self {
        let mut channels: BTreeMap<ChannelId, Vec<Locator>> = BTreeMap::new();
        let mut accesses: BTreeMap<AccessId, (Access, Resource)> = BTreeMap::new();
        let mut parts: HashMap<(ExchangeId, PartRef), SpanLocation> = HashMap::new();
        for item in evidence {
            let transmission = item.transmission();
            for detail in item.accesses() {
                learn_access(detail, bodies, &mut accesses, &mut parts);
                if let Route::Channel(channel) = &transmission.route {
                    let held = channels.entry(*channel).or_default();
                    let locator = detail.resource().locator.clone();
                    if !held.contains(&locator) {
                        held.push(locator);
                    }
                }
            }
        }
        Self {
            channels,
            accesses,
            parts,
        }
    }
}

/// Keeps `detail`'s access and resource, and the whole text of the part it
/// names when the blobs hold that message and the part has text. A part
/// left unknown makes the co-access unlocated, and its transmission is
/// dropped.
fn learn_access<B: Bodies>(
    detail: &AccessDetail,
    bodies: &mut Cached<B>,
    accesses: &mut BTreeMap<AccessId, (Access, Resource)>,
    parts: &mut HashMap<(ExchangeId, PartRef), SpanLocation>,
) {
    let access = detail.access();
    accesses
        .entry(access.id)
        .or_insert_with(|| (access.clone(), detail.resource().clone()));
    let part = match &access.op {
        AccessOp::Read { result } => *result,
        AccessOp::Write { call, .. } => *call,
    };
    if parts.contains_key(&(access.exchange, part)) {
        return;
    }
    let Ok(message) = bodies.get(part.message) else {
        return;
    };
    if let Ok(location) = whole_part(message, part.index) {
        parts.insert((access.exchange, part), location);
    }
}

impl Directory for SwarmDirectory {
    fn channel(&self, id: ChannelId) -> Option<&[Locator]> {
        self.channels.get(&id).map(Vec::as_slice)
    }

    // The saved export carries no span records (`SpanIndex::span` is not on
    // the API), so origins of content matches stay unknown.
    fn span(&self, _id: SpanId) -> Option<IndexedSpan> {
        None
    }

    fn access(&self, id: AccessId) -> Option<&(Access, Resource)> {
        self.accesses.get(&id)
    }

    fn whole_part(&self, exchange: ExchangeId, part: PartRef) -> Option<SpanLocation> {
        self.parts.get(&(exchange, part)).copied()
    }
}

/// The evidence a run's rows are written from: every exported
/// transmission's (one without evidence is logged and left out), then
/// every suspected or discarded transmission's the export cannot hold (a
/// settled export holds confirmed and discarded ones, so access-only
/// transmissions outside it come from their evidence lines alone), less
/// those whose every reader exchange is in `outside` (the log's exchanges
/// outside the run window: another run's), logged.
pub fn choose<'a>(
    exported: &Exported,
    evidence: &'a [TransmissionEvidence],
    outside: &HashSet<ExchangeId>,
) -> Vec<&'a TransmissionEvidence> {
    let by_id: BTreeMap<TransmissionId, &TransmissionEvidence> = evidence
        .iter()
        .map(|item| (item.transmission().id, item))
        .collect();
    let mut chosen = Vec::with_capacity(exported.transmissions.len());
    let in_export: BTreeSet<TransmissionId> = exported.transmissions.iter().copied().collect();
    for id in &exported.transmissions {
        match by_id.get(id) {
            Some(item) => chosen.push(*item),
            None => tracing::warn!(
                transmission = %id.ulid_text(),
                "an exported transmission has no evidence; not written"
            ),
        }
    }
    chosen.extend(
        by_id
            .iter()
            .filter(|(id, item)| !in_export.contains(id) && access_only(item))
            .map(|(_, item)| *item),
    );
    chosen.retain(|item| match outside_reader(item, outside) {
        Some(exchange) => {
            tracing::info!(
                transmission = %item.transmission().id.ulid_text(),
                exchange = %exchange.ulid_text(),
                "a transmission read outside the run window; not written"
            );
            false
        }
        None => true,
    });
    chosen
}

/// Whether `evidence` is of a suspected or discarded transmission: one the
/// export cannot hold, whose only evidence is co-access.
fn access_only(evidence: &TransmissionEvidence) -> bool {
    matches!(
        evidence.transmission().state,
        TransmissionState::Suspected { .. } | TransmissionState::Discarded { .. }
    )
}
