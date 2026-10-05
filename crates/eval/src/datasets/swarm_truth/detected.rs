//! The gateway's detections, from a saved L8 export and the evidence of
//! each exported transmission, as eval predictions.
//!
//! - **Export** (`POST /exports`, JSONL; spec `export::framing`): a header,
//!   one `transmission` row per confirmed transmission (its
//!   `TransmissionSummary`, strongest match class, optional content), and
//!   a trailer. It is read with the spec's `read_jsonl` and verified with
//!   `verify_export` (the trailer completes it, the counts agree, the BLAKE3
//!   row digest matches), so a cut-off export is refused. Its rows say
//!   which transmissions to score.
//! - **Evidence** (`GET /transmissions/{id}/evidence`, one
//!   `TransmissionEvidence` JSON per line): the stored `Transmission` with
//!   its content matches (reader exchange, read location, carrier, match
//!   kind) and the accesses of its co-access records (agent, exchange,
//!   resource). An export row with no evidence line is reported
//!   (`missing_evidence`).
//!
//! Gateway agent ids are tied to truth agents through the exchange log: a
//! content match's `reader_exchange` names the transmission's reader, and
//! each access's `exchange` names its agent. A channel's resources are the
//! locators of the accesses behind its transmissions.

use std::collections::{BTreeMap, BTreeSet};
use std::io::BufRead;

use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{AgentId, ChannelId, SpanId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::evidence::TransmissionEvidence;
use crosstalk_spec::interfaces::l8_surface::export::digest::{ROW_DIGEST_CONTEXT, RowHasher};
use crosstalk_spec::interfaces::l8_surface::export::framing::{JsonlExport, read_jsonl};
use crosstalk_spec::interfaces::l8_surface::export::rows::ExportRow;
use crosstalk_spec::support::Blake3;

use super::diagnostics::{Diagnostic, Diagnostics, Effect, JoinFailure, Side};
use super::resolve::AgentIndex;
use crate::keys::AgentKey;
use crate::predict::{Directory, PredictError, Prediction, from_transmission};

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

/// Gateway agent and channel ids, tied to the truth's agents and the
/// resources the gateway saw.
#[derive(Debug, Clone, Default)]
pub struct SwarmDirectory {
    agents: BTreeMap<AgentId, AgentKey>,
    channels: BTreeMap<ChannelId, Vec<Locator>>,
}

impl SwarmDirectory {
    /// Learns from `evidence` through `index`; conflicts go to
    /// `diagnostics`.
    pub fn learn(
        evidence: &[&TransmissionEvidence],
        index: &AgentIndex,
        diagnostics: &mut Diagnostics,
    ) -> Self {
        let mut seen: BTreeMap<AgentId, BTreeSet<AgentKey>> = BTreeMap::new();
        let mut channels: BTreeMap<ChannelId, Vec<Locator>> = BTreeMap::new();
        for item in evidence {
            let transmission = item.transmission();
            if let Some(confirmed) = transmission.state.confirmed() {
                for content in confirmed.content().iter() {
                    if let Some(key) = index.exchange(content.reader_exchange()) {
                        seen.entry(transmission.to).or_default().insert(key.clone());
                    }
                }
            }
            for detail in item.accesses() {
                let access = detail.access();
                if let Some(key) = index.exchange(access.exchange) {
                    for id in [access.agent, detail.agent()] {
                        seen.entry(id).or_default().insert(key.clone());
                    }
                }
                if let Route::Channel(channel) = &transmission.route {
                    let held = channels.entry(*channel).or_default();
                    let locator = detail.resource().locator.clone();
                    if !held.contains(&locator) {
                        held.push(locator);
                    }
                }
            }
        }
        let mut agents = BTreeMap::new();
        for (id, keys) in seen {
            if keys.len() > 1 {
                diagnostics.push(Diagnostic {
                    line: None,
                    row: None,
                    side: Side::Row,
                    failure: JoinFailure::DetectedAgentConflict {
                        agent: id,
                        agents: keys.iter().map(ToString::to_string).collect(),
                    },
                    effect: Effect::Noted,
                });
            }
            if let Some(first) = keys.into_iter().next() {
                agents.insert(id, first);
            }
        }
        Self { agents, channels }
    }
}

impl Directory for SwarmDirectory {
    fn agent(&self, id: AgentId) -> Option<AgentKey> {
        self.agents.get(&id).cloned()
    }

    fn channel(&self, id: ChannelId) -> Vec<Locator> {
        self.channels.get(&id).cloned().unwrap_or_default()
    }

    fn span(&self, _id: SpanId) -> Option<SpanLocation> {
        None
    }
}

/// The predictions of the exported transmissions, sorted. An exported
/// transmission without evidence, or with an agent no exchange ties to a
/// truth agent, is reported and yields none.
pub fn predictions(
    exported: &Exported,
    evidence: &[TransmissionEvidence],
    index: &AgentIndex,
    diagnostics: &mut Diagnostics,
) -> Vec<Prediction> {
    let by_id: BTreeMap<TransmissionId, &TransmissionEvidence> = evidence
        .iter()
        .map(|item| (item.transmission().id, item))
        .collect();
    let mut chosen = Vec::with_capacity(exported.transmissions.len());
    for id in &exported.transmissions {
        match by_id.get(id) {
            Some(item) => chosen.push(*item),
            None => diagnostics.push(Diagnostic {
                line: None,
                row: None,
                side: Side::Row,
                failure: JoinFailure::MissingEvidence { transmission: *id },
                effect: Effect::PredictionsDropped,
            }),
        }
    }
    let directory = SwarmDirectory::learn(&chosen, index, diagnostics);
    let mut out = Vec::new();
    for item in chosen {
        let transmission = item.transmission();
        match from_transmission(transmission, &directory) {
            Ok(made) => out.extend(made),
            Err(PredictError::UnknownAgent(agent)) => diagnostics.push(Diagnostic {
                line: None,
                row: None,
                side: Side::Row,
                failure: JoinFailure::UnknownDetectedAgent {
                    transmission: transmission.id,
                    agent,
                },
                effect: Effect::PredictionsDropped,
            }),
        }
    }
    out.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
    out
}
