//! One function per slot: what fills it in a live process.
//!
//! Each builds its layer's consumer from the [`StageContext`] (its stores
//! through the spec's traits on `ctx.stores` and `ctx.layers`, its events
//! through the bus or `ctx.publisher`) and puts it in its slot.

use crosstalk_flow::consumer::Extracted;
use crosstalk_provenance::config::ProvenanceConfig;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use super::classify::Classifier;
use super::evidence::{EvidenceFeeder, ProvenanceSpans};
use super::layers::{Extraction, ProvenanceStage, Reconstruct, Topology, l5};
use super::stage::{Slot, SlotTaken, StageContext, Stages};

/// Fill every layer slot and the evidence feeder.
pub fn wire_all(
    stages: &mut Stages,
    ctx: &StageContext,
    provenance: &ProvenanceConfig,
) -> Result<(), SlotTaken> {
    let (extracted, flow_inputs) = mpsc::unbounded_channel();
    wire_l3(stages, ctx)?;
    wire_l4(stages, ctx, provenance, extracted)?;
    wire_l5(stages, ctx, flow_inputs)?;
    wire_l6(stages, ctx)?;
    wire_l7(stages, ctx)?;
    wire_evidence(stages, ctx)?;
    Ok(())
}

/// L3 identity and threading: `ExchangeCaptured` in; agents into
/// `ctx.stores.agents`, conversations into `ctx.layers.conversations`;
/// `AgentSeen` and `ConversationDelta` out.
pub fn wire_l3(stages: &mut Stages, ctx: &StageContext) -> Result<(), SlotTaken> {
    stages.fill(Slot::L3Reconstruct, Reconstruct::new(ctx))
}

/// L4 provenance: `ExchangeCaptured` and `ConversationDelta` in; spans and
/// matches into `ctx.layers.provenance`; `SpanOriginated`, `SpanRelayed`
/// and `ContentMatched` out. Then L5's extraction step over each delta,
/// into `extracted`.
pub fn wire_l4(
    stages: &mut Stages,
    ctx: &StageContext,
    config: &ProvenanceConfig,
    extracted: UnboundedSender<Extracted>,
) -> Result<(), SlotTaken> {
    let extraction = Extraction::new(
        ctx.stores.blobs.clone(),
        ctx.layers.provenance.clone(),
        extracted,
    );
    stages.fill(
        Slot::L4Provenance,
        ProvenanceStage::new(ctx, config, extraction),
    )
}

/// L5 correlation: the extraction step's inputs and its bus subjects in;
/// resources, accesses and channels into `ctx.stores.channels`,
/// transmissions into `ctx.stores.transmissions`; `AccessRecorded`,
/// `ChannelCrossAccessed`, `TransmissionConfirmed` and
/// `TransmissionSuspected` out.
pub fn wire_l5(
    stages: &mut Stages,
    ctx: &StageContext,
    extracted: UnboundedReceiver<Extracted>,
) -> Result<(), SlotTaken> {
    l5::fill(stages, ctx, extracted)
}

/// L6 classification: the gateway's minimal [`Classifier`].
pub fn wire_l6(stages: &mut Stages, ctx: &StageContext) -> Result<(), SlotTaken> {
    stages.fill(
        Slot::L6Classify,
        Classifier::new(
            ctx.stores.catalog.clone(),
            ctx.stores.transmissions.clone(),
            ctx.publisher.clone(),
        ),
    )
}

/// L7 edges: `TransmissionClassified`, `AccessRecorded`, verdicts and topic
/// versions in, into `ctx.stores.edges`; `EdgeUpdated` out.
pub fn wire_l7(stages: &mut Stages, ctx: &StageContext) -> Result<(), SlotTaken> {
    stages.fill(Slot::L7Topology, Topology::new(ctx))
}

/// The evidence records, from L4's span events and L5's accesses.
pub fn wire_evidence(stages: &mut Stages, ctx: &StageContext) -> Result<(), SlotTaken> {
    stages.fill(
        Slot::Evidence,
        EvidenceFeeder::new(
            ctx.stores.evidence.clone(),
            ctx.stores.channels.clone(),
            ProvenanceSpans(ctx.layers.provenance.clone()),
        ),
    )
}
