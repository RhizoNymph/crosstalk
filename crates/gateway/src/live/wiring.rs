//! One function per slot: what fills it in a live process.
//!
//! Wiring a layer consumer that has landed is a few lines in its function:
//! build the consumer from the [`StageContext`] (its stores through the
//! spec's traits on `ctx.stores`, its events through `ctx.publisher`),
//! make it a [`Stage`](super::stage::Stage) (or wrap it in one), and
//! `stages.fill(Slot::.., stage)`. A slot whose crate has no consumer yet
//! stays unfilled and is logged at start.

use super::classify::Classifier;
use super::evidence::{EvidenceFeeder, NoSpans};
use super::stage::{SlotTaken, StageContext, Stages};

/// Fill every slot this build has a consumer for.
pub fn wire_all(stages: &mut Stages, ctx: &StageContext) -> Result<(), SlotTaken> {
    wire_l3(stages, ctx)?;
    wire_l4(stages, ctx)?;
    wire_l5(stages, ctx)?;
    wire_l6(stages, ctx)?;
    wire_l7(stages, ctx)?;
    wire_evidence(stages, ctx)?;
    Ok(())
}

/// L3 identity and threading: `ExchangeCaptured` in, agents and
/// conversations into `ctx.stores.agents` (`AgentLifecycle`,
/// `ActivityStore`), `AgentSeen` and `ConversationDelta` out.
// TODO(crosstalk-reconstruct): fill `Slot::L3Reconstruct` with its
// consumer once the crate has one.
pub fn wire_l3(_stages: &mut Stages, _ctx: &StageContext) -> Result<(), SlotTaken> {
    Ok(())
}

/// L4 provenance: `ConversationDelta` in, `SpanOriginated`, `SpanRelayed`
/// and `ContentMatched` out. Its span store also becomes the evidence
/// feeder's `SpanSource` (see [`wire_evidence`]).
// TODO(crosstalk-provenance): fill `Slot::L4Provenance` with its consumer
// once the crate has one.
pub fn wire_l4(_stages: &mut Stages, _ctx: &StageContext) -> Result<(), SlotTaken> {
    Ok(())
}

/// L5 extraction and correlation: `ConversationDelta` and
/// `ContentMatched` in; channels into `ctx.stores.channels`
/// (`ChannelTraffic`), `AccessRecorded`, `ChannelCrossAccessed`,
/// `TransmissionConfirmed` and `TransmissionSuspected` out.
// TODO(crosstalk-flow): fill `Slot::L5Flow` with its consumer once the
// crate has one.
pub fn wire_l5(_stages: &mut Stages, _ctx: &StageContext) -> Result<(), SlotTaken> {
    Ok(())
}

/// L6 classification: the gateway's minimal [`Classifier`].
pub fn wire_l6(stages: &mut Stages, ctx: &StageContext) -> Result<(), SlotTaken> {
    stages.fill(
        super::stage::Slot::L6Classify,
        Classifier::new(ctx.stores.catalog.clone(), ctx.publisher.clone()),
    )
}

/// L7 edges: `TransmissionClassified` and `AccessRecorded` in, into
/// `ctx.stores.edges` (`EdgeStore::apply`, `apply_access`,
/// `advance_watermark`).
// TODO(crosstalk-topology): fill `Slot::L7Topology` with its consumer once
// the crate has one.
pub fn wire_l7(_stages: &mut Stages, _ctx: &StageContext) -> Result<(), SlotTaken> {
    Ok(())
}

/// The evidence records, from L4's span events and L5's accesses.
// TODO(crosstalk-provenance): replace `NoSpans` with L4's span store.
pub fn wire_evidence(stages: &mut Stages, ctx: &StageContext) -> Result<(), SlotTaken> {
    stages.fill(
        super::stage::Slot::Evidence,
        EvidenceFeeder::new(
            ctx.stores.evidence.clone(),
            ctx.stores.channels.clone(),
            NoSpans,
        ),
    )
}
