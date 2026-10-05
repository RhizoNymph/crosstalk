//! Worlds of independent agents: the background (negative) corpora.
//!
//! A background world mixes trajectories that never talked to each other
//! (separate runs of separate tasks) into one world, so a detector meets
//! the boilerplate real agents share: harness banners, test-runner headers,
//! interpreter footers, the same repository's source quoted by two agents.
//! Nothing one of them wrote reached another, so:
//!
//! - the world's coverage is complete at the construction tier: every
//!   prediction is a false positive;
//! - every (sender, reader exchange) pair of distinct trajectories gets a
//!   negative control, `SharedSource` when the two worked on the same
//!   repository or task (`group`) and `Boilerplate` otherwise, so the report
//!   says which kind of shared text a false positive came from.
//!
//! Converters hand over [`Trajectory`]s; [`BackgroundWorld`] declares their
//! agents, adds their exchanges and, on [`finish`](BackgroundWorld::finish),
//! the controls. A generator that plants transmissions (the splice corpus)
//! adds its labels with [`expect`](BackgroundWorld::expect) before
//! finishing; the planted (sender, reader exchange) gets no control, so a
//! prediction there that misses the label (a wrong route, say) is a plain
//! false positive rather than one charged to boilerplate.

use std::collections::BTreeSet;

use crosstalk_spec::ids::ExchangeId;
use crosstalk_spec::observed::exchange::{StopReason, WireProtocol};
use crosstalk_spec::support::Timestamp;

use crate::corpus::{
    CorpusError, Coverage, Driven, ExchangeDraft, Fidelity, HashedMessage, World, WorldBuilder,
};
use crate::keys::{AgentKey, DatasetId, SourceRef, WorldKey};
use crate::truth::{
    Expectation, InvalidLabel, NegativeControl, NegativeLabel, NegativeReason, Tier,
};

/// One model call of a trajectory.
#[derive(Debug, Clone)]
pub struct Call {
    pub at: Timestamp,
    pub request: Vec<HashedMessage>,
    pub response: HashedMessage,
    pub stop: StopReason,
    pub fidelity: Fidelity,
    pub source: SourceRef,
}

/// One independent agent's calls.
#[derive(Debug, Clone)]
pub struct Trajectory {
    /// Unique within its world.
    pub name: String,
    pub model: String,
    /// The repository or task it worked on: two trajectories of one group
    /// share source text without talking.
    pub group: String,
    pub calls: Vec<Call>,
}

#[derive(Debug, thiserror::Error)]
pub enum BackgroundError {
    #[error(transparent)]
    Corpus(#[from] CorpusError),
    #[error(transparent)]
    Label(#[from] InvalidLabel),
}

struct Added {
    key: AgentKey,
    group: String,
    exchanges: Vec<(ExchangeId, SourceRef)>,
}

/// A world being assembled from independent trajectories.
pub struct BackgroundWorld {
    builder: WorldBuilder,
    added: Vec<Added>,
    planted: BTreeSet<(AgentKey, ExchangeId)>,
}

impl BackgroundWorld {
    pub fn new(dataset: DatasetId, key: WorldKey) -> Self {
        Self {
            builder: WorldBuilder::new(dataset, key),
            added: Vec::new(),
            planted: BTreeSet::new(),
        }
    }

    /// Declares the trajectory's agent and adds its calls; returns its key
    /// and the exchange id of each call, in call order.
    pub fn add(
        &mut self,
        trajectory: Trajectory,
    ) -> Result<(AgentKey, Vec<ExchangeId>), BackgroundError> {
        let key = self
            .builder
            .agent(&trajectory.name, Driven::Model, &trajectory.model)?;
        let mut exchanges = Vec::with_capacity(trajectory.calls.len());
        for call in trajectory.calls {
            let source = call.source.clone();
            let id = self.builder.exchange(ExchangeDraft {
                agent: key.clone(),
                at: call.at,
                protocol: WireProtocol::OpenAiChat,
                model: trajectory.model.clone(),
                request: call.request,
                response: call.response,
                stop: call.stop,
                usage: None,
                fidelity: call.fidelity,
                source: call.source,
            })?;
            exchanges.push((id, source));
        }
        let ids = exchanges.iter().map(|(id, _)| *id).collect();
        self.added.push(Added {
            key: key.clone(),
            group: trajectory.group,
            exchanges,
        });
        Ok((key, ids))
    }

    pub fn expect(&mut self, expectation: Expectation) {
        if let Expectation::Transmission(expected) = &expectation {
            let label = expected.label();
            self.planted
                .insert((label.from.clone(), label.reader_exchange));
        }
        self.builder.expect(expectation);
    }

    /// The world, with a negative control per (sender, reader exchange) of
    /// distinct trajectories, except where a transmission was planted.
    pub fn finish(mut self) -> Result<World, BackgroundError> {
        for reader in &self.added {
            for sender in self.added.iter().filter(|other| other.key != reader.key) {
                let reason = if sender.group == reader.group {
                    NegativeReason::SharedSource
                } else {
                    NegativeReason::Boilerplate
                };
                for (exchange, source) in &reader.exchanges {
                    if self.planted.contains(&(sender.key.clone(), *exchange)) {
                        continue;
                    }
                    self.builder
                        .expect(Expectation::NoTransmission(NegativeControl::new(
                            NegativeLabel {
                                from: sender.key.clone(),
                                to: reader.key.clone(),
                                reader_exchange: Some(*exchange),
                                at: None,
                                origin: None,
                                text: None,
                                reason,
                                tier: Tier::Structural,
                                source: source.clone(),
                            },
                        )?));
                }
            }
        }
        Ok(self.builder.finish(Coverage::Complete {
            tier: Tier::Construction,
        }))
    }
}

/// Stop reason of a recorded assistant message: a tool use when it calls a
/// tool.
pub fn stop_for(calls_tool: bool) -> StopReason {
    if calls_tool {
        StopReason::ToolUse
    } else {
        StopReason::EndTurn
    }
}
