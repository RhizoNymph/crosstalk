//! Generated scenarios run through the engine, and what they leave behind.

use std::collections::BTreeSet;

use crosstalk_spec::derived::provenance::fingerprint::Fingerprint;
use crosstalk_spec::derived::provenance::span::{Span, SpanState};
use crosstalk_spec::ids::{AgentId, MessageHash};
use crosstalk_spec::observed::message::{AssistantPart, Message, Text};
use crosstalk_testkit::build::message::{
    assistant, message, system_text, tool_call, tool_result, user_text,
};

use super::generate::{Piece, TurnPlan, Via};
use crate::decode::DecodePipeline;
use crate::fingerprint::Winnowing;
use crate::segment::{Coverage, text_parts, view};
use crate::store::{SpanRecord, StoredMatch};
use crate::tests::fixtures::{Ran, Turn, World, at, config};
use crate::text::normalize::normalized_string;

/// One turn as run.
#[derive(Debug, Clone)]
pub struct RunTurn {
    pub ran: Ran,
    pub turn: Turn,
}

/// A scenario as run: its turns in order and everything stored.
pub struct Outcome {
    pub turns: Vec<RunTurn>,
    pub spans: Vec<SpanRecord>,
    pub matches: Vec<StoredMatch>,
    pub winnowing: Winnowing,
    pub pipeline: DecodePipeline,
    pub world: World,
}

fn resolve(piece: &Piece, outputs: &[String]) -> Option<String> {
    match piece {
        Piece::Fresh(text) => Some(text.clone()),
        Piece::Copy(index) if !outputs.is_empty() => Some(outputs[index % outputs.len()].clone()),
        Piece::Copy(_) => None,
    }
}

/// Run `plans` in order, three agents taking turns.
pub async fn run(plans: &[TurnPlan]) -> Outcome {
    let config = config();
    let mut world = World::new(config.clone());
    let agents: Vec<AgentId> = (0..3).map(|_| world.agent()).collect();
    let mut outputs: Vec<String> = Vec::new();
    let mut turns = Vec::new();
    for (index, plan) in plans.iter().enumerate() {
        let mut turn = Turn::new(agents[plan.agent], at(index as u64 + 1));
        for history in &plan.history {
            if let Some(text) = resolve(&Piece::Copy(*history), &outputs) {
                turn = turn.history(crosstalk_testkit::build::message::assistant_text(&text));
            }
        }
        for (n, (piece, via)) in plan.reads.iter().enumerate() {
            let Some(text) = resolve(piece, &outputs) else {
                continue;
            };
            turn = match via {
                Via::ToolResult => turn.input(tool_result(&format!("call_{index}_{n}"), &text)),
                Via::SystemPrompt if turn.new_system.is_none() => turn.system(system_text(&text)),
                Via::UserTurn | Via::SystemPrompt => turn.input(user_text(&text)),
            };
        }
        let texts: Vec<String> = plan
            .output
            .iter()
            .filter_map(|piece| resolve(piece, &outputs))
            .collect();
        let mut parts: Vec<AssistantPart> = texts
            .iter()
            .map(|text| AssistantPart::Text(Text(text.clone())))
            .collect();
        if plan.tool_call
            && let Some(first) = texts.first()
        {
            parts.push(tool_call(
                &format!("write_{index}"),
                "write",
                &serde_json::json!({ "body": first }),
            ));
        }
        if parts.is_empty() {
            parts.push(AssistantPart::Text(Text(format!("turn {index} had nothing to say"))));
        }
        turn = turn.output(assistant(parts));
        let ran = world.run(turn.clone()).await;
        outputs.push(texts.join("\n"));
        turns.push(RunTurn { ran, turn });
    }
    let spans = world.store.all_spans();
    let matches = world.store.all_matches();
    Outcome {
        turns,
        spans,
        matches,
        winnowing: Winnowing::new(config.winnow()),
        pipeline: DecodePipeline::new(config.decode()),
        world,
    }
}

impl Outcome {
    /// The message `hash`, as one of the turns sent or received it.
    pub fn message(&self, hash: MessageHash) -> Option<Message> {
        let _ = message;
        self.world.messages.get(hash)
    }

    /// The turn whose exchange is `exchange`.
    pub fn turn(&self, exchange: crosstalk_spec::ids::ExchangeId) -> Option<&RunTurn> {
        self.turns.iter().find(|turn| turn.ran.exchange == exchange)
    }

    /// The text a span (or a read) locates.
    pub fn text_at(&self, part: crosstalk_spec::observed::message::PartRef, range: crosstalk_spec::support::ByteRange) -> Option<String> {
        let message = self.message(part.message)?;
        let text = message.part_text(part.index).ok()?;
        text.get(range.start() as usize..range.end() as usize).map(str::to_owned)
    }

    /// The text of a span through its part's view (unescaped arguments).
    pub fn span_view(&self, span: &Span) -> Option<String> {
        let message = self.message(span.location.part.message)?;
        let part = text_parts(&message)
            .into_iter()
            .find(|part| part.index == span.location.part.index)?;
        let text = part
            .text
            .get(span.location.range.start() as usize..span.location.range.end() as usize)?;
        Some(view(text, part.kind).into_text())
    }

    /// The winnowed fingerprints of a span, as indexed.
    pub fn span_fingerprints(&self, span: &Span) -> BTreeSet<Fingerprint> {
        self.span_view(span)
            .map(|text| {
                self.winnowing
                    .winnow(&text)
                    .into_iter()
                    .map(|kgram| kgram.fingerprint)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The coverage of a turn's inputs (its whole request).
    pub fn input_coverage(&self, turn: &RunTurn) -> Coverage {
        let request = turn.turn.request();
        let inputs: Vec<&Message> = request.iter().collect();
        Coverage::of_messages(&self.winnowing, &self.pipeline, &inputs)
    }

    /// Whether `needle`, normalized, occurs in some layer of some text part
    /// of `message`, normalized.
    pub fn message_contains(&self, message: &Message, needle: &str) -> bool {
        let needle = normalized_string(needle);
        let needle = needle.trim();
        text_parts(message).iter().any(|part| {
            let base = view(&part.text, part.kind);
            self.pipeline
                .layers(base.text())
                .iter()
                .any(|layer| normalized_string(layer.text.text()).contains(needle))
        })
    }

    /// Spans indexed by turns before `exchange`'s, of any agent.
    pub fn indexed_before(&self, exchange: crosstalk_spec::ids::ExchangeId) -> Vec<&SpanRecord> {
        let Some(position) = self.turns.iter().position(|t| t.ran.exchange == exchange) else {
            return Vec::new();
        };
        let earlier: BTreeSet<_> = self.turns[..position]
            .iter()
            .map(|t| t.ran.exchange)
            .collect();
        self.spans
            .iter()
            .filter(|record| earlier.contains(&record.span.exchange))
            .filter(|record| {
                matches!(
                    record.span.state,
                    SpanState::Indexed { .. } | SpanState::Propagated { .. }
                )
            })
            .collect()
    }
}
