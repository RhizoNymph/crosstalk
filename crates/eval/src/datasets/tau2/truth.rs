//! τ²-bench ground truth.
//!
//! **Positives (Structural).** The agent and the user simulator talk only
//! through their turns. Each text turn one side's model wrote is a
//! transmission to the other side's next call: Direct route, UserTurn
//! carrier (the reader sees it as a user turn either way), located at the
//! whole text, `Exact` (the reader's copy is verbatim). A turn the peer
//! never answers (the closing `###STOP###`) has no reader exchange and no
//! label.
//!
//! The scenario's `known_info` travels harness → user simulator's system
//! prompt → agent's user turn. The first hop is from no agent and is not
//! labelled; the second hop is the user simulator's turn, labelled where
//! the simulator originated the text. A turn that only relays its sender's
//! own system prompt verbatim (whitespace aside: a scenario line, or the
//! transfer message the agent's policy dictates) originates nothing; it is
//! a `Boilerplate` control instead.
//!
//! **Negative controls (Structural).**
//! - `Boilerplate`: the agent's hard-coded greeting, which no model wrote,
//!   and turns that relay their sender's system prompt.
//! - `SharedSource`: each side's tool results, read from the shared
//!   environment, not written by the peer.

use crosstalk_spec::ids::ExchangeId;

use super::Tau2Error;
use super::schema::RawMessage;
use super::views::{Side, View};
use crate::keys::{AgentKey, SourceRef};
use crate::location;
use crate::truth::{
    CarrierKind, Expectation, ExpectedContent, ExpectedTransmission, MatchNeed, NegativeControl,
    NegativeLabel, NegativeReason, RouteExpectation, Tier, TransmissionLabel,
};

/// One side of a simulation: its key, its view and its exchanges.
pub struct Participant {
    pub key: AgentKey,
    pub view: View,
    /// (record index, exchange), in record order.
    pub exchanges: Vec<(usize, ExchangeId)>,
}

impl Participant {
    /// The first exchange after record `raw`: the first whose request
    /// carries it.
    fn reader_exchange(&self, raw: usize) -> Option<ExchangeId> {
        self.exchanges
            .iter()
            .find(|(index, _)| *index > raw)
            .map(|(_, exchange)| *exchange)
    }

    fn exchange_of(&self, raw: usize) -> Option<ExchangeId> {
        self.exchanges
            .iter()
            .find(|(index, _)| *index == raw)
            .map(|(_, exchange)| *exchange)
    }
}

/// Everything labelling one simulation needs.
pub struct SimulationLabels<'a> {
    pub file: &'a str,
    pub simulation: usize,
    pub messages: &'a [RawMessage],
    pub agent: &'a Participant,
    /// Absent when the agent works alone.
    pub user: Option<&'a Participant>,
    /// The agent's system prompt.
    pub agent_prompt: &'a str,
    /// The user simulator's system prompt.
    pub user_prompt: &'a str,
}

impl SimulationLabels<'_> {
    fn side(&self, side: Side) -> Option<&Participant> {
        match side {
            Side::Agent => Some(self.agent),
            Side::User => self.user,
        }
    }

    fn source(&self, raw: usize) -> SourceRef {
        SourceRef::new(
            self.file,
            format!("/simulations/{}/messages/{raw}", self.simulation),
        )
    }

    pub fn label(&self) -> Result<Vec<Expectation>, Tau2Error> {
        let mut out = Vec::new();
        let agent_prompt = collapse(self.agent_prompt);
        let user_prompt = collapse(self.user_prompt);
        for (raw, message) in self.messages.iter().enumerate() {
            match message.role.as_str() {
                "assistant" => self.turn(Side::Agent, raw, message, &agent_prompt, &mut out)?,
                "user" => self.turn(Side::User, raw, message, &user_prompt, &mut out)?,
                "tool" => self.tool_result(raw, message, &mut out)?,
                _ => {}
            }
        }
        Ok(out)
    }

    fn turn(
        &self,
        sender_side: Side,
        raw: usize,
        message: &RawMessage,
        sender_prompt: &str,
        out: &mut Vec<Expectation>,
    ) -> Result<(), Tau2Error> {
        let (Some(sender), Some(reader)) = (self.side(sender_side), self.side(sender_side.peer()))
        else {
            return Ok(());
        };
        let Some(text) = message.text() else {
            return Ok(());
        };
        let Some(entry) = reader.view.entry(raw) else {
            return Ok(());
        };
        let Some(reader_exchange) = reader.reader_exchange(raw) else {
            return Ok(());
        };
        let at = location::whole_part(entry.message.message(), 0)?;
        if !message.is_model_call() || relays(text, sender_prompt) {
            out.push(Expectation::NoTransmission(NegativeControl::new(
                NegativeLabel {
                    from: sender.key.clone(),
                    to: reader.key.clone(),
                    reader_exchange: Some(reader_exchange),
                    at: Some(at),
                    origin: None,
                    text: Some(text.to_owned()),
                    reason: NegativeReason::Boilerplate,
                    tier: Tier::Structural,
                    source: self.source(raw),
                },
            )?));
            return Ok(());
        }
        out.push(Expectation::Transmission(ExpectedTransmission::new(
            TransmissionLabel {
                from: sender.key.clone(),
                to: reader.key.clone(),
                sender_exchange: sender.exchange_of(raw),
                reader_exchange,
                route: RouteExpectation::Direct,
                carrier: CarrierKind::UserTurn,
                content: ExpectedContent {
                    text: text.to_owned(),
                    at,
                },
                needs: MatchNeed::Exact,
                tier: Tier::Structural,
                source: self.source(raw),
            },
        )?));
        Ok(())
    }

    fn tool_result(
        &self,
        raw: usize,
        message: &RawMessage,
        out: &mut Vec<Expectation>,
    ) -> Result<(), Tau2Error> {
        let side = if message.requestor.as_deref() == Some("user") {
            Side::User
        } else {
            Side::Agent
        };
        let (Some(reader), Some(peer)) = (self.side(side), self.side(side.peer())) else {
            return Ok(());
        };
        if message.text().is_none_or(|text| text.trim().is_empty()) {
            return Ok(());
        }
        let Some(entry) = reader.view.entry(raw) else {
            return Ok(());
        };
        let at = location::whole_part(entry.message.message(), 0)?;
        out.push(Expectation::NoTransmission(NegativeControl::new(
            NegativeLabel {
                from: peer.key.clone(),
                to: reader.key.clone(),
                reader_exchange: reader.reader_exchange(raw),
                at: Some(at),
                origin: None,
                text: None,
                reason: NegativeReason::SharedSource,
                tier: Tier::Structural,
                source: self.source(raw),
            },
        )?));
        Ok(())
    }
}

/// The shortest turn (collapsed bytes) judged a relay: a shorter one that
/// happens to occur in the scenario ("Yes, please.") is still the
/// simulator's own.
const RELAY_MIN: usize = 24;

/// Whether a turn only relays its sender's system prompt (`collapsed`).
fn relays(text: &str, scenario: &str) -> bool {
    let turn = collapse(text);
    turn.len() >= RELAY_MIN && scenario.contains(&turn)
}

/// `text` with whitespace runs collapsed to one space and trimmed.
fn collapse(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}
