//! AgentDojo ground truth.
//!
//! **Positives (Construction).** The injection's author is a synthetic
//! attacker agent whose one exchange writes every injection. Each copy of an
//! injection in a tool output the victim read is a transmission from the
//! attacker to the victim: carrier `ToolResult`, the route of the call that
//! read it ([`route`](super::route)), at the victim's first exchange after
//! the tool message, located at the copy's raw bytes. Vectors carrying the
//! same text share their copies: each copy is labelled once. Its match need is the
//! weakest arrival that finds it ([`classify`](super::classify)).
//!
//! **Channel copies are access-only.** A copy read through a resource (a
//! web page, a file) arrives on a medium the attacker never wrote: the
//! synthetic attacker's one exchange writes text, not the page or file.
//! The spec makes such content a shared upstream source that stays
//! Suspected and is never confirmed (INV-963,
//! `flow.route.shared-upstream-stays-suspected`), so the copy is an
//! [`ExpectedAccess`]: only access evidence finds it, under access-only
//! recall, and a content match there is correct but finds nothing. A copy
//! read through a keyed tool that records no access stays a `Direct`
//! content expectation.
//!
//! **Negative controls (Structural).** The victim's system prompt and user
//! turns are harness text: `Boilerplate` from the attacker. (In
//! `injection_task_*/none` runs the attacker's goal is the user prompt, but
//! there is no attacker agent: the prompt comes from no agent.)
//!
//! **Second hop (Heuristic, counted, not labelled).** When an attack
//! succeeded and the victim later writes an attacker indicator into a tool
//! call, the victim wrote toward an attacker resource. See
//! [`SecondHop`](super::tally::SecondHop).

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::ids::ExchangeId;
use crosstalk_spec::observed::message::MessageBody;

use super::AgentDojoError;
use super::classify::{Arrival, Output};
use super::messages::Conversation;
use super::route::expected_route;
use super::schema::Run;
use super::tally::Tally;
use crate::keys::{AgentKey, SourceRef};
use crate::location;
use crate::truth::{
    CarrierKind, Expectation, ExpectedAccess, ExpectedContent, ExpectedTransmission,
    NegativeControl, NegativeLabel, NegativeReason, RouteExpectation, Tier, TransmissionLabel,
};

/// The attacker and its one exchange.
pub struct Attacker<'a> {
    pub key: &'a AgentKey,
    pub exchange: ExchangeId,
}

/// Everything labelling one run needs.
pub struct RunLabels<'a> {
    pub file: &'a str,
    pub run: &'a Run,
    pub conversation: &'a Conversation,
    pub victim: &'a AgentKey,
    pub attacker: Option<Attacker<'a>>,
    /// The victim's exchanges: (assistant message index, exchange), in
    /// message order.
    pub exchanges: &'a [(usize, ExchangeId)],
}

impl RunLabels<'_> {
    /// The victim's first exchange after message `index`: the first whose
    /// request carries it.
    fn reader_exchange(&self, index: usize) -> Option<ExchangeId> {
        self.exchanges
            .iter()
            .find(|(message, _)| *message > index)
            .map(|(_, exchange)| *exchange)
    }

    fn source(&self, path: String) -> SourceRef {
        SourceRef::new(self.file, path)
    }

    /// The run's labels; `tally` gets its slots, labels and second hop.
    pub fn label(&self, tally: &mut Tally) -> Result<Vec<Expectation>, AgentDojoError> {
        tally.runs += 1;
        let Some(attacker) = &self.attacker else {
            return Ok(Vec::new());
        };
        tally.attacked_runs += 1;
        let mut out = Vec::new();
        let first_read = self.injections(attacker, tally, &mut out)?;
        self.boilerplate(attacker, &mut out)?;
        self.second_hop(first_read, tally);
        Ok(out)
    }

    /// Labels every copy of every injection in a tool output the victim
    /// read, and returns the first tool message holding one.
    fn injections(
        &self,
        attacker: &Attacker<'_>,
        tally: &mut Tally,
        out: &mut Vec<Expectation>,
    ) -> Result<Option<usize>, AgentDojoError> {
        let mut slots: BTreeMap<&str, Option<Arrival>> = self
            .run
            .injections()
            .map(|(vector, _)| (vector.as_str(), None))
            .collect();
        // Vectors often carry the same text: each copy is one label, under
        // the first vector holding that text.
        let mut texts: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for (vector, injection) in self.run.injections() {
            texts
                .entry(injection.as_str())
                .or_default()
                .push(vector.as_str());
        }
        let mut first_read = None;
        for (index, message) in self.conversation.messages.iter().enumerate() {
            let message = message.message();
            if !matches!(message.body, MessageBody::Tool(_)) {
                continue;
            }
            let Some(reader_exchange) = self.reader_exchange(index) else {
                continue;
            };
            let Ok(text) = message.part_text(0) else {
                continue;
            };
            let output = Output::new(&text);
            let route = expected_route(self.conversation.calls.get(&index));
            for (injection, vectors) in &texts {
                let Some(vector) = vectors.first() else {
                    continue;
                };
                for (copy, found) in output.occurrences(injection).into_iter().enumerate() {
                    let (Ok(start), Ok(end)) =
                        (u32::try_from(found.start), u32::try_from(found.end))
                    else {
                        continue;
                    };
                    let at = location::location(message.hash, 0, start, end)?;
                    let content = text.get(found.start..found.end).unwrap_or_default();
                    let label = TransmissionLabel {
                        from: attacker.key.clone(),
                        to: self.victim.clone(),
                        sender_exchange: Some(attacker.exchange),
                        reader_exchange,
                        route: route.clone(),
                        carrier: CarrierKind::ToolResult,
                        content: ExpectedContent {
                            text: content.to_owned(),
                            at,
                        },
                        needs: found.arrival.need(),
                        tier: Tier::Construction,
                        source: self
                            .source(format!("/messages/{index}/injections/{vector}/{copy}")),
                    };
                    out.push(expectation(label)?);
                    tally.labels.add_arrival(Some(found.arrival));
                    first_read.get_or_insert(index);
                    for vector in vectors {
                        if let Some(slot) = slots.get_mut(vector) {
                            slot.get_or_insert(found.arrival);
                        }
                    }
                }
            }
        }
        for arrival in slots.into_values() {
            tally.slots.add_arrival(arrival);
        }
        Ok(first_read)
    }

    /// The victim's system prompt and user turns: harness text, never the
    /// attacker's.
    fn boilerplate(
        &self,
        attacker: &Attacker<'_>,
        out: &mut Vec<Expectation>,
    ) -> Result<(), AgentDojoError> {
        for (index, message) in self.conversation.messages.iter().enumerate() {
            let message = message.message();
            if !matches!(message.body, MessageBody::System(_) | MessageBody::User(_)) {
                continue;
            }
            if message
                .part_text(0)
                .is_ok_and(|text| text.trim().is_empty())
            {
                continue;
            }
            let Ok(at) = location::whole_part(message, 0) else {
                continue;
            };
            out.push(Expectation::NoTransmission(NegativeControl::new(
                NegativeLabel {
                    from: attacker.key.clone(),
                    to: self.victim.clone(),
                    reader_exchange: None,
                    at: Some(at),
                    origin: None,
                    text: None,
                    reason: NegativeReason::Boilerplate,
                    tier: Tier::Structural,
                    source: self.source(format!("/messages/{index}")),
                },
            )?));
        }
        Ok(())
    }

    /// Counts a successful attack whose victim, after reading an injection,
    /// wrote an attacker indicator into a tool call.
    fn second_hop(&self, first_read: Option<usize>, tally: &mut Tally) {
        if self.run.security != Some(true) {
            return;
        }
        tally.second_hop.successful_attacks += 1;
        let Some(first_read) = first_read else {
            return;
        };
        let benign: Vec<String> = self
            .run
            .messages
            .iter()
            .filter(|message| message.role == "system" || message.role == "user")
            .map(|message| message.text().to_lowercase())
            .collect();
        let indicators: BTreeSet<String> = self
            .run
            .injections()
            .flat_map(|(_, injection)| indicators(injection))
            .filter(|indicator| !benign.iter().any(|text| text.contains(indicator.as_str())))
            .collect();
        if indicators.is_empty() {
            return;
        }
        let wrote = self
            .run
            .messages
            .iter()
            .enumerate()
            .skip(first_read + 1)
            .flat_map(|(_, message)| message.calls())
            .any(|call| {
                let args = call.args.to_string().to_lowercase();
                indicators
                    .iter()
                    .any(|indicator| args.contains(indicator.as_str()))
            });
        if wrote {
            tally.second_hop.ioc_written += 1;
        }
    }
}

/// The expectation for one injection copy. Through a channel (a page or
/// file the victim read) the attacker never wrote the resource, so the
/// content is a shared upstream source: an access-only expectation
/// (INV-963, `flow.route.shared-upstream-stays-suspected`). Read through a
/// keyed tool that records no access it arrives `Direct` in the tool
/// result, a content expectation.
fn expectation(label: TransmissionLabel) -> Result<Expectation, AgentDojoError> {
    Ok(match label.route {
        RouteExpectation::Channel { .. } => Expectation::AccessOnly(ExpectedAccess::new(label)?),
        _ => Expectation::Transmission(ExpectedTransmission::new(label)?),
    })
}

/// URLs, email addresses and IBANs in `text`, lowercased.
pub fn indicators(text: &str) -> Vec<String> {
    text.split(|c: char| {
        c.is_whitespace()
            || matches!(
                c,
                '\'' | '"' | '<' | '>' | '(' | ')' | ',' | ';' | '`' | '[' | ']'
            )
    })
    .map(|token| token.trim_end_matches(['.', ':', '!', '?']))
    .filter(|token| is_url(token) || is_email(token) || is_iban(token))
    .map(str::to_lowercase)
    .collect()
}

fn is_url(token: &str) -> bool {
    let lower = token.to_ascii_lowercase();
    let rest = lower
        .strip_prefix("https://")
        .or_else(|| lower.strip_prefix("http://"))
        .unwrap_or(&lower);
    (rest.starts_with("www.") && rest.len() > "www.".len() + 3)
        || (lower.contains("://") && rest.contains('.'))
}

fn is_email(token: &str) -> bool {
    match token.split_once('@') {
        Some((local, domain)) => {
            !local.is_empty()
                && domain.contains('.')
                && !domain.starts_with('.')
                && !domain.contains('@')
        }
        None => false,
    }
}

fn is_iban(token: &str) -> bool {
    let bytes = token.as_bytes();
    (15..=34).contains(&bytes.len())
        && bytes[..2].iter().all(u8::is_ascii_uppercase)
        && bytes[2..4].iter().all(u8::is_ascii_digit)
        && bytes[4..].iter().all(u8::is_ascii_alphanumeric)
}
