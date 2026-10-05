//! Inherited fragments (`provenance.match.inherited-fragment-dropped`).
//!
//! A short match is empty when its origin agent added nothing to it: each
//! of its runs repeats, token for token, a part the agent was given in its
//! own exchange's request (a tool result, a user turn, a system prompt).
//! Writer and reader then hold it because they share an upstream, as when
//! an orchestrator names one page to its writer ("... in the team wiki,
//! page `queue-backpressure-39`.") and to its readers ("read the wiki page
//! `queue-backpressure-39` ..."): the writer's narration "I'll update the
//! wiki page `queue-backpressure-39` ..." shares a k-gram with every
//! reader's prompt.
//!
//! The run's whole tokens (`fingerprint::token`) must appear consecutively
//! in one given part, so text the agent composed from words scattered over
//! its history (a reply in a conversation) is never inherited; and a run
//! needs at least [`MIN_RUN_TOKENS`] tokens, so a lone common token is
//! not enough. Missing evidence keeps the match: an unrecorded or pruned
//! request, bodies no longer stored.

use std::collections::HashMap;
use std::sync::Arc;

use crosstalk_spec::derived::provenance::fingerprint::Fingerprint;
use crosstalk_spec::derived::provenance::span::Origin;
use crosstalk_spec::ids::{ExchangeId, SpanId};
use crosstalk_spec::interfaces::l4_provenance::{FingerprintIndex, SemanticMatcher};
use crosstalk_spec::observed::message::Message;

use super::hits::merge;
use super::messages::MessageSource;
use super::{ScanError, Scanner, Session, reads};
use crate::config::InheritedFragments;
use crate::fingerprint::token;
use crate::segment::{text_parts, view};
use crate::store::ProvenanceStore;

/// The fewest whole tokens a run needs to be inherited.
pub const MIN_RUN_TOKENS: usize = 2;

/// The token sequences an exchange's agent was given, one per carrying
/// part of its request, with every token's positions.
#[derive(Debug, Default)]
pub struct Given {
    parts: Vec<Arc<Vec<Vec<Fingerprint>>>>,
    at: HashMap<Fingerprint, Vec<(usize, usize, usize)>>,
}

impl Given {
    fn add(&mut self, message: Arc<Vec<Vec<Fingerprint>>>) {
        let m = self.parts.len();
        for (p, sequence) in message.iter().enumerate() {
            for (i, token) in sequence.iter().enumerate() {
                self.at.entry(*token).or_default().push((m, p, i));
            }
        }
        self.parts.push(message);
    }

    /// Whether `run` (at least [`MIN_RUN_TOKENS`] tokens) occurs
    /// consecutively in one given part.
    pub fn holds(&self, run: &[Fingerprint]) -> bool {
        let Some(first) = run.first() else {
            return false;
        };
        if run.len() < MIN_RUN_TOKENS {
            return false;
        }
        self.at.get(first).into_iter().flatten().any(|&(m, p, i)| {
            self.parts
                .get(m)
                .and_then(|message| message.get(p))
                .and_then(|sequence| sequence.get(i..i + run.len()))
                .is_some_and(|window| window == run)
        })
    }
}

/// The token sequences of the parts of `message` that carry something to
/// its reader (`reads::carrier`), not an agent's own text or tool calls.
fn given_by(message: &Message) -> Vec<Vec<Fingerprint>> {
    text_parts(message)
        .into_iter()
        .filter(|part| reads::carrier(message, part).is_some())
        .map(|part| token::sequence(view(&part.text, part.kind).text()))
        .collect()
}

impl Scanner {
    /// What the agent of `exchange` was given in its request; `None` when
    /// the exchange is not recorded or its request list was pruned. A body
    /// no longer stored gives nothing.
    async fn given<I, S, M, L>(
        &self,
        session: &mut Session<'_, I, S, M, L>,
        exchange: ExchangeId,
    ) -> Result<Option<Arc<Given>>, ScanError>
    where
        I: FingerprintIndex + Sync,
        S: ProvenanceStore + Sync,
        M: SemanticMatcher + Sync,
        L: MessageSource + Sync,
    {
        if let Some(given) = session.given.get(&exchange) {
            return Ok(given.clone());
        }
        let request = match session.env.store.exchange(exchange).await? {
            Some((record, _)) if !record.request.is_empty() => record.request,
            _ => {
                session.given.insert(exchange, None);
                return Ok(None);
            }
        };
        let mut given = Given::default();
        for hash in request {
            let cached = self
                .given
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(hash);
            let sequences = match cached {
                Some(sequences) => sequences,
                None => {
                    let Some(message) = session.env.messages.message(hash).await? else {
                        continue;
                    };
                    let sequences = Arc::new(given_by(&message));
                    self.given
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .put(hash, Arc::clone(&sequences));
                    sequences
                }
            };
            given.add(sequences);
        }
        let given = Some(Arc::new(given));
        session.given.insert(exchange, given.clone());
        Ok(given)
    }

    /// Whether the candidate match on `span` (its hit extents in `layer`)
    /// is an inherited fragment: inherited fragments are dropped
    /// (`SpreadRule::inherited`), none of its merged runs reaches
    /// `SpreadRule::distinctive_chars`, the span is originated (a forwarded
    /// span holds its input's text by definition), and the whole tokens of
    /// every run occur consecutively in one part the span's agent was given
    /// in its own exchange's request ([`Given::holds`]).
    pub(crate) async fn inherited_fragment<I, S, M, L>(
        &self,
        session: &mut Session<'_, I, S, M, L>,
        layer: &str,
        span: SpanId,
        extents: &[(u32, u32)],
    ) -> Result<bool, ScanError>
    where
        I: FingerprintIndex + Sync,
        S: ProvenanceStore + Sync,
        M: SemanticMatcher + Sync,
        L: MessageSource + Sync,
    {
        if self.spread().inherited() == InheritedFragments::Kept || self.distinctive(layer, extents)
        {
            return Ok(false);
        }
        let Some(record) = session.live.get(span) else {
            return Ok(false);
        };
        if record.span.state.origin() != Some(Origin::Originated) {
            return Ok(false);
        }
        let exchange = record.span.exchange;
        let runs: Vec<Vec<Fingerprint>> = merge(extents.to_vec())
            .into_iter()
            .map(|(start, end)| {
                token::whole_tokens_in(
                    layer,
                    usize::try_from(start).unwrap_or(usize::MAX),
                    usize::try_from(end).unwrap_or(usize::MAX),
                )
            })
            .collect();
        if runs.is_empty() || runs.iter().any(|run| run.len() < MIN_RUN_TOKENS) {
            return Ok(false);
        }
        let Some(given) = self.given(session, exchange).await? else {
            return Ok(false);
        };
        Ok(runs.iter().all(|run| given.holds(run)))
    }
}
