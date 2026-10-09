//! The capture: a run's exchanges as one bench world, without labels.
//!
//! - **Exchanges.** The log's exchanges that started inside the run window
//!   (`window::split`), every session's and the
//!   session-less ones, in (`started_at`, id) order. Each keeps the
//!   gateway's minted id, `at_us` = its start, `client.session` = the
//!   harness session and `client.turn` = its ordinal among the session's
//!   in-window exchanges (`Sessions::index`, the ordinal the swarm's truth
//!   rows name). Bodies come from the gateway's blobs, converted by
//!   `to_bench::world`'s builder.
//! - **Agents.** Every name the truth gives (public, as every dataset's
//!   agent list is), model-driven, sorted. Which agent made which exchange
//!   is not written: that is truth.
//! - **Same-microsecond exchanges.** Times are never nudged. Two exchanges
//!   of one agent (the truth's session owner, as the converter assigns
//!   them) at the same microsecond are reported ([`SameMicros`]).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use a2a_bench_format as bench;
use bench::exchange::{AgentDecl, Driven, Fidelity, WorldDecl};
use bench::ids::SourceRef;
use crosstalk_spec::ids::{ExchangeId, MessageHash};
use crosstalk_spec::observed::exchange::{Exchange, ExchangeOutcome};
use crosstalk_spec::observed::message::Message;
use serde::Serialize;

use super::FromExportError;
use crate::swarm::MODEL;
use crate::swarm::bodies::{Bodies, Cached};
use crate::swarm::exchange_log::{ExchangeLog, Sessions};
use crate::swarm::truth_file::{Row, TruthFile};
use crate::swarm::window::{self, Margins, RunWindow};
use crate::to_bench::world::{Draft, WorldBuilder};
use crate::to_bench::{ToBenchError, WorldExport, ids};

/// Exchanges of one agent at one microsecond.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SameMicros {
    pub agent: String,
    pub at_us: u64,
    pub exchanges: Vec<String>,
}

/// A run's capture as one world.
#[derive(Debug, Clone)]
pub struct Capture {
    pub world: WorldExport,
    pub window: RunWindow,
    /// The log's exchanges that started outside the window.
    pub outside: HashSet<ExchangeId>,
    pub same_micros: Vec<SameMicros>,
}

/// Every agent name the truth's rows give.
pub fn agent_names(truth: &TruthFile) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for numbered in &truth.rows {
        match &numbered.row {
            Row::Session(row) => {
                out.insert(row.agent.clone());
            }
            Row::Delivery { row, .. } => {
                out.insert(row.writer.clone());
                out.insert(row.reader.clone());
            }
            Row::Miss(row) => {
                out.insert(row.reader.clone());
            }
            Row::Unattributed(row) => {
                out.insert(row.reader.clone());
            }
            Row::Cluster(row) => out.extend(row.agents.iter().cloned()),
        }
    }
    out
}

/// Each session's owner by the truth's `session` rows: the alphabetically
/// first agent claiming it. Used only to report same-microsecond pairs.
pub fn session_owners(truth: &TruthFile) -> BTreeMap<String, String> {
    let mut out: BTreeMap<String, String> = BTreeMap::new();
    for numbered in &truth.rows {
        if let Row::Session(row) = &numbered.row {
            let owner = out
                .entry(row.session.clone())
                .or_insert_with(|| row.agent.clone());
            if row.agent < *owner {
                owner.clone_from(&row.agent);
            }
        }
    }
    out
}

/// The in-window exchanges with their ordinals, in (`started_at`, id) order.
fn ordered(sessions: &Sessions, without_session: Vec<Exchange>) -> Vec<(Exchange, Option<u32>)> {
    let mut out: Vec<(Exchange, Option<u32>)> = Vec::new();
    for (_, session) in sessions.iter() {
        for (turn, exchange) in session.exchanges.iter().enumerate() {
            out.push((
                exchange.clone(),
                Some(u32::try_from(turn).unwrap_or(u32::MAX)),
            ));
        }
    }
    out.extend(without_session.into_iter().map(|exchange| (exchange, None)));
    out.sort_by_key(|(exchange, _)| (exchange.meta.started_at, exchange.meta.id));
    out
}

/// Builds the capture of `truth`'s run from `log` and `bodies`.
pub fn build<B: Bodies>(
    truth: &TruthFile,
    log: ExchangeLog,
    bodies: &mut Cached<B>,
    margins: Margins,
    log_name: &str,
) -> Result<Capture, FromExportError> {
    let run_window = RunWindow::of(truth, margins);
    let split = window::split(log.exchanges, run_window, &window::truth_sessions(truth));
    let (with_session, without_session): (Vec<Exchange>, Vec<Exchange>) = split
        .inside
        .into_iter()
        .partition(|exchange| exchange.meta.client.ids.session.is_some());
    let sessions = Sessions::index(with_session);
    let owners = session_owners(truth);
    let mut builder = WorldBuilder::default();
    let mut by_agent: BTreeMap<(String, u64), Vec<String>> = BTreeMap::new();
    for (exchange, turn) in ordered(&sessions, without_session) {
        let mut hashes: Vec<MessageHash> = exchange.request.clone();
        match &exchange.outcome {
            ExchangeOutcome::Completed { response, .. } => hashes.push(*response),
            ExchangeOutcome::Failed {
                partial_response, ..
            } => hashes.extend(partial_response.iter().copied()),
        }
        let mut messages: HashMap<MessageHash, Message> = HashMap::new();
        for hash in hashes {
            if let Ok(message) = bodies.get(hash) {
                messages.insert(hash, message.clone());
            }
        }
        let source = SourceRef::new(log_name, format!("/{}", exchange.meta.id.ulid_text()));
        builder
            .exchange(
                Draft {
                    exchange: &exchange,
                    at: exchange.meta.started_at,
                    fidelity: Fidelity::Exact,
                    source: &source,
                    turn,
                },
                |hash| messages.get(&hash),
            )
            .map_err(FromExportError::from)?;
        if let Some(owner) = exchange
            .meta
            .client
            .ids
            .session
            .as_ref()
            .and_then(|session| owners.get(session))
        {
            by_agent
                .entry((owner.clone(), exchange.meta.started_at.as_micros()))
                .or_default()
                .push(exchange.meta.id.ulid_text());
        }
    }
    let same_micros: Vec<SameMicros> = by_agent
        .into_iter()
        .filter(|(_, exchanges)| exchanges.len() > 1)
        .map(|((agent, at_us), exchanges)| SameMicros {
            agent,
            at_us,
            exchanges,
        })
        .collect();
    for pair in &same_micros {
        tracing::warn!(
            agent = %pair.agent,
            at_us = pair.at_us,
            exchanges = pair.exchanges.len(),
            "two exchanges of one agent at the same microsecond; times are kept"
        );
    }
    let key = bench::ids::WorldKey::new(truth.header.world.as_str())
        .map_err(|error| FromExportError::from(ToBenchError::Key(error)))?;
    let agents = agent_names(truth)
        .iter()
        .map(|name| {
            Ok(AgentDecl {
                key: ids::agent(name)?,
                driven: Driven::Model,
                model: Some(MODEL.to_owned()),
            })
        })
        .collect::<Result<Vec<_>, ToBenchError>>()?;
    let decl = WorldDecl {
        key: key.clone(),
        agents,
    };
    Ok(Capture {
        world: builder.finish(key, decl),
        window: run_window,
        outside: split.outside,
        same_micros,
    })
}
