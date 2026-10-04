//! Which lifecycle state a generated transmission is in, and building it:
//! co-access evidence for the unconfirmed states, content matches with
//! their text, topic assignments and a classification for the rest.

use crosstalk_spec::derived::flow::evidence::CoAccess;
use crosstalk_spec::derived::flow::transmission::{
    Classification, Confirmed, Route, Transmission, TransmissionState,
};
use crosstalk_spec::derived::provenance::matching::Carrier;
use crosstalk_spec::ids::{AccessId, AgentId, ExchangeId, TransmissionId};
use crosstalk_spec::support::{NonEmpty, Timestamp};

use crate::backend::fixture::clock::{DAY, MINUTE, Mint, NOW, SECOND, minus, plus};
use crate::backend::fixture::rng::Rng;
use crate::backend::fixture::text::Theme;

use super::rules::{WATCH_RULE_AT, WATCHED_THEMES};
use super::{GenError, TopicModel, TxRecord, evidence, topics};

/// How long after a read the correlator waits for content evidence.
pub const CONTENT_WINDOW: u64 = 15 * MINUTE;
/// How long a suspected transmission waits before it is discarded.
pub const EXPIRY: u64 = 2 * DAY;

/// The state a generated transmission should end up in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Want {
    Detected,
    Awaiting,
    Suspected,
    Discarded,
    Confirmed,
    Classified,
    Aggregated,
}

/// Everything needed to build one transmission.
pub struct Planned {
    pub at: Timestamp,
    pub from: AgentId,
    pub to: AgentId,
    pub route: Route,
    pub theme: Theme,
    pub co: Vec<CoAccess>,
    pub accesses: Vec<AccessId>,
    pub exchange: ExchangeId,
    pub carrier: Carrier,
    pub want: Want,
}

/// The co-access records a transmission state holds.
pub fn co_accesses(state: &TransmissionState) -> Vec<CoAccess> {
    match state {
        TransmissionState::Detected => Vec::new(),
        TransmissionState::AwaitingContent { co_access, .. } => vec![*co_access],
        TransmissionState::Suspected { co_access, .. }
        | TransmissionState::Discarded { co_access, .. } => co_access.iter().copied().collect(),
        TransmissionState::Confirmed(c)
        | TransmissionState::Classified { confirmed: c, .. }
        | TransmissionState::Aggregated { confirmed: c, .. } => c.co_access().to_vec(),
    }
}

/// The confirmation a transmission state holds, if any.
pub fn confirmed(state: &TransmissionState) -> Option<&Confirmed> {
    match state {
        TransmissionState::Confirmed(c)
        | TransmissionState::Classified { confirmed: c, .. }
        | TransmissionState::Aggregated { confirmed: c, .. } => Some(c),
        _ => None,
    }
}

fn age(at: Timestamp) -> u64 {
    NOW.as_micros().saturating_sub(at.as_micros())
}

/// A channel-routed transmission read at `at`. Recent ones are in flight;
/// older ones are mostly aggregated, some suspected or (past expiry)
/// discarded. A channel that never confirms only has the unconfirmed
/// states. `burst` cycles through the in-flight states.
pub fn want_channel(rng: &mut Rng, at: Timestamp, confirms: bool, burst: Option<usize>) -> Want {
    let age = age(at);
    let unconfirmed = if age < CONTENT_WINDOW {
        Want::Awaiting
    } else if age < CONTENT_WINDOW + EXPIRY {
        Want::Suspected
    } else {
        Want::Discarded
    };
    if !confirms {
        return unconfirmed;
    }
    const IN_FLIGHT: [Want; 5] = [
        Want::Detected,
        Want::Awaiting,
        Want::Confirmed,
        Want::Classified,
        Want::Aggregated,
    ];
    if age < CONTENT_WINDOW {
        if let Some(i) = burst {
            return IN_FLIGHT[i % IN_FLIGHT.len()];
        }
        let i = rng.weighted(&[0.1, 0.2, 0.25, 0.25, 0.2]).unwrap_or(4);
        return IN_FLIGHT[i.min(IN_FLIGHT.len() - 1)];
    }
    if rng.chance(0.1) {
        unconfirmed
    } else {
        Want::Aggregated
    }
}

/// A transmission on a route that needs no channel: confirmed in one step,
/// so it is never suspected.
pub fn want_direct(rng: &mut Rng, at: Timestamp, burst: Option<usize>) -> Want {
    const STATES: [Want; 4] = [
        Want::Detected,
        Want::Confirmed,
        Want::Classified,
        Want::Aggregated,
    ];
    if let Some(i) = burst {
        return STATES[i % STATES.len()];
    }
    if age(at) < CONTENT_WINDOW {
        return STATES[rng.index(STATES.len())];
    }
    Want::Aggregated
}

pub fn build(
    rng: &mut Rng,
    mint: &mut Mint,
    topic_model: &TopicModel,
    p: Planned,
) -> Result<TxRecord, GenError> {
    let id = TransmissionId::from_ulid(mint.ulid(p.at));
    let co_list =
        || NonEmpty::from_vec(p.co.clone()).ok_or_else(|| GenError::Missing("co-access".into()));
    let mut record = TxRecord {
        transmission: Transmission {
            id,
            to: p.to,
            route: p.route.clone(),
            opened_at: p.at,
            state: TransmissionState::Detected,
        },
        theme: p.theme,
        from: None,
        matched_bytes: 0,
        texts: Vec::new(),
        accesses: p.accesses.clone(),
        assignments: Vec::new(),
        lower: Vec::new(),
    };
    record.transmission.state = match p.want {
        Want::Detected => TransmissionState::Detected,
        Want::Awaiting => TransmissionState::AwaitingContent {
            co_access: *co_list()?.first(),
            window_closes_at: plus(p.at, CONTENT_WINDOW),
        },
        Want::Suspected => TransmissionState::Suspected {
            co_access: co_list()?,
            since: plus(p.at, CONTENT_WINDOW),
        },
        Want::Discarded => TransmissionState::Discarded {
            at: plus(p.at, CONTENT_WINDOW + EXPIRY),
            co_access: co_list()?,
        },
        Want::Confirmed | Want::Classified | Want::Aggregated => {
            let at = plus(p.at, rng.between(SECOND, 90 * SECOND)).min(minus(NOW, SECOND));
            let count = 1 + rng.weighted(&[0.7, 0.25, 0.05]).unwrap_or(0);
            let mut matches = Vec::with_capacity(count);
            for _ in 0..count {
                let kind = evidence::pick_kind(rng)?;
                let built = evidence::build(
                    rng,
                    mint,
                    evidence::Ends {
                        theme: p.theme,
                        from: p.from,
                        to: p.to,
                        exchange: p.exchange,
                        carrier: p.carrier.clone(),
                    },
                    kind,
                    at,
                )?;
                record.texts.push(built.text);
                matches.push(built.content);
            }
            let content = NonEmpty::from_vec(matches)
                .ok_or_else(|| GenError::Missing("content matches".into()))?;
            let confirmed = Confirmed::new(content, p.co.clone(), at)
                .map_err(|e| GenError::invalid("Confirmed", e))?;
            record.from = Some(confirmed.from());
            record.matched_bytes = confirmed.matched_bytes().get();
            record.assignments = topics::assign(topic_model, p.theme, rng)?;
            let version = topics::version_at(at);
            let topic = record.topic(version);
            let classification = Classification {
                version,
                topic,
                watched: version.0 == 2
                    && topic.is_some()
                    && at >= WATCH_RULE_AT
                    && WATCHED_THEMES.contains(&p.theme),
            };
            match p.want {
                Want::Confirmed => TransmissionState::Confirmed(confirmed),
                Want::Classified => TransmissionState::Classified {
                    confirmed,
                    classification,
                },
                _ => TransmissionState::Aggregated {
                    confirmed,
                    classification,
                },
            }
        }
    };
    Ok(record)
}
