//! Which lifecycle state a generated transmission ends in, and building
//! it: co-access evidence for the unconfirmed states, content matches with
//! their text, topic assignments and a classification for the rest.

use crosstalk_spec::aggregates::topic::{Assignment, TopicModelVersion};
use crosstalk_spec::derived::flow::evidence::CoAccess;
use crosstalk_spec::derived::flow::transmission::{
    Classification, Confirmed, Route, Transmission, TransmissionState,
};
use crosstalk_spec::derived::provenance::matching::Carrier;
use crosstalk_spec::ids::{AccessId, AgentId, ExchangeId, TopicId, TransmissionId};
use crosstalk_spec::support::{NonEmpty, Timestamp};

use crate::clock::{SECOND, minus, plus};
use crate::config::{CONTENT_WINDOW, EXPIRY};
use crate::error::WorldError;
use crate::mint::Mint;
use crate::rng::Rng;
use crate::text::Theme;

use super::bodies::Blobs;
use super::evidence::{self, MatchText};
use super::rules::WATCHED_THEMES;
use super::times::Times;
use super::topics::{self, TopicModel};

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

/// A transmission with what the world knows about it beyond the record.
#[derive(Debug, Clone, PartialEq)]
pub struct TxRecord {
    /// In its final state.
    pub transmission: Transmission,
    pub theme: Theme,
    /// The sender, once confirmed.
    pub from: Option<AgentId>,
    /// One per content match, in match order. Empty until confirmed.
    pub texts: Vec<MatchText>,
    /// The accesses named by its co-access records.
    pub accesses: Vec<AccessId>,
    /// Topic assignment per version, indexed by version number. Empty
    /// unless classified.
    pub assignments: Vec<Assignment>,
}

impl TxRecord {
    pub fn id(&self) -> TransmissionId {
        self.transmission.id
    }

    pub fn assignment(&self, version: TopicModelVersion) -> Option<Assignment> {
        let index = usize::try_from(version.0).ok()?;
        self.assignments.get(index).copied()
    }

    pub fn topic(&self, version: TopicModelVersion) -> Option<TopicId> {
        match self.assignment(version)? {
            Assignment::Topic { topic, .. } => Some(topic),
            Assignment::Outlier => None,
        }
    }

    pub fn confirmed(&self) -> Option<&Confirmed> {
        confirmed(&self.transmission.state)
    }

    pub fn is_confirmed(&self) -> bool {
        self.from.is_some()
    }

    /// Classified (or aggregated) as the transmission was when confirmed.
    pub fn classification(&self) -> Option<&Classification> {
        match &self.transmission.state {
            TransmissionState::Classified { classification, .. }
            | TransmissionState::Aggregated { classification, .. } => Some(classification),
            _ => None,
        }
    }
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

fn age(times: &Times, at: Timestamp) -> u64 {
    times.now.as_micros().saturating_sub(at.as_micros())
}

/// A channel-routed transmission read at `at`. Recent ones are in flight;
/// older ones are mostly aggregated, some suspected or (past expiry)
/// discarded. A channel that never confirms only has the unconfirmed
/// states. `burst` cycles through the in-flight states.
pub fn want_channel(
    rng: &mut Rng,
    times: &Times,
    at: Timestamp,
    confirms: bool,
    burst: Option<usize>,
) -> Want {
    let age = age(times, at);
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
pub fn want_direct(rng: &mut Rng, times: &Times, at: Timestamp, burst: Option<usize>) -> Want {
    const STATES: [Want; 4] = [
        Want::Detected,
        Want::Confirmed,
        Want::Classified,
        Want::Aggregated,
    ];
    if let Some(i) = burst {
        return STATES[i % STATES.len()];
    }
    if age(times, at) < CONTENT_WINDOW {
        return STATES[rng.index(STATES.len())];
    }
    Want::Aggregated
}

/// Everything [`build`] draws from or writes to.
pub struct Builder<'a> {
    pub rng: &'a mut Rng,
    pub mint: &'a mut Mint,
    pub times: &'a Times,
    pub topics: &'a TopicModel,
    pub blobs: &'a mut Blobs,
}

pub fn build(b: &mut Builder<'_>, p: Planned) -> Result<TxRecord, WorldError> {
    let id: TransmissionId = b.mint.at(p.at)?;
    let co_list =
        || NonEmpty::from_vec(p.co.clone()).ok_or_else(|| WorldError::missing("co-access"));
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
        texts: Vec::new(),
        accesses: p.accesses.clone(),
        assignments: Vec::new(),
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
            let at = plus(p.at, b.rng.between(SECOND, 90 * SECOND)).min(minus(b.times.now, SECOND));
            let count = 1 + b.rng.weighted(&[0.7, 0.25, 0.05]).unwrap_or(0);
            let mut matches = Vec::with_capacity(count);
            for _ in 0..count {
                let kind = evidence::pick_kind(b.rng)?;
                let built = evidence::build(
                    b.rng,
                    b.mint,
                    b.blobs,
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
                .ok_or_else(|| WorldError::missing("content matches"))?;
            let confirmed = Confirmed::new(content, p.co.clone(), at)
                .map_err(|e| WorldError::invalid("Confirmed", e))?;
            record.from = Some(confirmed.from());
            record.assignments = topics::assign(b.topics, p.theme, b.rng)?;
            let version = topics::version_at(b.times, at);
            let topic = record.topic(version);
            let classification = Classification {
                version,
                topic,
                watched: version == topics::V2
                    && topic.is_some()
                    && at >= b.times.watch_rule_at
                    && WATCHED_THEMES.contains(&p.theme),
            };
            match p.want {
                Want::Confirmed => {
                    // Not classified yet: it has no topic under any version.
                    // The assignment is still drawn so the stream of random
                    // draws does not depend on the state.
                    record.assignments.clear();
                    TransmissionState::Confirmed(confirmed)
                }
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
