//! Randomised exchanges for the simulation tests: each is one generation
//! request with one way of ending, drawn from the seed.

use std::num::NonZeroU64;
use std::time::Duration;

use bytes::Bytes;
use crosstalk_sim::{CheckFailed, SimRng};
use crosstalk_spec::observed::exchange::ExchangeFailure;
use crosstalk_testkit::corpus::{Case, CorpusRequest};
use crosstalk_testkit::upstream::{Fault, Pacing, Reply};
use hyper::StatusCode;

use super::sim_support::{Read, Sim, SimReply, open};
use super::support::case;
use crate::decode::RequestDecoder;

/// How one exchange ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scenario {
    /// The whole stream.
    Complete,
    /// A 429 JSON error.
    Status,
    /// The stream ends cleanly after `keep` frames, before `message_stop`.
    Truncated { keep: usize },
    /// The upstream drops the connection after `after` frames.
    Disconnect { after: usize },
    /// The upstream stalls after `after` frames; the client gives up after
    /// `patience`.
    ClientLeaves { after: usize, patience: Duration },
    /// The request body is not JSON: forwarded, never captured.
    BadBody,
    /// The upstream refuses the connection.
    Unreachable,
}

impl Scenario {
    /// One drawn from `rng`, for a stream of `frames` frames.
    pub fn draw(rng: &mut SimRng, frames: usize) -> Self {
        let below = |rng: &mut SimRng, bound: usize| {
            NonZeroU64::new(bound as u64).map_or(0, |bound| rng.below(bound) as usize)
        };
        match below(rng, 7) {
            0 => Self::Complete,
            1 => Self::Status,
            2 => Self::Truncated {
                keep: below(rng, frames - 1),
            },
            3 => Self::Disconnect {
                after: below(rng, frames),
            },
            4 => Self::ClientLeaves {
                after: below(rng, frames),
                patience: Duration::from_millis(100 + below(rng, 5000) as u64),
            },
            5 => Self::BadBody,
            _ => Self::Unreachable,
        }
    }

    /// Whether the exchange should produce a `RawExchange`.
    pub fn captured(self) -> bool {
        self != Self::BadBody
    }

    /// The failure it should end with, `None` for completion.
    pub fn failure(self) -> Option<ExchangeFailure> {
        match self {
            Self::Complete | Self::BadBody => None,
            Self::Status => Some(ExchangeFailure::Upstream { status: 429 }),
            Self::Truncated { .. } | Self::Disconnect { .. } => {
                Some(ExchangeFailure::StreamTruncated)
            }
            Self::ClientLeaves { .. } => Some(ExchangeFailure::ClientDisconnected),
            Self::Unreachable => Some(ExchangeFailure::UpstreamUnreachable),
        }
    }
}

/// The streamed corpus case every scenario starts from.
pub fn stream_case() -> Case {
    case("text_turn_streaming")
}

/// `case`'s request with its model renamed `model`, so the exchange can be
/// recognised among others.
pub fn tagged(case: &Case, model: &str) -> CorpusRequest {
    let mut request = case.request.clone();
    let mut json: serde_json::Value = request.json().unwrap_or_default();
    json["model"] = serde_json::Value::String(model.to_owned());
    request.body = Bytes::from(json.to_string());
    request
}

/// Run one exchange through `sim`, as `scenario` says. Returns once the
/// client is done with it.
pub async fn run<D: RequestDecoder>(
    sim: &Sim<D>,
    case: &Case,
    scenario: Scenario,
    model: &str,
) -> Result<(), CheckFailed> {
    let mut request = tagged(case, model);
    let frames = case.response.body.chunks();
    let full = Reply::from_case(case);
    match scenario {
        Scenario::Complete => sim.upstream.push(SimReply::Scripted(full)),
        Scenario::Status => sim.upstream.push(SimReply::Scripted(Reply::error(
            StatusCode::TOO_MANY_REQUESTS,
            "Number of request tokens has exceeded your rate limit.",
        ))),
        Scenario::Truncated { keep } => {
            let mut reply = full;
            reply.chunks = frames[..keep].to_vec();
            sim.upstream.push(SimReply::Scripted(reply));
        }
        Scenario::Disconnect { after } => {
            sim.upstream
                .push(SimReply::Scripted(full.with_fault(Fault::Disconnect {
                    after_chunks: after,
                })))
        }
        Scenario::ClientLeaves { after, .. } => sim.upstream.push(SimReply::Scripted(
            full.with_fault(Fault::Stall {
                after_chunks: after,
            })
            .paced(Pacing::IMMEDIATE),
        )),
        Scenario::BadBody => {
            request.body = Bytes::from_static(b"{\"model\": not json");
            sim.upstream.push(SimReply::Scripted(full));
        }
        Scenario::Unreachable => sim.connector.refuse_next(1),
    }
    let Some(mut response) = open(&sim.proxy, &request).await else {
        return Err(CheckFailed::new(format!("{model}: no response head")));
    };
    match scenario {
        Scenario::Unreachable => {
            if response.status != StatusCode::BAD_GATEWAY {
                return Err(CheckFailed::new(format!(
                    "{model}: an unreachable upstream answered {}",
                    response.status
                )));
            }
            let _ = response.collect().await;
        }
        Scenario::ClientLeaves { after, patience } => {
            for _ in 0..after {
                if !matches!(response.next().await, Read::Chunk { .. }) {
                    return Err(CheckFailed::new(format!("{model}: the stream ended early")));
                }
            }
            let _ = tokio::time::timeout(patience, response.next()).await;
            drop(response);
        }
        _ => {
            let _ = response.collect().await;
        }
    }
    Ok(())
}
