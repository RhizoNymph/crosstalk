//! The rest of the world's past: span records, message bodies, the
//! projection jobs, and the dead letters.
//!
//! - **Spans.** Every content match's origin span, recorded through L4's
//!   `SpanIndex` when its exchange was captured: where the evidence page
//!   reads a sender-side excerpt's location from.
//! - **Bodies.** Every message body a content match or span names, put in
//!   the blob store when its exchange was captured, except the ones
//!   content retention dropped.
//! - **Projection jobs**, one per status the fitter does not leave a new
//!   job in: an expired fit of the first day under v1; a job pinned to v0,
//!   queued just before v2's activation dropped v0 and failed; the last
//!   day, fitting since a minute ago; the whole week, queued behind it.
//! - **Dead letters**, one per consumer group: `analyze`, `topology`,
//!   `alerts`, `flow`.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::Arc;

use crosstalk_spec::aggregates::edge::{EdgeKey, TopicSlot, TopologyFilter};
use crosstalk_spec::aggregates::projection::frame::{FrameHeader, ProjectionFrame};
use crosstalk_spec::aggregates::projection::{
    FitFailure, PointParts, PointRoute, ProjectedPoint, ProjectionInfo, ProjectionLimit,
    ProjectionParams, ProjectionSpec,
};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::watermark::PipelineFrontier;
use crosstalk_spec::derived::flow::channel::policy::{Decision, Policy, PolicyAuthor};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::derived::provenance::span::{OriginatedSpan, Span, SpanState};
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::insight::InsightEvent;
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{AgentId, ProjectionId};
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crosstalk_spec::support::{Finite, TimeWindow, Timestamp, Watermark};

use crate::clock::{Anchor, BUCKET, DAY, HOUR, MINUTE, SECOND, WorldClock, minus, plus};
use crate::config::{OPERATOR_RESEARCHER, WorldConfig};
use crate::error::WorldError;
use crate::generate::Generated;
use crate::generate::states::TxRecord;
use crate::generate::topics::{V0, V1, V2};
use crate::mint::Mint;
use crate::rng::Rng;
use crate::scenario::{ChannelKey, JobKey};
use crate::script::{Op, Script};
use crate::text::Theme;

use super::channels::{Placement, promotion};

/// Every content match's origin span, recorded through L4 when its
/// exchange was captured, in span id order.
pub fn spans(generated: &Generated, script: &mut Script) -> Result<(), WorldError> {
    for (id, recorded) in generated.traffic.blobs.spans() {
        let span = OriginatedSpan::new(Span {
            id,
            location: recorded.indexed.location,
            agent: recorded.indexed.author,
            exchange: recorded.indexed.exchange,
            state: SpanState::Originated,
        })
        .ok_or_else(|| WorldError::missing(format!("an originated span {id:?}")))?;
        script.push(recorded.at, Op::Span(Box::new(span)));
    }
    Ok(())
}

pub fn bodies(generated: &Generated, script: &mut Script) {
    for (hash, body) in generated.traffic.blobs.clone().into_bodies() {
        script.push(
            body.at,
            Op::Body {
                hash,
                bytes: body.bytes,
            },
        );
    }
}

/// The aggregate watermark a fitter reading at `at` saw: the world's
/// frontier rule applied at that time.
fn watermark_at(config: &WorldConfig, at: Timestamp) -> Watermark {
    Watermark::settled(
        PipelineFrontier {
            ticked_through: at,
            oldest_pending: None,
        },
        config.timing,
        config.bucket_width,
    )
}

fn params(seed: u64, limit: u32) -> Result<ProjectionParams, WorldError> {
    let limit =
        ProjectionLimit::new(limit).map_err(|e| WorldError::invalid("ProjectionLimit", e))?;
    ProjectionParams::new(
        limit,
        ProjectionParams::DEFAULT_NEIGHBORS,
        ProjectionParams::DEFAULT_MIN_DIST_MILLI,
        seed,
    )
    .map_err(|e| WorldError::invalid("ProjectionParams", e))
}

fn window(start: Timestamp, end: Timestamp) -> Result<TimeWindow, WorldError> {
    TimeWindow::new(start, end).map_err(|e| WorldError::invalid("TimeWindow", e))
}

/// The seeded jobs. Returns each job's id.
pub fn projections(
    generated: &Generated,
    config: &WorldConfig,
    anchor: Anchor,
    script: &mut Script,
) -> Result<BTreeMap<JobKey, ProjectionId>, WorldError> {
    let times = &generated.times;
    let mut mint = Mint::new(
        generated.seed,
        "projections",
        Arc::new(WorldClock::Fixed(anchor)),
    );
    let mut jobs = BTreeMap::new();
    let mut queue = |key: JobKey,
                     mint: &mut Mint,
                     request: (TimeWindow, TopicModelVersion, ProjectionParams),
                     at: Timestamp,
                     script: &mut Script|
     -> Result<ProjectionInfo, WorldError> {
        let (window, version, params) = request;
        let id: ProjectionId = mint.at(at)?;
        let spec = ProjectionSpec::new(
            window,
            TopologyFilter::default(),
            version,
            params,
            config.embedding.clone(),
        );
        let info = ProjectionInfo::queued(id, spec, OPERATOR_RESEARCHER, at);
        script.push(at, Op::Enqueue(Box::new(info.clone())));
        jobs.insert(key, id);
        Ok(info)
    };

    let first_day = window(times.start, plus(times.start, DAY))?;
    let requested = plus(times.start, DAY + HOUR);
    let expired = queue(
        JobKey::Expired,
        &mut mint,
        (first_day, V1, params(42, 5_000)?),
        requested,
        script,
    )?;
    script.push(requested, Op::StartFit { job: expired.id() });
    let fitted = plus(requested, MINUTE);
    let frame = frame(generated, config, &expired, requested)?;
    script.push(
        fitted,
        Op::CompleteJob {
            job: expired.id(),
            frame: Box::new(frame),
        },
    );
    let retention = config.frame_retention.as_duration();
    let retention = u64::try_from(retention.as_micros()).unwrap_or(u64::MAX);
    script.push(plus(fitted, retention + SECOND), Op::ExpireFrames);

    let failed = queue(
        JobKey::Failed,
        &mut mint,
        (first_day, V0, params(42, 5_000)?),
        minus(times.v2_at, 5 * MINUTE),
        script,
    )?;
    script.push(
        times.v2_at,
        Op::FailFit {
            job: failed.id(),
            failure: FitFailure::VersionNotRetained { version: V0 },
        },
    );

    let last_day = window(minus(times.now, DAY), times.now)?;
    let fitting = queue(
        JobKey::Fitting,
        &mut mint,
        (last_day, V2, params(7, 20_000)?),
        minus(times.now, 2 * MINUTE),
        script,
    )?;
    script.push(minus(times.now, MINUTE), Op::StartFit { job: fitting.id() });

    let week = window(times.start, times.now)?;
    queue(
        JobKey::Queued,
        &mut mint,
        (week, V2, params(8, 50_000)?),
        minus(times.now, 30 * SECOND),
        script,
    )?;
    Ok(jobs)
}

/// The sample key of a transmission under a fit's seed (SplitMix64 over
/// the seed and the id: a stand-in for the spec's keyed BLAKE3, enough for
/// a frame that expires before anyone reads it).
fn sample_key(seed: u64, record: &TxRecord) -> u64 {
    let raw = record.id().as_ulid();
    let low = u64::try_from(raw & u128::from(u64::MAX)).unwrap_or(0);
    let high = u64::try_from(raw >> 64).unwrap_or(0);
    Rng::new(seed ^ low ^ high.rotate_left(32)).next_u64()
}

fn center(theme: Theme) -> (f64, f64) {
    let angle = std::f64::consts::TAU * theme.index() as f64 / Theme::ALL.len() as f64;
    (6.0 * angle.cos(), 6.0 * angle.sin())
}

/// The frame a fit of `job` started at `at` lays out: every transmission
/// classified under the spec's version and confirmed in its window, with
/// agents and channels resolved as of `at`, bottom-k by sample key, each
/// theme a cluster on a ring (the UI fixture's stand-in for UMAP).
fn frame(
    generated: &Generated,
    config: &WorldConfig,
    job: &ProjectionInfo,
    at: Timestamp,
) -> Result<ProjectionFrame, WorldError> {
    let spec = job.spec();
    let version = spec.topic_version();
    let params = spec.params();
    let promotion = promotion(generated)?;
    let mut admitted: Vec<&TxRecord> = generated
        .traffic
        .transmissions
        .iter()
        .filter(|t| t.classification().is_some())
        .filter(|t| {
            t.confirmed().is_some_and(|c| {
                spec.window().contains(c.at())
                    // No projection holds a transmission within one agent.
                    && canonical(generated, c.from(), at) != canonical(generated, t.transmission.to, at)
            })
        })
        .collect();
    let matching = u64::try_from(admitted.len()).map_err(|e| WorldError::invalid("matching", e))?;
    admitted.sort_by_key(|t| (sample_key(params.seed(), t), t.id()));
    let limit = usize::try_from(params.limit().get().get()).unwrap_or(usize::MAX);
    admitted.truncate(limit);
    let spread = 0.35 + 0.9 * f64::from(params.min_dist());
    let mut points = Vec::with_capacity(admitted.len());
    for record in admitted {
        let Some(confirmed) = record.confirmed() else {
            continue;
        };
        let topic = record.topic(version);
        let raw = record.id().as_ulid();
        let low = u64::try_from(raw & u128::from(u64::MAX)).unwrap_or(0);
        let high = u64::try_from(raw >> 64).unwrap_or(0);
        let mut rng = Rng::new(params.seed() ^ low ^ high);
        let (x, y) = match topic {
            Some(_) => {
                let (cx, cy) = center(record.theme);
                (cx + rng.gaussian() * spread, cy + rng.gaussian() * spread)
            }
            None => (rng.unit() * 18.0 - 9.0, rng.unit() * 18.0 - 9.0),
        };
        let route = match &record.transmission.route {
            Route::Channel(channel) => Route::Channel(promotion.canonical(*channel, at)),
            other => other.clone(),
        };
        let point = ProjectedPoint::new(PointParts {
            transmission: record.id(),
            from: canonical(generated, confirmed.from(), at),
            to: canonical(generated, record.transmission.to, at),
            route: PointRoute::of(&route),
            topic,
            confirmed_at: confirmed.at(),
            x: Finite::new(x as f32).map_err(|e| WorldError::invalid("x", e))?,
            y: Finite::new(y as f32).map_err(|e| WorldError::invalid("y", e))?,
        })
        .map_err(|e| WorldError::invalid("ProjectedPoint", e))?;
        points.push(point);
    }
    let header = FrameHeader {
        projection: job.id(),
        topic_version: version,
        watermark: watermark_at(config, at),
        limit: params.limit(),
        matching,
    };
    ProjectionFrame::from_points(header, &points).map_err(|e| WorldError::invalid("frame", e))
}

/// The agent `agent` resolved to at `at`, following the planned merges
/// and the revert.
fn canonical(generated: &Generated, agent: AgentId, at: Timestamp) -> AgentId {
    let mut into: BTreeMap<AgentId, AgentId> = BTreeMap::new();
    for merge in &generated.cast.merges {
        if merge.at > at {
            break;
        }
        let (from, target) = (merge.request.source(), merge.request.target());
        for canonical in into.values_mut() {
            if *canonical == from {
                *canonical = target;
            }
        }
        into.insert(from, target);
        if merge.reverted.is_some_and(|(_, reverted)| reverted <= at) {
            into.remove(&from);
        }
    }
    into.get(&agent).copied().unwrap_or(agent)
}

/// Four deliveries that ran out of retries, one per consumer group.
pub fn letters(
    generated: &Generated,
    placement: &Placement,
    anchor: Anchor,
    script: &mut Script,
) -> Result<(), WorldError> {
    let times = &generated.times;
    let mut mint = Mint::new(
        generated.seed,
        "letters",
        Arc::new(WorldClock::Fixed(anchor)),
    );
    let mut letter = |at: Timestamp,
                      group: &str,
                      event: BusEvent,
                      attempts: u32,
                      error: &str,
                      script: &mut Script|
     -> Result<(), WorldError> {
        let envelope = Envelope {
            id: mint.at(at)?,
            at,
            event,
        };
        script.push(
            at,
            Op::DeadLetter(Box::new(DeadLetter {
                group: ConsumerGroup(group.to_owned()),
                envelope,
                attempts: NonZeroU32::new(attempts).unwrap_or(NonZeroU32::MIN),
                last_error: error.to_owned(),
            })),
        );
        Ok(())
    };

    let last_confirmed = generated
        .traffic
        .transmissions
        .iter()
        .rev()
        .find(|t| t.is_confirmed());
    if let Some(record) = last_confirmed
        && let Some(confirmed) = record.confirmed()
    {
        let at = record.transmission.opened_at;
        letter(
            at,
            "analyze",
            BusEvent::Detect(DetectEvent::TransmissionConfirmed {
                transmission: record.id(),
                from: confirmed.from(),
                to: record.transmission.to,
                route: record.transmission.route.clone(),
                at: confirmed.at(),
                matched_bytes: confirmed.matched_bytes(),
            }),
            5,
            "embedder: request timed out after 30s",
            script,
        )?;
        let width = BUCKET.as_micros().get();
        let bucket_start = minus(at, at.as_micros() % width);
        let bucket = window(bucket_start, plus(bucket_start, width))?;
        if let Ok(key) = EdgeKey::new(
            confirmed.from(),
            record.transmission.to,
            record.transmission.route.clone(),
            TopicSlot {
                version: V2,
                topic: record.topic(V2),
            },
            bucket,
        ) {
            letter(
                at,
                "topology",
                BusEvent::Insight(InsightEvent::EdgeUpdated(key)),
                3,
                "edge store: deadlock detected, transaction rolled back",
                script,
            )?;
        }
    }

    let pastebin = generated.plan.id(ChannelKey::Pastebin)?;
    let decided = times.pastebin_decided_at;
    letter(
        decided,
        "alerts",
        BusEvent::Insight(InsightEvent::PolicyChanged {
            channel: pastebin,
            policy: Policy::Unsanctioned(Decision {
                by: PolicyAuthor::Operator(OPERATOR_RESEARCHER),
                at: decided,
                note: Some("credentials leaked through public pastes".to_owned()),
            }),
        }),
        5,
        "sink soc-webhook rejected the delivery: HTTP 503",
        script,
    )?;

    let recorded = generated.traffic.accesses.iter().rev().nth(3);
    if let Some(access) = recorded {
        // The channel the lookup named when it was recorded, or none.
        let channel = placement.channel_at(access.resource, access.at);
        letter(
            access.at,
            "flow",
            BusEvent::Detect(DetectEvent::AccessRecorded {
                access: access.clone(),
                channel,
            }),
            4,
            "resource extractor: unparseable bash command",
            script,
        )?;
    }
    Ok(())
}
