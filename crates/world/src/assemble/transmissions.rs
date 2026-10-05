//! Transmissions as the pipeline wrote them: each state the correlator
//! reached, saved when it was reached (and, for a channel transmission,
//! recorded as its channel's traffic); the analyze consumer's indexing,
//! assignment and classification; L7's
//! contribution; the two re-fits that re-classified every earlier
//! transmission; the operators' verdicts; and the watermark.
//!
//! - A channel transmission opens `AwaitingContent` at its read (or
//!   `Detected` without a co-access), is suspected when the evidence window
//!   closes and discarded when it expires; a confirmation is saved when
//!   its content matched.
//! - A transmission on another route is confirmed in one step.
//! - Classified ones are assigned under the version active when they were
//!   confirmed, indexed for search, and (when aggregated) counted by L7.
//! - Each re-fit (`begin_fit`, `complete_fit`) assigns every transmission
//!   classified before it started under the new version and hands L7 the
//!   same classifications with cause `Refit`; `TopicVersionReady` counts
//!   them, L7 activates the version, and the catalog's retention then
//!   drops v0 when v2 is activated.

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::watermark::PipelineFrontier;
use crosstalk_spec::derived::flow::transmission::{
    Classification, Route, Transmission, TransmissionState,
};
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::derived::provenance::matching::MatchKind;
use crosstalk_spec::events::insight::ClassificationCause;
use crosstalk_spec::ids::TransmissionId;
use crosstalk_spec::interfaces::l6_analysis::corpus::IndexedTransmission;
use crosstalk_spec::interfaces::l6_analysis::lifecycle::StoredAssignment;
use crosstalk_spec::interfaces::l7_topology::EdgeContribution;
use crosstalk_spec::support::Timestamp;

use crate::clock::{HOUR, MINUTE, minus, plus};
use crate::config::{CONTENT_WINDOW, OPERATOR_ONCALL, OPERATOR_RESEARCHER};
use crate::embed::WorldEmbedder;
use crate::error::WorldError;
use crate::generate::Generated;
use crate::generate::states::{TxRecord, co_accesses};
use crate::generate::times::Times;
use crate::generate::topics::{V1, V2};
use crate::rng::Rng;
use crate::script::{Op, Script};

pub fn assemble(
    generated: &Generated,
    embedder: &WorldEmbedder,
    script: &mut Script,
) -> Result<(), WorldError> {
    fits(generated, script)?;
    for record in &generated.traffic.transmissions {
        lifecycle(record, embedder, script)?;
    }
    script.push(
        generated.times.v1_pinned_at,
        Op::Pin {
            version: V1,
            by: OPERATOR_RESEARCHER,
        },
    );
    verdicts(generated, script)?;
    script.push(
        generated.times.now,
        Op::Watermark(PipelineFrontier {
            ticked_through: generated.times.now,
            oldest_pending: None,
        }),
    );
    Ok(())
}

fn with_state(record: &TxRecord, state: TransmissionState) -> Box<Transmission> {
    Box::new(Transmission {
        state,
        ..record.transmission.clone()
    })
}

/// The steps of one transmission's life.
fn lifecycle(
    record: &TxRecord,
    embedder: &WorldEmbedder,
    script: &mut Script,
) -> Result<(), WorldError> {
    let opened = record.transmission.opened_at;
    let state = &record.transmission.state;
    let co = co_accesses(state);
    match (&record.transmission.route, co.first()) {
        (Route::Channel(_), Some(first)) => script.push(
            opened,
            Op::Save(with_state(
                record,
                TransmissionState::AwaitingContent {
                    co_access: *first,
                    window_closes_at: plus(opened, CONTENT_WINDOW),
                },
            )),
        ),
        (_, _) if matches!(state, TransmissionState::Detected) => {
            script.push(
                opened,
                Op::Save(with_state(record, TransmissionState::Detected)),
            );
        }
        _ => {}
    }
    match state {
        TransmissionState::Detected | TransmissionState::AwaitingContent { .. } => {}
        TransmissionState::Suspected { .. } => {
            let at = plus(opened, CONTENT_WINDOW);
            script.push(at, Op::Save(with_state(record, state.clone())));
        }
        TransmissionState::Discarded { at, co_access } => {
            let since = plus(opened, CONTENT_WINDOW);
            let suspected = TransmissionState::Suspected {
                co_access: co_access.clone(),
                since,
            };
            script.push(since, Op::Save(with_state(record, suspected)));
            script.push(*at, Op::Save(with_state(record, state.clone())));
        }
        TransmissionState::Confirmed(confirmed)
        | TransmissionState::Classified { confirmed, .. }
        | TransmissionState::Aggregated { confirmed, .. } => {
            let at = confirmed.at();
            script.push(
                at,
                Op::Save(with_state(
                    record,
                    TransmissionState::Confirmed(confirmed.clone()),
                )),
            );
            let Some(classification) = record.classification() else {
                return Ok(());
            };
            script.push(
                at,
                Op::Assign {
                    transmission: record.id(),
                    version: classification.version,
                    assignment: stored(record, classification.topic)?,
                },
            );
            script.push(at, Op::Index(Box::new(indexed(record, embedder)?)));
            script.push(
                at,
                Op::Save(with_state(
                    record,
                    TransmissionState::Classified {
                        confirmed: confirmed.clone(),
                        classification: classification.clone(),
                    },
                )),
            );
            if matches!(state, TransmissionState::Aggregated { .. }) {
                let contribution = contribution(
                    record,
                    classification.clone(),
                    ClassificationCause::Confirmation,
                )?;
                script.push(at, Op::Edge(Box::new(contribution)));
                script.push(at, Op::Save(with_state(record, state.clone())));
            }
        }
    }
    Ok(())
}

/// The assignment the catalog stores for `record` under one version.
fn stored(
    record: &TxRecord,
    topic: Option<crosstalk_spec::ids::TopicId>,
) -> Result<StoredAssignment, WorldError> {
    let confirmed = record
        .confirmed()
        .ok_or_else(|| WorldError::missing("a confirmation to assign"))?;
    Ok(StoredAssignment {
        topic,
        confirmed_at: confirmed.at(),
        matched_bytes: confirmed.matched_bytes(),
        from: confirmed.from(),
        to: record.transmission.to,
    })
}

/// `record` as the edge store counts one classification of it.
fn contribution(
    record: &TxRecord,
    classification: Classification,
    cause: ClassificationCause,
) -> Result<EdgeContribution, WorldError> {
    let confirmed = record
        .confirmed()
        .ok_or_else(|| WorldError::missing("a confirmation to count"))?;
    Ok(EdgeContribution {
        transmission: record.id(),
        from: confirmed.from(),
        to: record.transmission.to,
        route: record.transmission.route.clone(),
        at: confirmed.at(),
        matched_bytes: confirmed.matched_bytes(),
        classification,
        cause,
    })
}

/// `record` as the search corpus stores it: the senders' text, embedded.
fn indexed(record: &TxRecord, embedder: &WorldEmbedder) -> Result<IndexedTransmission, WorldError> {
    let confirmed = record
        .confirmed()
        .ok_or_else(|| WorldError::missing("a confirmation to index"))?;
    let text = record
        .texts
        .iter()
        .map(|text| text.origin.as_ref())
        .collect::<Vec<_>>()
        .join("\n\n");
    let embedding = embedder
        .embed_prefix(&text)
        .map_err(|error| WorldError::Embed {
            what: "a transmission's text",
            error,
        })?;
    Ok(IndexedTransmission {
        transmission: record.id(),
        from: confirmed.from(),
        to: record.transmission.to,
        route: record.transmission.route.clone(),
        confirmed_at: confirmed.at(),
        text,
        embedding: Some(embedding),
    })
}

/// The re-fits that made v1 and v2, each re-classifying every transmission
/// classified before it started.
fn fits(generated: &Generated, script: &mut Script) -> Result<(), WorldError> {
    let times = &generated.times;
    for (version, activated) in [(V1, times.v1_at), (V2, times.v2_at)] {
        let started = Times::fit_started(activated);
        let fitted = Times::fitted_at(activated);
        script.push(started, Op::BeginFit { version });
        let topics = generated.topics.topics_of(version).cloned().collect();
        script.push(fitted, Op::CompleteFit { version, topics });
        let count = refit(generated, version, started, fitted, script)?;
        script.push(activated, Op::Ready { version, count });
        script.push(activated, Op::Activate { version });
    }
    Ok(())
}

fn refit(
    generated: &Generated,
    version: TopicModelVersion,
    started: Timestamp,
    fitted: Timestamp,
    script: &mut Script,
) -> Result<u64, WorldError> {
    let mut count = 0;
    for record in &generated.traffic.transmissions {
        let Some(confirmed) = record.confirmed() else {
            continue;
        };
        if record.classification().is_none() || confirmed.at() >= started {
            continue;
        }
        let topic = record.topic(version);
        script.push(
            fitted,
            Op::Assign {
                transmission: record.id(),
                version,
                assignment: stored(record, topic)?,
            },
        );
        let classification = Classification {
            version,
            topic,
            watched: false,
        };
        let contribution = contribution(record, classification, ClassificationCause::Refit)?;
        script.push(fitted, Op::Edge(Box::new(contribution)));
        count += 1;
    }
    Ok(count)
}

/// About sixty verdicts over the week: suspected transmissions judged
/// false, discarded ones genuine, semantic matches often false, other
/// confirmed ones genuine; one genuine verdict withdrawn.
fn verdicts(generated: &Generated, script: &mut Script) -> Result<(), WorldError> {
    let now = generated.times.now;
    let mut rng = Rng::fork(generated.seed, "verdicts");
    let mut withdrawn = false;
    for record in &generated.traffic.transmissions {
        let t = &record.transmission;
        let age = now.as_micros().saturating_sub(t.opened_at.as_micros());
        if age < 2 * HOUR || !rng.chance(0.012) {
            continue;
        }
        let at = plus(t.opened_at, rng.between(HOUR, 20 * HOUR)).min(minus(now, 30 * MINUTE));
        let by = if rng.chance(0.6) {
            OPERATOR_ONCALL
        } else {
            OPERATOR_RESEARCHER
        };
        let semantic = record.confirmed().is_some_and(|c| {
            c.content()
                .iter()
                .any(|m| matches!(m.kind(), MatchKind::Semantic(_)))
        });
        let (verdict, note) = match &t.state {
            TransmissionState::Suspected { .. } => {
                (Verdict::FalseDetection, "unrelated read of the same page")
            }
            TransmissionState::Discarded { .. } => {
                (Verdict::Genuine, "content was paraphrased beyond matching")
            }
            _ if semantic && rng.chance(0.6) => {
                (Verdict::FalseDetection, "same topic, different text")
            }
            _ if record.is_confirmed() => (Verdict::Genuine, "checked the excerpts"),
            _ => continue,
        };
        push_verdict(script, at, t.id, Some(verdict), by, note);
        if !withdrawn && verdict == Verdict::Genuine && record.is_confirmed() {
            withdrawn = true;
            let later = plus(at, 20 * MINUTE).min(minus(now, 10 * MINUTE));
            push_verdict(script, later, t.id, None, by, "judged the wrong row");
        }
    }
    Ok(())
}

fn push_verdict(
    script: &mut Script,
    at: Timestamp,
    transmission: TransmissionId,
    verdict: Option<Verdict>,
    by: crosstalk_spec::ids::OperatorId,
    note: &str,
) {
    script.push(
        at,
        Op::Verdict {
            transmission,
            verdict,
            by,
            note: Some(note.to_owned()),
        },
    );
}
