//! Replaying a saved bench run through the gateway's live composition,
//! offline: the run's exchange log and blobs go through
//! `crosstalk_gateway::live::Live` in memory, with the run's flow settings,
//! and its export and evidence are read through the same L8 surface the
//! gateway serves `POST /exports` and `GET /transmissions/{id}/evidence`
//! from. The result is scored like a fetched run ([`super::score`]).
//!
//! ```text
//! exchange-log.jsonl ─▶ entries captured at or after `since` (log order)
//!   + blobs/          ─▶ NormalizedExchange (request + response bodies, media)
//! Live::start(Manual clock, FlowConfig from bench.env, Ticking::OnSettle)
//!   for each entry at `at`:
//!     settle(t) at every tick boundary t < at        (the periodic ticker)
//!     clock ─▶ at; pipeline().ingest(exchange, at)    (what the capture stage does)
//!     settle(at)                                      (drained; ticks at at)
//!   settle tick by tick until watermark ≥ `until`    (the bench's wait_caught_up)
//! surface().export(fetch::export_request(since .. clock + 1 h))  ─▶ JSONL bytes ─▶ read_export
//! surface().transmission_evidence(id, context 0) for each exported row
//! ```
//!
//! Deterministic for a given input: the clock is manual, ticks run only in
//! `settle`, and every id generator is seeded. It differs from the running
//! gateway only in when ticks fall: the gateway ticks every `tick_ms` of
//! wall time while the stages process concurrently; the replay ticks at each
//! tick boundary and at each exchange's capture time, after its processing
//! has drained.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;

use crosstalk_flow::consumer::FlowConfig;
use crosstalk_gateway::live::{DEFAULT_BUCKET, Live, LiveClock, LiveConfig};
use crosstalk_memory::support::ManualClock;
use crosstalk_spec::ids::{ExchangeId, MessageHash, SpanId, TransmissionId};
use crosstalk_spec::interfaces::l1_canonical::{InvalidNormalizedExchange, NormalizedExchange};
use crosstalk_spec::interfaces::l8_surface::QueryApi;
use crosstalk_spec::interfaces::l8_surface::evidence::TransmissionEvidence;
use crosstalk_spec::interfaces::l8_surface::excerpt::ExcerptWindow;
use crosstalk_spec::interfaces::l8_surface::export::{
    Export, ExportLine, ExportRow, ExportStep, ExportStream,
};
use crosstalk_spec::interfaces::l8_surface::operators::RequestIdentity;
use crosstalk_spec::observed::exchange::{Exchange, ExchangeOutcome};
use crosstalk_spec::observed::message::{MediaBlob, Message};
use crosstalk_spec::support::{Clock, TimeWindow, Timestamp};

use super::bodies::{Bodies, BodyError, Cached};
use super::detected::{DetectedError, Exported, read_export};
use super::exchange_log::ExchangeLog;
use super::fetch::export_request;
use super::queried::{Queried, batches, origin_spans};

/// How long `shutdown` waits for the stages to drain.
const DRAIN: Duration = Duration::from_secs(5);

/// The export window's end past the settled clock, as `swarm-fetch` asks
/// (the surface cuts it at the watermark anyway).
const WINDOW_TAIL: Duration = Duration::from_secs(3600);

/// The demo flow config (`deploy/demo/crosstalk.demo.json`) with the run's
/// two windows: what the bench's gateway ran with.
pub fn demo_flow(evidence_window_ms: u64, suspected_ttl_ms: u64) -> FlowConfig {
    FlowConfig {
        evidence_window_ms,
        suspected_ttl_ms,
        ..FlowConfig::default()
    }
}

/// The keys of a run's `bench.env` the replay reads. Every other key is
/// ignored and never echoed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BenchEnv {
    pub evidence_window_ms: Option<u64>,
    pub suspected_ttl_ms: Option<u64>,
    pub swarm_end_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("bench.env line {line}: {key} is not a whole number")]
pub struct BenchEnvError {
    pub line: usize,
    pub key: &'static str,
}

impl BenchEnv {
    /// Reads `key=value` lines; only the three known keys are parsed.
    pub fn parse(text: &str) -> Result<Self, BenchEnvError> {
        let mut out = Self::default();
        for (at, line) in text.lines().enumerate() {
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            let slot = match key.trim() {
                "evidence_window_ms" => (&mut out.evidence_window_ms, "evidence_window_ms"),
                "suspected_ttl_ms" => (&mut out.suspected_ttl_ms, "suspected_ttl_ms"),
                "swarm_end_unix_ms" => (&mut out.swarm_end_unix_ms, "swarm_end_unix_ms"),
                _ => continue,
            };
            let parsed = value.trim().parse::<u64>().map_err(|_| BenchEnvError {
                line: at + 1,
                key: slot.1,
            })?;
            *slot.0 = Some(parsed);
        }
        Ok(out)
    }
}

/// What a replay runs with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplaySettings {
    /// L5's windows and tick (the gateway's `flow` section).
    pub flow: FlowConfig,
    /// Seeds the composition's id generators.
    pub seed: u64,
    /// Replay the log's exchanges captured at or after this time (the
    /// truth header's `started_at_unix_ms`: the gateway restarted just
    /// before, so earlier entries are other runs').
    pub since: Timestamp,
    /// Settle until the watermark has passed this time (the swarm's end,
    /// as the bench waits for it); the last replayed exchange when `None`.
    pub until: Option<Timestamp>,
}

/// A finished replay: the export and evidence the run's gateway would have
/// served, as `swarm-fetch` saves them.
#[derive(Debug, Clone)]
pub struct Replayed {
    /// The export's JSONL body.
    pub export_bytes: Vec<u8>,
    pub exported: Exported,
    /// One per exported transmission, in export order.
    pub evidence: Vec<TransmissionEvidence>,
    /// The conversation reads of every ingested exchange and of every
    /// span the evidence matched, as `POST /query/exchange-turns` and
    /// `POST /query/span-points` answer them.
    pub queried: Queried,
    /// Exchanges ingested.
    pub ingested: usize,
    /// Log entries before `since`, not replayed.
    pub skipped: usize,
    /// The clock when the export was read.
    pub settled_at: Timestamp,
    pub watermark: Timestamp,
}

#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error("no exchange in the log was captured at or after {since:?}")]
    Empty { since: Timestamp },
    #[error("exchange {exchange}: body {hash:?}: {source}")]
    Body {
        exchange: String,
        hash: MessageHash,
        #[source]
        source: BodyError,
    },
    #[error("exchange {exchange} does not normalize: {reason:?}")]
    Invalid {
        exchange: String,
        reason: InvalidNormalizedExchange,
    },
    #[error("starting the replay runtime: {0}")]
    Runtime(#[source] std::io::Error),
    #[error("the live composition did not start: {0}")]
    Start(String),
    #[error("ingesting exchange {exchange}: {reason}")]
    Ingest { exchange: String, reason: String },
    #[error("settling at {at:?}: {reason}")]
    Settle { at: Timestamp, reason: String },
    #[error("the watermark ({watermark:?}) did not pass {until:?} by {deadline:?}")]
    NotCaughtUp {
        watermark: Timestamp,
        until: Timestamp,
        deadline: Timestamp,
    },
    #[error("the surface refused the {read}: {reason}")]
    Surface { read: &'static str, reason: String },
    #[error("encoding an export line: {0}")]
    Encode(#[source] serde_json::Error),
    #[error(transparent)]
    Export(#[from] DetectedError),
    #[error("transmission {0} is exported but has no evidence")]
    NoEvidence(String),
}

/// One log entry, ready to ingest.
struct Entry {
    at: Timestamp,
    exchange: NormalizedExchange,
}

/// The exchange with its bodies and media from `bodies`.
fn normalized<B: Bodies>(
    exchange: &Exchange,
    bodies: &mut Cached<B>,
) -> Result<NormalizedExchange, ReplayError> {
    let id = || exchange.meta.id.ulid_text();
    let response = match &exchange.outcome {
        ExchangeOutcome::Completed { response, .. } => Some(*response),
        ExchangeOutcome::Failed {
            partial_response, ..
        } => *partial_response,
    };
    let mut messages: Vec<Message> = Vec::new();
    for hash in exchange.request.iter().copied().chain(response) {
        if messages.iter().any(|message| message.hash == hash) {
            continue;
        }
        let message = bodies.get(hash).map_err(|source| ReplayError::Body {
            exchange: id(),
            hash,
            source,
        })?;
        messages.push(message.clone());
    }
    let mut out = NormalizedExchange {
        exchange: exchange.clone(),
        messages,
        warnings: Vec::new(),
        media: Vec::new(),
    };
    // Each pass adds the first media blob a part names and `media` lacks;
    // a body names finitely many, so this ends.
    loop {
        match out.check() {
            Ok(()) => return Ok(out),
            Err(InvalidNormalizedExchange::MissingMedia { hash }) => {
                let bytes = bodies.media(hash).map_err(|source| ReplayError::Body {
                    exchange: id(),
                    hash,
                    source,
                })?;
                out.media.push(MediaBlob::new(bytes));
                out.media.sort_by_key(MediaBlob::hash);
            }
            Err(reason) => {
                return Err(ReplayError::Invalid {
                    exchange: id(),
                    reason,
                });
            }
        }
    }
}

/// `at` plus `duration`, saturating.
fn after(at: Timestamp, duration: Duration) -> Timestamp {
    let micros = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
    Timestamp::from_micros(at.as_micros().saturating_add(micros))
}

/// Replays the entries of `log` captured at or after `settings.since`,
/// reading bodies from `bodies`, and reads the export and evidence back.
pub fn replay<B: Bodies>(
    log: &ExchangeLog,
    bodies: &mut Cached<B>,
    settings: &ReplaySettings,
) -> Result<Replayed, ReplayError> {
    let mut entries = Vec::new();
    let mut skipped = 0;
    for exchange in &log.exchanges {
        match log.captured_at.get(&exchange.meta.id) {
            Some(at) if *at >= settings.since => entries.push(Entry {
                at: *at,
                exchange: normalized(exchange, bodies)?,
            }),
            _ => skipped += 1,
        }
    }
    if entries.is_empty() {
        return Err(ReplayError::Empty {
            since: settings.since,
        });
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .map_err(ReplayError::Runtime)?;
    let mut replayed = runtime.block_on(run(entries, settings))?;
    replayed.skipped = skipped;
    Ok(replayed)
}

/// The next multiple of `tick` strictly after `at`.
fn next_boundary(at: Timestamp, tick: u64) -> Timestamp {
    let micros = at.as_micros();
    Timestamp::from_micros((micros / tick + 1).saturating_mul(tick))
}

async fn settle(live: &Live, at: Timestamp) -> Result<(), ReplayError> {
    live.settle(at)
        .await
        .map(|_| ())
        .map_err(|error| ReplayError::Settle {
            at,
            reason: error.to_string(),
        })
}

async fn run(entries: Vec<Entry>, settings: &ReplaySettings) -> Result<Replayed, ReplayError> {
    let first = entries.first().map_or(settings.since, |entry| entry.at);
    let last = entries.last().map_or(first, |entry| entry.at);
    let tick = settings.flow.tick_ms.max(1).saturating_mul(1000);
    let clock = ManualClock::at(first);
    let config = LiveConfig::new(
        LiveClock::Manual(clock.clone()),
        settings.flow,
        settings.seed,
    )
    .map_err(|error| ReplayError::Start(error.to_string()))?;
    let live = Live::start(config)
        .await
        .map_err(|error| ReplayError::Start(error.to_string()))?;
    let outcome = drive(&live, &clock, entries, tick, last, settings).await;
    let drained = live.shutdown(tokio::time::Instant::now() + DRAIN).await;
    tracing::debug!(?drained, "replay stopped");
    outcome
}

async fn drive(
    live: &Live,
    clock: &ManualClock,
    entries: Vec<Entry>,
    tick: u64,
    last: Timestamp,
    settings: &ReplaySettings,
) -> Result<Replayed, ReplayError> {
    let mut boundary = next_boundary(clock.now(), tick);
    let ingested = entries.len();
    let ids: BTreeSet<ExchangeId> = entries
        .iter()
        .map(|entry| entry.exchange.exchange.meta.id)
        .collect();
    for Entry { at, exchange } in entries {
        while boundary < at {
            settle(live, boundary).await?;
            boundary = next_boundary(boundary, tick);
        }
        if clock.now() < at {
            clock.set(at);
        }
        let id = exchange.exchange.meta.id;
        live.pipeline()
            .ingest(exchange, at)
            .await
            .map_err(|error| ReplayError::Ingest {
                exchange: id.ulid_text(),
                reason: error.to_string(),
            })?;
        settle(live, clock.now()).await?;
    }
    let until = settings.until.map_or(last, |until| until.max(last));
    let windows = Duration::from_millis(
        settings
            .flow
            .evidence_window_ms
            .saturating_add(settings.flow.suspected_ttl_ms),
    );
    let deadline = after(
        after(until, windows),
        DEFAULT_BUCKET + Duration::from_micros(tick.saturating_mul(2)),
    );
    while live.watermark() < until {
        if boundary > deadline {
            return Err(ReplayError::NotCaughtUp {
                watermark: live.watermark(),
                until,
                deadline,
            });
        }
        settle(live, boundary).await?;
        boundary = next_boundary(boundary, tick);
    }
    let settled_at = clock.now();
    let (export_bytes, evidence) = read_back(live, settings.since, settled_at).await?;
    let exported = read_export(&export_bytes)?;
    let evidence = order_evidence(&exported, evidence)?;
    let queried = query_back(live, &ids, &origin_spans(&evidence)).await?;
    Ok(Replayed {
        evidence,
        queried,
        export_bytes,
        exported,
        ingested,
        skipped: 0,
        settled_at,
        watermark: live.watermark(),
    })
}

/// The evidence in export order; every exported row must have one.
fn order_evidence(
    exported: &Exported,
    evidence: Vec<(TransmissionId, TransmissionEvidence)>,
) -> Result<Vec<TransmissionEvidence>, ReplayError> {
    let mut out = Vec::with_capacity(exported.transmissions.len());
    let mut held: BTreeMap<TransmissionId, TransmissionEvidence> = evidence.into_iter().collect();
    for id in &exported.transmissions {
        match held.remove(id) {
            Some(item) => out.push(item),
            None => return Err(ReplayError::NoEvidence(id.ulid_text())),
        }
    }
    Ok(out)
}

/// The export's JSONL body, as the HTTP API writes it, and each exported
/// transmission's evidence at context 0, as `swarm-fetch` asks.
async fn read_back(
    live: &Live,
    since: Timestamp,
    settled_at: Timestamp,
) -> Result<(Vec<u8>, Vec<(TransmissionId, TransmissionEvidence)>), ReplayError> {
    let surface = |read: &'static str| move |reason: String| ReplayError::Surface { read, reason };
    let caller = live
        .caller(RequestIdentity::Anonymous)
        .await
        .map_err(|error| surface("caller")(format!("{error:?}")))?;
    let window = TimeWindow::new(since, after(settled_at, WINDOW_TAIL))
        .map_err(|error| surface("export window")(format!("{error:?}")))?;
    let request =
        export_request(window).map_err(|error| surface("export request")(error.to_string()))?;
    let Export { header, mut rows } = live
        .surface()
        .export(&caller, &request)
        .await
        .map_err(|error| surface("export")(format!("{error:?}")))?;
    let mut bytes = Vec::new();
    let mut ids = Vec::new();
    push_line(&mut bytes, &ExportLine::Header(Box::new(header)))?;
    loop {
        match rows.next().await {
            ExportStep::Row(row, rest) => {
                if let ExportRow::Transmission(transmission) = &row {
                    ids.push(transmission.summary().id);
                }
                push_line(&mut bytes, &ExportLine::Row(row))?;
                rows = rest;
            }
            ExportStep::End(trailer) => {
                push_line(&mut bytes, &ExportLine::Trailer(trailer))?;
                break;
            }
        }
    }
    let context =
        ExcerptWindow::new(0).map_err(|error| surface("evidence window")(format!("{error:?}")))?;
    let mut evidence = Vec::with_capacity(ids.len());
    for id in ids {
        let item = live
            .surface()
            .transmission_evidence(&caller, id, context)
            .await
            .map_err(|error| surface("evidence")(format!("{error:?}")))?;
        if let Some(item) = item {
            evidence.push((id, item));
        }
    }
    Ok((bytes, evidence))
}

/// What the conversation reads answer for `exchanges` and `spans`.
async fn query_back(
    live: &Live,
    exchanges: &BTreeSet<ExchangeId>,
    spans: &BTreeSet<SpanId>,
) -> Result<Queried, ReplayError> {
    let surface = |read: &'static str| move |reason: String| ReplayError::Surface { read, reason };
    let caller = live
        .caller(RequestIdentity::Anonymous)
        .await
        .map_err(|error| surface("caller")(format!("{error:?}")))?;
    let mut out = Queried::default();
    for batch in batches(exchanges) {
        out.turns.extend(
            live.surface()
                .exchange_turns(&caller, &batch)
                .await
                .map_err(|error| surface("exchange turns")(format!("{error:?}")))?,
        );
    }
    for batch in batches(spans) {
        out.spans.extend(
            live.surface()
                .span_points(&caller, &batch)
                .await
                .map_err(|error| surface("span points")(format!("{error:?}")))?,
        );
    }
    Ok(out)
}

fn push_line(bytes: &mut Vec<u8>, line: &ExportLine) -> Result<(), ReplayError> {
    serde_json::to_writer(&mut *bytes, line).map_err(ReplayError::Encode)?;
    bytes.push(b'\n');
    Ok(())
}

/// `bench.env` in `dir`, if there is one.
pub fn read_bench_env(dir: &Path) -> Result<Option<BenchEnv>, ReadBenchEnvError> {
    let path = dir.join("bench.env");
    match std::fs::read_to_string(&path) {
        Ok(text) => BenchEnv::parse(&text)
            .map(Some)
            .map_err(ReadBenchEnvError::Parse),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(ReadBenchEnvError::Read {
            path: path.display().to_string(),
            source,
        }),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReadBenchEnvError {
    #[error("reading {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Parse(BenchEnvError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bench_env_reads_only_its_three_keys() {
        let env = BenchEnv::parse(
            "run=x\nscenario=headline\nevidence_window_ms=10000\nsuspected_ttl_ms=60000\n\
             crosstalk_image=sha256:00\nswarm_end_unix_ms=1791225869000\nnot a pair\n",
        );
        assert_eq!(
            env,
            Ok(BenchEnv {
                evidence_window_ms: Some(10_000),
                suspected_ttl_ms: Some(60_000),
                swarm_end_unix_ms: Some(1_791_225_869_000),
            })
        );
    }

    #[test]
    fn a_malformed_window_names_its_key_not_its_value() {
        let error = BenchEnv::parse("suspected_ttl_ms=soon\n");
        assert_eq!(
            error,
            Err(BenchEnvError {
                line: 1,
                key: "suspected_ttl_ms"
            })
        );
    }

    #[test]
    fn boundaries_are_strictly_after() {
        let tick = 1_000_000;
        assert_eq!(
            next_boundary(Timestamp::from_micros(2_000_000), tick),
            Timestamp::from_micros(3_000_000)
        );
        assert_eq!(
            next_boundary(Timestamp::from_micros(2_000_001), tick),
            Timestamp::from_micros(3_000_000)
        );
    }

    #[test]
    fn the_demo_flow_keeps_the_deploy_defaults() {
        let flow = demo_flow(10_000, 60_000);
        assert_eq!(flow.correlation_window_ms, 600_000);
        assert_eq!(flow.tick_ms, 1_000);
        assert_eq!(flow.shards, 1);
        assert_eq!(
            (flow.evidence_window_ms, flow.suspected_ttl_ms),
            (10_000, 60_000)
        );
    }
}
