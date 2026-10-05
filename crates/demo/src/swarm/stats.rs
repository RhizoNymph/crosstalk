//! What the agents report and the report built from it. Agents send
//! [`Event`]s over a channel; one collector task owns every figure, logs
//! progress, optionally writes the expected transmissions as JSON lines,
//! and returns the [`Report`] when the last sender is gone.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::protocol::PageSlug;

/// How one request ended, from the client's side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Outcome {
    Ok,
    /// A non-2xx status.
    Status(u16),
    /// No connection, no head, a cut body or a stall.
    Transport,
    /// A 2xx whose body is not a whole message.
    Malformed,
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Outcome::Ok => f.write_str("ok"),
            Outcome::Status(status) => write!(f, "http {status}"),
            Outcome::Transport => f.write_str("transport"),
            Outcome::Malformed => f.write_str("malformed"),
        }
    }
}

/// One request through the gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestSample {
    pub streaming: bool,
    /// A tool-result follow-up rather than a new prompt.
    pub followup: bool,
    pub outcome: Outcome,
    /// Until the first body byte.
    pub ttfb: Option<Duration>,
    /// Until the body ended.
    pub total: Duration,
    pub request_bytes: usize,
    pub response_bytes: usize,
}

/// Something an agent did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Request(RequestSample),
    WikiWrite {
        author: String,
        page: PageSlug,
        version: u64,
    },
    WikiRead {
        reader: String,
        page: PageSlug,
        /// The writer and version read; `None` when the page did not exist.
        found: Option<(String, u64)>,
    },
    /// A wiki call that failed outright.
    WikiError,
    ConversationEnded {
        completed: bool,
    },
}

/// Latency percentiles in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Percentiles {
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
}

/// The nearest-rank `p`th percentile (`0 < p <= 100`) of sorted values.
pub fn percentile(sorted: &[u64], p: f64) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted.get(rank.clamp(1, sorted.len()) - 1).copied()
}

impl Percentiles {
    /// Of `values` in microseconds, reported in milliseconds.
    pub fn of(mut values: Vec<u64>) -> Option<Self> {
        values.sort_unstable();
        let ms = |p| percentile(&values, p).map(|us| us / 1000);
        Some(Self {
            p50: ms(50.0)?,
            p95: ms(95.0)?,
            p99: ms(99.0)?,
            max: values.last().map(|us| us / 1000)?,
        })
    }
}

/// Latency of one kind of request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Latency {
    pub count: usize,
    pub ttfb: Option<Percentiles>,
    pub total: Option<Percentiles>,
}

/// The run's figures.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Report {
    pub agents: u32,
    pub keys: u32,
    pub seed: u64,
    pub elapsed_secs: f64,
    pub requests: u64,
    pub ok: u64,
    pub failures: BTreeMap<String, u64>,
    pub followups: u64,
    pub requests_per_sec: f64,
    pub request_mib: f64,
    pub response_mib: f64,
    pub streaming: Latency,
    pub non_streaming: Latency,
    pub conversations_completed: u64,
    pub conversations_abandoned: u64,
    pub wiki_writes: u64,
    pub wiki_reads: u64,
    pub wiki_misses: u64,
    pub wiki_errors: u64,
    pub pages_written: usize,
    /// Reads of a page last written by another agent: each puts that
    /// agent's model output into the reader's next request.
    pub expected_transmissions: u64,
    pub writer_reader_pairs: usize,
    pub self_reads: u64,
}

/// One expected transmission, as written to the ground-truth file.
#[derive(Debug, Clone, Serialize)]
struct Transmission<'a> {
    writer: &'a str,
    reader: &'a str,
    page: &'a str,
    version: u64,
    at_ms: u128,
}

/// What the collector is told up front.
#[derive(Debug, Clone)]
pub struct CollectorSetup {
    pub agents: u32,
    pub keys: u32,
    pub seed: u64,
    pub ground_truth: Option<PathBuf>,
    pub progress_every: Duration,
}

#[derive(Debug, Default)]
struct Kind {
    ttfb_us: Vec<u64>,
    total_us: Vec<u64>,
}

/// Why the collector stopped writing ground truth (the run goes on).
#[derive(Debug, thiserror::Error)]
pub enum GroundTruthError {
    #[error("ground-truth file {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}

/// Collects every event until all senders are dropped.
pub async fn collect(setup: CollectorSetup, mut events: mpsc::Receiver<Event>) -> Report {
    let started = Instant::now();
    let mut truth = match &setup.ground_truth {
        Some(path) => match tokio::fs::File::create(path).await {
            Ok(file) => Some((path.clone(), tokio::io::BufWriter::new(file))),
            Err(source) => {
                let error = GroundTruthError::Io {
                    path: path.clone(),
                    source,
                };
                tracing::warn!(%error, "not writing ground truth");
                None
            }
        },
        None => None,
    };
    let mut streaming = Kind::default();
    let mut whole = Kind::default();
    let (mut requests, mut ok, mut followups, mut req_bytes, mut resp_bytes) =
        (0, 0, 0, 0u64, 0u64);
    let mut failures: BTreeMap<String, u64> = BTreeMap::new();
    let (mut completed, mut abandoned) = (0, 0);
    let (mut writes, mut reads, mut misses, mut wiki_errors, mut transmissions, mut self_reads) =
        (0, 0, 0, 0, 0, 0);
    let mut pages = BTreeSet::new();
    let mut pairs = BTreeSet::new();
    let mut progress = tokio::time::interval(setup.progress_every);
    progress.tick().await;
    let mut last_requests = 0u64;
    loop {
        let event = tokio::select! {
            event = events.recv() => match event {
                Some(event) => event,
                None => break,
            },
            _ = progress.tick() => {
                let window = setup.progress_every.as_secs_f64().max(0.001);
                tracing::info!(
                    elapsed_s = started.elapsed().as_secs(),
                    requests,
                    ok,
                    failed = requests - ok,
                    rps = format!("{:.1}", (requests - last_requests) as f64 / window),
                    wiki_writes = writes,
                    wiki_reads = reads,
                    expected_transmissions = transmissions,
                    "swarm progress"
                );
                last_requests = requests;
                continue;
            }
        };
        match event {
            Event::Request(sample) => {
                requests += 1;
                followups += u64::from(sample.followup);
                req_bytes += sample.request_bytes as u64;
                resp_bytes += sample.response_bytes as u64;
                if sample.outcome == Outcome::Ok {
                    ok += 1;
                    let kind = if sample.streaming {
                        &mut streaming
                    } else {
                        &mut whole
                    };
                    if let Some(ttfb) = sample.ttfb {
                        kind.ttfb_us.push(micros(ttfb));
                    }
                    kind.total_us.push(micros(sample.total));
                } else {
                    *failures.entry(sample.outcome.to_string()).or_default() += 1;
                }
            }
            Event::WikiWrite { page, .. } => {
                writes += 1;
                pages.insert(page);
            }
            Event::WikiRead {
                reader,
                page,
                found,
            } => {
                reads += 1;
                match found {
                    None => misses += 1,
                    Some((writer, _)) if writer == reader => self_reads += 1,
                    Some((writer, version)) => {
                        transmissions += 1;
                        if let Some((path, file)) = truth.as_mut() {
                            let line = Transmission {
                                writer: &writer,
                                reader: &reader,
                                page: page.as_str(),
                                version,
                                at_ms: started.elapsed().as_millis(),
                            };
                            let mut bytes = serde_json::to_vec(&line).unwrap_or_default();
                            bytes.push(b'\n');
                            if let Err(source) = file.write_all(&bytes).await {
                                let error = GroundTruthError::Io {
                                    path: path.clone(),
                                    source,
                                };
                                tracing::warn!(%error, "ground truth stopped");
                                truth = None;
                            }
                        }
                        pairs.insert((writer, reader));
                    }
                }
            }
            Event::WikiError => wiki_errors += 1,
            Event::ConversationEnded { completed: true } => completed += 1,
            Event::ConversationEnded { completed: false } => abandoned += 1,
        }
    }
    if let Some((path, mut file)) = truth
        && let Err(source) = file.flush().await
    {
        let error = GroundTruthError::Io { path, source };
        tracing::warn!(%error, "ground truth not flushed");
    }
    let elapsed = started.elapsed().as_secs_f64();
    let latency = |kind: Kind| Latency {
        count: kind.total_us.len(),
        ttfb: Percentiles::of(kind.ttfb_us),
        total: Percentiles::of(kind.total_us),
    };
    Report {
        agents: setup.agents,
        keys: setup.keys,
        seed: setup.seed,
        elapsed_secs: elapsed,
        requests,
        ok,
        failures,
        followups,
        requests_per_sec: requests as f64 / elapsed.max(0.001),
        request_mib: req_bytes as f64 / (1024.0 * 1024.0),
        response_mib: resp_bytes as f64 / (1024.0 * 1024.0),
        streaming: latency(streaming),
        non_streaming: latency(whole),
        conversations_completed: completed,
        conversations_abandoned: abandoned,
        wiki_writes: writes,
        wiki_reads: reads,
        wiki_misses: misses,
        wiki_errors,
        pages_written: pages.len(),
        expected_transmissions: transmissions,
        writer_reader_pairs: pairs.len(),
        self_reads,
    }
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn latency_line(f: &mut fmt::Formatter<'_>, name: &str, latency: &Latency) -> fmt::Result {
    let show = |p: &Option<Percentiles>| match p {
        Some(p) => format!(
            "p50 {:>6} ms  p95 {:>6} ms  p99 {:>6} ms  max {:>6} ms",
            p.p50, p.p95, p.p99, p.max
        ),
        None => "-".to_owned(),
    };
    writeln!(f, "  {name} ({} ok)", latency.count)?;
    writeln!(f, "    first byte  {}", show(&latency.ttfb))?;
    writeln!(f, "    total       {}", show(&latency.total))
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "crosstalk demo swarm: {} agents on {} keys, {:.1} s, seed {}",
            self.agents, self.keys, self.elapsed_secs, self.seed
        )?;
        let failed: Vec<String> = self
            .failures
            .iter()
            .map(|(why, n)| format!("{why}: {n}"))
            .collect();
        writeln!(
            f,
            "requests       {} ({} ok, {} failed{}), {} tool follow-ups, {:.1} req/s",
            self.requests,
            self.ok,
            self.requests - self.ok,
            if failed.is_empty() {
                String::new()
            } else {
                format!(" [{}]", failed.join(", "))
            },
            self.followups,
            self.requests_per_sec
        )?;
        writeln!(
            f,
            "bytes          {:.1} MiB sent, {:.1} MiB received",
            self.request_mib, self.response_mib
        )?;
        writeln!(f, "latency (client-observed)")?;
        latency_line(f, "streaming", &self.streaming)?;
        latency_line(f, "non-streaming", &self.non_streaming)?;
        writeln!(
            f,
            "conversations  {} completed, {} abandoned",
            self.conversations_completed, self.conversations_abandoned
        )?;
        writeln!(
            f,
            "wiki           {} writes to {} pages, {} reads ({} missing pages, {} own pages), {} errors",
            self.wiki_writes,
            self.pages_written,
            self.wiki_reads,
            self.wiki_misses,
            self.self_reads,
            self.wiki_errors
        )?;
        writeln!(
            f,
            "expected transmissions  {} cross-agent reads over {} writer->reader pairs",
            self.expected_transmissions, self.writer_reader_pairs
        )
    }
}
