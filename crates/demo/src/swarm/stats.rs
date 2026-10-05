//! What the agents report and the report built from it. Agents send
//! [`Event`]s over a channel; one collector task owns every figure and the
//! [`TruthBook`] that pairs reads with writes, logs progress, optionally
//! writes the ground truth (schema v2, [`super::truth`]) as JSON lines, and
//! returns the [`Report`] when the last sender is gone.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::truth::{ReadRecord, Row, RunInfo, TruthBook, WriteRecord};

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
    /// A page version the wiki accepted.
    WikiWrite(WriteRecord),
    /// A read, sent once its result is in a request being sent.
    WikiRead(ReadRecord),
    /// A wiki call that failed outright.
    WikiError,
    ConversationEnded {
        session: String,
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
    /// First reads in a session of a page version another agent wrote:
    /// each puts that agent's model output into the reader's next request.
    pub expected_transmissions: u64,
    pub writer_reader_pairs: usize,
    pub self_reads: u64,
    /// Reads of a page version another agent wrote that this session had
    /// already read.
    pub rereads: u64,
    /// Found reads of a version whose write this run never reported (written
    /// before the run, or by a writer cut off before reporting): no row.
    pub unattributed_reads: u64,
    /// The run's id; the ground truth's world is `swarm-<run>`.
    pub run: String,
}

/// What the collector is told up front.
#[derive(Debug, Clone)]
pub struct CollectorSetup {
    pub info: RunInfo,
    /// Agent `i`'s name is `agent_names[i]`.
    pub agent_names: Vec<String>,
    pub ground_truth: Option<PathBuf>,
    pub progress_every: Duration,
}

/// The ground-truth file, while it is being written.
struct TruthFile {
    path: PathBuf,
    file: tokio::io::BufWriter<tokio::fs::File>,
}

impl TruthFile {
    async fn create(path: &PathBuf) -> Option<Self> {
        match tokio::fs::File::create(path).await {
            Ok(file) => Some(Self {
                path: path.clone(),
                file: tokio::io::BufWriter::new(file),
            }),
            Err(source) => {
                let error = GroundTruthError::Io {
                    path: path.clone(),
                    source,
                };
                tracing::warn!(%error, "not writing ground truth");
                None
            }
        }
    }
}

/// Appends `rows` to the file; on an error, stops writing (the run goes on).
async fn append(truth: &mut Option<TruthFile>, rows: &[Row]) {
    let Some(out) = truth.as_mut() else {
        return;
    };
    let mut bytes = Vec::new();
    for row in rows {
        match serde_json::to_vec(row) {
            Ok(line) => {
                bytes.extend_from_slice(&line);
                bytes.push(b'\n');
            }
            Err(source) => {
                let error = GroundTruthError::Encode(source);
                tracing::warn!(%error, "ground-truth row skipped");
            }
        }
    }
    if let Err(source) = out.file.write_all(&bytes).await {
        let error = GroundTruthError::Io {
            path: out.path.clone(),
            source,
        };
        tracing::warn!(%error, "ground truth stopped");
        *truth = None;
    }
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
    #[error("encoding a row: {0}")]
    Encode(serde_json::Error),
}

/// Collects every event until all senders are dropped.
pub async fn collect(setup: CollectorSetup, mut events: mpsc::Receiver<Event>) -> Report {
    let started = Instant::now();
    let mut truth = match &setup.ground_truth {
        Some(path) => TruthFile::create(path).await,
        None => None,
    };
    let names = &setup.agent_names;
    let opening = Row::opening(&setup.info, |i| {
        names
            .get(i as usize)
            .cloned()
            .unwrap_or_else(|| format!("agent-{i:03}"))
    });
    append(&mut truth, &opening).await;
    let mut book = TruthBook::new(setup.info.world());
    let mut streaming = Kind::default();
    let mut whole = Kind::default();
    let (mut requests, mut ok, mut followups, mut req_bytes, mut resp_bytes) =
        (0, 0, 0, 0u64, 0u64);
    let mut failures: BTreeMap<String, u64> = BTreeMap::new();
    let (mut completed_count, mut abandoned) = (0, 0);
    let (mut writes, mut reads, mut wiki_errors) = (0, 0, 0);
    let mut pages = BTreeSet::new();
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
                    expected_transmissions = book.counts().transmissions,
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
            Event::WikiWrite(write) => {
                writes += 1;
                pages.insert(write.page.clone());
                let rows = book.write(write);
                append(&mut truth, &rows).await;
            }
            Event::WikiRead(read) => {
                reads += 1;
                if let Some(row) = book.read(read) {
                    append(&mut truth, &[row]).await;
                }
            }
            Event::WikiError => wiki_errors += 1,
            Event::ConversationEnded { session, completed } => {
                if completed {
                    completed_count += 1;
                } else {
                    abandoned += 1;
                }
                book.end_session(&session);
            }
        }
    }
    let unattributed = book.finish();
    for row in &unattributed {
        if let Row::UnattributedRead(read) = row {
            tracing::info!(
                reader = %read.reader,
                page = %read.page,
                version = read.version,
                session = %read.reader_session,
                "read of a version this run never saw written; writing an unattributed_read row"
            );
        }
    }
    append(&mut truth, &unattributed).await;
    if let Some(mut out) = truth
        && let Err(source) = out.file.flush().await
    {
        let error = GroundTruthError::Io {
            path: out.path,
            source,
        };
        tracing::warn!(%error, "ground truth not flushed");
    }
    let counts = book.counts();
    let elapsed = started.elapsed().as_secs_f64();
    let latency = |kind: Kind| Latency {
        count: kind.total_us.len(),
        ttfb: Percentiles::of(kind.ttfb_us),
        total: Percentiles::of(kind.total_us),
    };
    Report {
        agents: setup.info.agents,
        keys: setup.info.keys,
        seed: setup.info.seed,
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
        conversations_completed: completed_count,
        conversations_abandoned: abandoned,
        wiki_writes: writes,
        wiki_reads: reads,
        wiki_misses: counts.misses,
        wiki_errors,
        pages_written: pages.len(),
        expected_transmissions: counts.transmissions,
        writer_reader_pairs: book.pairs(),
        self_reads: counts.self_reads,
        rereads: counts.rereads,
        unattributed_reads: counts.unattributed,
        run: setup.info.run.clone(),
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
            "crosstalk demo swarm: {} agents on {} keys, {:.1} s, seed {}, run {}",
            self.agents, self.keys, self.elapsed_secs, self.seed, self.run
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
            "expected transmissions  {} cross-agent reads over {} writer->reader pairs, {} rereads{}",
            self.expected_transmissions,
            self.writer_reader_pairs,
            self.rereads,
            if self.unattributed_reads == 0 {
                String::new()
            } else {
                format!(", {} unattributed reads", self.unattributed_reads)
            }
        )
    }
}
