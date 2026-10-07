//! `ct-bench-detect`: crosstalk's detector for the a2a-transmission-bench
//! (`crosstalk_bench_adapter`).
//!
//! ```text
//! ct-bench-detect --input DIR --output FILE [--mode live|pipeline] [--forwarding off|on]
//!                 [--extract-config FILE] [--correlation-window S] [--evidence-window S]
//!                 [--suspected-ttl S] [--seed N]
//! ct-bench-detect from-export --run DIR --out DIR [--detector-version TEXT]
//! ct-bench-detect replay      --run DIR --out DIR [--evidence-window-ms MS] [--suspected-ttl-ms MS]
//!                             [--since-unix-ms MS] [--seed N]
//! ct-bench-detect fetch       --api URL [--token-env VAR] --truth FILE --out RUNDIR
//! ct-bench-detect swarm-fetch --api URL [--token-env VAR] [--truth FILE | --since-unix-ms MS] --out RUNDIR
//! ```
//!
//! Exit 0: a complete predictions file (trailer present). A world that
//! cannot be processed is a `failed { reason }` world row; a non-zero exit
//! means the whole run failed.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use clap::{Args, Parser, Subcommand, ValueEnum};
use crosstalk_bench_adapter::detect::live::{Forwarding, LiveSettings};
use crosstalk_bench_adapter::from_export::{
    self, Outcome, RunFiles, gateway_version, query_ids, read_truth, replayed_detections,
    saved_detections,
};
use crosstalk_bench_adapter::run::{Mode, run};
use crosstalk_bench_adapter::swarm::fetch::{FetchConfig, fetch as fetch_api, fetch_queried};
use crosstalk_bench_adapter::swarm::replay::{ReplaySettings, demo_flow, read_bench_env};
use crosstalk_bench_adapter::swarm::truth_file;
use crosstalk_bench_adapter::swarm::window::Margins;
use crosstalk_flow::extract::ExtractConfig;
use crosstalk_spec::support::{TimeWindow, Timestamp};
use tracing_subscriber::EnvFilter;

/// The demo config's windows (`deploy/demo/crosstalk.demo.json`), used
/// when a run has no `bench.env`.
const DEMO_EVIDENCE_WINDOW_MS: u64 = 10_000;
const DEMO_SUSPECTED_TTL_MS: u64 = 60_000;

#[derive(Parser)]
#[command(
    name = "ct-bench-detect",
    about = "crosstalk's detector for the a2a-transmission-bench",
    args_conflicts_with_subcommands = true,
    subcommand_negates_reqs = true
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    #[command(flatten)]
    detect: DetectArgs,
}

#[derive(Subcommand)]
enum Command {
    /// A saved node0 bench run as a bench input directory and the
    /// gateway's predictions.
    FromExport(FromExportArgs),
    /// A saved bench run replayed through the live composition in memory,
    /// as a bench input directory and its predictions.
    Replay(ReplayArgs),
    /// Save the gateway's conversation reads for a run beside its export.
    Fetch(FetchArgs),
    /// Save the gateway's transmissions export and their evidence.
    SwarmFetch(SwarmFetchArgs),
}

#[derive(Clone, Copy, ValueEnum)]
enum ModeChoice {
    Live,
    Pipeline,
}

#[derive(Clone, Copy, ValueEnum)]
enum ForwardingChoice {
    Off,
    On,
}

#[derive(Args)]
struct DetectArgs {
    /// The input directory: manifest.json (input view), messages.jsonl,
    /// exchanges.jsonl.
    #[arg(long, required = true)]
    input: Option<PathBuf>,
    /// The predictions file to write.
    #[arg(long, required = true)]
    output: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = ModeChoice::Live)]
    mode: ModeChoice,
    /// Whether L4 indexes text an agent forwards under that agent.
    #[arg(long, value_enum, default_value_t = ForwardingChoice::Off)]
    forwarding: ForwardingChoice,
    /// L5's extractor configuration (`ExtractConfig` JSON).
    #[arg(long)]
    extract_config: Option<PathBuf>,
    /// L5's correlation window in seconds (default 60).
    #[arg(long)]
    correlation_window: Option<u64>,
    /// L5's evidence window in seconds (default 10).
    #[arg(long)]
    evidence_window: Option<u64>,
    /// How long a suspected transmission lives, in seconds (default 60).
    #[arg(long)]
    suspected_ttl: Option<u64>,
    /// Seeds the composition's ids.
    #[arg(long, default_value_t = 0)]
    seed: u64,
}

#[derive(Args)]
struct FromExportArgs {
    /// The run directory (truth.jsonl, exchange-log.jsonl, blobs/,
    /// export.jsonl, evidence.jsonl; exchange-turns.json and
    /// span-points.json when fetched).
    #[arg(long)]
    run: PathBuf,
    /// Write manifest.json, messages.jsonl, exchanges.jsonl and
    /// predictions.jsonl here.
    #[arg(long)]
    out: PathBuf,
    /// The gateway build that ran (default: bench.env's crosstalk_image).
    #[arg(long)]
    detector_version: Option<String>,
}

#[derive(Args)]
struct ReplayArgs {
    /// The run directory, as for `from-export`.
    #[arg(long)]
    run: PathBuf,
    #[arg(long)]
    out: PathBuf,
    /// Override bench.env's `evidence_window_ms`.
    #[arg(long)]
    evidence_window_ms: Option<u64>,
    /// Override bench.env's `suspected_ttl_ms`.
    #[arg(long)]
    suspected_ttl_ms: Option<u64>,
    /// Replay exchanges captured at or after this Unix ms (default: the
    /// truth header's `started_at_unix_ms`).
    #[arg(long)]
    since_unix_ms: Option<u64>,
    #[arg(long, default_value_t = 0)]
    seed: u64,
}

#[derive(Args)]
struct FetchArgs {
    /// The L8 API's base URL.
    #[arg(long, default_value = "http://localhost:8081")]
    api: String,
    /// The environment variable holding a bearer token, if the API needs one.
    #[arg(long)]
    token_env: Option<String>,
    /// The run's truth file (its run window).
    #[arg(long)]
    truth: PathBuf,
    /// The run directory: exchange-log.jsonl and evidence.jsonl are read,
    /// exchange-turns.json and span-points.json written.
    #[arg(long)]
    out: PathBuf,
}

#[derive(Args)]
struct SwarmFetchArgs {
    /// The L8 API's base URL.
    #[arg(long, default_value = "http://localhost:8081")]
    api: String,
    /// The environment variable holding a bearer token, if the API needs
    /// one.
    #[arg(long)]
    token_env: Option<String>,
    /// Start the export window at the truth header's `started_at_unix_ms`.
    #[arg(long, conflicts_with = "since_unix_ms")]
    truth: Option<PathBuf>,
    /// Start the export window here (Unix milliseconds; default 0).
    #[arg(long)]
    since_unix_ms: Option<u64>,
    /// Write export.jsonl and evidence.jsonl here.
    #[arg(long)]
    out: PathBuf,
}

fn live_mode(args: &DetectArgs) -> Result<Mode> {
    let secs = |value: Option<u64>| value.map(Duration::from_secs);
    let forwarding = match args.forwarding {
        ForwardingChoice::Off => Forwarding::Off,
        ForwardingChoice::On => Forwarding::On,
    };
    let settings = LiveSettings::short(args.seed)?
        .with_windows(
            secs(args.correlation_window),
            secs(args.evidence_window),
            secs(args.suspected_ttl),
        )?
        .with_forwarding(forwarding);
    let extract = match &args.extract_config {
        Some(path) => {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("reading {}", path.display()))?;
            ExtractConfig::from_json(&text)
                .with_context(|| format!("parsing {}", path.display()))?
        }
        None => ExtractConfig::default(),
    };
    Ok(Mode::Live {
        settings,
        extract: Box::new(extract),
    })
}

fn detect(args: DetectArgs) -> Result<ExitCode> {
    let (Some(input), Some(output)) = (&args.input, &args.output) else {
        anyhow::bail!("--input and --output are required");
    };
    let mode = match args.mode {
        ModeChoice::Live => live_mode(&args)?,
        ModeChoice::Pipeline => Mode::Pipeline { seed: args.seed },
    };
    let summary = run(input, output, &mode)?;
    eprintln!(
        "{} worlds: {} scored, {} unscored, {} failed; {} rows",
        summary.worlds, summary.scored, summary.unscored, summary.failed, summary.rows
    );
    Ok(ExitCode::SUCCESS)
}

fn report(outcome: &Outcome, out: &Path) -> Result<()> {
    let text = serde_json::to_string_pretty(outcome)? + "\n";
    std::fs::write(out.join("from-export.json"), &text)
        .with_context(|| format!("writing {}", out.display()))?;
    eprint!("{text}");
    Ok(())
}

fn from_export_command(args: FromExportArgs) -> Result<ExitCode> {
    let files = RunFiles::in_dir(&args.run);
    let version = args
        .detector_version
        .or_else(|| gateway_version(&args.run))
        .unwrap_or_else(|| "unrecorded".to_owned());
    let detections = saved_detections(&files, version)?;
    let outcome = from_export::write(&files, &detections, Margins::default(), &args.out)?;
    report(&outcome, &args.out)?;
    Ok(ExitCode::SUCCESS)
}

fn micros(ms: u64) -> Timestamp {
    Timestamp::from_micros(ms.saturating_mul(1000))
}

fn replay_command(args: ReplayArgs) -> Result<ExitCode> {
    let files = RunFiles::in_dir(&args.run);
    let truth = read_truth(&files)?;
    let env = read_bench_env(&args.run)?.unwrap_or_default();
    let settings = ReplaySettings {
        flow: demo_flow(
            args.evidence_window_ms
                .or(env.evidence_window_ms)
                .unwrap_or(DEMO_EVIDENCE_WINDOW_MS),
            args.suspected_ttl_ms
                .or(env.suspected_ttl_ms)
                .unwrap_or(DEMO_SUSPECTED_TTL_MS),
        ),
        seed: args.seed,
        since: micros(
            args.since_unix_ms
                .unwrap_or(truth.header.started_at_unix_ms),
        ),
        until: env.swarm_end_unix_ms.map(micros),
    };
    let (detections, replayed) = replayed_detections(&files, &settings)?;
    eprintln!(
        "replayed {} exchanges ({} earlier log entries skipped)",
        replayed.ingested, replayed.skipped
    );
    let outcome = from_export::write(&files, &detections, Margins::default(), &args.out)?;
    report(&outcome, &args.out)?;
    Ok(ExitCode::SUCCESS)
}

fn fetch_command(args: FetchArgs) -> Result<ExitCode> {
    let mut files = RunFiles::in_dir(&args.out);
    files.truth = args.truth.clone();
    let truth = read_truth(&files)?;
    let (exchanges, spans) = query_ids(&files, &truth, Margins::default())?;
    let token = match &args.token_env {
        Some(name) => {
            Some(std::env::var(name).with_context(|| format!("reading the token from ${name}"))?)
        }
        None => None,
    };
    let queried = fetch_queried(&args.api, token, &exchanges, &spans)?;
    queried.write(&args.out)?;
    eprintln!(
        "saved {} of {} exchange placements and {} of {} span points in {}",
        queried.turns.len(),
        exchanges.len(),
        queried.spans.len(),
        spans.len(),
        args.out.display()
    );
    Ok(ExitCode::SUCCESS)
}

/// One hour past now, in Unix microseconds: the export window's end (the
/// gateway cuts it at its watermark anyway).
fn window_end() -> Result<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("the clock is before 1970")?;
    let micros = u64::try_from(now.as_micros()).context("the clock overflows")?;
    Ok(micros.saturating_add(3_600_000_000))
}

fn swarm_fetch_command(args: SwarmFetchArgs) -> Result<ExitCode> {
    let since_ms = match (&args.truth, args.since_unix_ms) {
        (Some(path), _) => {
            let file =
                std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
            let truth = truth_file::read(std::io::BufReader::new(file))
                .with_context(|| format!("reading {}", path.display()))?;
            truth.header.started_at_unix_ms
        }
        (None, Some(ms)) => ms,
        (None, None) => 0,
    };
    let start = Timestamp::from_micros(since_ms.saturating_mul(1000));
    let end = Timestamp::from_micros(window_end()?);
    let window = TimeWindow::new(start, end)
        .map_err(|_| anyhow!("the export window starts after it ends"))?;
    let token = match &args.token_env {
        Some(name) => {
            Some(std::env::var(name).with_context(|| format!("reading the token from ${name}"))?)
        }
        None => None,
    };
    std::fs::create_dir_all(&args.out)
        .with_context(|| format!("creating {}", args.out.display()))?;
    let fetched = fetch_api(
        &FetchConfig {
            api: args.api,
            token,
            window,
        },
        &args.out,
    )?;
    println!(
        "saved {} ({} transmissions) and {} ({} without evidence)",
        fetched.export.display(),
        fetched.transmissions,
        fetched.evidence.display(),
        fetched.without_evidence
    );
    Ok(ExitCode::SUCCESS)
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let result = match cli.command {
        Some(Command::FromExport(args)) => from_export_command(args),
        Some(Command::Replay(args)) => replay_command(args),
        Some(Command::Fetch(args)) => fetch_command(args),
        Some(Command::SwarmFetch(args)) => swarm_fetch_command(args),
        None => detect(cli.detect),
    };
    match result {
        Ok(code) => code,
        Err(error) => {
            tracing::error!(error = %format!("{error:#}"), "ct-bench-detect failed");
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}
