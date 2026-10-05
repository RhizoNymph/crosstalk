//! `ct-eval swarm` and `ct-eval swarm-fetch`: the demo swarm benchmark.

use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use clap::Args;
use crosstalk_eval::datasets::swarm_truth::fetch::{FetchConfig, fetch as fetch_api};
use crosstalk_eval::datasets::swarm_truth::window::{
    DEFAULT_LEAD_MS, DEFAULT_SLACK_MS, Margins, RunWindow,
};
use crosstalk_eval::datasets::swarm_truth::{
    Inputs, Options, default_blobs, default_evidence, run_with as run_swarm, truth_file,
};
use crosstalk_eval::report::table::render;
use crosstalk_spec::support::{TimeWindow, Timestamp};
use serde::Serialize;

use super::load_gates;

#[derive(Args)]
pub struct SwarmArgs {
    /// The swarm's ground truth (`swarm --ground-truth PATH`, v2).
    #[arg(long)]
    truth: PathBuf,
    /// The gateway's exchange log (`<data dir>/exchanges/exchange-log.jsonl`).
    #[arg(long)]
    exchanges: PathBuf,
    /// The gateway's blob directory (default: `<data dir>/blobs`).
    #[arg(long)]
    blobs: Option<PathBuf>,
    /// The saved transmissions export (JSONL).
    #[arg(long)]
    export: PathBuf,
    /// The saved evidence, one per line (default: `evidence.jsonl` beside
    /// the export).
    #[arg(long)]
    evidence: Option<PathBuf>,
    /// Write report.json, report.txt and diagnostics.json here.
    #[arg(long)]
    out: Option<PathBuf>,
    /// Regression gates (default: `CT_EVAL_GATES`, then
    /// `/usr/local/share/crosstalk-eval/gates.toml`, then the crate's
    /// `gates.toml`, then none).
    #[arg(long)]
    gates: Option<PathBuf>,
    /// How many misses and false positives to keep as examples.
    #[arg(long, default_value_t = 50)]
    examples: usize,
    /// The run window's slack past the truth's latest row time, in
    /// milliseconds: exchanges of the truth's sessions that started
    /// outside the window are another run's and are left out.
    #[arg(long, default_value_t = DEFAULT_SLACK_MS)]
    run_slack_ms: u64,
    /// The run window's lead before the truth header's start, in
    /// milliseconds: room for clock skew between the swarm and the gateway.
    #[arg(long, default_value_t = DEFAULT_LEAD_MS)]
    run_lead_ms: u64,
}

#[derive(Args)]
pub struct FetchArgs {
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

/// Counts written beside the report.
#[derive(Serialize)]
struct Written<'a> {
    window: RunWindow,
    resolved: &'a crosstalk_eval::datasets::swarm_truth::ResolveCounts,
    detected: &'a crosstalk_eval::datasets::swarm_truth::DetectedCounts,
    key_groups: usize,
    table: Vec<crosstalk_eval::datasets::swarm_truth::diagnostics::DiagnosticCount>,
    diagnostics: &'a [crosstalk_eval::datasets::swarm_truth::Diagnostic],
}

pub fn run(args: SwarmArgs) -> Result<ExitCode> {
    let gates = load_gates(args.gates)?;
    let inputs = Inputs {
        blobs: args.blobs.unwrap_or_else(|| default_blobs(&args.exchanges)),
        evidence: args
            .evidence
            .unwrap_or_else(|| default_evidence(&args.export)),
        truth: args.truth,
        exchanges: args.exchanges,
        export: args.export,
    };
    let options = Options {
        examples: args.examples,
        margins: Margins {
            lead_ms: args.run_lead_ms,
            slack_ms: args.run_slack_ms,
        },
    };
    let outcome = run_swarm(&inputs, options, &gates)?;
    let mut text = render(&outcome.report);
    let resolved = &outcome.resolved;
    text.push_str(&format!(
        "\ntruth rows {}: sessions {} ({} exchanges, {} excluded outside run window), transmissions {} ({} without a sender exchange), self-reads {}, rereads {}, misses {} ({} controls), unattributed reads {} (unjudged), key groups {}, dropped {}\n",
        resolved.rows,
        resolved.sessions,
        resolved.exchanges,
        resolved.excluded_outside_window,
        resolved.transmissions,
        resolved.without_sender,
        resolved.self_reads,
        resolved.rereads,
        resolved.misses,
        resolved.miss_controls,
        resolved.unattributed,
        resolved.key_groups,
        resolved.dropped,
    ));
    text.push_str(&format!(
        "gateway: {} exported transmissions, {} with evidence, {} predictions\n\n",
        outcome.detected.exported, outcome.detected.evidence, outcome.detected.predictions
    ));
    text.push_str(&outcome.diagnostics.render());
    print!("{text}");
    if let Some(out) = &args.out {
        fs::create_dir_all(out).with_context(|| format!("creating {}", out.display()))?;
        let json = serde_json::to_string_pretty(&outcome.report)?;
        fs::write(out.join("report.json"), json + "\n")?;
        fs::write(out.join("report.txt"), &text)?;
        let written = Written {
            window: outcome.window,
            resolved: &outcome.resolved,
            detected: &outcome.detected,
            key_groups: outcome.key_groups,
            table: outcome.diagnostics.table(),
            diagnostics: &outcome.diagnostics.entries,
        };
        let json = serde_json::to_string_pretty(&written)?;
        fs::write(out.join("diagnostics.json"), json + "\n")?;
    }
    Ok(if outcome.report.gates_failed() {
        ExitCode::from(2)
    } else {
        ExitCode::SUCCESS
    })
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

pub fn fetch(args: FetchArgs) -> Result<ExitCode> {
    let since_ms = match (&args.truth, args.since_unix_ms) {
        (Some(path), _) => {
            let file =
                fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
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
    fs::create_dir_all(&args.out).with_context(|| format!("creating {}", args.out.display()))?;
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
