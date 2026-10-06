//! `ct-eval replay`: a saved bench run replayed offline through the
//! gateway's live composition and scored like `ct-eval swarm`.

use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result, anyhow};
use clap::Args;
use crosstalk_eval::datasets::swarm_truth::replay::{demo_flow, read_bench_env};
use crosstalk_eval::datasets::swarm_truth::{ReplayInputs, ReplayOptions, run_replay};
use crosstalk_spec::support::Timestamp;

use super::load_gates;
use super::swarm::{outcome_text, write_report};

/// The demo config's windows (`deploy/demo/crosstalk.demo.json`), used
/// when the run has no `bench.env`.
const DEMO_EVIDENCE_WINDOW_MS: u64 = 10_000;
const DEMO_SUSPECTED_TTL_MS: u64 = 60_000;

#[derive(Args)]
pub struct ReplayArgs {
    /// The run directory: `truth.jsonl`, `bench.env`, and (unless
    /// `--exchanges`/`--blobs` say otherwise) `exchange-log.jsonl` and
    /// `blobs/`.
    #[arg(long)]
    run: PathBuf,
    /// The gateway's exchange log (default: `<run>/exchange-log.jsonl`).
    #[arg(long)]
    exchanges: Option<PathBuf>,
    /// The gateway's blob directory (default: `<run>/blobs`).
    #[arg(long)]
    blobs: Option<PathBuf>,
    /// Write export.jsonl, evidence.jsonl, score.txt and report/ here
    /// (default: `<run>/replay`).
    #[arg(long)]
    out: Option<PathBuf>,
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
    /// Seeds the composition's ids.
    #[arg(long, default_value_t = 0)]
    seed: u64,
    /// Regression gates, as for `ct-eval swarm`.
    #[arg(long)]
    gates: Option<PathBuf>,
    /// How many misses and false positives to keep as examples.
    #[arg(long, default_value_t = 50)]
    examples: usize,
}

fn micros(ms: u64) -> Timestamp {
    Timestamp::from_micros(ms.saturating_mul(1000))
}

pub fn run(args: ReplayArgs) -> Result<ExitCode> {
    let gates = load_gates(args.gates)?;
    let env = read_bench_env(&args.run)?.unwrap_or_default();
    let evidence_window_ms = args
        .evidence_window_ms
        .or(env.evidence_window_ms)
        .unwrap_or(DEMO_EVIDENCE_WINDOW_MS);
    let suspected_ttl_ms = args
        .suspected_ttl_ms
        .or(env.suspected_ttl_ms)
        .unwrap_or(DEMO_SUSPECTED_TTL_MS);
    let inputs = ReplayInputs {
        truth: args.run.join("truth.jsonl"),
        exchanges: args
            .exchanges
            .unwrap_or_else(|| args.run.join("exchange-log.jsonl")),
        blobs: args.blobs.unwrap_or_else(|| args.run.join("blobs")),
    };
    let options = ReplayOptions {
        flow: demo_flow(evidence_window_ms, suspected_ttl_ms),
        seed: args.seed,
        since: args.since_unix_ms.map(micros),
        until: env.swarm_end_unix_ms.map(micros),
    };
    let outcome = run_replay(&inputs, &options, args.examples, &gates)?;
    let replayed = &outcome.replayed;
    eprintln!(
        "replay {}: {} exchanges ingested ({} earlier log entries skipped), evidence window {} ms, suspected ttl {} ms, settled at {} µs, watermark {} µs",
        args.run.display(),
        replayed.ingested,
        replayed.skipped,
        outcome.settings.flow.evidence_window_ms,
        outcome.settings.flow.suspected_ttl_ms,
        replayed.settled_at.as_micros(),
        replayed.watermark.as_micros(),
    );
    let text = outcome_text(&outcome.scored);
    print!("{text}");
    let out = args.out.unwrap_or_else(|| args.run.join("replay"));
    fs::create_dir_all(&out).with_context(|| format!("creating {}", out.display()))?;
    fs::write(out.join("export.jsonl"), &replayed.export_bytes)?;
    let mut evidence = String::new();
    for item in &replayed.evidence {
        evidence.push_str(
            &serde_json::to_string(item).map_err(|error| anyhow!("encoding evidence: {error}"))?,
        );
        evidence.push('\n');
    }
    fs::write(out.join("evidence.jsonl"), evidence)?;
    fs::write(out.join("score.txt"), &text)?;
    write_report(&out.join("report"), &outcome.scored, &text)?;
    Ok(if outcome.scored.report.gates_failed() {
        ExitCode::from(2)
    } else {
        ExitCode::SUCCESS
    })
}
