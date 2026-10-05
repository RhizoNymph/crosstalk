//! `ct-eval`: convert a dataset, run a detector over it, score, report.
//!
//! ```text
//! ct-eval run   --dataset salt [--root DIR] [--limit N] [--include TEXT]… [--out DIR] [--gates FILE]
//! ct-eval truth --dataset salt [--root DIR] [--limit N] [--include TEXT]… [--out FILE]
//! ct-eval run   --dataset ai-village [--mode window|claude-code] [--from DAY] [--to DAY] [--limit N] …
//! ```
//!
//! `run` prints the table, writes `report.json` and `report.txt` to `--out`,
//! and exits 2 when a gate fails. `truth` writes the labels as JSONL.

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use crosstalk_eval::config::EvalConfig;
use crosstalk_eval::corpus::{SourceError, TraceSource, World};
use crosstalk_eval::datasets::ai_village::report::Unlabelled;
use crosstalk_eval::datasets::ai_village::time::Day;
use crosstalk_eval::datasets::ai_village::{self as ai_village, AiVillageSource};
use crosstalk_eval::datasets::salt::{SaltSource, Selection};
use crosstalk_eval::gateway::PipelineDetector;
use crosstalk_eval::keys::DatasetId;
use crosstalk_eval::pipeline::{Detector, ReferenceDetector, run};
use crosstalk_eval::reference::ReferenceConfig;
use crosstalk_eval::report::table::render;
use crosstalk_eval::report::{Gates, Report};
use crosstalk_eval::truth::jsonl;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "ct-eval", about = "crosstalk evaluation harness")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Convert, run the reference detector, score and report.
    Run(RunArgs),
    /// Dump the dataset's labels as JSONL.
    Truth(TruthArgs),
}

#[derive(Clone, Copy, ValueEnum)]
enum Dataset {
    Salt,
    AiVillage,
}

impl Dataset {
    fn name(self) -> &'static str {
        match self {
            Self::Salt => "salt",
            Self::AiVillage => ai_village::DATASET,
        }
    }
}

/// Which part of AI Village to convert.
#[derive(Clone, Copy, ValueEnum)]
enum VillageMode {
    /// Every agent over `--from`..=`--to`, one world per village day.
    Window,
    /// The Claude Code agent's stream, one world per context (`--limit`
    /// caps the contexts).
    ClaudeCode,
}

#[derive(Args)]
struct SourceArgs {
    #[arg(long, value_enum)]
    dataset: Dataset,
    /// The dataset's directory (overrides the config).
    #[arg(long)]
    root: Option<PathBuf>,
    /// The datasets config (default: the crate's `datasets.toml`).
    #[arg(long)]
    config: Option<PathBuf>,
    /// Read at most this many trace files (stratified across conditions).
    #[arg(long)]
    limit: Option<usize>,
    /// Keep only files whose path contains this (repeatable).
    #[arg(long)]
    include: Vec<String>,
    /// AI Village: which part to convert.
    #[arg(long, value_enum, default_value_t = VillageMode::Window)]
    mode: VillageMode,
    /// AI Village window: the first village day (YYYY-MM-DD).
    #[arg(long, default_value = ai_village::DEFAULT_FROM)]
    from: String,
    /// AI Village window: the last village day, included.
    #[arg(long, default_value = ai_village::DEFAULT_TO)]
    to: String,
}

/// The dataset being read.
enum Source {
    Salt(SaltSource),
    AiVillage(Box<AiVillageSource>),
}

impl TraceSource for Source {
    fn id(&self) -> DatasetId {
        match self {
            Self::Salt(source) => source.id(),
            Self::AiVillage(source) => source.id(),
        }
    }

    fn worlds(&mut self) -> impl Iterator<Item = Result<World, SourceError>> + '_ {
        let worlds: Box<dyn Iterator<Item = Result<World, SourceError>> + '_> = match self {
            Self::Salt(source) => Box::new(source.worlds()),
            Self::AiVillage(source) => Box::new(source.worlds()),
        };
        worlds
    }
}

#[derive(Args)]
struct RunArgs {
    #[command(flatten)]
    source: SourceArgs,
    /// Write report.json and report.txt here.
    #[arg(long)]
    out: Option<PathBuf>,
    /// Regression gates (default: the crate's `gates.toml`).
    #[arg(long)]
    gates: Option<PathBuf>,
    /// How many misses and false positives to keep as examples.
    #[arg(long, default_value_t = 50)]
    examples: usize,
    /// Which detector to run.
    #[arg(long, value_enum, default_value_t = DetectorChoice::Reference)]
    detector: DetectorChoice,
    /// Seeds the gateway pipeline's envelope ids (`--detector pipeline`).
    #[arg(long, default_value_t = 0)]
    seed: u64,
    #[command(flatten)]
    matcher: MatcherArgs,
}

#[derive(Clone, Copy, ValueEnum)]
enum DetectorChoice {
    /// The naive reference matcher.
    Reference,
    /// The gateway pipeline (`Pipeline::ingest`); unscored until the
    /// detection layers consume the bus.
    Pipeline,
}

/// Reference matcher parameters (defaults: `ReferenceConfig::default`).
#[derive(Args)]
struct MatcherArgs {
    /// Shingle length, in folded bytes.
    #[arg(long)]
    k: Option<usize>,
    /// Shortest span and match, in folded bytes.
    #[arg(long)]
    min_span: Option<usize>,
    /// Fewest letters and digits a span or match must hold.
    #[arg(long)]
    min_word_chars: Option<usize>,
}

impl MatcherArgs {
    fn config(&self) -> ReferenceConfig {
        let base = ReferenceConfig::default();
        ReferenceConfig {
            k: self.k.unwrap_or(base.k),
            min_span: self.min_span.unwrap_or(base.min_span),
            min_word_chars: self.min_word_chars.unwrap_or(base.min_word_chars),
            ..base
        }
    }
}

#[derive(Args)]
struct TruthArgs {
    #[command(flatten)]
    source: SourceArgs,
    /// Write the JSONL here instead of stdout.
    #[arg(long)]
    out: Option<PathBuf>,
}

fn crate_file(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(name)
}

fn open_source(args: &SourceArgs) -> Result<Source> {
    let root = match &args.root {
        Some(root) => root.clone(),
        None => {
            let path = args
                .config
                .clone()
                .unwrap_or_else(|| crate_file("datasets.toml"));
            let config = if path.exists() {
                EvalConfig::load(&path)?
            } else {
                EvalConfig::default()
            };
            let home = std::env::var_os("HOME").map(PathBuf::from);
            config.dataset_root(args.dataset.name(), home.as_deref())?
        }
    };
    let selection = Selection {
        limit: args.limit,
        include: args.include.clone(),
    };
    match args.dataset {
        Dataset::Salt => SaltSource::open(&root, &selection)
            .map(Source::Salt)
            .with_context(|| format!("opening SALT at {}", root.display())),
        Dataset::AiVillage => {
            let mode = match args.mode {
                VillageMode::ClaudeCode => ai_village::Mode::ClaudeCode { limit: args.limit },
                VillageMode::Window => ai_village::Mode::Window {
                    from: Day::parse(&args.from)?,
                    to: Day::parse(&args.to)?,
                },
            };
            AiVillageSource::open(&root, mode)
                .map(|source| Source::AiVillage(Box::new(source)))
                .with_context(|| format!("opening AI Village at {}", root.display()))
        }
    }
}

fn run_command(args: RunArgs) -> Result<ExitCode> {
    let mut source = open_source(&args.source)?;
    let gates_path = args
        .gates
        .clone()
        .unwrap_or_else(|| crate_file("gates.toml"));
    let gates = if gates_path.exists() {
        Gates::load(&gates_path)?
    } else {
        Gates::default()
    };
    let dataset = source.id();
    let mut unlabelled = Unlabelled::default();
    let observe = |world: &World, predicted: &[_]| {
        if matches!(args.source.dataset, Dataset::AiVillage) {
            unlabelled.observe(world, predicted);
        }
    };
    let (name, summary) = match args.detector {
        DetectorChoice::Reference => {
            let mut detector = ReferenceDetector {
                config: args.matcher.config(),
            };
            let summary = run(&mut source, &mut detector, args.examples, observe);
            (detector.name().to_owned(), summary)
        }
        DetectorChoice::Pipeline => {
            let mut detector = PipelineDetector::new(args.seed)?;
            let summary = run(&mut source, &mut detector, args.examples, observe);
            (detector.name().to_owned(), summary)
        }
    };
    let outcomes = gates.evaluate(&summary.score);
    let failures = summary.failures.iter().map(ToString::to_string).collect();
    let report = Report::new(
        dataset,
        &name,
        summary.score,
        outcomes,
        failures,
        summary.unscored,
    );
    let table = render(&report);
    print!("{table}");
    let village = match &source {
        Source::AiVillage(source) => Some(serde_json::json!({
            "stats": source.stats(),
            "unlabelled_predictions": unlabelled,
        })),
        Source::Salt(_) => None,
    };
    if let Some(village) = &village {
        println!("{}", serde_json::to_string_pretty(village)?);
    }
    if let Some(out) = &args.out {
        fs::create_dir_all(out).with_context(|| format!("creating {}", out.display()))?;
        let json = serde_json::to_string_pretty(&report)?;
        fs::write(out.join("report.json"), json + "\n")?;
        fs::write(out.join("report.txt"), &table)?;
        if let Some(village) = &village {
            fs::write(
                out.join("ai-village.json"),
                serde_json::to_string_pretty(village)? + "\n",
            )?;
        }
    }
    Ok(if report.gates_failed() {
        ExitCode::from(2)
    } else {
        ExitCode::SUCCESS
    })
}

fn truth_command(args: TruthArgs) -> Result<ExitCode> {
    let mut source = open_source(&args.source)?;
    let mut out: Box<dyn Write> = match &args.out {
        Some(path) => Box::new(BufWriter::new(
            File::create(path).with_context(|| format!("creating {}", path.display()))?,
        )),
        None => Box::new(BufWriter::new(std::io::stdout().lock())),
    };
    let mut failed = 0usize;
    for world in source.worlds() {
        match world {
            Ok(world) => jsonl::write(&mut out, world.truth())?,
            Err(error) => {
                failed += 1;
                tracing::warn!(error = %error, "world skipped");
            }
        }
    }
    out.flush()?;
    Ok(if failed > 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
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
        Command::Run(args) => run_command(args),
        Command::Truth(args) => truth_command(args),
    };
    match result {
        Ok(code) => code,
        Err(error) => {
            tracing::error!(error = %format!("{error:#}"), "ct-eval failed");
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}
