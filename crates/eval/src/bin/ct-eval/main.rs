//! `ct-eval`: convert a dataset, run a detector over it, score, report.
//!
//! ```text
//! ct-eval run   --dataset salt [--root DIR] [--limit N] [--include TEXT]… [--out DIR] [--gates FILE]
//! ct-eval truth --dataset salt [--root DIR] [--limit N] [--include TEXT]… [--out FILE]
//! ```
//!
//! `--dataset` is `salt`, `agentdojo`, `tau2`, `wiki` (collusion-wiki) or
//! `swarm` (swarm-traces). For the wiki, `--family`, `--wiki`,
//! `--min-agents` and `--max-agents` select worlds. For AgentDojo, `--include
//! pipeline=…`, `suite=…`, `attack=…` and `task=…` match a path component
//! exactly, and `run` also prints how the injections arrived.
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
use crosstalk_eval::datasets::agentdojo::{self, AgentDojoSource};
use crosstalk_eval::datasets::salt::{SaltSource, Selection};
use crosstalk_eval::datasets::swarm::{SwarmSelection, SwarmSource};
use crosstalk_eval::datasets::tau2::{self, Tau2Source};
use crosstalk_eval::datasets::wiki::{WikiSelection, WikiSource};
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
    Agentdojo,
    Tau2,
    /// collusion-wiki: public wikis as dead drops.
    Wiki,
    /// swarm-traces: the decoder corpus.
    Swarm,
}

impl Dataset {
    fn name(self) -> &'static str {
        match self {
            Self::Salt => "salt",
            Self::Agentdojo => "agentdojo",
            Self::Tau2 => "tau2",
            Self::Wiki => crosstalk_eval::datasets::wiki::DATASET,
            Self::Swarm => crosstalk_eval::datasets::swarm::DATASET,
        }
    }
}

/// Any dataset's source.
enum AnySource {
    Salt(SaltSource),
    AgentDojo(AgentDojoSource),
    Tau2(Tau2Source),
    Wiki(WikiSource),
    Swarm(SwarmSource),
}

impl TraceSource for AnySource {
    fn id(&self) -> DatasetId {
        match self {
            Self::Salt(source) => source.id(),
            Self::AgentDojo(source) => source.id(),
            Self::Tau2(source) => source.id(),
            Self::Wiki(source) => source.id(),
            Self::Swarm(source) => source.id(),
        }
    }

    fn worlds(&mut self) -> impl Iterator<Item = Result<World, SourceError>> + '_ {
        let worlds: Box<dyn Iterator<Item = Result<World, SourceError>> + '_> = match self {
            Self::Salt(source) => Box::new(source.worlds()),
            Self::AgentDojo(source) => Box::new(source.worlds()),
            Self::Tau2(source) => Box::new(source.worlds()),
            Self::Wiki(source) => Box::new(source.worlds()),
            Self::Swarm(source) => Box::new(source.worlds()),
        };
        worlds
    }
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
    /// Read at most this many worlds (SALT: trace files, stratified across
    /// conditions; wiki: worlds, largest first).
    #[arg(long)]
    limit: Option<usize>,
    /// Keep only files whose path contains this (repeatable).
    #[arg(long)]
    include: Vec<String>,
    /// Wiki: keep only pages in these task clusters, e.g. relay-coordination
    /// (repeatable).
    #[arg(long)]
    family: Vec<String>,
    /// Wiki: keep only pages on these wikis, e.g. dse (repeatable).
    #[arg(long)]
    wiki: Vec<String>,
    /// Wiki: keep only worlds with at least this many agents.
    #[arg(long)]
    min_agents: Option<usize>,
    /// Wiki: drop worlds with more than this many agents (bounds a demo).
    #[arg(long)]
    max_agents: Option<usize>,
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

fn open_source(args: &SourceArgs) -> Result<AnySource> {
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
            .map(AnySource::Salt)
            .with_context(|| format!("opening SALT at {}", root.display())),
        Dataset::Agentdojo => AgentDojoSource::open(
            &root,
            &agentdojo::Selection {
                limit: selection.limit,
                include: selection.include,
            },
        )
        .map(AnySource::AgentDojo)
        .with_context(|| format!("opening AgentDojo at {}", root.display())),
        Dataset::Tau2 => Tau2Source::open(
            &root,
            &tau2::Selection {
                limit: selection.limit,
                include: selection.include,
            },
        )
        .map(AnySource::Tau2)
        .with_context(|| format!("opening τ²-bench at {}", root.display())),
        Dataset::Wiki => WikiSource::open(
            &root,
            &WikiSelection {
                families: args.family.clone(),
                wikis: args.wiki.clone(),
                min_agents: args.min_agents,
                max_agents: args.max_agents,
                limit: args.limit,
            },
        )
        .map(AnySource::Wiki)
        .with_context(|| format!("opening collusion-wiki at {}", root.display())),
        Dataset::Swarm => SwarmSource::open(&root, &SwarmSelection { limit: args.limit })
            .map(AnySource::Swarm)
            .with_context(|| format!("opening swarm-traces at {}", root.display())),
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
    let (name, summary) = match args.detector {
        DetectorChoice::Reference => {
            let mut detector = ReferenceDetector {
                config: args.matcher.config(),
            };
            let summary = run(&mut source, &mut detector, args.examples, |_, _| {});
            (detector.name().to_owned(), summary)
        }
        DetectorChoice::Pipeline => {
            let mut detector = PipelineDetector::new(args.seed)?;
            let summary = run(&mut source, &mut detector, args.examples, |_, _| {});
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
    let mut table = render(&report);
    if let AnySource::AgentDojo(source) = &source {
        table.push('\n');
        table.push_str(&source.tally().to_string());
    }
    print!("{table}");
    if let Some(out) = &args.out {
        fs::create_dir_all(out).with_context(|| format!("creating {}", out.display()))?;
        let json = serde_json::to_string_pretty(&report)?;
        fs::write(out.join("report.json"), json + "\n")?;
        fs::write(out.join("report.txt"), &table)?;
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
