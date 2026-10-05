//! `ct-eval`: convert a dataset, run a detector over it, score, report.
//!
//! ```text
//! ct-eval run   --dataset salt [--root DIR] [--limit N] [--include TEXT]… [--out DIR] [--gates FILE]
//!               [--detector reference|pipeline|live] [--seed N]
//! ct-eval truth --dataset salt [--root DIR] [--limit N] [--include TEXT]… [--out FILE]
//! ct-eval swarm --truth FILE --exchanges LOG [--blobs DIR] --export FILE [--evidence FILE] [--out DIR] [--gates FILE]
//! ct-eval swarm-fetch --api URL [--token-env VAR] [--truth FILE | --since-unix-ms MS] --out DIR
//! ct-eval run   --dataset ai-village [--mode window|claude-code] [--from DAY] [--to DAY] [--limit N] …
//! ```
//!
//! `swarm` scores the gateway's saved export against a demo swarm's ground
//! truth (see `datasets::swarm_truth`); `swarm-fetch` saves that export and
//! its evidence from the L8 API.
//!
//! `--dataset` is `salt`, `agentdojo`, `tau2`, `ai-village`, `open-swe`,
//! `lmcache`, `swe-splice`, `cipher`, `wiki` (collusion-wiki) or `swarm`
//! (swarm-traces). For the wiki, `--family`, `--wiki`, `--min-agents` and
//! `--max-agents` select worlds, and `--demo` picks the small
//! relay-coordination demo subset. For AgentDojo, `--include
//! pipeline=…`, `suite=…`, `attack=…` and `task=…` match a path component
//! exactly, and `run` also prints how the injections arrived.
//!
//! `--dataset open-swe | lmcache` mixes `--agents-per-world` independent
//! trajectories per world from `--limit` shards (`--count` rows or sessions
//! from each); `--dataset swe-splice` plants `--count` splices and
//! `--dataset cipher` builds `--count` pairs per cipher, both seeded by
//! `--corpus-seed`.
//!
//! `run` prints the table, writes `report.json` and `report.txt` to `--out`,
//! and exits 2 when a gate fails. `truth` writes the labels as JSONL.
//! `--detector live` scores the gateway's live composition
//! (`crosstalk_gateway::live::Live`, a fresh one per world) through
//! `detect::live`.

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use crosstalk_eval::config::EvalConfig;
use crosstalk_eval::corpus::{SourceError, TraceSource, World};
use crosstalk_eval::datasets::agentdojo::{self, AgentDojoSource};
use crosstalk_eval::datasets::ai_village::report::Unlabelled;
use crosstalk_eval::datasets::ai_village::time::Day;
use crosstalk_eval::datasets::ai_village::{self as ai_village, AiVillageSource};
use crosstalk_eval::datasets::cipher::{self, CipherSource};
use crosstalk_eval::datasets::lmcache::LmcacheSource;
use crosstalk_eval::datasets::open_swe::{self, Mixing, OpenSweSource};
use crosstalk_eval::datasets::salt::{SaltSource, Selection};
use crosstalk_eval::datasets::swarm::{SwarmSelection, SwarmSource};
use crosstalk_eval::datasets::swe_splice::{self, SpliceSource};
use crosstalk_eval::datasets::tau2::{self, Tau2Source};
use crosstalk_eval::datasets::wiki::{WikiSelection, WikiSource};
use crosstalk_eval::detect::live::{LiveDetector, LiveSettings, gateway_backend};
use crosstalk_eval::gateway::PipelineDetector;
use crosstalk_eval::keys::DatasetId;
use crosstalk_eval::pipeline::{Detector, ReferenceDetector, run};
use crosstalk_eval::predict::Prediction;
use crosstalk_eval::reference::ReferenceConfig;
use crosstalk_eval::report::gates::{GATES_ENV, GateDetector, GateSearch, GatesFrom};
use crosstalk_eval::report::table::render;
use crosstalk_eval::report::{Gates, Report};
use crosstalk_eval::truth::jsonl;
use tracing_subscriber::EnvFilter;

mod swarm;

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
    /// Score the gateway's saved export against a demo swarm's ground truth.
    Swarm(swarm::SwarmArgs),
    /// Save the gateway's transmissions export and their evidence.
    SwarmFetch(swarm::FetchArgs),
}

#[derive(Clone, Copy, ValueEnum)]
enum Dataset {
    Salt,
    Agentdojo,
    Tau2,
    OpenSwe,
    Lmcache,
    SweSplice,
    Cipher,
    AiVillage,
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
            Self::OpenSwe => "open_swe",
            Self::Lmcache => "lmcache",
            Self::SweSplice => "swe_splice",
            Self::Cipher => "cipher",
            Self::AiVillage => ai_village::DATASET,
            Self::Wiki => crosstalk_eval::datasets::wiki::DATASET,
            Self::Swarm => crosstalk_eval::datasets::swarm::DATASET,
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

/// Any dataset's source.
enum AnySource {
    Salt(SaltSource),
    AgentDojo(AgentDojoSource),
    Tau2(Tau2Source),
    OpenSwe(OpenSweSource),
    Lmcache(LmcacheSource),
    Splice(SpliceSource),
    Cipher(CipherSource),
    AiVillage(Box<AiVillageSource>),
    Wiki(WikiSource),
    Swarm(SwarmSource),
}

impl TraceSource for AnySource {
    fn id(&self) -> DatasetId {
        match self {
            Self::Salt(source) => source.id(),
            Self::AgentDojo(source) => source.id(),
            Self::Tau2(source) => source.id(),
            Self::OpenSwe(source) => source.id(),
            Self::Lmcache(source) => source.id(),
            Self::Splice(source) => source.id(),
            Self::Cipher(source) => source.id(),
            Self::AiVillage(source) => source.id(),
            Self::Wiki(source) => source.id(),
            Self::Swarm(source) => source.id(),
        }
    }

    fn worlds(&mut self) -> impl Iterator<Item = Result<World, SourceError>> + '_ {
        let worlds: Box<dyn Iterator<Item = Result<World, SourceError>> + '_> = match self {
            Self::Salt(source) => Box::new(source.worlds()),
            Self::AgentDojo(source) => Box::new(source.worlds()),
            Self::Tau2(source) => Box::new(source.worlds()),
            Self::OpenSwe(source) => Box::new(source.worlds()),
            Self::Lmcache(source) => Box::new(source.worlds()),
            Self::Splice(source) => Box::new(source.worlds()),
            Self::Cipher(source) => Box::new(source.worlds()),
            Self::AiVillage(source) => Box::new(source.worlds()),
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
    /// Trajectories mixed into one world (open-swe, lmcache).
    #[arg(long, default_value_t = open_swe::AGENTS_PER_WORLD)]
    agents_per_world: usize,
    /// Rows (open-swe) or sessions (lmcache) read from each file, splices
    /// (swe-splice) or pairs per cipher (cipher).
    #[arg(long)]
    count: Option<usize>,
    /// Seeds the synthetic corpora (swe-splice, cipher).
    #[arg(long, default_value_t = 0)]
    corpus_seed: u64,
    /// AI Village: which part to convert.
    #[arg(long, value_enum, default_value_t = VillageMode::Window)]
    mode: VillageMode,
    /// AI Village window: the first village day (YYYY-MM-DD).
    #[arg(long, default_value = ai_village::DEFAULT_FROM)]
    from: String,
    /// AI Village window: the last village day, included.
    #[arg(long, default_value = ai_village::DEFAULT_TO)]
    to: String,
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
    /// Wiki: the demo subset (`WikiSelection::demo`); overrides the other
    /// wiki filters and `--limit`.
    #[arg(long)]
    demo: bool,
}

#[derive(Args)]
struct RunArgs {
    #[command(flatten)]
    source: SourceArgs,
    /// Write report.json and report.txt here.
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
    /// Write every prediction here as JSONL (`{"world", "prediction"}`):
    /// ids, agents, routes and locations, never text.
    #[arg(long)]
    predictions: Option<PathBuf>,
    /// Which detector to run.
    #[arg(long, value_enum, default_value_t = DetectorChoice::Reference)]
    detector: DetectorChoice,
    /// Seeds the gateway's envelope ids (`--detector pipeline` or `live`).
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
    /// The gateway's live composition (L3–L7, `crosstalk_gateway::live::Live`)
    /// through `LiveBackend`.
    Live,
}

impl DetectorChoice {
    /// The gates that apply to this detector's runs.
    fn gated(self) -> GateDetector {
        match self {
            Self::Reference => GateDetector::Reference,
            Self::Pipeline => GateDetector::Pipeline,
            Self::Live => GateDetector::Live,
        }
    }
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
    /// The most distinct originated spans a shingle may be posted for
    /// before it is boilerplate.
    #[arg(long)]
    max_postings: Option<usize>,
}

impl MatcherArgs {
    fn config(&self) -> ReferenceConfig {
        let base = ReferenceConfig::default();
        ReferenceConfig {
            k: self.k.unwrap_or(base.k),
            min_span: self.min_span.unwrap_or(base.min_span),
            min_word_chars: self.min_word_chars.unwrap_or(base.min_word_chars),
            max_postings: self.max_postings.unwrap_or(base.max_postings),
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

/// The run's gates: `--gates`, else `CT_EVAL_GATES`, else the bench image's
/// installed file, else the crate's own in a source checkout, else none.
/// Says which on stderr. Only a missing `--gates` file is an error.
fn load_gates(flag: Option<PathBuf>) -> Result<Gates> {
    let search = GateSearch::from_env(flag, crate_file("gates.toml"));
    let (gates, location) = search.load()?;
    match location {
        Some(location) => eprintln!(
            "gates: {} ({})",
            location.path.display(),
            match location.from {
                GatesFrom::Flag => "--gates",
                GatesFrom::Env => GATES_ENV,
                GatesFrom::Installed => "installed",
                GatesFrom::Crate => "crate",
            }
        ),
        None => eprintln!("no gates"),
    }
    Ok(gates)
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
    let opening = || format!("opening {} at {}", args.dataset.name(), root.display());
    let mixing = Mixing {
        agents_per_world: args.agents_per_world,
        per_shard: args.count,
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
        Dataset::OpenSwe => OpenSweSource::open(&root, &selection, mixing)
            .map(AnySource::OpenSwe)
            .with_context(opening),
        Dataset::Lmcache => LmcacheSource::open(&root, &selection, mixing)
            .map(AnySource::Lmcache)
            .with_context(opening),
        Dataset::SweSplice => SpliceSource::open(
            &root,
            &selection,
            args.count.unwrap_or(swe_splice::SPLICES),
            args.corpus_seed,
        )
        .map(AnySource::Splice)
        .with_context(opening),
        Dataset::Cipher => CipherSource::open(
            &root,
            &selection,
            args.count.unwrap_or(cipher::PAIRS_PER_CIPHER),
            args.corpus_seed,
        )
        .map(AnySource::Cipher)
        .with_context(opening),
        Dataset::AiVillage => {
            let mode = match args.mode {
                VillageMode::ClaudeCode => ai_village::Mode::ClaudeCode { limit: args.limit },
                VillageMode::Window => ai_village::Mode::Window {
                    from: Day::parse(&args.from)?,
                    to: Day::parse(&args.to)?,
                },
            };
            AiVillageSource::open(&root, mode)
                .map(|source| AnySource::AiVillage(Box::new(source)))
                .with_context(|| format!("opening AI Village at {}", root.display()))
        }
        Dataset::Wiki => WikiSource::open(
            &root,
            &if args.demo {
                WikiSelection::demo()
            } else {
                WikiSelection {
                    families: args.family.clone(),
                    wikis: args.wiki.clone(),
                    min_agents: args.min_agents,
                    max_agents: args.max_agents,
                    limit: args.limit,
                }
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
    // swarm-traces labels hold real attack payloads: its reports carry only
    // counts, lengths and codec chains, never a miss or false-positive
    // example (which would print the label's text).
    let examples = match args.source.dataset {
        Dataset::Swarm => 0,
        _ => args.examples,
    };
    let mut source = open_source(&args.source)?;
    let gates = load_gates(args.gates.clone())?;
    let dataset = source.id();
    let mut unlabelled = Unlabelled::default();
    let mut dump = match &args.predictions {
        Some(path) => Some(BufWriter::new(
            File::create(path).with_context(|| format!("creating {}", path.display()))?,
        )),
        None => None,
    };
    let mut dumped: std::io::Result<()> = Ok(());
    let observe = |world: &World, predicted: &[Prediction]| {
        if matches!(args.source.dataset, Dataset::AiVillage) {
            unlabelled.observe(world, predicted);
        }
        if let (Some(out), Ok(())) = (dump.as_mut(), &dumped) {
            dumped = write_predictions(out, world, predicted);
        }
    };
    let (name, summary) = match args.detector {
        DetectorChoice::Reference => {
            let mut detector = ReferenceDetector {
                config: args.matcher.config(),
            };
            let summary = run(&mut source, &mut detector, examples, observe);
            (detector.name().to_owned(), summary)
        }
        DetectorChoice::Pipeline => {
            let mut detector = PipelineDetector::new(args.seed)?;
            let summary = run(&mut source, &mut detector, examples, observe);
            (detector.name().to_owned(), summary)
        }
        DetectorChoice::Live => {
            let mut detector = LiveDetector::new(gateway_backend(), LiveSettings::short(args.seed)?)?;
            let summary = run(&mut source, &mut detector, examples, observe);
            (detector.name().to_owned(), summary)
        }
    };
    dumped.context("writing predictions")?;
    if let Some(mut out) = dump {
        out.flush().context("writing predictions")?;
    }
    let gates = gates.for_detector(args.detector.gated());
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
    if let AnySource::Wiki(source) = &source {
        table.push('\n');
        table.push_str(&source.families().to_string());
    }
    if let AnySource::Swarm(source) = &source {
        table.push('\n');
        table.push_str(&source.tally().to_string());
    }
    print!("{table}");
    let village = match &source {
        AnySource::AiVillage(source) => Some(serde_json::json!({
            "stats": source.stats(),
            "unlabelled_predictions": unlabelled,
        })),
        AnySource::Salt(_)
        | AnySource::AgentDojo(_)
        | AnySource::Tau2(_)
        | AnySource::Wiki(_)
        | AnySource::Swarm(_)
        | AnySource::OpenSwe(_)
        | AnySource::Lmcache(_)
        | AnySource::Splice(_)
        | AnySource::Cipher(_) => None,
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

/// One JSONL line per prediction of `world`.
fn write_predictions(
    out: &mut impl Write,
    world: &World,
    predicted: &[Prediction],
) -> std::io::Result<()> {
    let world = world.key().to_string();
    for prediction in predicted {
        let line = serde_json::json!({ "world": world, "prediction": prediction });
        serde_json::to_writer(&mut *out, &line)?;
        out.write_all(b"\n")?;
    }
    Ok(())
}

fn truth_command(args: TruthArgs) -> Result<ExitCode> {
    if let Dataset::Swarm = args.source.dataset {
        // The labels' text is the encoded payload itself.
        anyhow::bail!(
            "swarm-traces labels hold real attack payloads; `truth` does not dump them (use `run`, whose report carries only codec chains, counts and lengths)"
        );
    }
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
        Command::Swarm(args) => swarm::run(args),
        Command::SwarmFetch(args) => swarm::fetch(args),
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
