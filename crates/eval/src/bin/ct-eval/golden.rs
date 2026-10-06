//! `ct-eval export --format a2a-bench/1`, `ct-eval run --predictions-out`
//! and `ct-eval verify`: the golden export (`crosstalk_eval::golden`).
//!
//! ```text
//! ct-eval export --format a2a-bench/1 --dataset salt [the selection flags of run] --out DIR
//! ct-eval run    --dataset salt [...] --predictions-out FILE
//! ct-eval verify --export DIR [--predictions FILE]
//! ```
//!
//! An export and a run over the same selection derive the same manifest
//! (the run writes its export to sinks), so a run's predictions name the
//! export's manifest digest.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;

use a2a_bench_format::files::DetectorInfo;
use a2a_bench_format::manifest::{Setting, Source};
use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use crosstalk_eval::config::{EvalConfig, expand};
use crosstalk_eval::corpus::TraceSource;
use crosstalk_eval::golden::manifest::{self, digest_tree, files_under, int, list, revision, text};
use crosstalk_eval::golden::run::write_manifest;
use crosstalk_eval::golden::{
    ExportWriter, Finished, GoldenRun, ManifestSpec, PredictionsWriter, Verified, ids, verify,
};
use crosstalk_eval::reference::ReferenceConfig;

use super::{
    AnySource, Dataset, DetectorChoice, ForwardingChoice, RunArgs, SourceArgs, VillageMode,
    crate_file, open_source,
};

/// The export formats `ct-eval export` writes.
#[derive(Clone, Copy, ValueEnum)]
pub enum ExportFormat {
    /// The bench's format, `a2a-bench/1`.
    #[value(name = "a2a-bench/1")]
    A2aBench1,
}

#[derive(Args)]
pub struct ExportArgs {
    #[command(flatten)]
    pub source: SourceArgs,
    #[arg(long, value_enum)]
    pub format: ExportFormat,
    /// The export directory: manifest.json, messages.jsonl, exchanges.jsonl,
    /// labels.jsonl.
    #[arg(long)]
    pub out: PathBuf,
}

#[derive(Args)]
pub struct VerifyArgs {
    /// An export directory.
    #[arg(long)]
    pub export: PathBuf,
    /// A predictions file made on it.
    #[arg(long)]
    pub predictions: Option<PathBuf>,
}

/// Where a dataset's files are, and the data root above them.
struct Located {
    root: PathBuf,
    data_root: Option<PathBuf>,
}

fn locate(args: &SourceArgs) -> Result<Located> {
    if let Some(root) = &args.root {
        return Ok(Located {
            root: root.clone(),
            data_root: root.parent().map(Path::to_owned),
        });
    }
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
    Ok(Located {
        root: config.dataset_root(args.dataset.name(), home.as_deref())?,
        data_root: Some(expand(&config.root, home.as_deref())),
    })
}

/// The selection and pace settings of `args` that apply to its dataset.
fn settings(args: &SourceArgs) -> (BTreeMap<String, Setting>, BTreeMap<String, Setting>) {
    let mut selection = BTreeMap::new();
    let limit = |selection: &mut BTreeMap<String, Setting>| {
        if let Some(limit) = args.limit {
            selection.insert("limit".to_owned(), int(limit as u64));
        }
    };
    let count = |selection: &mut BTreeMap<String, Setting>| {
        if let Some(count) = args.count {
            selection.insert("count".to_owned(), int(count as u64));
        }
    };
    let paced = |pace: &mut BTreeMap<String, Setting>| {
        pace.insert("seed".to_owned(), int(args.corpus_seed));
        pace.insert("min_ms".to_owned(), int(args.pace_min_ms));
        pace.insert("max_ms".to_owned(), int(args.pace_max_ms));
    };
    let mut pace = BTreeMap::new();
    match args.dataset {
        Dataset::Salt | Dataset::Agentdojo => {
            limit(&mut selection);
            list(&mut selection, "include", &args.include);
            paced(&mut pace);
        }
        Dataset::Tau2 => {
            limit(&mut selection);
            list(&mut selection, "include", &args.include);
        }
        Dataset::OpenSwe | Dataset::Lmcache => {
            limit(&mut selection);
            list(&mut selection, "include", &args.include);
            count(&mut selection);
            selection.insert(
                "agents_per_world".to_owned(),
                int(args.agents_per_world as u64),
            );
            if let Dataset::OpenSwe = args.dataset {
                paced(&mut pace);
            }
        }
        Dataset::SweSplice | Dataset::Cipher => {
            limit(&mut selection);
            list(&mut selection, "include", &args.include);
            count(&mut selection);
            selection.insert("corpus_seed".to_owned(), int(args.corpus_seed));
            paced(&mut pace);
        }
        Dataset::AiVillage => match args.mode {
            VillageMode::ClaudeCode => {
                selection.insert("mode".to_owned(), text("claude-code"));
                limit(&mut selection);
            }
            VillageMode::Window => {
                selection.insert("mode".to_owned(), text("window"));
                selection.insert("from".to_owned(), text(args.from.clone()));
                selection.insert("to".to_owned(), text(args.to.clone()));
                if let Some(hours) = args.hours {
                    selection.insert("hours".to_owned(), int(u64::from(hours)));
                }
            }
        },
        Dataset::Wiki => {
            if args.demo {
                selection.insert("demo".to_owned(), Setting::Bool(true));
            } else {
                limit(&mut selection);
                list(&mut selection, "family", &args.family);
                list(&mut selection, "wiki", &args.wiki);
                if let Some(min) = args.min_agents {
                    selection.insert("min_agents".to_owned(), int(min as u64));
                }
                if let Some(max) = args.max_agents {
                    selection.insert("max_agents".to_owned(), int(max as u64));
                }
            }
            paced(&mut pace);
        }
        Dataset::Swarm => {
            limit(&mut selection);
            paced(&mut pace);
        }
    }
    (selection, pace)
}

/// The manifest an export of `args` pins, beside its written files: the
/// source digest is over the trace files a SALT selection reads, and over
/// every file under the dataset's directory for the others.
pub fn manifest_spec(args: &SourceArgs, source: &AnySource) -> Result<ManifestSpec> {
    let located = locate(args)?;
    let started = Instant::now();
    let files = match source {
        AnySource::Salt(salt) => {
            let mut files = salt.files().to_vec();
            files.sort();
            files
        }
        _ => files_under(&located.root)?,
    };
    let digest = digest_tree(&located.root, &files)?;
    tracing::info!(
        files = files.len(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "digested the source"
    );
    let path = located
        .data_root
        .as_deref()
        .and_then(|data_root| located.root.strip_prefix(data_root).ok())
        .map(|relative| relative.to_string_lossy().into_owned())
        .or_else(|| {
            located
                .root
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .unwrap_or_default();
    let (selection, pace) = settings(args);
    Ok(ManifestSpec {
        dataset: ids::dataset(&source.id())?,
        source: Source {
            path,
            revision: revision(&located.root, located.data_root.as_deref()),
            digest,
        },
        converter: manifest::converter(),
        selection,
        pace,
    })
}

/// Who wrote a run's predictions: the detector, the crosstalk commit, and
/// the setting gates select on.
pub fn detector_info(args: &RunArgs) -> DetectorInfo {
    let version = manifest::crosstalk_commit().unwrap_or_else(|| manifest::UNKNOWN.to_owned());
    let (name, variant) = match args.detector {
        DetectorChoice::Reference => {
            let config = args.matcher.config();
            let variant = if config == ReferenceConfig::default() {
                "default".to_owned()
            } else {
                format!(
                    "k{}-span{}-decoded{}-words{}-postings{}",
                    config.k,
                    config.min_span,
                    config.min_decoded,
                    config.min_word_chars,
                    config.max_postings
                )
            };
            ("reference", variant)
        }
        DetectorChoice::Pipeline => ("crosstalk-pipeline", "default".to_owned()),
        DetectorChoice::Live => (
            "crosstalk-live",
            match args.forwarding {
                ForwardingChoice::Off => "forwarding-off".to_owned(),
                ForwardingChoice::On => "forwarding-on".to_owned(),
            },
        ),
    };
    DetectorInfo {
        name: name.to_owned(),
        version,
        variant,
        config_digest: None,
    }
}

/// A run's golden side: its export to sinks and its predictions file.
pub fn predictions_run(
    args: &RunArgs,
    source: &AnySource,
    path: &Path,
) -> Result<(GoldenRun<std::io::Sink>, ManifestSpec)> {
    let spec = manifest_spec(&args.source, source)?;
    let writer = ExportWriter::sink(&spec.dataset)?;
    let predictions = PredictionsWriter::create(path)?;
    Ok((GoldenRun::new(writer, Some(predictions)), spec))
}

/// Prints what a finished export or run wrote, with what the format could
/// not hold.
pub fn report(finished: &Finished, verified: Option<Verified>) -> Result<()> {
    let manifest = &finished.manifest;
    eprintln!(
        "a2a-bench/1: {} worlds, {} messages, {} exchanges, {} labels; manifest digest {}",
        manifest.worlds.len(),
        finished.written.messages.rows,
        finished.written.exchanges.rows,
        finished.written.labels.rows,
        manifest.digest().context("the manifest digest")?
    );
    if let Some(trailer) = &finished.predictions {
        eprintln!(
            "predictions: {} rows, digest {}",
            trailer.rows, trailer.digest
        );
    }
    eprintln!(
        "not held by the format: {}",
        serde_json::to_string(&finished.lossy)?
    );
    if let Some(verified) = verified {
        eprintln!(
            "verified: {} worlds, {} messages, {} exchanges, {} labels, {} predictions",
            verified.worlds,
            verified.messages,
            verified.exchanges,
            verified.labels,
            verified.predictions
        );
    }
    Ok(())
}

pub fn export(args: ExportArgs) -> Result<ExitCode> {
    let ExportFormat::A2aBench1 = args.format;
    let mut source = open_source(&args.source)?;
    let spec = manifest_spec(&args.source, &source)?;
    let writer = ExportWriter::create(&args.out, &spec.dataset)?;
    let mut golden = GoldenRun::new(writer, None);
    let mut skipped = 0usize;
    for world in source.worlds() {
        match world {
            Ok(world) => golden
                .world(&world)
                .with_context(|| format!("exporting world {}", world.key()))?,
            Err(error) => {
                skipped += 1;
                tracing::warn!(error = %error, "world skipped");
            }
        }
    }
    let finished = golden.finish(&spec, None)?;
    write_manifest(&args.out, &finished.manifest)?;
    let verified = verify(&args.out, None).context("checking the export")?;
    report(&finished, Some(verified))?;
    if skipped > 0 {
        eprintln!("{skipped} worlds failed to load and are not exported");
    }
    Ok(ExitCode::SUCCESS)
}

pub fn verify_command(args: VerifyArgs) -> Result<ExitCode> {
    let verified = verify(&args.export, args.predictions.as_deref())?;
    println!(
        "verified: {} worlds, {} messages, {} exchanges, {} labels, {} predictions",
        verified.worlds,
        verified.messages,
        verified.exchanges,
        verified.labels,
        verified.predictions
    );
    Ok(ExitCode::SUCCESS)
}
