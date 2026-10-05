//! Finding run files: `runs/<pipeline>/<suite>/<task>/<attack>/<file>.json`
//! under the dataset root, filtered and in a stratified order.
//!
//! Runs are grouped by (pipeline, suite, attack), groups in sorted order,
//! runs in each group sorted by (task, file); the groups are then
//! interleaved round-robin, so `--limit N` samples every group it can.
//!
//! `include` entries select runs:
//!
//! - `pipeline=<name>`, `suite=<name>`, `attack=<name>` and `task=<name>`
//!   match that path component exactly;
//! - anything else is a substring of the relative path.
//!
//! Entries with the same key are alternatives; different keys (and
//! substrings) must all hold.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use super::AgentDojoError;

/// Which runs to read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Selection {
    /// At most this many runs.
    pub limit: Option<usize>,
    /// Filters (see the module docs).
    pub include: Vec<String>,
}

/// One run file and the path components it was found under.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RunFile {
    /// Relative to the dataset root (`runs/…/x.json`).
    pub relative: PathBuf,
    pub pipeline: String,
    pub suite: String,
    pub task: String,
    pub attack: String,
}

/// Component filters and substrings, parsed from `include`.
#[derive(Debug, Default)]
struct Filters {
    pipelines: Vec<String>,
    suites: Vec<String>,
    tasks: Vec<String>,
    attacks: Vec<String>,
    substrings: Vec<String>,
}

impl Filters {
    fn parse(include: &[String]) -> Self {
        let mut filters = Self::default();
        for entry in include {
            match entry.split_once('=') {
                Some(("pipeline", name)) => filters.pipelines.push(name.to_owned()),
                Some(("suite", name)) => filters.suites.push(name.to_owned()),
                Some(("task", name)) => filters.tasks.push(name.to_owned()),
                Some(("attack", name)) => filters.attacks.push(name.to_owned()),
                _ => filters.substrings.push(entry.clone()),
            }
        }
        filters
    }

    fn admits(names: &[String], name: &str) -> bool {
        names.is_empty() || names.iter().any(|candidate| candidate == name)
    }
}

/// The run files under `root` that `selection` picks, stratified.
pub fn discover(root: &Path, selection: &Selection) -> Result<Vec<RunFile>, AgentDojoError> {
    let runs = root.join("runs");
    if !runs.is_dir() {
        return Err(AgentDojoError::NoRuns {
            root: root.display().to_string(),
        });
    }
    let filters = Filters::parse(&selection.include);
    let mut groups: BTreeMap<(String, String, String), Vec<RunFile>> = BTreeMap::new();
    for (pipeline, pipeline_dir) in children(&runs, &filters.pipelines)? {
        for (suite, suite_dir) in children(&pipeline_dir, &filters.suites)? {
            for (task, task_dir) in children(&suite_dir, &filters.tasks)? {
                for (attack, attack_dir) in children(&task_dir, &filters.attacks)? {
                    for path in entries(&attack_dir)? {
                        let is_json = path.extension().is_some_and(|ext| ext == "json");
                        if !is_json || !path.is_file() {
                            continue;
                        }
                        let Ok(relative) = path.strip_prefix(root) else {
                            continue;
                        };
                        let shown = relative.to_string_lossy();
                        if !filters
                            .substrings
                            .iter()
                            .all(|needle| shown.contains(needle.as_str()))
                        {
                            continue;
                        }
                        groups
                            .entry((pipeline.clone(), suite.clone(), attack.clone()))
                            .or_default()
                            .push(RunFile {
                                relative: relative.to_path_buf(),
                                pipeline: pipeline.clone(),
                                suite: suite.clone(),
                                task: task.clone(),
                                attack: attack.clone(),
                            });
                    }
                }
            }
        }
    }
    let mut groups: Vec<Vec<RunFile>> = groups.into_values().collect();
    for group in &mut groups {
        group.sort_by(|a, b| (&a.task, &a.relative).cmp(&(&b.task, &b.relative)));
    }
    let longest = groups.iter().map(Vec::len).max().unwrap_or(0);
    let mut ordered = Vec::new();
    for round in 0..longest {
        for group in &groups {
            if let Some(file) = group.get(round) {
                ordered.push(file.clone());
            }
        }
    }
    if let Some(limit) = selection.limit {
        ordered.truncate(limit);
    }
    Ok(ordered)
}

/// The world key of a run: its path under `runs/` without `.json`.
pub fn world_name(relative: &Path) -> String {
    let text = relative.to_string_lossy().replace('\\', "/");
    let text = text.strip_prefix("runs/").unwrap_or(&text);
    text.trim_end_matches(".json").to_owned()
}

/// Sub-directories of `dir` whose names `names` admits, sorted by name.
fn children(dir: &Path, names: &[String]) -> Result<Vec<(String, PathBuf)>, AgentDojoError> {
    let mut out: Vec<(String, PathBuf)> = entries(dir)?
        .into_iter()
        .filter(|path| path.is_dir())
        .filter_map(|path| {
            let name = path.file_name()?.to_str()?.to_owned();
            Filters::admits(names, &name).then_some((name, path))
        })
        .collect();
    out.sort();
    Ok(out)
}

fn entries(dir: &Path) -> Result<Vec<PathBuf>, AgentDojoError> {
    let io = |source| AgentDojoError::Io {
        path: dir.display().to_string(),
        source,
    };
    fs::read_dir(dir)
        .map_err(io)?
        .map(|entry| entry.map(|entry| entry.path()).map_err(io))
        .collect()
}
