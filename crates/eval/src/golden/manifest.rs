//! `manifest.json`: everything that fixes an export's bytes.
//!
//! - **Source.** The dataset's directory relative to the data root, its
//!   own revision ([`revision`]) and the format's source digest of the
//!   files read ([`digest_files`]).
//! - **Converter.** This crate's version and the crosstalk commit it was
//!   built from ([`crosstalk_commit`]).
//! - **Selection and pace.** The flags that pick the worlds and step the
//!   virtual clock, as the caller lists them.
//! - **Worlds and files.** The writer's world entries (exchanges, label
//!   rows, notes) and file digests.
//!
//! The dataset version is 1 for every ct-eval converter: the golden export
//! is the baseline the bench's own converters are diffed against.

use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use a2a_bench_format as bench;
use bench::ids::{DatasetId, Digest};
use bench::manifest::{Converter, Manifest, Setting, Source, Split};
use bench::source::SourceDigest;
use bench::version::FORMAT;

use super::GoldenError;
use super::writer::Written;

/// The dataset version of every ct-eval converter's export.
pub const DATASET_VERSION: u32 = 1;

/// Directory names never part of a dataset's bytes: version control and
/// download caches.
pub const SKIPPED_DIRS: [&str; 2] = [".git", ".cache"];

/// What a manifest says beside the written files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestSpec {
    pub dataset: DatasetId,
    pub source: Source,
    pub converter: Converter,
    pub selection: BTreeMap<String, Setting>,
    pub pace: BTreeMap<String, Setting>,
}

impl ManifestSpec {
    pub fn manifest(&self, written: &Written) -> Manifest {
        Manifest {
            format: FORMAT,
            dataset: self.dataset.clone(),
            dataset_version: DATASET_VERSION,
            split: Split::Dev,
            source: self.source.clone(),
            converter: self.converter.clone(),
            selection: self.selection.clone(),
            pace: self.pace.clone(),
            worlds: written.worlds.clone(),
            files: written.files.clone(),
        }
    }
}

/// This converter: the crate's version and the crosstalk commit.
pub fn converter() -> Converter {
    Converter {
        version: env!("CARGO_PKG_VERSION").to_owned(),
        git: crosstalk_commit().unwrap_or_else(|| UNKNOWN.to_owned()),
    }
}

/// What a revision or commit that cannot be found is recorded as.
pub const UNKNOWN: &str = "unknown";

/// The commit of the crosstalk checkout this crate was built from (its
/// `.git`, read without running git), if the checkout is still there.
pub fn crosstalk_commit() -> Option<String> {
    git_head(Path::new(env!("CARGO_MANIFEST_DIR")), None)
}

/// A dataset's own revision:
///
/// 1. a Hugging Face snapshot (the root resolves to `…/snapshots/<hash>`):
///    the snapshot hash;
/// 2. a Hugging Face local-dir download (`.cache/huggingface/download/*.metadata`
///    under the root): the commit those files name, if they all name one;
/// 3. a git checkout at or above the root, but below `stop` (the data
///    root): its `HEAD` commit;
/// 4. otherwise [`UNKNOWN`]: the source digest still pins the bytes.
pub fn revision(root: &Path, stop: Option<&Path>) -> String {
    let resolved = fs::canonicalize(root).unwrap_or_else(|_| root.to_owned());
    if let Some(hash) = snapshot(&resolved) {
        return hash;
    }
    if let Some(commit) = local_dir_commit(root) {
        return commit;
    }
    git_head(root, stop).unwrap_or_else(|| UNKNOWN.to_owned())
}

fn snapshot(path: &Path) -> Option<String> {
    let mut components = path.components().rev();
    let hash = components.next()?.as_os_str().to_str()?.to_owned();
    let parent = components.next()?.as_os_str().to_str()?;
    (parent == "snapshots" && hash.len() == 40 && hash.bytes().all(|b| b.is_ascii_hexdigit()))
        .then_some(hash)
}

fn local_dir_commit(root: &Path) -> Option<String> {
    let dir = root.join(".cache").join("huggingface").join("download");
    let mut commits = std::collections::BTreeSet::new();
    let mut stack = vec![dir];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "metadata")
                && let Ok(text) = fs::read_to_string(&path)
                && let Some(first) = text.lines().next()
            {
                commits.insert(first.trim().to_owned());
            }
        }
    }
    let mut commits = commits.into_iter();
    match (commits.next(), commits.next()) {
        (Some(one), None) if !one.is_empty() => Some(one),
        _ => None,
    }
}

/// The `HEAD` commit of the git checkout at `start` or the nearest one above
/// it, stopping before `stop` (never looking at or above it).
fn git_head(start: &Path, stop: Option<&Path>) -> Option<String> {
    let start = fs::canonicalize(start).ok()?;
    let stop = stop.and_then(|stop| fs::canonicalize(stop).ok());
    let mut at = Some(start.as_path());
    while let Some(dir) = at {
        if stop
            .as_deref()
            .is_some_and(|stop| dir == stop || stop.starts_with(dir))
        {
            return None;
        }
        let dot_git = dir.join(".git");
        if dot_git.exists() {
            return read_head(&git_dir(&dot_git)?);
        }
        at = dir.parent();
    }
    None
}

/// `.git` itself, or the directory a worktree's `.git` file points at.
fn git_dir(dot_git: &Path) -> Option<PathBuf> {
    if dot_git.is_dir() {
        return Some(dot_git.to_owned());
    }
    let text = fs::read_to_string(dot_git).ok()?;
    let pointed = text.strip_prefix("gitdir:")?.trim();
    let path = Path::new(pointed);
    Some(if path.is_absolute() {
        path.to_owned()
    } else {
        dot_git.parent()?.join(path)
    })
}

fn read_head(git: &Path) -> Option<String> {
    let head = fs::read_to_string(git.join("HEAD")).ok()?;
    let head = head.trim();
    let Some(reference) = head.strip_prefix("ref:") else {
        return Some(head.to_owned());
    };
    let reference = reference.trim();
    // A worktree's refs live in the common directory.
    let common = fs::read_to_string(git.join("commondir"))
        .ok()
        .map(|dir| git.join(dir.trim()))
        .unwrap_or_else(|| git.to_owned());
    for base in [git, common.as_path()] {
        if let Ok(commit) = fs::read_to_string(base.join(reference)) {
            return Some(commit.trim().to_owned());
        }
    }
    let packed = fs::read_to_string(common.join("packed-refs")).ok()?;
    packed.lines().find_map(|line| {
        let (commit, name) = line.split_once(' ')?;
        (name == reference).then(|| commit.to_owned())
    })
}

/// Every regular file under `root` (following symbolic links, skipping
/// [`SKIPPED_DIRS`]), by its `/`-separated path relative to `root`, in
/// byte order: the order a source digest takes them in.
pub fn files_under(root: &Path) -> Result<Vec<String>, GoldenError> {
    let mut out = Vec::new();
    let mut stack = vec![PathBuf::new()];
    while let Some(relative) = stack.pop() {
        let dir = root.join(&relative);
        let entries = fs::read_dir(&dir).map_err(|source| GoldenError::io(&dir, source))?;
        for entry in entries {
            let entry = entry.map_err(|source| GoldenError::io(&dir, source))?;
            let name = entry.file_name();
            let path = relative.join(&name);
            let full = root.join(&path);
            let meta = fs::metadata(&full).map_err(|source| GoldenError::io(&full, source))?;
            if meta.is_dir() {
                if !SKIPPED_DIRS.iter().any(|skip| name == *skip) {
                    stack.push(path);
                }
            } else if meta.is_file() {
                out.push(slashed(&path));
            }
        }
    }
    out.sort();
    Ok(out)
}

/// `path` with `/` between its components.
pub fn slashed(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

/// The format's source digest (`a2a_bench_format::source::SourceDigest`)
/// of `files`: each a `/`-separated name relative to the dataset root and
/// the path to read it from. They are taken in byte order of their names.
pub fn digest_files(files: &[(String, PathBuf)]) -> Result<Digest, GoldenError> {
    let mut sorted: Vec<&(String, PathBuf)> = files.iter().collect();
    sorted.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let mut digest = SourceDigest::new();
    let mut buffer = vec![0u8; 1 << 20];
    for (name, full) in sorted {
        let mut file = fs::File::open(full).map_err(|source| GoldenError::io(full, source))?;
        let len = file
            .metadata()
            .map_err(|source| GoldenError::io(full, source))?
            .len();
        let mut part = digest.file(name, len).map_err(GoldenError::Source)?;
        loop {
            let read = file
                .read(&mut buffer)
                .map_err(|source| GoldenError::io(full, source))?;
            if read == 0 {
                break;
            }
            part.update(&buffer[..read]);
        }
        part.end().map_err(GoldenError::Source)?;
    }
    Ok(digest.finish())
}

/// [`digest_files`] over `files` (`/`-separated, relative to `root`).
pub fn digest_tree(root: &Path, files: &[String]) -> Result<Digest, GoldenError> {
    let named: Vec<(String, PathBuf)> = files
        .iter()
        .map(|relative| (relative.clone(), root.join(relative)))
        .collect();
    digest_files(&named)
}

/// A setting's value from text, an integer or a flag.
pub fn text(value: impl Into<String>) -> Setting {
    Setting::Text(value.into())
}

pub fn int(value: u64) -> Setting {
    Setting::Int(i64::try_from(value).unwrap_or(i64::MAX))
}

/// Puts a repeatable flag's values in `settings` as one list, when there
/// are any.
pub fn list(settings: &mut BTreeMap<String, Setting>, name: &str, values: &[String]) {
    if !values.is_empty() {
        settings.insert(
            name.to_owned(),
            Setting::List(values.iter().cloned().map(Setting::Text).collect()),
        );
    }
}
