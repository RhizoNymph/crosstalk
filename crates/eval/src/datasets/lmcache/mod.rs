//! LMCache agentic traces as a background (negative) corpus.
//!
//! Each row (`data/*.parquet`) is one request of one session: the
//! cumulative OpenAI-chat `input`, the `model`, and `pre_gap`, the seconds
//! since the session's previous request. A session's rows are contiguous
//! and in order. The response to a request is not recorded; it is the
//! assistant message the session's next request appends, so the last
//! request of every session has no response and is dropped.
//!
//! - **Calls.** Request `r` sends `input_r` and receives the first assistant
//!   message of `input_{r+1}` past `input_r`'s length. `Reconstructed` when
//!   `input_{r+1}` extends `input_r` unchanged, `Synthetic` (with a debug
//!   line) when the history was rewritten in between.
//! - **Clock.** A session starts at the corpus epoch; request `r` is at the
//!   sum of the `pre_gap`s up to it, in microseconds, kept strictly
//!   increasing.
//! - **Worlds.** Sessions are taken in turn from every row group of every
//!   selected file ([`Sessions`]), `agents_per_world` to a world, so a world
//!   spans repositories and models although each file is sorted by session. They never talked, so every world is a
//!   [background world](crate::datasets::background). Two sessions share a
//!   group (`SharedSource`) when they solve the same SWE-bench repository's
//!   tasks or the same task otherwise.

pub mod schema;

use std::path::{Path, PathBuf};

use crosstalk_spec::support::Timestamp;

pub use schema::LmcacheRow;

use crate::corpus::clock::EPOCH_MICROS;
use crate::corpus::{Fidelity, SourceError, TraceSource, World};
use crate::datasets::background::{BackgroundError, BackgroundWorld, Call, Trajectory, stop_for};
use crate::datasets::chat::{ChatError, convert};
use crate::datasets::open_swe::Mixing;
use crate::datasets::parquet_rows::{ParquetError, ParquetRows, row_groups};
use crate::datasets::salt::Selection;
use crate::keys::{DatasetId, SourceRef, WorldKey};

/// The dataset's id.
pub const DATASET: &str = "lmcache";

/// The columns read.
pub const COLUMNS: &[&str] = &["session_id", "model", "input", "pre_gap"];

#[derive(Debug, thiserror::Error)]
pub enum LmcacheError {
    #[error("{root} has no data/ directory")]
    NoData { root: String },
    #[error("reading {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Parquet(#[from] ParquetError),
    #[error("{file} row {row}: {source}")]
    Chat {
        file: String,
        row: usize,
        #[source]
        source: ChatError,
    },
    #[error(transparent)]
    Background(#[from] BackgroundError),
}

/// One session's rows, with their row numbers in `file`.
#[derive(Debug, Clone)]
pub struct Session {
    pub file: String,
    pub id: String,
    pub rows: Vec<(usize, LmcacheRow)>,
}

/// The repository (SWE-bench) or task a session works on: `swebench__django__django-15695__claude`
/// is `swebench/django`, `gaia__L3_…__claude` is `gaia/L3_…`.
pub fn group(session: &str) -> String {
    let parts: Vec<&str> = session.split("__").collect();
    match parts.as_slice() {
        ["swebench", repo, ..] => format!("swebench/{repo}"),
        [kind, task, ..] => format!("{kind}/{task}"),
        _ => session.to_owned(),
    }
}

/// The session's calls: every request but the last.
pub fn calls(session: &Session) -> Result<Vec<Call>, LmcacheError> {
    let convert_row = |row: usize, input: &[crate::datasets::chat::ChatMessage]| {
        convert(input).map_err(|source| LmcacheError::Chat {
            file: session.file.clone(),
            row,
            source,
        })
    };
    let mut calls = Vec::new();
    let mut elapsed = 0.0f64;
    let mut last: Option<Timestamp> = None;
    let mut current = match session.rows.first() {
        Some((row, first)) => convert_row(*row, &first.input)?,
        None => return Ok(calls),
    };
    for pair in session.rows.windows(2) {
        let [(row, request), (next_row, next)] = pair else {
            continue;
        };
        let following = convert_row(*next_row, &next.input)?;
        let messages = std::mem::replace(&mut current, following);
        elapsed += request.pre_gap.unwrap_or(0.0).max(0.0);
        let sent = request.input.len();
        let Some(offset) = next
            .input
            .iter()
            .skip(sent)
            .position(|message| message.is_assistant())
        else {
            tracing::debug!(file = %session.file, row, "next request appends no assistant message; call dropped");
            continue;
        };
        let reply = sent + offset;
        let Some(response) = current.get(reply).cloned() else {
            continue;
        };
        let extends = next.input.get(..sent) == Some(request.input.as_slice());
        if !extends {
            tracing::debug!(file = %session.file, row, "history rewritten between requests; call is synthetic");
        }
        // Seconds to microseconds; gaps are small and non-negative.
        let micros = (elapsed * 1_000_000.0).round() as u64;
        let mut at = Timestamp::from_micros(EPOCH_MICROS + micros);
        if let Some(previous) = last
            && at <= previous
        {
            at = Timestamp::from_micros(previous.as_micros() + 1);
        }
        last = Some(at);
        calls.push(Call {
            at,
            request: messages,
            response,
            stop: stop_for(!next.input[reply].calls().is_empty()),
            fidelity: if extends {
                Fidelity::Reconstructed
            } else {
                Fidelity::Synthetic
            },
            source: SourceRef::new(session.file.clone(), format!("/rows/{row}")),
        });
    }
    Ok(calls)
}

/// One session as a trajectory.
pub fn trajectory(session: &Session) -> Result<Trajectory, LmcacheError> {
    let model = session
        .rows
        .first()
        .map_or_else(|| "unknown".to_owned(), |(_, row)| row.model.clone());
    Ok(Trajectory {
        name: session.id.clone(),
        model,
        group: group(&session.id),
        calls: calls(session)?,
    })
}

/// One world from sessions.
pub fn mix(key: WorldKey, sessions: &[Session]) -> Result<World, LmcacheError> {
    let mut world = BackgroundWorld::new(DatasetId::new(DATASET), key);
    for session in sessions {
        world.add(trajectory(session)?)?;
    }
    Ok(world.finish()?)
}

/// The data files `selection` picks, relative to `root`, in name order.
pub fn discover(root: &Path, selection: &Selection) -> Result<Vec<String>, LmcacheError> {
    let data = root.join("data");
    if !data.is_dir() {
        return Err(LmcacheError::NoData {
            root: root.display().to_string(),
        });
    }
    let io = |source| LmcacheError::Io {
        path: data.display().to_string(),
        source,
    };
    let mut files = Vec::new();
    for entry in std::fs::read_dir(&data).map_err(io)? {
        let name = entry
            .map_err(io)?
            .file_name()
            .to_string_lossy()
            .into_owned();
        let relative = format!("data/{name}");
        if name.ends_with(".parquet")
            && (selection.include.is_empty()
                || selection
                    .include
                    .iter()
                    .any(|needle| relative.contains(needle.as_str())))
        {
            files.push(relative);
        }
    }
    files.sort();
    if let Some(limit) = selection.limit {
        files.truncate(limit);
    }
    Ok(files)
}

/// One row group of one file: where reading can start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    pub file: String,
    pub group: usize,
}

/// The row groups of the selected files, interleaved: every file's first
/// group, then every file's second, and so on.
pub fn segments(root: &Path, files: &[String]) -> Result<Vec<Segment>, LmcacheError> {
    let mut per_file = Vec::with_capacity(files.len());
    for file in files {
        per_file.push(row_groups(&root.join(file))?.len());
    }
    let most = per_file.iter().copied().max().unwrap_or(0);
    let mut out = Vec::new();
    for group in 0..most {
        for (file, groups) in files.iter().zip(&per_file) {
            if group < *groups {
                out.push(Segment {
                    file: file.clone(),
                    group,
                });
            }
        }
    }
    Ok(out)
}

struct Cursor {
    segment: Segment,
    rows: Option<ParquetRows<LmcacheRow>>,
    peeked: Option<(usize, LmcacheRow)>,
    started: bool,
    done: bool,
}

/// Sessions taken from each segment in turn, at most `per_file` from a
/// file. A file is sorted by session, so starting in every row group
/// spreads a sample over repositories and models. A session cut by a row
/// group boundary is read up to the boundary; the next group skips its
/// first session, which may be that one's continuation.
pub struct Sessions {
    root: PathBuf,
    cursors: Vec<Cursor>,
    per_file: Option<usize>,
    taken: std::collections::BTreeMap<String, usize>,
    turn: usize,
}

impl Sessions {
    pub fn new(root: &Path, segments: Vec<Segment>, per_file: Option<usize>) -> Self {
        Self {
            root: root.to_path_buf(),
            cursors: segments
                .into_iter()
                .map(|segment| Cursor {
                    segment,
                    rows: None,
                    peeked: None,
                    started: false,
                    done: false,
                })
                .collect(),
            per_file,
            taken: std::collections::BTreeMap::new(),
            turn: 0,
        }
    }

    /// The next whole session of one cursor, `None` when it has no more.
    fn read(root: &Path, cursor: &mut Cursor) -> Option<Result<Session, LmcacheError>> {
        if cursor.rows.is_none() {
            let path = root.join(&cursor.segment.file);
            match ParquetRows::open_group(&path, COLUMNS, cursor.segment.group) {
                Ok(rows) => cursor.rows = Some(rows),
                Err(error) => {
                    cursor.done = true;
                    return Some(Err(error.into()));
                }
            }
        }
        loop {
            let first = match cursor.peeked.take() {
                Some(row) => row,
                None => match cursor.rows.as_mut().and_then(Iterator::next) {
                    None => {
                        cursor.done = true;
                        return None;
                    }
                    Some(Err(error)) => {
                        cursor.done = true;
                        return Some(Err(error.into()));
                    }
                    Some(Ok(row)) => row,
                },
            };
            let id = first.1.session_id.clone();
            let mut rows = vec![first];
            loop {
                match cursor.rows.as_mut().and_then(Iterator::next) {
                    Some(Ok(row)) if row.1.session_id == id => rows.push(row),
                    Some(Ok(row)) => {
                        cursor.peeked = Some(row);
                        break;
                    }
                    Some(Err(error)) => {
                        cursor.done = true;
                        return Some(Err(error.into()));
                    }
                    None => {
                        cursor.done = true;
                        break;
                    }
                }
            }
            let skip = !cursor.started && cursor.segment.group > 0;
            cursor.started = true;
            if skip {
                if cursor.done {
                    return None;
                }
                continue;
            }
            return Some(Ok(Session {
                file: cursor.segment.file.clone(),
                id,
                rows,
            }));
        }
    }
}

impl Iterator for Sessions {
    type Item = Result<Session, LmcacheError>;

    fn next(&mut self) -> Option<Self::Item> {
        let count = self.cursors.len();
        for _ in 0..count {
            let at = self.turn % count;
            self.turn = self.turn.wrapping_add(1);
            let cursor = &mut self.cursors[at];
            if cursor.done {
                continue;
            }
            let taken = self.taken.get(&cursor.segment.file).copied().unwrap_or(0);
            if self.per_file.is_some_and(|limit| taken >= limit) {
                cursor.done = true;
                cursor.rows = None;
                continue;
            }
            match Self::read(&self.root, cursor) {
                None => {
                    cursor.rows = None;
                }
                Some(Ok(session)) => {
                    *self.taken.entry(session.file.clone()).or_insert(0) += 1;
                    if cursor.done {
                        cursor.rows = None;
                    }
                    return Some(Ok(session));
                }
                Some(Err(error)) => {
                    cursor.rows = None;
                    return Some(Err(error));
                }
            }
        }
        None
    }
}

/// LMCache traces as a stream of mixed worlds.
pub struct LmcacheSource {
    root: PathBuf,
    files: Vec<String>,
    mixing: Mixing,
}

impl LmcacheSource {
    pub fn open(root: &Path, selection: &Selection, mixing: Mixing) -> Result<Self, LmcacheError> {
        Ok(Self {
            root: root.to_path_buf(),
            files: discover(root, selection)?,
            mixing,
        })
    }

    pub fn files(&self) -> &[String] {
        &self.files
    }
}

impl TraceSource for LmcacheSource {
    fn id(&self) -> DatasetId {
        DatasetId::new(DATASET)
    }

    fn worlds(&mut self) -> impl Iterator<Item = Result<World, SourceError>> + '_ {
        let size = self.mixing.agents_per_world.max(1);
        let mut pending: Vec<LmcacheError> = Vec::new();
        let segments = match segments(&self.root, &self.files) {
            Ok(segments) => segments,
            Err(error) => {
                pending.push(error);
                Vec::new()
            }
        };
        let mut sessions = Sessions::new(&self.root, segments, self.mixing.per_shard);
        let mut number = 0usize;
        std::iter::from_fn(move || {
            if let Some(error) = pending.pop() {
                return Some(Err(SourceError::from(error)));
            }
            let mut batch = Vec::with_capacity(size);
            while batch.len() < size {
                match sessions.next() {
                    None => break,
                    Some(Ok(session)) => batch.push(session),
                    Some(Err(error)) => pending.push(error),
                }
            }
            if batch.is_empty() {
                return pending.pop().map(|error| Err(error.into()));
            }
            let key = WorldKey::new(format!("mix-{number:05}"));
            number += 1;
            Some(mix(key, &batch).map_err(SourceError::from))
        })
    }
}
