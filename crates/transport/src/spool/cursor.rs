//! The spool's `cursor` file: the last record the inner bus holds.
//!
//! One small JSON object, `{"last": 41, "segment": 1, "offset": 9230}`:
//! the number of the last drained record, the first record number of the
//! segment it is in, and the byte offset just past it. It is replaced
//! atomically: write `cursor.tmp`, `fdatasync` it, rename it over
//! `cursor`, `fsync` the directory. A crash at any step leaves the old
//! cursor or the new one, never a mix; a leftover `cursor.tmp` is ignored
//! and removed at open.

use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::error::{SpoolError, SpoolOp};

pub(crate) const CURSOR: &str = "cursor";
pub(crate) const CURSOR_TMP: &str = "cursor.tmp";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CursorFile {
    /// The last drained record's number; 0 before any.
    pub(crate) last: u64,
    /// The first record number of the segment `last` is in.
    pub(crate) segment: u64,
    /// The byte offset in that segment just past `last`.
    pub(crate) offset: u64,
}

/// The steps of a cursor replacement, for crash tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Step {
    /// `cursor.tmp` created and written, not synced.
    Written,
    /// `cursor.tmp` synced.
    Synced,
    /// Renamed over `cursor`.
    Renamed,
    /// The directory synced: done.
    Done,
}

/// `fsync` a directory, so a create, rename or remove in it is durable.
pub(crate) fn sync_dir(dir: &Path) -> Result<(), SpoolError> {
    File::open(dir)
        .and_then(|handle| handle.sync_all())
        .map_err(SpoolError::io(SpoolOp::SyncDir, dir))
}

/// The stored cursor; the default (nothing drained) when there is none.
pub(crate) fn read(dir: &Path) -> Result<CursorFile, SpoolError> {
    let path = dir.join(CURSOR);
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|_| SpoolError::Cursor { path }),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(CursorFile::default()),
        Err(error) => Err(SpoolError::io(SpoolOp::Read, path)(error)),
    }
}

/// Replace the cursor atomically.
pub(crate) fn write(dir: &Path, cursor: &CursorFile) -> Result<(), SpoolError> {
    write_until(dir, cursor, Step::Done)
}

/// Replace the cursor, stopping after `stop` (a crash there, in tests).
pub(crate) fn write_until(dir: &Path, cursor: &CursorFile, stop: Step) -> Result<(), SpoolError> {
    let tmp = dir.join(CURSOR_TMP);
    // Serializing three integers cannot fail.
    let bytes = serde_json::to_vec(cursor).unwrap_or_default();
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)
        .map_err(SpoolError::io(SpoolOp::Create, &tmp))?;
    file.write_all(&bytes)
        .map_err(SpoolError::io(SpoolOp::Write, &tmp))?;
    if stop == Step::Written {
        return Ok(());
    }
    file.sync_data()
        .map_err(SpoolError::io(SpoolOp::Sync, &tmp))?;
    drop(file);
    if stop == Step::Synced {
        return Ok(());
    }
    let path = dir.join(CURSOR);
    fs::rename(&tmp, &path).map_err(SpoolError::io(SpoolOp::Rename, &path))?;
    if stop == Step::Renamed {
        return Ok(());
    }
    sync_dir(dir)
}

/// Remove a `cursor.tmp` a crash left behind.
pub(crate) fn remove_leftover(dir: &Path) -> Result<(), SpoolError> {
    let tmp = dir.join(CURSOR_TMP);
    match fs::remove_file(&tmp) {
        Ok(()) => sync_dir(dir),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(SpoolError::io(SpoolOp::Remove, tmp)(error)),
    }
}
