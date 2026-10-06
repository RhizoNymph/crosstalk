//! How the spool fails.

use std::fmt;
use std::io;
use std::path::PathBuf;

/// The filesystem operation a spool failure happened in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpoolOp {
    CreateDir,
    Lock,
    List,
    Read,
    Create,
    Write,
    Sync,
    Truncate,
    Rename,
    Remove,
    SyncDir,
}

impl fmt::Display for SpoolOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::CreateDir => "create directory",
            Self::Lock => "lock",
            Self::List => "list",
            Self::Read => "read",
            Self::Create => "create",
            Self::Write => "write",
            Self::Sync => "sync",
            Self::Truncate => "truncate",
            Self::Rename => "rename",
            Self::Remove => "remove",
            Self::SyncDir => "sync directory",
        })
    }
}

/// Why the spool could not open, append, drain or discard. Names files and
/// offsets, never record bytes.
#[derive(Debug, thiserror::Error)]
pub enum SpoolError {
    /// Another process (or another open in this one) holds the spool
    /// directory's `LOCK`.
    #[error("spool directory {} is locked by another process", dir.display())]
    Locked { dir: PathBuf },
    /// A record that is not the torn tail of the last segment is
    /// incomplete or fails its checksum (`transport.spool.torn-tail-only`).
    /// Draining stops before it; the records stay on disk.
    #[error("spool segment {segment} is corrupt at byte {offset}")]
    Corrupt { segment: String, offset: u64 },
    /// The `cursor` file does not decode. It is only ever replaced
    /// atomically, so this is corruption too.
    #[error("spool cursor {} does not decode", path.display())]
    Cursor { path: PathBuf },
    #[error("spool {op} {}: {source}", path.display())]
    Io {
        op: SpoolOp,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    /// A blocking spool task stopped without finishing (it panicked).
    #[error("a spool task stopped without finishing")]
    Interrupted,
}

impl SpoolError {
    pub(crate) fn io(op: SpoolOp, path: impl Into<PathBuf>) -> impl FnOnce(io::Error) -> Self {
        let path = path.into();
        move |source| Self::Io { op, path, source }
    }
}
