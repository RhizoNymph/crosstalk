//! The blocking half of [`super::FsBlobStore`]: every filesystem call the
//! store makes, in plain `std::fs`. Nothing here is async; the store runs
//! each operation as one `spawn_blocking` task, which is the only boundary
//! between the two.
//!
//! Layout: the body under hash `h` is the file `<root>/<h[..2]>/<h[2..]>`
//! (`h` as 64 lower-case hex digits). A write goes to a fresh temporary file
//! in the same shard directory (`.<h[2..]>.<pid>-<n>.tmp`, which no hash
//! names), is synced, renamed over the final name and the shard directory
//! synced, so a reader sees either no file or the whole body, and a put that
//! returned survives a crash.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};

use crosstalk_spec::ids::MessageHash;

use super::super::digest::{matches, message_hash};

/// How many temporary names a put tries before giving up. Names are unique
/// per process (pid and a process-wide counter), so a collision needs a
/// leftover file from an earlier process with the same pid.
const TEMP_ATTEMPTS: u32 = 16;

/// The process-wide counter that makes temporary names unique among the
/// puts of this process, across every store instance.
static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

/// The resolved store directory: absolute, existing, a directory.
#[derive(Debug)]
pub(super) struct Root(PathBuf);

impl Root {
    pub(super) fn path(&self) -> &Path {
        &self.0
    }
}

/// The filesystem operation a fault happened in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Op {
    Read,
    CreateShard,
    CreateTemp,
    WriteTemp,
    SyncTemp,
    Rename,
    SyncDir,
}

impl fmt::Display for Op {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Read => "read",
            Self::CreateShard => "create shard directory",
            Self::CreateTemp => "create temporary file",
            Self::WriteTemp => "write temporary file",
            Self::SyncTemp => "sync temporary file",
            Self::Rename => "rename into place",
            Self::SyncDir => "sync directory",
        })
    }
}

/// Why a blocking operation failed. Names files and hashes, never bytes.
#[derive(Debug, thiserror::Error)]
pub(super) enum Fault {
    #[error("{op} {}: {source}", path.display())]
    Io {
        op: Op,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("blob {} does not match its hash", .0.digest().to_hex())]
    Corrupt(MessageHash),
    #[error("no free temporary file name in {} after {TEMP_ATTEMPTS} attempts", shard.display())]
    TempNamesExhausted { shard: PathBuf },
}

impl Fault {
    fn io(op: Op, path: &Path) -> impl FnOnce(io::Error) -> Self + '_ {
        move |source| Self::Io {
            op,
            path: path.to_owned(),
            source,
        }
    }
}

/// Why the store directory could not be opened.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("cannot create blob root {}: {source}", path.display())]
    Create {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("cannot resolve blob root {}: {source}", path.display())]
    Resolve {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("blob root {} is not a directory", path.display())]
    NotADirectory { path: PathBuf },
    #[error("the runtime shut down before the blob root was opened")]
    Cancelled,
}

/// What a successful put did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PutOutcome {
    /// The same bytes were already stored; nothing was written.
    AlreadyStored,
    /// The body was written.
    Written,
    /// A file under the hash held other bytes; it was replaced.
    Repaired,
}

impl PutOutcome {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::AlreadyStored => "already_stored",
            Self::Written => "written",
            Self::Repaired => "repaired",
        }
    }
}

/// Where one blob lives.
struct Location {
    shard: PathBuf,
    file: PathBuf,
    /// The hex after the shard prefix: the file's name.
    rest: String,
}

impl Location {
    fn of(root: &Root, hash: MessageHash) -> Self {
        let hex = hash.digest().to_hex();
        // `to_hex` is always 64 ASCII digits, so index 2 is a char boundary.
        let (prefix, rest) = hex.split_at(2);
        let shard = root.path().join(prefix);
        let file = shard.join(rest);
        Self {
            shard,
            file,
            rest: rest.to_owned(),
        }
    }
}

/// Create `path` if missing and resolve it to the absolute directory the
/// store uses.
pub(super) fn open(path: &Path) -> Result<Root, OpenError> {
    fs::create_dir_all(path).map_err(|source| OpenError::Create {
        path: path.to_owned(),
        source,
    })?;
    let resolved = fs::canonicalize(path).map_err(|source| OpenError::Resolve {
        path: path.to_owned(),
        source,
    })?;
    if !resolved.is_dir() {
        return Err(OpenError::NotADirectory { path: resolved });
    }
    Ok(Root(resolved))
}

/// Store `bytes` under their hash. Idempotent: identical bytes already in
/// place are left untouched; other bytes under the hash are replaced.
pub(super) fn put(root: &Root, bytes: &[u8]) -> Result<(MessageHash, PutOutcome), Fault> {
    let hash = message_hash(bytes);
    let location = Location::of(root, hash);
    let outcome = match read(&location.file)? {
        Some(existing) if existing == bytes => {
            // Another put may have renamed this file in without having
            // synced the directory yet; sync it so this put's Ok is durable.
            sync_dir(&location.shard)?;
            return Ok((hash, PutOutcome::AlreadyStored));
        }
        Some(_) => PutOutcome::Repaired,
        None => PutOutcome::Written,
    };
    ensure_shard(root, &location.shard)?;
    let temp = TempFile::create(&location)?;
    temp.fill(bytes)?;
    temp.rename_to(&location.file)?;
    sync_dir(&location.shard)?;
    Ok((hash, outcome))
}

/// The body stored under `hash`: `None` when there is none, and
/// [`Fault::Corrupt`] when the stored bytes do not hash to `hash`.
pub(super) fn get(root: &Root, hash: MessageHash) -> Result<Option<Vec<u8>>, Fault> {
    let location = Location::of(root, hash);
    match read(&location.file)? {
        Some(bytes) if !matches(hash, &bytes) => Err(Fault::Corrupt(hash)),
        found => Ok(found),
    }
}

/// The file's bytes, or `None` when it (or its shard directory) does not
/// exist.
fn read(file: &Path) -> Result<Option<Vec<u8>>, Fault> {
    match fs::read(file) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(Fault::io(Op::Read, file)(error)),
    }
}

/// Create the shard directory if missing, syncing the root when it is new
/// so the directory entry is durable.
fn ensure_shard(root: &Root, shard: &Path) -> Result<(), Fault> {
    match fs::create_dir(shard) {
        Ok(()) => sync_dir(root.path()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(Fault::io(Op::CreateShard, shard)(error)),
    }
}

fn sync_dir(dir: &Path) -> Result<(), Fault> {
    File::open(dir)
        .and_then(|handle| handle.sync_all())
        .map_err(Fault::io(Op::SyncDir, dir))
}

/// A temporary file being filled. Removed on drop unless it was renamed
/// into place.
struct TempFile {
    path: PathBuf,
    file: Option<File>,
    renamed: bool,
}

impl TempFile {
    fn create(location: &Location) -> Result<Self, Fault> {
        for _ in 0..TEMP_ATTEMPTS {
            let n = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let path = location
                .shard
                .join(format!(".{}.{}-{n}.tmp", location.rest, process::id()));
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(file) => {
                    return Ok(Self {
                        path,
                        file: Some(file),
                        renamed: false,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(Fault::io(Op::CreateTemp, &path)(error)),
            }
        }
        Err(Fault::TempNamesExhausted {
            shard: location.shard.clone(),
        })
    }

    /// Write every byte and sync the file, then close it.
    fn fill(&self, bytes: &[u8]) -> Result<(), Fault> {
        let Some(mut file) = self.file.as_ref() else {
            // `file` is only taken by `rename_to`, which consumes `self`.
            return Ok(());
        };
        file.write_all(bytes)
            .map_err(Fault::io(Op::WriteTemp, &self.path))?;
        file.sync_all().map_err(Fault::io(Op::SyncTemp, &self.path))
    }

    fn rename_to(mut self, target: &Path) -> Result<(), Fault> {
        // Close before renaming; the data is already synced.
        drop(self.file.take());
        fs::rename(&self.path, target).map_err(Fault::io(Op::Rename, target))?;
        self.renamed = true;
        Ok(())
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if self.renamed {
            return;
        }
        drop(self.file.take());
        if let Err(error) = fs::remove_file(&self.path) {
            tracing::debug!(
                path = %self.path.display(),
                error = %error,
                "could not remove temporary blob file",
            );
        }
    }
}
