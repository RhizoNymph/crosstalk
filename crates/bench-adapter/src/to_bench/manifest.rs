//! What a `from-export` manifest pins beside its worlds and files: the
//! dataset version, the crosstalk commit this adapter was built from
//! ([`CROSSTALK_COMMIT`], recorded by the build script), and the source
//! digest of the run files read ([`digest_files`]).

use std::fs;
use std::io::Read;
use std::path::PathBuf;

use a2a_bench_format as bench;
use bench::ids::Digest;
use bench::manifest::Setting;
use bench::source::SourceDigest;

use super::ToBenchError;

/// The dataset version of a `from-export` capture, which the bench's
/// demo-swarm converter labels against.
pub const DATASET_VERSION: u32 = 1;

/// The crosstalk commit this binary was built from, as `build.rs` recorded
/// it: `<sha>`, `<sha>-dirty` when tracked files differed from it, or
/// `unknown` when built without git. Fixed at build time, so every file one
/// binary writes names the same commit.
pub const CROSSTALK_COMMIT: &str = env!("CROSSTALK_ADAPTER_GIT");

/// The format's source digest (`a2a_bench_format::source::SourceDigest`)
/// of `files`: each a `/`-separated name relative to the dataset root and
/// the path to read it from. They are taken in byte order of their names.
pub fn digest_files(files: &[(String, PathBuf)]) -> Result<Digest, ToBenchError> {
    let mut sorted: Vec<&(String, PathBuf)> = files.iter().collect();
    sorted.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let mut digest = SourceDigest::new();
    let mut buffer = vec![0u8; 1 << 20];
    for (name, full) in sorted {
        let mut file = fs::File::open(full).map_err(|source| ToBenchError::io(full, source))?;
        let len = file
            .metadata()
            .map_err(|source| ToBenchError::io(full, source))?
            .len();
        let mut part = digest.file(name, len).map_err(ToBenchError::Source)?;
        loop {
            let read = file
                .read(&mut buffer)
                .map_err(|source| ToBenchError::io(full, source))?;
            if read == 0 {
                break;
            }
            part.update(&buffer[..read]);
        }
        part.end().map_err(ToBenchError::Source)?;
    }
    Ok(digest.finish())
}

/// A setting's integer value.
pub fn int(value: u64) -> Setting {
    Setting::Int(i64::try_from(value).unwrap_or(i64::MAX))
}
