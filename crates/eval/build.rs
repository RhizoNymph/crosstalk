//! Records the crosstalk commit this crate is built from as
//! `CROSSTALK_EVAL_GIT` (`<sha>`, or `<sha>-dirty` when tracked files
//! differ from it), which the golden export's manifest names as its
//! converter. Without git (a source tarball) it is `unknown`; the build
//! never fails over it. The script reruns when `HEAD`, the ref it points at,
//! the packed refs or the index change.

use std::path::{Path, PathBuf};
use std::process::Command;

/// The trimmed stdout of `git args` in this crate's directory, if git ran
/// and succeeded.
fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|text| text.trim().to_owned())
}

fn absolute(path: &str) -> PathBuf {
    let path = Path::new(path);
    if path.is_absolute() {
        path.to_owned()
    } else {
        Path::new(env!("CARGO_MANIFEST_DIR")).join(path)
    }
}

fn rerun_on_git_changes() {
    let Some(git_dir) = git(&["rev-parse", "--git-dir"]).map(|dir| absolute(&dir)) else {
        return;
    };
    let common = git(&["rev-parse", "--git-common-dir"])
        .map(|dir| absolute(&dir))
        .unwrap_or_else(|| git_dir.clone());
    let mut watched = vec![
        git_dir.join("HEAD"),
        git_dir.join("index"),
        common.join("packed-refs"),
    ];
    if let Some(reference) = git(&["symbolic-ref", "-q", "HEAD"]) {
        watched.push(common.join(&reference));
        watched.push(git_dir.join(&reference));
    }
    for path in watched.into_iter().filter(|path| path.exists()) {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    rerun_on_git_changes();
    let commit = match git(&["rev-parse", "HEAD"]) {
        Some(sha) if !sha.is_empty() => {
            let dirty = git(&["status", "--porcelain", "--untracked-files=no"])
                .is_some_and(|status| !status.is_empty());
            if dirty { format!("{sha}-dirty") } else { sha }
        }
        _ => "unknown".to_owned(),
    };
    println!("cargo:rustc-env=CROSSTALK_EVAL_GIT={commit}");
}
