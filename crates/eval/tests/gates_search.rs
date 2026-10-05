//! Where `ct-eval` finds its regression gates: `--gates`, then
//! `CT_EVAL_GATES`, then the bench image's installed file, then the crate's
//! own file, then none. Only an explicit `--gates` that is missing fails.

use std::path::{Path, PathBuf};

use crosstalk_eval::report::gates::{
    GateError, GateSearch, GatesFrom, GatesLocation, INSTALLED_GATES,
};

const GATES: &str = "[[gate]]\nname = \"g\"\nmetric = \"recall\"\nmin = 0.5\n";

struct Dirs {
    root: PathBuf,
}

impl Dirs {
    fn new(name: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "ct-eval-gates-search-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap_or_else(|e| panic!("{e}"));
        Self { root }
    }

    /// A gates file at `name` under the root.
    fn file(&self, name: &str) -> PathBuf {
        let path = self.root.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap_or_else(|e| panic!("{e}"));
        }
        std::fs::write(&path, GATES).unwrap_or_else(|e| panic!("{e}"));
        path
    }

    /// A path under the root nothing is written to.
    fn missing(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }
}

impl Drop for Dirs {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn search(
    flag: Option<&Path>,
    env: Option<&Path>,
    installed: &Path,
    crate_file: &Path,
) -> GateSearch {
    GateSearch {
        flag: flag.map(Path::to_path_buf),
        env: env.map(Path::to_path_buf),
        installed: installed.to_path_buf(),
        crate_file: crate_file.to_path_buf(),
    }
}

fn located(search: &GateSearch) -> Option<GatesLocation> {
    search.locate().unwrap_or_else(|e| panic!("{e}"))
}

#[test]
fn the_flag_wins_over_everything() {
    let dirs = Dirs::new("flag");
    let flag = dirs.file("flag.toml");
    let found = located(&search(
        Some(&flag),
        Some(&dirs.file("env.toml")),
        &dirs.file("installed.toml"),
        &dirs.file("crate.toml"),
    ));
    assert_eq!(
        found,
        Some(GatesLocation {
            from: GatesFrom::Flag,
            path: flag,
        })
    );
}

#[test]
fn a_missing_flag_path_is_an_error() {
    let dirs = Dirs::new("flag-missing");
    let flag = dirs.missing("nope.toml");
    let error = search(
        Some(&flag),
        None,
        &dirs.file("installed.toml"),
        &dirs.file("crate.toml"),
    )
    .locate()
    .err();
    assert!(
        matches!(&error, Some(GateError::Missing { path }) if path == &flag.display().to_string()),
        "{error:?}"
    );
}

#[test]
fn the_env_var_comes_next() {
    let dirs = Dirs::new("env");
    let env = dirs.file("env.toml");
    let found = located(&search(
        None,
        Some(&env),
        &dirs.file("installed.toml"),
        &dirs.file("crate.toml"),
    ));
    assert_eq!(
        found,
        Some(GatesLocation {
            from: GatesFrom::Env,
            path: env,
        })
    );
}

#[test]
fn the_installed_file_then_the_crate_file() {
    let dirs = Dirs::new("defaults");
    let installed = dirs.file("installed.toml");
    let crate_file = dirs.file("crate.toml");
    assert_eq!(
        located(&search(None, None, &installed, &crate_file)),
        Some(GatesLocation {
            from: GatesFrom::Installed,
            path: installed,
        })
    );
    let found = located(&search(
        None,
        Some(&dirs.missing("env-missing.toml")),
        &dirs.missing("installed-missing.toml"),
        &crate_file,
    ));
    assert_eq!(
        found,
        Some(GatesLocation {
            from: GatesFrom::Crate,
            path: crate_file,
        }),
        "a missing default falls through, never fails"
    );
}

#[test]
fn no_default_found_means_no_gates() {
    let dirs = Dirs::new("none");
    let search = search(
        None,
        Some(&dirs.missing("env.toml")),
        &dirs.missing("installed.toml"),
        &dirs.missing("crate.toml"),
    );
    assert_eq!(located(&search), None);
    let (gates, location) = search.load().unwrap_or_else(|e| panic!("{e}"));
    assert!(gates.gates.is_empty());
    assert_eq!(location, None);
}

#[test]
fn load_reads_the_located_file() {
    let dirs = Dirs::new("load");
    let installed = dirs.file("installed.toml");
    let (gates, location) = search(None, None, &installed, &dirs.missing("crate.toml"))
        .load()
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(gates.gates.len(), 1);
    assert_eq!(location.map(|l| l.from), Some(GatesFrom::Installed));
}

#[test]
fn the_installed_path_is_the_bench_images() {
    assert_eq!(
        INSTALLED_GATES,
        "/usr/local/share/crosstalk-eval/gates.toml"
    );
}

#[test]
fn the_environment_supplies_the_env_var_and_skips_an_empty_one() {
    let crate_file = Path::new("/nonexistent/crate/gates.toml");
    let set = GateSearch::new(
        None,
        Some("/some/gates.toml".into()),
        crate_file.to_path_buf(),
    );
    assert_eq!(set.env, Some(PathBuf::from("/some/gates.toml")));
    assert_eq!(set.installed, PathBuf::from(INSTALLED_GATES));
    let empty = GateSearch::new(None, Some(String::new().into()), crate_file.to_path_buf());
    assert_eq!(empty.env, None);
}
