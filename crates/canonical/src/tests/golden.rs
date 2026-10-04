//! Golden files: every captured corpus case's normalized exchange.
//!
//! A golden is the spec's JSON of a `NormalizedExchange`
//! (`{"exchange", "messages": [{"hash", "body"}], "warnings", "media":
//! [{"hash", "bytes"}]}`, each body in its encoding's shape), pretty-printed
//! with one trailing newline under `crates/canonical/tests/golden/`. A test
//! fails when the value no longer encodes to its file; to rewrite the files
//! after an intended change, run the tests with `CROSSTALK_BLESS=1` and
//! review the diff:
//!
//! ```sh
//! CROSSTALK_BLESS=1 cargo test -p crosstalk-canonical golden
//! git diff crates/canonical/tests/golden
//! ```
//!
//! Checking also decodes the file through the spec's serde, which checks
//! every message's hash against its body's encoding, every media blob's
//! against its bytes, and the references between them.

use std::path::PathBuf;

use crosstalk_spec::interfaces::l1_canonical::NormalizedExchange;

pub const BLESS: &str = "CROSSTALK_BLESS";

fn golden_path(area: &str, name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(area)
        .join(format!("{name}.json"))
}

fn blessing() -> bool {
    std::env::var(BLESS).is_ok_and(|value| value == "1")
}

/// `normalized` encodes to the golden `area/name` byte for byte (or, when
/// blessing, is written there), and the golden decodes back to it.
pub fn assert_normalization_golden(area: &str, name: &str, normalized: &NormalizedExchange) {
    let mut text = serde_json::to_string_pretty(normalized)
        .unwrap_or_else(|error| panic!("{name}: a normalized exchange encodes: {error}"));
    text.push('\n');
    let path = golden_path(area, name);
    if blessing() {
        let dir = path
            .parent()
            .unwrap_or_else(|| panic!("{path:?} has a parent"));
        std::fs::create_dir_all(dir).unwrap_or_else(|error| panic!("create {dir:?}: {error}"));
        std::fs::write(&path, &text).unwrap_or_else(|error| panic!("write {path:?}: {error}"));
    }
    let golden = std::fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!("golden {path:?} unreadable ({error}); run with {BLESS}=1 to write it")
    });
    assert_eq!(
        text, golden,
        "{area}/{name}: differs from its golden; if the change is intended, run with \
         {BLESS}=1 and review the diff"
    );
    let decoded: NormalizedExchange = serde_json::from_str(&golden)
        .unwrap_or_else(|error| panic!("{area}/{name}: the golden decodes: {error}"));
    assert_eq!(&decoded, normalized, "{area}/{name}: decoding the golden");
}
