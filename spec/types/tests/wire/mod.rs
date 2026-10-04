//! The wire contract ([`crate::wire`]): the encodings of ids, digests and
//! timestamps, the tagging and strictness conventions, the request and
//! authority split, and one golden file per wire shape
//! (`spec/types/tests/golden/<area>/<name>.json`, see [`harness`]).
//!
//! One module per area. A new area adds its module here and its goldens
//! under its own directory.

pub mod harness;

mod agents;
mod alerts;
mod analysis;
mod bus;
mod errors;
mod flow;
mod ids;
mod observed;
mod paging;
mod provenance;
mod requests;
mod support;
mod time;
mod topology;

use std::path::Path;

use crate::ids::InvalidUlidText;
use crate::support::Timestamp;

/// An id from its ULID text, for readable fixtures.
pub fn id<T>(parse: fn(&str) -> Result<T, InvalidUlidText>, text: &str) -> T {
    parse(text).unwrap_or_else(|error| panic!("{text} is not ULID text: {error:?}"))
}

/// A timestamp from its RFC 3339 text, for readable fixtures.
pub fn ts(text: &str) -> Timestamp {
    Timestamp::parse_rfc3339(text).unwrap_or_else(|error| panic!("{text}: {error:?}"))
}

/// Ids for fixtures: three realistic ULIDs.
pub const ULID_A: &str = "01J9Z3K8M4Q7R2T5V6W8X9Y0ZA";
pub const ULID_B: &str = "01J9Z3M2C5D6E7F8G9H0J1K2M3";
pub const ULID_C: &str = "01J9Z3N4P5Q6R7S8T9V0W1X2Y3";

/// Every golden file is JSON in the harness's layout: two-space
/// indentation, no tabs, carriage returns or trailing spaces, and exactly
/// one trailing newline. (Key order is the encoder's field order, which
/// only the golden's own test can check: `serde_json::Value` sorts keys.)
#[test]
fn every_golden_is_pretty_json_with_one_trailing_newline() {
    fn visit(dir: &Path, found: &mut usize) {
        let entries =
            std::fs::read_dir(dir).unwrap_or_else(|error| panic!("read {dir:?}: {error}"));
        for entry in entries {
            let path = entry
                .unwrap_or_else(|error| panic!("entry of {dir:?}: {error}"))
                .path();
            if path.is_dir() {
                visit(&path, found);
                continue;
            }
            assert_eq!(
                path.extension().and_then(|ext| ext.to_str()),
                Some("json"),
                "{path:?}: only .json files belong under golden/"
            );
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {path:?}: {error}"));
            serde_json::from_str::<serde_json::Value>(&text)
                .unwrap_or_else(|error| panic!("{path:?} is not JSON: {error}"));
            assert!(
                text.ends_with('\n') && !text.ends_with("\n\n"),
                "{path:?}: one trailing newline"
            );
            for line in text.lines() {
                let indent = line.len() - line.trim_start_matches(' ').len();
                assert!(
                    indent % 2 == 0
                        && !line.ends_with(' ')
                        && !line.contains('\t')
                        && !line.contains('\r'),
                    "{path:?}: `{line}` is not in the encoder's layout"
                );
            }
            *found += 1;
        }
    }
    let mut found = 0;
    visit(&harness::golden_root(), &mut found);
    assert!(found > 0, "no goldens found");
}
