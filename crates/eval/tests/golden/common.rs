//! Exporting a source with its reference (or live) predictions, and
//! reading what was written.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use a2a_bench_format::files::{DetectorInfo, Labels};
use a2a_bench_format::ids::Digest;
use a2a_bench_format::jsonl::FileReader;
use a2a_bench_format::labels::Label;
use a2a_bench_format::manifest::{Converter, Source};
use crosstalk_eval::corpus::{TraceSource, World};
use crosstalk_eval::golden::run::write_manifest;
use crosstalk_eval::golden::{
    ExportWriter, Finished, GoldenRun, ManifestSpec, PredictionsWriter, Verified, ids, verify,
};
use crosstalk_eval::pipeline::{Detector, ReferenceDetector, run_with};

/// A fresh, empty directory under the test target's scratch space.
pub fn dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("golden")
        .join(name);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display()));
    }
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display()));
    dir
}

pub fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// A manifest spec with fixed source and converter fields.
pub fn spec(dataset: &crosstalk_eval::keys::DatasetId) -> ManifestSpec {
    ManifestSpec {
        dataset: ids::dataset(dataset).unwrap_or_else(|e| panic!("{e}")),
        source: Source {
            path: "fixtures".to_owned(),
            revision: "test".to_owned(),
            digest: Digest::from_bytes([7; 32]),
        },
        converter: Converter {
            version: "test".to_owned(),
            git: "test".to_owned(),
        },
        selection: BTreeMap::new(),
        pace: BTreeMap::new(),
    }
}

pub fn detector_info(name: &str) -> DetectorInfo {
    DetectorInfo {
        name: name.to_owned(),
        version: "test".to_owned(),
        variant: "default".to_owned(),
        config_digest: None,
    }
}

pub const PREDICTIONS: &str = "predictions.jsonl";

/// Exports every world of `source` to `out` with `detector`'s predictions
/// beside it (`predictions.jsonl`), then verifies both with the format's
/// checks. Panics on any failure, the run's included.
pub fn export_with(
    source: &mut impl TraceSource,
    detector: &mut impl Detector,
    out: &Path,
) -> (Finished, Verified) {
    let spec = spec(&source.id());
    let writer = ExportWriter::create(out, &spec.dataset).unwrap_or_else(|e| panic!("{e}"));
    let predictions =
        PredictionsWriter::create(&out.join(PREDICTIONS)).unwrap_or_else(|e| panic!("{e}"));
    let mut golden = GoldenRun::new(writer, Some(predictions));
    let summary = run_with(source, detector, 0, |outcome| golden.observe(outcome));
    assert!(summary.failures.is_empty(), "{:?}", summary.failures);
    let finished = golden
        .finish(&spec, Some(detector_info(detector.name())))
        .unwrap_or_else(|e| panic!("{e}"));
    write_manifest(out, &finished.manifest).unwrap_or_else(|e| panic!("{e}"));
    let verified = verify(out, Some(&out.join(PREDICTIONS))).unwrap_or_else(|e| panic!("{e}"));
    (finished, verified)
}

/// [`export_with`] the reference detector.
pub fn export_reference(source: &mut impl TraceSource, out: &Path) -> (Finished, Verified) {
    export_with(source, &mut ReferenceDetector::default(), out)
}

pub fn worlds(source: &mut impl TraceSource) -> Vec<World> {
    source
        .worlds()
        .map(|world| world.unwrap_or_else(|e| panic!("{e}")))
        .collect()
}

/// Every world's label rows, in file order.
pub fn labels(out: &Path) -> Vec<(String, Vec<Label>)> {
    let path = out.join("labels.jsonl");
    let file = std::fs::File::open(&path).unwrap_or_else(|e| panic!("{e}"));
    let mut reader = FileReader::<Labels, _>::open(std::io::BufReader::new(file))
        .unwrap_or_else(|e| panic!("{e}"));
    let mut out = Vec::new();
    while let Some(section) = reader.next_world().unwrap_or_else(|e| panic!("{e}")) {
        out.push((section.world.key.to_string(), section.rows));
    }
    out
}
