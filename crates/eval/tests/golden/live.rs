//! The live composition's predictions (`--detector live`): its own
//! attribution of every exchange (L3's placements) and its evidence pass
//! `check_predictions` against the exported worlds.

use a2a_bench_format::files::Predictions;
use a2a_bench_format::jsonl::FileReader;
use a2a_bench_format::predictions::Prediction;
use crosstalk_eval::datasets::salt::{SaltSource, Selection};
use crosstalk_eval::detect::live::{LiveDetector, LiveSettings, gateway_backend};

use super::common::{PREDICTIONS, dir, export_with, fixtures};

#[test]
fn live_predictions_on_salt_pass_the_checks() {
    let mut source = SaltSource::open(&fixtures().join("salt"), &Selection::default())
        .unwrap_or_else(|e| panic!("{e}"));
    let settings = LiveSettings::short(0).unwrap_or_else(|e| panic!("{e}"));
    let mut detector =
        LiveDetector::new(gateway_backend(), settings).unwrap_or_else(|e| panic!("{e}"));
    let out = dir("live-salt");
    let (_, verified) = export_with(&mut source, &mut detector, &out);
    let file = std::fs::File::open(out.join(PREDICTIONS)).unwrap_or_else(|e| panic!("{e}"));
    let mut reader = FileReader::<Predictions, _>::open(std::io::BufReader::new(file))
        .unwrap_or_else(|e| panic!("{e}"));
    let (mut attributed, mut transmissions) = (0, 0);
    while let Some(section) = reader.next_world().unwrap_or_else(|e| panic!("{e}")) {
        for row in &section.rows {
            match row {
                Prediction::Attribution(row) => attributed += row.exchanges.len() as u64,
                Prediction::Transmission(_) => transmissions += 1,
                Prediction::Unattributed(_) => {}
            }
        }
    }
    assert_eq!(attributed, verified.exchanges, "L3 places every exchange");
    assert!(
        transmissions > 0,
        "the live composition confirms the deliveries"
    );
}
