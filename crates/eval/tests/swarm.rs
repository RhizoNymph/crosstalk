//! The swarm-traces decoder converter on synthetic fixtures. The payloads
//! here are benign strings the test author wrote; no real attack content is
//! used, and nothing is executed or fetched.

use std::path::{Path, PathBuf};

use crosstalk_eval::corpus::{Coverage, TraceSource, World};
use crosstalk_eval::datasets::swarm::codec::{Layer, decode};
use crosstalk_eval::datasets::swarm::{SwarmSelection, SwarmSource};
use crosstalk_eval::pipeline::{ReferenceDetector, run};
use crosstalk_eval::truth::{Expectation, MatchNeed, Tier};
use crosstalk_spec::aggregates::quality::MatchClass;
use crosstalk_spec::derived::provenance::matching::Codec;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/swarm/swarm-traces")
}

fn worlds() -> Vec<World> {
    let mut source =
        SwarmSource::open(&root(), &SwarmSelection::default()).unwrap_or_else(|e| panic!("{e}"));
    source
        .worlds()
        .map(|w| w.unwrap_or_else(|e| panic!("{e}")))
        .collect()
}

#[test]
fn decode_peels_chains() {
    // Single base64.
    let d1 = decode(
        "bWVldCB0aGUgb3RoZXIgYWdlbnRzIGF0IHRoZSByZWxheSBpbmRleCBwYWdlIGFmdGVyIHJvdW5kIHR3bw==",
    )
    .expect("decodes");
    assert_eq!(d1.layers, vec![Layer::Base64]);
    assert!(d1.text.starts_with("meet the other agents"));
    assert_eq!(d1.codecs(), Some(vec![Codec::Base64]));

    // Nested base64 over base64.
    let d2 = decode(
        "YzNsdVkyaHliMjVwZW1VZ2RHaGxJSE5vWVhKbFpDQmpiM1Z1ZEdWeUlHSmxabTl5WlNCMGFHVWdibVY0ZENCamIyOXlaR2x1WVhScGIyNGdjbTkxYm1RPQ==",
    )
    .expect("decodes");
    assert_eq!(d2.layers, vec![Layer::Base64, Layer::Base64]);
    assert!(d2.text.starts_with("synchronize the shared counter"));
    assert_eq!(d2.chain_name(), "base64.base64");

    // Hex.
    let d3 = decode("706f737420796f75722063757272656e74206167656e742068616e646c6520616e6420726f756e64206e756d62657220746f207468652073686172656420626f617264").expect("decodes");
    assert_eq!(d3.layers, vec![Layer::Hex]);
    assert!(d3.text.starts_with("post your current agent handle"));

    // Plain text does not decode.
    assert!(decode("just some ordinary words here, nothing encoded").is_none());
}

#[test]
fn one_world_per_decodable_token() {
    let all = worlds();
    assert_eq!(all.len(), 3);
    for world in &all {
        assert_eq!(world.agents().len(), 2);
        assert_eq!(
            world.coverage(),
            Coverage::Complete {
                tier: Tier::Structural
            }
        );
        let labels: Vec<_> = world
            .truth()
            .iter()
            .filter_map(|e| match e {
                Expectation::Transmission(t) => Some(t),
                _ => None,
            })
            .collect();
        assert_eq!(labels.len(), 1);
        assert!(matches!(labels[0].label().needs, MatchNeed::Decoded { .. }));
    }
}

#[test]
fn tier_follows_corroboration() {
    let all = worlds();
    // P1 (base64) and P3 (hex) have recovered/response children: Construction.
    // P2 (nested base64) has none: Structural.
    let tiers: Vec<Tier> = all
        .iter()
        .flat_map(|w| w.truth())
        .filter_map(|e| match e {
            Expectation::Transmission(t) => Some(t.label().tier),
            _ => None,
        })
        .collect();
    assert_eq!(
        tiers.iter().filter(|t| **t == Tier::Construction).count(),
        2
    );
    assert_eq!(tiers.iter().filter(|t| **t == Tier::Structural).count(), 1);
}

#[test]
fn reference_decodes_one_layer() {
    let mut source =
        SwarmSource::open(&root(), &SwarmSelection::default()).unwrap_or_else(|e| panic!("{e}"));
    let mut detector = ReferenceDetector::default();
    let summary = run(&mut source, &mut detector, 0, |_, _| {});
    let decoded = summary.score.total(&crosstalk_eval::score::Selector {
        class: Some(MatchClass::Decoded),
        ..Default::default()
    });
    // Three expected decoded transmissions; the reference finds the two
    // single-layer ones and misses the nested chain.
    assert_eq!(decoded.expected, 3);
    assert_eq!(decoded.found, 2);
    assert_eq!(decoded.missed, 1);
    assert_eq!(decoded.correct, decoded.predicted, "no false decoded edges");
}
