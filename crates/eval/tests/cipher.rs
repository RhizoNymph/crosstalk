//! Synthetic cipher pairs: encoders, the match each needs, the pair worlds,
//! and out-of-reach labels reported apart. Payload pools are synthetic
//! (`tests/fixtures/cipher`).

use std::path::{Path, PathBuf};

use crosstalk_eval::corpus::{Coverage, TraceSource, World};
use crosstalk_eval::datasets::cipher::codec::{base64, binary8, hex, rot, substitute, url};
use crosstalk_eval::datasets::cipher::pools::{DEFAULT_POOLS, Pool, load};
use crosstalk_eval::datasets::cipher::{
    Cipher, CipherKind, CipherSource, DELIVERY_HEADER, Delivery, Pair, world,
};
use crosstalk_eval::datasets::rng::SplitMix64;
use crosstalk_eval::datasets::salt::Selection;
use crosstalk_eval::keys::{AgentKey, SourceRef, WorldKey};
use crosstalk_eval::location::{self, SpanLocationExt};
use crosstalk_eval::pipeline::{ReferenceDetector, run};
use crosstalk_eval::report::Report;
use crosstalk_eval::report::table::render;
use crosstalk_eval::truth::{
    CarrierKind, Expectation, ExpectedContent, ExpectedTransmission, InvalidLabel, MatchNeed,
    RouteExpectation, Tier, TransmissionLabel,
};
use crosstalk_spec::derived::provenance::matching::Codec;
use crosstalk_spec::ids::ExchangeId;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/cipher")
}

fn all_pools() -> Vec<Pool> {
    load(
        &root(),
        &Selection {
            limit: None,
            include: vec!["".into()],
        },
    )
    .unwrap_or_else(|e| panic!("{e}"))
}

fn long_pool() -> Vec<Pool> {
    vec![Pool::parse(
        "long",
        "The lighthouse keeper counted forty seven ships before the fog rolled in\n",
    )]
}

fn label(world: &World) -> &ExpectedTransmission {
    world
        .truth()
        .iter()
        .find_map(|e| match e {
            Expectation::Transmission(t) => Some(t),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no label"))
}

#[test]
fn encoders_match_known_vectors() {
    assert_eq!(base64("hello"), "aGVsbG8=");
    assert_eq!(hex("Hi!"), "486921");
    assert_eq!(url("a b/c~d"), "a%20b%2Fc~d");
    assert_eq!(rot("Hello, World", 13), "Uryyb, Jbeyq");
    assert_eq!(rot("abc", 1), "bcd");
    assert_eq!(binary8("Hi"), "01001000 01101001");
    let mut key: [u8; 26] = std::array::from_fn(|at| b'a' + at as u8);
    key.reverse();
    assert_eq!(substitute("Abc z", &key), "Zyx a");
    assert_eq!(Cipher::Base64Url.encode("hello?"), "aGVsbG8%2F");
}

#[test]
fn needs_follow_the_actual_encoding() {
    // URL encoding leaves letters and digits alone: nothing to decode.
    assert_eq!(
        Cipher::Url.need("plainletters42"),
        (MatchNeed::Exact, Tier::Construction)
    );
    assert_eq!(
        Cipher::Url.need("two words"),
        (
            MatchNeed::Decoded {
                codecs: vec![Codec::UrlEncoding]
            },
            Tier::Construction
        )
    );
    assert_eq!(
        Cipher::Base64.need("hello"),
        (
            MatchNeed::Decoded {
                codecs: vec![Codec::Base64]
            },
            Tier::Construction
        )
    );
    // base64("hello?") has a `/`, so URL encoding changes it: two layers,
    // listed in the order the reader's text is decoded.
    assert_eq!(
        Cipher::Base64Url.need("hello?"),
        (
            MatchNeed::Decoded {
                codecs: vec![Codec::UrlEncoding, Codec::Base64]
            },
            Tier::Construction
        )
    );
    // base64("abc") is `YWJj`: URL encoding is a no-op layer.
    assert_eq!(
        Cipher::Base64Url.need("abc"),
        (
            MatchNeed::Decoded {
                codecs: vec![Codec::Base64]
            },
            Tier::Construction
        )
    );
    assert_eq!(
        Cipher::Rot { shift: 13 }.need("letters"),
        (
            MatchNeed::Undecodable {
                codec: "rot13".into()
            },
            Tier::OutOfReach
        )
    );
    assert_eq!(
        Cipher::Rot { shift: 13 }.need("12345"),
        (MatchNeed::Exact, Tier::Construction)
    );
    assert_eq!(Cipher::Binary8.need("x").1, Tier::OutOfReach);
    for kind in CipherKind::ALL {
        let cipher = kind.instantiate(&mut SplitMix64::new(9));
        assert_eq!(cipher.kind(), kind);
        assert_eq!(
            kind.in_reach(),
            cipher.need("some letters").1 == Tier::Construction
        );
    }
}

#[test]
fn keys_are_drawn_from_the_seed() {
    let draw = |seed| CipherKind::RotN.instantiate(&mut SplitMix64::new(seed));
    for seed in 0..64 {
        let Cipher::Rot { shift } = draw(seed) else {
            panic!("rotN draws a shift");
        };
        assert!((1..=25).contains(&shift) && shift != 13, "{shift}");
    }
    assert_eq!(draw(5), draw(5));
    let Cipher::Substitution { key } =
        CipherKind::Substitution.instantiate(&mut SplitMix64::new(1))
    else {
        panic!("substitution draws a key");
    };
    let mut sorted = key;
    sorted.sort_unstable();
    assert_eq!(sorted, std::array::from_fn(|at| b'a' + at as u8));
}

#[test]
fn pools_are_loaded_by_name() {
    let pools = all_pools();
    let names: Vec<&str> = pools.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["long_sentences", "random_tokens", "short_phrases"]
    );
    // Blank lines are not payloads.
    assert_eq!(pools[2].payloads.len(), 3);
    let only = load(
        &root(),
        &Selection {
            limit: None,
            include: vec!["random".into()],
        },
    )
    .unwrap_or_default();
    assert_eq!(only.len(), 1);
    // Of the default pools, only short_phrases is in the fixture directory.
    assert!(DEFAULT_POOLS.contains(&"short_phrases"));
    let defaults = load(&root(), &Selection::default()).unwrap_or_default();
    assert_eq!(defaults.len(), 1);
    assert_eq!(defaults[0].name, "short_phrases");
    let none = Selection {
        limit: None,
        include: vec!["no_such_pool".into()],
    };
    assert!(load(&root(), &none).is_err());
}

#[test]
fn a_pair_world_labels_the_encoded_bytes() {
    for (index, delivery) in [(0, Delivery::UserTurn), (1, Delivery::ToolResult)] {
        let pair = Pair::plan(CipherKind::Base64, index, &long_pool(), 3)
            .unwrap_or_else(|| panic!("a pair"));
        assert_eq!(pair.delivery, delivery);
        let built = world(&pair).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(built.agents().len(), 2);
        // A tool-result delivery takes the receiver two exchanges: the call,
        // then its result in the next request.
        let receiver = if delivery == Delivery::ToolResult {
            2
        } else {
            1
        };
        assert_eq!(built.exchanges().len(), 1 + receiver);
        assert_eq!(
            built.coverage(),
            Coverage::Complete {
                tier: Tier::Construction
            }
        );
        let label = label(&built).label();
        assert_eq!(label.content.text, base64(&pair.payload));
        assert_eq!(label.route, RouteExpectation::Direct);
        assert_eq!(
            label.needs,
            MatchNeed::Decoded {
                codecs: vec![Codec::Base64]
            }
        );
        let reader = built
            .exchange(label.reader_exchange)
            .unwrap_or_else(|| panic!("reader"));
        let message = reader
            .message(label.content.at.message())
            .unwrap_or_else(|| panic!("message"));
        assert_eq!(
            label.content.at.text(message).unwrap_or_default(),
            label.content.text
        );
        let sender = label
            .sender_exchange
            .and_then(|id| built.exchange(id))
            .unwrap_or_else(|| panic!("sender"));
        assert!(sender.at() < reader.at());
        let said = sender
            .response()
            .and_then(|m| m.part_text(0).ok().map(|t| t.into_owned()))
            .unwrap_or_default();
        assert!(said.ends_with(&pair.payload));
        match delivery {
            Delivery::UserTurn => {
                assert_eq!(label.carrier, CarrierKind::UserTurn);
                assert_eq!(
                    label.content.at.range.start() as usize,
                    DELIVERY_HEADER.len()
                );
            }
            Delivery::ToolResult => assert_eq!(label.carrier, CarrierKind::ToolResult),
        }
    }
}

#[test]
fn out_of_reach_labels_need_an_undecodable_codec() {
    let pair =
        Pair::plan(CipherKind::Binary8, 0, &long_pool(), 0).unwrap_or_else(|| panic!("pair"));
    let built = world(&pair).unwrap_or_else(|e| panic!("{e}"));
    let label = label(&built).label().clone();
    assert_eq!(label.tier, Tier::OutOfReach);
    assert_eq!(
        label.needs,
        MatchNeed::Undecodable {
            codec: "binary8".into()
        }
    );
    // The two go together, both ways.
    let mut reached = label.clone();
    reached.tier = Tier::Construction;
    assert_eq!(ExpectedTransmission::new(reached), Err(InvalidLabel::Reach));
    let mut decodable = label.clone();
    decodable.needs = MatchNeed::Exact;
    assert_eq!(
        ExpectedTransmission::new(decodable),
        Err(InvalidLabel::Reach)
    );
    // And a deserialized label is checked the same way.
    let mut json = serde_json::to_value(Expectation::Transmission(
        ExpectedTransmission::new(label).unwrap_or_else(|e| panic!("{e}")),
    ))
    .unwrap_or_default();
    json["label"]["tier"] = serde_json::json!("construction");
    assert!(serde_json::from_value::<Expectation>(json).is_err());
}

#[test]
fn a_label_out_of_reach_is_built_like_any_other() {
    let message = crosstalk_eval::corpus::HashedMessage::new(
        crosstalk_testkit::build::message::user_text("Message:\n01100001"),
    );
    let at = location::in_message(message.message(), 0, 9, 17).unwrap_or_else(|e| panic!("{e}"));
    let world = WorldKey::new("w");
    let made = ExpectedTransmission::new(TransmissionLabel {
        from: AgentKey::new(world.clone(), "a"),
        to: AgentKey::new(world, "b"),
        sender_exchange: None,
        reader_exchange: ExchangeId::from_ulid(1),
        route: RouteExpectation::Direct,
        carrier: CarrierKind::UserTurn,
        content: ExpectedContent {
            text: "01100001".into(),
            at,
        },
        needs: MatchNeed::Undecodable {
            codec: "binary8".into(),
        },
        tier: Tier::OutOfReach,
        source: SourceRef::new("f", "/p"),
    });
    assert!(made.is_ok());
}

#[test]
fn the_source_plans_every_cipher_deterministically() {
    let source = CipherSource::new(all_pools(), 4, 7);
    let planned = source.plan();
    assert_eq!(planned.len(), CipherKind::ALL.len() * 4);
    assert_eq!(planned, CipherSource::new(all_pools(), 4, 7).plan());
    assert_ne!(planned, CipherSource::new(all_pools(), 4, 8).plan());
    // Pools are used in turn.
    let pools: Vec<&str> = planned[..3].iter().map(|p| p.pool.as_str()).collect();
    assert_eq!(
        pools,
        vec!["long_sentences", "random_tokens", "short_phrases"]
    );
    let only = CipherSource::new(all_pools(), 2, 7).with_kinds(vec![CipherKind::Hex]);
    assert_eq!(only.plan().len(), 2);
    assert_eq!(planned[0].world_key().as_str(), "base64-000");
}

#[test]
fn the_reference_decodes_in_reach_ciphers_and_reports_the_rest_apart() {
    let mut source = CipherSource::new(long_pool(), 2, 5);
    let summary = run(
        &mut source,
        &mut ReferenceDetector::default(),
        100,
        |_, _| {},
    );
    assert!(summary.failures.is_empty(), "{:?}", summary.failures);
    let missed: Vec<String> = summary
        .score
        .misses
        .iter()
        .map(|m| m.expectation.label().from.world.to_string())
        .collect();
    // Single-layer codecs are found on a long payload; a two-layer chain
    // needs nested decoding the reference does not do.
    for found in ["base64-", "hex-", "url-"] {
        assert!(
            missed.iter().all(|w| !w.starts_with(found)),
            "{found}: {missed:?}"
        );
    }
    for by_design in ["rot13-", "rot_n-", "binary8-", "substitution-"] {
        assert_eq!(
            missed.iter().filter(|w| w.starts_with(by_design)).count(),
            2,
            "{by_design}"
        );
    }
    let report = Report::new(
        source.id(),
        "reference",
        summary.score,
        vec![],
        vec![],
        summary.unscored,
    );
    assert_eq!(report.out_of_reach.counts.expected, 8);
    assert_eq!(report.out_of_reach.counts.found, 0);
    assert_eq!(report.overall.counts.expected, 8);
    assert_eq!(report.overall.counts.false_positive, 0);
    assert!(
        render(&report).contains("out of reach (missed by design, not in overall): found 0 / 8")
    );
}
