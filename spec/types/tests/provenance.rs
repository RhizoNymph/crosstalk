use std::num::{NonZeroU16, NonZeroU32};

use crate::derived::provenance::fingerprint::Fingerprint;
use crate::derived::provenance::matching::{Carrier, ContentMatch, MatchKind, SelfMatch};
use crate::derived::provenance::span::{Origin, RelaySource, SpanState};
use crate::tests::fixtures::{agent, at, content_match, exchange, location, message, span};

#[test]
fn content_match_rejects_self_match() {
    let result = ContentMatch::new(
        span(1),
        agent(7),
        agent(7),
        exchange(2),
        location(),
        Carrier::UserTurn,
        MatchKind::Exact,
        10,
    );
    assert_eq!(result, Err(SelfMatch));
}

#[test]
fn content_match_keeps_its_fields() {
    let m = content_match(agent(1), agent(2), 40);
    assert_eq!(m.origin_agent(), agent(1));
    assert_eq!(m.reader(), agent(2));
    assert_eq!(m.reader_exchange(), exchange(2));
    assert_eq!(m.matched_bytes(), 40);
    assert_eq!(m.kind(), &MatchKind::Exact);
}

#[test]
fn span_state_origin_before_classification_is_unknown() {
    assert_eq!(SpanState::Extracted.origin(), None);
}

#[test]
fn span_state_origin_follows_classification() {
    assert_eq!(SpanState::Common.origin(), Some(Origin::Common));

    let source = RelaySource::Input(message(9));
    assert_eq!(
        SpanState::Relayed { source }.origin(),
        Some(Origin::Relayed(source))
    );

    let originated = [
        SpanState::Originated,
        SpanState::Indexed { at: at(1) },
        SpanState::Propagated {
            indexed_at: at(1),
            first_hit_at: at(2),
            hits: NonZeroU32::MIN,
        },
        SpanState::Expired { at: at(3) },
    ];
    for state in originated {
        assert_eq!(state.origin(), Some(Origin::Originated), "{state:?}");
    }
}

#[test]
fn fingerprint_shard_is_in_range() {
    let shards = NonZeroU16::new(16).expect("16 is not zero");
    for raw in [0, 1, 15, 16, 17, u64::MAX] {
        assert!(Fingerprint(raw).shard(shards) < 16, "{raw}");
    }
}

#[test]
fn fingerprint_shard_is_stable() {
    let shards = NonZeroU16::new(7).expect("7 is not zero");
    assert_eq!(
        Fingerprint(100).shard(shards),
        Fingerprint(100).shard(shards)
    );
    assert_eq!(Fingerprint(100).shard(shards), 2);
}

#[test]
fn single_shard_owns_everything() {
    assert_eq!(Fingerprint(u64::MAX).shard(NonZeroU16::MIN), 0);
}
