use std::num::{NonZeroU16, NonZeroU32};

use crate::derived::provenance::fingerprint::Fingerprint;
use crate::derived::provenance::matching::{Carrier, ContentMatch, InvalidMatch, MatchKind};
use crate::derived::provenance::span::{
    Origin, OriginatedSpan, RelaySource, Span, SpanEvent, SpanState,
};
use crate::tests::fixtures::{agent, at, bytes, content_match, exchange, location, message, span};

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
        bytes(10),
    );
    assert_eq!(result, Err(InvalidMatch::SelfMatch));
}

#[test]
fn content_match_rejects_more_bytes_than_read() {
    let result = ContentMatch::new(
        span(1),
        agent(1),
        agent(2),
        exchange(2),
        location(),
        Carrier::UserTurn,
        MatchKind::Exact,
        bytes(65),
    );
    assert_eq!(result, Err(InvalidMatch::ExceedsReadRange));
}

#[test]
fn content_match_accepts_whole_read_range() {
    let m = content_match(agent(1), agent(2), 64);
    assert_eq!(m.matched_bytes().get(), 64);
}

#[test]
fn content_match_keeps_its_fields() {
    let m = content_match(agent(1), agent(2), 40);
    assert_eq!(m.origin_agent(), agent(1));
    assert_eq!(m.reader(), agent(2));
    assert_eq!(m.reader_exchange(), exchange(2));
    assert_eq!(m.matched_bytes().get(), 40);
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
fn span_advances_along_the_lifecycle() {
    let state = SpanState::Extracted
        .advance(SpanEvent::Classify(Origin::Originated))
        .and_then(|s| s.advance(SpanEvent::Index { at: at(1) }))
        .and_then(|s| s.advance(SpanEvent::Hit { at: at(2) }))
        .and_then(|s| s.advance(SpanEvent::Hit { at: at(3) }))
        .expect("every step is a legal edge");
    assert_eq!(
        state,
        SpanState::Propagated {
            indexed_at: at(1),
            first_hit_at: at(2),
            hits: NonZeroU32::new(2).expect("2 is not zero"),
        }
    );
    assert_eq!(
        state.advance(SpanEvent::Expire { at: at(4) }),
        Ok(SpanState::Expired { at: at(4) })
    );
}

#[test]
fn span_rejects_illegal_edges() {
    let illegal = [
        (SpanState::Extracted, SpanEvent::Index { at: at(1) }),
        (SpanState::Extracted, SpanEvent::Hit { at: at(1) }),
        (SpanState::Common, SpanEvent::Index { at: at(1) }),
        (
            SpanState::Relayed {
                source: RelaySource::Span(span(1)),
            },
            SpanEvent::Index { at: at(1) },
        ),
        (SpanState::Originated, SpanEvent::Hit { at: at(1) }),
        (SpanState::Originated, SpanEvent::Expire { at: at(1) }),
        (
            SpanState::Indexed { at: at(1) },
            SpanEvent::Classify(Origin::Common),
        ),
        (
            SpanState::Expired { at: at(1) },
            SpanEvent::Hit { at: at(2) },
        ),
    ];
    for (state, event) in illegal {
        assert!(state.advance(event).is_err(), "{state:?} on {event:?}");
    }
}

fn span_in(state: SpanState) -> Span {
    Span {
        id: span(1),
        location: location(),
        agent: agent(1),
        exchange: exchange(1),
        state,
    }
}

#[test]
fn only_live_originated_spans_can_be_indexed() {
    assert!(OriginatedSpan::new(span_in(SpanState::Originated)).is_some());
    assert!(OriginatedSpan::new(span_in(SpanState::Indexed { at: at(1) })).is_some());
    assert!(OriginatedSpan::new(span_in(SpanState::Extracted)).is_none());
    assert!(OriginatedSpan::new(span_in(SpanState::Common)).is_none());
    // Forwarded text (relayed from an input) is indexed under the
    // forwarding agent (`provenance.index.forwarded-indexed`); text relayed
    // from another indexed span is not.
    assert!(
        OriginatedSpan::new(span_in(SpanState::Relayed {
            source: RelaySource::Input(message(1))
        }))
        .is_some()
    );
    assert!(
        OriginatedSpan::new(span_in(SpanState::Relayed {
            source: RelaySource::Span(span(7))
        }))
        .is_none()
    );
    assert!(OriginatedSpan::new(span_in(SpanState::Expired { at: at(1) })).is_none());
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

#[test]
fn span_rejects_hits_and_expiry_before_indexing() {
    let indexed = SpanState::Indexed { at: at(10) };
    assert!(indexed.advance(SpanEvent::Hit { at: at(9) }).is_err());
    assert!(indexed.advance(SpanEvent::Expire { at: at(9) }).is_err());
    let propagated = SpanState::Propagated {
        indexed_at: at(10),
        first_hit_at: at(11),
        hits: NonZeroU32::MIN,
    };
    assert!(propagated.advance(SpanEvent::Hit { at: at(9) }).is_err());
    assert!(propagated.advance(SpanEvent::Expire { at: at(9) }).is_err());
    assert!(indexed.advance(SpanEvent::Hit { at: at(10) }).is_ok());
}
