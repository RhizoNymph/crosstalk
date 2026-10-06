//! The conversation reads' checked types and rules: part text slices,
//! turn windows, the replay and conversation filters, inline readers,
//! span statuses, match keys and the error mapping.

use std::collections::BTreeSet;

use crate::derived::provenance::span::{RelaySource, SpanState};
use crate::events::ingest::ConversationDelta;
use crate::ids::ConversationId;
use crate::interfaces::l1_canonical::exchanges::ExchangeStoreError;
use crate::interfaces::l3_reconstruction::ThreadOutcome;
use crate::interfaces::l3_reconstruction::conversations::{
    ConversationQuery, ConversationReadError, ReplayFilter, StoredConversation, ThreadOutcomeKind,
    TurnIndex, TurnWindow,
};
use crate::interfaces::l4_provenance::SpanIndexError;
use crate::interfaces::l4_provenance::reads::{ProvenanceReadError, ScanFailureKind, ScanStatus};
use crate::interfaces::l5_flow::transmissions::MatchKey;
use crate::interfaces::l8_surface::conversation::text::{
    InvalidTextLimit, PartText, TextError, TextLimit,
};
use crate::interfaces::l8_surface::conversation::turn::{InvalidReadBy, OriginatedStatus, ReadBy};
use crate::interfaces::l8_surface::{InputError, QueryError};
use crate::observed::client::{CorpusId, IngressMode, RouteName, TrafficSource};
use crate::observed::conversation::{Conversation, ConversationOrigin, OriginKind};
use crate::observed::message::text::NoPartText;
use crate::paging::PageSize;
use crate::tests::fixtures::{agent, at, bytes, content_match, exchange, message, span};

fn limit(bytes: u32) -> TextLimit {
    TextLimit::new(bytes).expect("in range")
}

fn cut(part: &str, from: u32, bytes: u32) -> Result<PartText, TextError> {
    PartText::cut(part, from, limit(bytes))
}

#[test]
fn text_limit_is_one_to_max() {
    assert_eq!(
        TextLimit::new(0),
        Err(InvalidTextLimit {
            max: TextLimit::MAX,
            got: 0
        })
    );
    assert_eq!(
        TextLimit::new(TextLimit::MAX + 1),
        Err(InvalidTextLimit {
            max: TextLimit::MAX,
            got: TextLimit::MAX + 1
        })
    );
    assert_eq!(limit(1).get(), 1);
    assert_eq!(limit(TextLimit::MAX).get(), TextLimit::MAX);
    assert_eq!(TextLimit::DEFAULT.get(), 8192);
}

#[test]
fn a_cut_is_the_bytes_from_its_start_up_to_the_limit() {
    let part = "release plan: ship on friday";
    let whole = cut(part, 0, 8192).expect("from the start");
    assert_eq!(whole.text(), part);
    assert_eq!(whole.remaining(), 0);
    assert_eq!(whole.part_len() as usize, part.len());
    let head = cut(part, 0, 7).expect("from the start");
    assert_eq!(head.text(), "release");
    assert_eq!(head.from(), 0);
    assert_eq!(head.remaining() as usize, part.len() - 7);
    let tail = cut(part, 22, 100).expect("on a boundary");
    assert_eq!(tail.text(), "friday");
    assert_eq!(tail.remaining(), 0);
}

#[test]
fn a_cut_ends_on_a_character_boundary() {
    // "ü" and "ï" are two bytes each.
    let part = "ünïcode";
    let clipped = cut(part, 0, 3).expect("from the start");
    assert_eq!(clipped.text(), "ün");
    let clipped = cut(part, 0, 1).expect("from the start");
    assert_eq!(
        clipped.text(),
        "ü",
        "at least one character, even past the limit"
    );
    assert_eq!(clipped.remaining() as usize, part.len() - 2);
    let emoji = "🦀 crab";
    assert_eq!(cut(emoji, 0, 2).expect("a whole emoji").text(), "🦀");
}

#[test]
fn a_cut_from_the_end_is_empty() {
    let part = "abc";
    let empty = cut(part, 3, 10).expect("at the end");
    assert_eq!(empty.text(), "");
    assert_eq!(empty.remaining(), 0);
    let empty = cut("", 0, 10).expect("an empty part");
    assert_eq!(empty.text(), "");
    assert_eq!(empty.part_len(), 0);
}

#[test]
fn a_cut_refuses_a_start_past_the_end_or_inside_a_character() {
    assert_eq!(
        cut("abc", 4, 10),
        Err(TextError::Slice {
            from: 4,
            part_len: 3
        })
    );
    assert_eq!(
        cut("ünïcode", 1, 10),
        Err(TextError::Slice {
            from: 1,
            part_len: 9
        })
    );
}

#[test]
fn a_turn_window_is_clipped_to_the_turns_there_are() {
    let window = |from: u32, size: u16| TurnWindow {
        from: TurnIndex(from),
        size: PageSize::new(size).expect("in range"),
    };
    assert_eq!(window(0, 20).range(84), 0..20);
    assert_eq!(window(80, 20).range(84), 80..84);
    assert_eq!(window(84, 20).range(84), 84..84);
    assert!(window(100, 20).range(84).is_empty());
    assert_eq!(
        window(u32::MAX - 1, 500).range(u32::MAX),
        u32::MAX - 1..u32::MAX
    );
    assert!(window(0, 20).range(0).is_empty());
}

fn replayed(corpus: &str) -> TrafficSource {
    TrafficSource::Replay {
        corpus: CorpusId(corpus.into()),
    }
}

#[test]
fn replay_filter_keeps_by_source() {
    let live = TrafficSource::Live;
    let salt = replayed("salt-nlp");
    let dojo = replayed("agentdojo");
    assert!(ReplayFilter::Include.admits(&live));
    assert!(ReplayFilter::Include.admits(&salt));
    assert!(ReplayFilter::Exclude.admits(&live));
    assert!(!ReplayFilter::Exclude.admits(&salt));
    let any = ReplayFilter::Only { corpus: None };
    assert!(!any.admits(&live));
    assert!(any.admits(&salt) && any.admits(&dojo));
    let one = ReplayFilter::Only {
        corpus: Some(CorpusId("salt-nlp".into())),
    };
    assert!(one.admits(&salt));
    assert!(!one.admits(&dojo) && !one.admits(&live));
    assert_eq!(ReplayFilter::default(), ReplayFilter::Include);
}

#[test]
fn ingress_names_its_traffic_source() {
    assert_eq!(
        IngressMode::ReverseProxy {
            route: RouteName("anthropic".into())
        }
        .source(),
        TrafficSource::Live
    );
    assert_eq!(
        IngressMode::Replay {
            corpus: CorpusId("salt-nlp".into())
        }
        .source(),
        replayed("salt-nlp")
    );
}

fn stored(
    id: u128,
    owner: u128,
    origin: ConversationOrigin,
    source: TrafficSource,
) -> StoredConversation {
    StoredConversation {
        conversation: Conversation {
            id: ConversationId::from_ulid(id),
            agent: agent(owner),
            messages: Vec::new(),
            origin,
        },
        source,
        started_at: at(1),
        last_turn_at: at(2),
        turns: 1,
    }
}

#[test]
fn conversation_query_keeps_every_condition() {
    let fork = ConversationOrigin::Fork {
        parent: ConversationId::from_ulid(1),
        shared_prefix: 3,
    };
    let root = stored(1, 1, ConversationOrigin::Root, TrafficSource::Live);
    let forked = stored(2, 2, fork, replayed("salt-nlp"));
    let every = ConversationQuery::default();
    assert!(every.admits(&root) && every.admits(&forked));
    let cluster = ConversationQuery {
        agents: Some(BTreeSet::from([agent(1), agent(3)])),
        ..ConversationQuery::default()
    };
    assert!(cluster.admits(&root));
    assert!(!cluster.admits(&forked));
    let forks = ConversationQuery {
        origins: BTreeSet::from([OriginKind::Fork]),
        ..ConversationQuery::default()
    };
    assert!(!forks.admits(&root) && forks.admits(&forked));
    let live = ConversationQuery {
        replay: ReplayFilter::Exclude,
        ..ConversationQuery::default()
    };
    assert!(live.admits(&root) && !live.admits(&forked));
}

#[test]
fn origins_name_their_kind_and_source() {
    let parent = ConversationId::from_ulid(9);
    assert_eq!(ConversationOrigin::Root.kind(), OriginKind::Root);
    assert_eq!(ConversationOrigin::Root.source(), None);
    let fork = ConversationOrigin::Fork {
        parent,
        shared_prefix: 4,
    };
    assert_eq!(fork.kind(), OriginKind::Fork);
    assert_eq!(fork.source(), Some(parent));
    let compaction = ConversationOrigin::Compaction {
        predecessor: parent,
    };
    assert_eq!(compaction.kind(), OriginKind::Compaction);
    assert_eq!(compaction.source(), Some(parent));
}

#[test]
fn thread_outcomes_name_their_kind() {
    let delta = ConversationDelta {
        exchange: exchange(1),
        agent: agent(1),
        conversation: ConversationId::from_ulid(2),
        new_inputs: Vec::new(),
        new_system: None,
        output: None,
    };
    let conversation = ConversationId::from_ulid(2);
    let parent = ConversationId::from_ulid(1);
    let cases = [
        (
            ThreadOutcome::Starts {
                conversation,
                delta: delta.clone(),
            },
            ThreadOutcomeKind::Starts,
        ),
        (
            ThreadOutcome::Extends {
                conversation,
                delta: delta.clone(),
            },
            ThreadOutcomeKind::Extends,
        ),
        (
            ThreadOutcome::Forks {
                parent,
                shared_prefix: 2,
                conversation,
                delta: delta.clone(),
            },
            ThreadOutcomeKind::Forks,
        ),
        (
            ThreadOutcome::Compacts {
                predecessor: parent,
                conversation,
                delta,
            },
            ThreadOutcomeKind::Compacts,
        ),
    ];
    for (outcome, kind) in cases {
        assert_eq!(outcome.kind(), kind);
    }
}

#[test]
fn read_by_carries_exactly_the_newest_inline_readers() {
    assert_eq!(ReadBy::none().total(), 0);
    assert!(ReadBy::new(Vec::new(), 0).is_ok());
    assert_eq!(
        ReadBy::new(Vec::new(), 1),
        Err(InvalidReadBy::InlineShort {
            total: 1,
            inline: 0
        })
    );
}

#[test]
fn originated_status_follows_the_span_state() {
    assert_eq!(
        OriginatedStatus::of(&SpanState::Originated),
        Some(OriginatedStatus::Pending)
    );
    assert_eq!(
        OriginatedStatus::of(&SpanState::Indexed { at: at(5) }),
        Some(OriginatedStatus::Indexed { at: at(5) })
    );
    assert_eq!(
        OriginatedStatus::of(&SpanState::Propagated {
            indexed_at: at(5),
            first_hit_at: at(6),
            hits: bytes(2),
        }),
        Some(OriginatedStatus::Propagated {
            indexed_at: at(5),
            first_hit_at: at(6),
            hits: bytes(2),
        })
    );
    assert_eq!(
        OriginatedStatus::of(&SpanState::Expired { at: at(9) }),
        Some(OriginatedStatus::Expired { at: at(9) })
    );
    for state in [
        SpanState::Extracted,
        SpanState::Common,
        SpanState::Relayed {
            source: RelaySource::Span(span(1)),
        },
        SpanState::Relayed {
            source: RelaySource::Input(message(1)),
        },
    ] {
        assert_eq!(OriginatedStatus::of(&state), None, "{state:?}");
    }
}

#[test]
fn scan_marks_are_complete_once_committed() {
    assert!(!ScanStatus::Pending.marks_complete());
    assert!(ScanStatus::Scanned { at: at(1) }.marks_complete());
    assert!(ScanStatus::Indexed { at: at(1) }.marks_complete());
    assert!(
        !ScanStatus::Failed {
            at: at(1),
            failure: ScanFailureKind::BodyMissing
        }
        .marks_complete()
    );
}

#[test]
fn a_match_key_is_its_origin_reader_exchange_and_location() {
    let content = content_match(agent(1), agent(2), 4);
    let key = MatchKey::of(&content);
    assert_eq!(key.origin, content.origin());
    assert_eq!(key.reader_exchange, content.reader_exchange());
    assert_eq!(key.read_at, content.read_at());
}

#[test]
fn conversation_read_errors_map_to_query_errors() {
    let reason = || "connection reset".to_owned();
    assert_eq!(
        QueryError::from(ConversationReadError::Store { reason: reason() }),
        QueryError::Store { reason: reason() }
    );
    assert_eq!(
        QueryError::from(ConversationReadError::InvalidCursor),
        QueryError::InvalidCursor
    );
    assert_eq!(
        QueryError::from(ProvenanceReadError::Store { reason: reason() }),
        QueryError::Store { reason: reason() }
    );
    assert_eq!(
        QueryError::from(ProvenanceReadError::InvalidCursor),
        QueryError::InvalidCursor
    );
    assert_eq!(
        QueryError::from(ExchangeStoreError::Store { reason: reason() }),
        QueryError::Store { reason: reason() }
    );
    assert_eq!(
        QueryError::from(ExchangeStoreError::InvalidCursor),
        QueryError::InvalidCursor
    );
    assert_eq!(
        QueryError::from(SpanIndexError::Store { reason: reason() }),
        QueryError::Store { reason: reason() }
    );
}

#[test]
fn a_requested_part_or_slice_without_text_is_invalid_input() {
    assert_eq!(
        QueryError::from(TextError::Part(NoPartText::NotText { index: 2 })),
        QueryError::InvalidInput(InputError::PartWithoutText { index: 2 })
    );
    assert_eq!(
        QueryError::from(TextError::Part(NoPartText::NoSuchPart {
            index: 7,
            parts: 3
        })),
        QueryError::InvalidInput(InputError::PartWithoutText { index: 7 })
    );
    assert_eq!(
        QueryError::from(TextError::Slice {
            from: 9,
            part_len: 8
        }),
        QueryError::InvalidInput(InputError::SliceOutsideText {
            from: 9,
            part_len: 8
        })
    );
    assert_eq!(
        QueryError::from(InvalidTextLimit {
            max: TextLimit::MAX,
            got: 0
        }),
        QueryError::InvalidInput(InputError::TextLimitOutOfRange {
            max: TextLimit::MAX,
            got: 0
        })
    );
}
