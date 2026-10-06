//! The fixture's conversation reads, over the shared harness world: the
//! behaviour of INV-1000..1029 the fixture can show.

use std::collections::BTreeSet;

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::transmission::{DelegationDirection, Route};
use crosstalk_spec::interfaces::l4_provenance::reads::ScanStatus;
use crosstalk_spec::interfaces::l8_surface::conversation::text::{BodyText, TextLimit, TextSlice};
use crosstalk_spec::interfaces::l8_surface::conversation::turn::{
    IncrementHistory, MessageParts, MessagePlacement, SpanOrigin, TurnContinuation, TurnOutcome,
};
use crosstalk_spec::interfaces::l8_surface::conversation::{
    ConversationFilter, OriginKind, OriginLink, ReplayFilter, SuccessorKind, TrafficSource,
    TurnIndex, TurnWindow,
};
use crosstalk_spec::interfaces::l8_surface::{InputError, Permission, QueryApi, QueryError};
use crosstalk_spec::observed::message::{PartRef, Role};
use crosstalk_spec::paging::{ConversationList, Page, PageRequest, PageSize};

use crate::backend::fixture::FixtureBackend;
use crate::backend::fixture::world::conversations::Cases;
use crate::testing::{caller_with, operator, world};

fn everyone() -> crosstalk_spec::interfaces::l8_surface::Caller {
    operator().caller()
}

fn backend() -> &'static FixtureBackend {
    world()
}

fn cases() -> Cases {
    backend()
        .world
        .conversations
        .cases()
        .expect("cases")
        .clone()
}

fn window(from: u32, size: u16) -> TurnWindow {
    TurnWindow {
        from: TurnIndex(from),
        size: PageSize::new(size).expect("size"),
    }
}

fn page<L>(size: u16) -> PageRequest<L> {
    PageRequest {
        size: PageSize::new(size).expect("size"),
        after: None,
    }
}

/// Every row `filter` lists, page after page.
async fn all(
    filter: &ConversationFilter,
) -> Vec<crosstalk_spec::interfaces::l8_surface::conversation::ConversationRow> {
    let mut out = Vec::new();
    let mut request: PageRequest<ConversationList> = page(50);
    loop {
        let page: Page<_, ConversationList> = backend()
            .conversations(&everyone(), filter, &request)
            .await
            .expect("conversations");
        let (items, next) = page.into_parts();
        out.extend(items);
        match next {
            Some(next) => request.after = Some(next),
            None => return out,
        }
    }
}

#[tokio::test]
async fn an_agents_conversations_are_its_own_newest_first_each_once() {
    let fork = cases().fork.0;
    let agent = backend().world.conversations.get(fork).expect("fork").agent;
    let rows = all(&ConversationFilter {
        agent: Some(agent),
        ..ConversationFilter::default()
    })
    .await;
    assert!(rows.len() >= 2, "the agent has several conversations");
    assert!(rows.iter().all(|r| r.agent == agent));
    let ids: Vec<_> = rows.iter().map(|r| r.id).collect();
    let mut sorted = ids.clone();
    sorted.sort_by(|a, b| b.cmp(a));
    assert_eq!(ids, sorted, "newest first");
    let distinct: BTreeSet<_> = ids.iter().collect();
    assert_eq!(distinct.len(), ids.len(), "each once");
    assert!(ids.contains(&fork));
}

#[tokio::test]
async fn filters_keep_origins_and_traffic_sources() {
    let forks = all(&ConversationFilter {
        origins: vec![OriginKind::Fork],
        ..ConversationFilter::default()
    })
    .await;
    assert!(!forks.is_empty());
    assert!(
        forks
            .iter()
            .all(|r| matches!(r.origin, OriginLink::Fork { .. }))
    );
    let (corpus, one) = cases().replay;
    let replayed = all(&ConversationFilter {
        replay: ReplayFilter::Only { corpus: None },
        ..ConversationFilter::default()
    })
    .await;
    assert!(replayed.iter().any(|r| r.id == one));
    assert!(replayed.iter().all(|r| r.source
        == TrafficSource::Replay {
            corpus: corpus.clone()
        }));
    let live = all(&ConversationFilter {
        replay: ReplayFilter::Exclude,
        ..ConversationFilter::default()
    })
    .await;
    assert!(live.iter().all(|r| r.source == TrafficSource::Live));
    let every = all(&ConversationFilter::default()).await;
    assert_eq!(every.len(), live.len() + replayed.len());
}

#[tokio::test]
async fn an_unknown_agent_has_no_conversations() {
    let rows = all(&ConversationFilter {
        agent: Some(crosstalk_spec::ids::AgentId::from_ulid(1)),
        ..ConversationFilter::default()
    })
    .await;
    assert!(rows.is_empty());
}

#[tokio::test]
async fn the_fork_and_compaction_resolve_their_links() {
    let (fork, parent) = cases().fork;
    let head = backend()
        .conversation(&everyone(), fork)
        .await
        .expect("read")
        .expect("fork");
    let OriginLink::Fork {
        parent: named,
        shared_prefix,
        branch_turn,
        ..
    } = head.row.origin
    else {
        panic!("a fork");
    };
    assert_eq!(named, parent);
    assert!(shared_prefix > 0);
    assert_eq!(
        branch_turn,
        Some(TurnIndex(1)),
        "branches after the parent's second turn"
    );
    let parent_head = backend()
        .conversation(&everyone(), parent)
        .await
        .expect("read")
        .expect("parent");
    assert!(
        parent_head
            .successors
            .iter()
            .any(|s| s.conversation == fork && matches!(s.kind, SuccessorKind::Fork { .. }))
    );

    let (compacted, old) = cases().compaction;
    let head = backend()
        .conversation(&everyone(), compacted)
        .await
        .expect("read")
        .expect("compaction");
    assert!(matches!(
        head.row.origin,
        OriginLink::Compaction { predecessor, carried_over: 2, .. } if predecessor == old
    ));
    let turns = backend()
        .conversation_turns(&everyone(), compacted, &window(0, 20))
        .await
        .expect("read")
        .expect("turns");
    let carried = turns.turns[0]
        .inputs
        .iter()
        .filter(|m| m.placement == MessagePlacement::CarriedOver)
        .count();
    assert_eq!(carried, 2);
    assert!(turns.turns.iter().skip(1).all(|t| {
        t.inputs
            .iter()
            .all(|m| m.placement != MessagePlacement::CarriedOver)
    }));
}

#[tokio::test]
async fn a_window_covers_exactly_its_turns() {
    let (id, _) = cases().compaction;
    let full = backend()
        .conversation_turns(&everyone(), id, &window(0, 500))
        .await
        .expect("read")
        .expect("turns");
    assert_eq!(full.turns.len() as u32, full.total);
    for (i, turn) in full.turns.iter().enumerate() {
        assert_eq!(turn.index, TurnIndex(i as u32));
        let output = turn.output.as_ref().expect("an output");
        assert_eq!(output.placement, MessagePlacement::Output);
    }
    let part = backend()
        .conversation_turns(&everyone(), id, &window(1, 2))
        .await
        .expect("read")
        .expect("turns");
    let indexes: Vec<_> = part.turns.iter().map(|t| t.index.0).collect();
    assert_eq!(indexes, vec![1, 2]);
    let past = backend()
        .conversation_turns(&everyone(), id, &window(full.total, 20))
        .await
        .expect("read")
        .expect("turns");
    assert!(past.turns.is_empty());
    assert_eq!(past.total, full.total);
    let unknown = backend()
        .conversation_turns(
            &everyone(),
            crosstalk_spec::ids::ConversationId::from_ulid(1),
            &window(0, 20),
        )
        .await
        .expect("read");
    assert!(unknown.is_none());
}

#[tokio::test]
async fn every_read_match_is_an_inbound_mark_on_its_part() {
    let b = backend();
    let mut checked = 0;
    for record in b.world.transmissions.iter().take(400) {
        let Some(confirmed) = record.transmission.state.confirmed() else {
            continue;
        };
        for content in confirmed.content().iter() {
            let placement = b
                .exchange_turns(
                    &everyone(),
                    &IdBatch::new(vec![content.reader_exchange()]).expect("batch"),
                )
                .await
                .expect("read")
                .remove(&content.reader_exchange())
                .expect("threaded");
            let page = b
                .conversation_turns(
                    &everyone(),
                    placement.conversation,
                    &window(placement.turn.0, 1),
                )
                .await
                .expect("read")
                .expect("turns");
            let turn = &page.turns[0];
            if !turn.provenance.marks_complete() {
                continue;
            }
            let read_at = content.read_at();
            let messages = turn.inputs.iter().chain(turn.output.iter());
            let found = messages
                .filter(|m| m.hash == read_at.part.message)
                .any(|m| match &m.parts {
                    MessageParts::Shown(parts) => parts.iter().any(|p| {
                        p.index == read_at.part.index
                            && p.inbound.iter().any(|i| {
                                i.range == read_at.range
                                    && i.origin.span == content.origin()
                                    && i.transmission.as_ref().map(|t| t.id)
                                        == Some(record.transmission.id)
                            })
                    }),
                    MessageParts::BodyDropped(parts) => parts.iter().any(|p| {
                        p.index == read_at.part.index
                            && p.inbound.iter().any(|i| i.origin.span == content.origin())
                    }),
                });
            assert!(found, "the match is marked where it was read");
            checked += 1;
        }
    }
    assert!(checked > 20, "checked {checked}");
}

#[tokio::test]
async fn an_origin_spans_readers_are_its_matches() {
    let b = backend();
    let record = b
        .world
        .transmissions
        .iter()
        .find(|r| r.transmission.state.confirmed().is_some())
        .expect("a confirmed transmission");
    let content = record
        .transmission
        .state
        .confirmed()
        .expect("confirmed")
        .content()
        .first()
        .clone();
    let points = b
        .span_points(
            &everyone(),
            &IdBatch::new(vec![content.origin()]).expect("batch"),
        )
        .await
        .expect("read");
    let point = points.get(&content.origin()).expect("recorded").clone();
    assert_eq!(
        point.agent,
        b.world
            .scenario
            .cast
            .identity
            .canonical(content.origin_agent())
    );
    let turn_point = point.turn.expect("threaded");
    let page = b
        .conversation_turns(
            &everyone(),
            turn_point.conversation,
            &window(turn_point.turn.0, 1),
        )
        .await
        .expect("read")
        .expect("turns");
    let output = page.turns[0].output.as_ref().expect("output");
    let spans: Vec<_> = match &output.parts {
        MessageParts::Shown(parts) => parts.iter().flat_map(|p| p.spans.iter()).collect(),
        MessageParts::BodyDropped(parts) => parts.iter().flat_map(|p| p.spans.iter()).collect(),
    };
    let span = spans
        .iter()
        .find(|s| s.span == content.origin())
        .expect("the span is on its output");
    let SpanOrigin::Originated { read_by, .. } = &span.origin else {
        panic!("originated");
    };
    assert!(read_by.total() >= 1);
    assert!(
        read_by
            .first()
            .iter()
            .any(|r| r.exchange == content.reader_exchange())
    );
    let readers = b
        .span_readers(&everyone(), content.origin(), &page_of(500))
        .await
        .expect("read")
        .expect("span");
    assert_eq!(readers.items().len() as u32, read_by.total());
}

fn page_of<L>(size: u16) -> PageRequest<L> {
    page(size)
}

#[tokio::test]
async fn text_aligns_with_the_turns_and_needs_content() {
    let (id, _) = cases().mid_system;
    let b = backend();
    let turns = b
        .conversation_turns(&everyone(), id, &window(0, 20))
        .await
        .expect("read")
        .expect("turns");
    let text = b
        .conversation_text(&everyone(), id, &window(0, 20), TextLimit::DEFAULT)
        .await
        .expect("read")
        .expect("text");
    assert_eq!(turns.turns.len(), text.turns.len());
    for (turn, text) in turns.turns.iter().zip(&text.turns) {
        assert_eq!(turn.index, text.index);
        let hashes: Vec<_> = turn.inputs.iter().map(|m| m.hash).collect();
        let text_hashes: Vec<_> = text.inputs.iter().map(|m| m.hash).collect();
        assert_eq!(hashes, text_hashes);
        for (message, text) in turn.inputs.iter().zip(&text.inputs) {
            match (&message.parts, &text.body) {
                (MessageParts::Shown(parts), BodyText::Shown(texts)) => {
                    assert_eq!(parts.len(), texts.len());
                    for (part, text) in parts.iter().zip(texts) {
                        assert_eq!(
                            part.text_bytes,
                            text.as_ref().map(|t| t.part_len()),
                            "the shape's length is the text's"
                        );
                    }
                }
                (MessageParts::BodyDropped(_), BodyText::BodyDropped) => {}
                other => panic!("misaligned: {other:?}"),
            }
        }
    }
    assert!(
        turns.turns[1].inputs.iter().any(|m| m.role == Role::System),
        "the system turn is in request order"
    );
    let viewer = caller_with(&[Permission::View]);
    assert_eq!(
        b.conversation_text(&viewer, id, &window(0, 20), TextLimit::DEFAULT)
            .await,
        Err(QueryError::Forbidden {
            missing: Permission::Content
        })
    );
    assert!(
        b.conversation_turns(&viewer, id, &window(0, 20))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn part_text_slices_and_refuses_what_has_no_text() {
    let (id, _) = cases().mid_system;
    let b = backend();
    let turns = b
        .conversation_turns(&everyone(), id, &window(0, 1))
        .await
        .expect("read")
        .expect("turns");
    let first = &turns.turns[0].inputs[0];
    let part = PartRef {
        message: first.hash,
        index: 0,
    };
    let limit = TextLimit::new(10).expect("limit");
    let slice = b
        .part_text(&everyone(), part, TextSlice { from: 0, limit })
        .await
        .expect("read")
        .expect("text");
    assert_eq!(slice.text().len(), 10);
    assert!(slice.remaining() > 0);
    let missing = b
        .part_text(
            &everyone(),
            PartRef {
                message: first.hash,
                index: 9,
            },
            TextSlice { from: 0, limit },
        )
        .await;
    assert_eq!(
        missing,
        Err(QueryError::InvalidInput(InputError::PartWithoutText {
            index: 9
        }))
    );
    let past = b
        .part_text(
            &everyone(),
            part,
            TextSlice {
                from: 1_000_000,
                limit,
            },
        )
        .await;
    assert!(matches!(
        past,
        Err(QueryError::InvalidInput(
            InputError::SliceOutsideText { .. }
        ))
    ));
    let viewer = caller_with(&[Permission::View]);
    assert!(matches!(
        b.part_text(&viewer, part, TextSlice { from: 0, limit })
            .await,
        Err(QueryError::Forbidden { .. })
    ));
}

#[tokio::test]
async fn the_unseen_increment_failed_turn_and_pending_scan_read_as_such() {
    let b = backend();
    let unseen = b
        .conversation_turns(&everyone(), cases().unseen_increment, &window(0, 2))
        .await
        .expect("read")
        .expect("turns");
    assert!(matches!(
        unseen.turns[0].continuation,
        TurnContinuation::Increment {
            history: IncrementHistory::Unseen,
            ..
        }
    ));
    assert!(matches!(
        unseen.turns[1].continuation,
        TurnContinuation::Increment {
            history: IncrementHistory::Resolved,
            ..
        }
    ));
    let (id, index) = cases().failed;
    let failed = b
        .conversation_turns(&everyone(), id, &window(index, 1))
        .await
        .expect("read")
        .expect("turns");
    assert!(matches!(
        failed.turns[0].outcome,
        TurnOutcome::Failed { .. }
    ));
    let pending = cases().pending_scan;
    let placement = b
        .exchange_turns(&everyone(), &IdBatch::new(vec![pending]).expect("batch"))
        .await
        .expect("read")
        .remove(&pending)
        .expect("threaded");
    let page = b
        .conversation_turns(
            &everyone(),
            placement.conversation,
            &window(placement.turn.0, 1),
        )
        .await
        .expect("read")
        .expect("turns");
    assert_eq!(page.turns[0].provenance, ScanStatus::Pending);
}

#[tokio::test]
async fn a_delegated_childs_head_names_its_parent() {
    let b = backend();
    let delegation = b
        .world
        .transmissions
        .iter()
        .find(|r| {
            matches!(
                r.transmission.route,
                Route::Delegation(DelegationDirection::ParentToChild)
            ) && r.transmission.state.confirmed().is_some()
        })
        .expect("a delegation");
    let content = delegation
        .transmission
        .state
        .confirmed()
        .expect("confirmed")
        .content()
        .first()
        .clone();
    let placement = b
        .exchange_turns(
            &everyone(),
            &IdBatch::new(vec![content.reader_exchange()]).expect("batch"),
        )
        .await
        .expect("read")
        .remove(&content.reader_exchange())
        .expect("threaded");
    let head = b
        .conversation(&everyone(), placement.conversation)
        .await
        .expect("read")
        .expect("head");
    let link = head.delegated_from.expect("spawned by a delegation");
    assert_eq!(link.child.conversation, placement.conversation);
    assert!(head.traffic.received >= 1);
}
