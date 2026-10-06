//! The world backend's conversation reads: the seeded world's exchanges,
//! threaded by L3 and scanned by L4, read back through the surface. Every
//! read answers non-empty and agrees with the others and with the world's
//! own transmissions; the permission checks are the surface's.

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_api::world::{SeededWorld, WorldOptions, seed_world};
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::ids::{AgentId, ChannelId, ConversationId, ExchangeId, SpanId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l3_reconstruction::conversations::{TurnIndex, TurnWindow};
use crosstalk_spec::interfaces::l8_surface::conversation::text::{BodyText, TextLimit, TextSlice};
use crosstalk_spec::interfaces::l8_surface::conversation::turn::{
    MessageParts, MessagePlacement, Turn, TurnOutcome,
};
use crosstalk_spec::interfaces::l8_surface::conversation::{
    ConversationFilter, ConversationRow, OriginLink,
};
use crosstalk_spec::interfaces::l8_surface::{
    Caller, Permission, PermissionSet, QueryApi, QueryError,
};
use crosstalk_spec::observed::message::PartRef;
use crosstalk_spec::paging::{ConversationList, PageRequest, PageSize, SpanReaderList};
use crosstalk_world::clock::{DAY, minus};
use crosstalk_world::config::{OPERATOR_ONCALL, OPERATOR_RESEARCHER};
use crosstalk_world::generate::{self, Generated};
use crosstalk_world::{Anchor, ChannelKey, UI_ANCHOR, World};

const SEED: u64 = 7;

fn caller(operator: crosstalk_spec::ids::OperatorId, holds: PermissionSet) -> Caller {
    crosstalk_conformance::harness::caller(operator, holds)
        .unwrap_or_else(|e| panic!("caller: {e}"))
}

fn all() -> Caller {
    caller(OPERATOR_RESEARCHER, PermissionSet::ALL)
}

fn size(n: u16) -> PageSize {
    PageSize::new(n).unwrap_or_else(|e| panic!("page size: {e:?}"))
}

fn window(from: u32, n: u16) -> TurnWindow {
    TurnWindow {
        from: TurnIndex(from),
        size: size(n),
    }
}

fn batch<T: Ord + Copy>(ids: impl IntoIterator<Item = T>) -> IdBatch<T> {
    IdBatch::new(ids).unwrap_or_else(|e| panic!("batch: {e:?}"))
}

async fn seeded() -> SeededWorld {
    let options = WorldOptions::new(SEED, UI_ANCHOR).unwrap_or_else(|e| panic!("options: {e}"));
    seed_world(options)
        .await
        .unwrap_or_else(|e| panic!("seed: {e}"))
}

/// The world's generated data, regenerated with the ids the registry
/// gave the declared channels.
fn generated(world: &SeededWorld) -> Generated {
    let declared: BTreeMap<ChannelKey, ChannelId> = [
        ChannelKey::InternalWiki,
        ChannelKey::Monorepo,
        ChannelKey::IssueTracker,
        ChannelKey::ReleaseBucket,
        ChannelKey::DesignDocs,
    ]
    .into_iter()
    .filter_map(|key| world.scenario.channel(key).map(|id| (key, id)))
    .collect();
    let anchor = Anchor::new(UI_ANCHOR).unwrap_or_else(|e| panic!("anchor: {e}"));
    let config = World::new(SEED, UI_ANCHOR)
        .unwrap_or_else(|e| panic!("world: {e}"))
        .config()
        .clone();
    generate::generate(SEED, anchor, &config, &declared).unwrap_or_else(|e| panic!("generate: {e}"))
}

/// Every conversation, all pages.
async fn every_row(world: &SeededWorld, filter: &ConversationFilter) -> Vec<ConversationRow> {
    let surface = &world.in_process.surface;
    let mut rows = Vec::new();
    let mut page = PageRequest::<ConversationList> {
        size: size(200),
        after: None,
    };
    loop {
        let got = surface
            .conversations(&all(), filter, &page)
            .await
            .unwrap_or_else(|e| panic!("conversations: {e:?}"));
        rows.extend(got.items().iter().cloned());
        match got.next() {
            Some(next) => page.after = Some(next.clone()),
            None => return rows,
        }
    }
}

async fn every_turn(world: &SeededWorld, id: ConversationId) -> Vec<Turn> {
    let surface = &world.in_process.surface;
    let mut turns = Vec::new();
    let mut from = 0;
    loop {
        let page = surface
            .conversation_turns(&all(), id, &window(from, 50))
            .await
            .unwrap_or_else(|e| panic!("turns: {e:?}"))
            .unwrap_or_else(|| panic!("conversation {id:?} has no turns page"));
        let got = u32::try_from(page.turns.len()).unwrap_or(u32::MAX);
        turns.extend(page.turns);
        from += got;
        if got == 0 || from >= page.total {
            return turns;
        }
    }
}

fn agent(world: &SeededWorld, key: &str) -> AgentId {
    world
        .scenario
        .agent(key)
        .unwrap_or_else(|| panic!("agent {key}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_world_backend_serves_consistent_conversations() {
    let world = seeded().await;
    let surface = &world.in_process.surface;
    let directory = &world.in_process.stores.agents;
    let recorded = world.recorded;
    assert!(recorded.exchanges > 1_000, "{recorded:?}");
    assert!(
        recorded.forks > 0 && recorded.compactions > 0,
        "{recorded:?}"
    );
    assert!(recorded.provenance_events > 0, "{recorded:?}");

    // conversations: one row per conversation threading started, every
    // origin kind, rows by canonical agent.
    let rows = every_row(&world, &ConversationFilter::default()).await;
    assert_eq!(
        rows.len(),
        recorded.starts + recorded.forks + recorded.compactions
    );
    for row in &rows {
        assert_eq!(row.agent, AgentDirectory::canonical(directory, row.agent));
        assert!(row.turns > 0);
        assert!(row.started_at <= row.last_turn_at);
    }
    let ids: BTreeSet<ConversationId> = rows.iter().map(|row| row.id).collect();
    assert_eq!(ids.len(), rows.len());
    let fork = rows
        .iter()
        .find(|row| matches!(row.origin, OriginLink::Fork { .. }))
        .unwrap_or_else(|| panic!("no fork"));
    let compaction = rows
        .iter()
        .find(|row| matches!(row.origin, OriginLink::Compaction { .. }))
        .unwrap_or_else(|| panic!("no compaction"));
    assert!(rows.iter().any(|row| row.origin == OriginLink::Root));

    // One agent's list holds that agent's rows only; an alias merged into
    // it lists under it.
    let cc0 = agent(&world, "cc0");
    let mine = every_row(
        &world,
        &ConversationFilter {
            agent: Some(agent(&world, "al0")),
            ..ConversationFilter::default()
        },
    )
    .await;
    assert!(!mine.is_empty());
    assert!(mine.iter().all(|row| row.agent == cc0));

    // conversation: the head agrees with its row; parents list their
    // successors.
    for row in [fork, compaction] {
        let head = surface
            .conversation(&all(), row.id)
            .await
            .unwrap_or_else(|e| panic!("conversation: {e:?}"))
            .unwrap_or_else(|| panic!("no head"));
        assert_eq!(&head.row, row);
        let parent = match row.origin {
            OriginLink::Fork { parent, .. } => parent,
            OriginLink::Compaction { predecessor, .. } => predecessor,
            OriginLink::Root => unreachable!("a fork or compaction"),
        };
        let parent = surface
            .conversation(&all(), parent)
            .await
            .unwrap_or_else(|e| panic!("parent: {e:?}"))
            .unwrap_or_else(|| panic!("no parent"));
        assert!(parent.successors.iter().any(|s| s.conversation == row.id));
    }

    // turns: dense indexes, as many as the row says; a compaction's first
    // turn carries messages over.
    let compacted = every_turn(&world, compaction.id).await;
    assert_eq!(compacted.len(), compaction.turns as usize);
    assert!(
        compacted
            .iter()
            .enumerate()
            .all(|(i, t)| t.index == TurnIndex(i as u32))
    );
    let carried = compacted
        .first()
        .map(|t| {
            t.inputs
                .iter()
                .filter(|m| m.placement == MessagePlacement::CarriedOver)
                .count()
        })
        .unwrap_or(0);
    assert!(
        carried > 0,
        "the compaction's first turn carries nothing over"
    );

    // The world's transmissions the default scope holds (the last day's,
    // and those retention dropped a side of): every confirmed match's
    // reader exchange is a turn of the reader's (canonical) conversations.
    let generated = generated(&world);
    let since = minus(UI_ANCHOR, DAY);
    let retained: BTreeSet<_> = world.scenario.dropped().iter().map(|(id, _)| *id).collect();
    let mut readers: BTreeMap<ExchangeId, AgentId> = BTreeMap::new();
    let mut dropped_readers = BTreeSet::new();
    let dropped: BTreeSet<_> = world
        .scenario
        .dropped()
        .iter()
        .filter(|(_, side)| *side == crosstalk_world::BodySide::Reader)
        .map(|(id, _)| *id)
        .collect();
    let in_scope = generated
        .traffic
        .transmissions
        .iter()
        .filter(|r| r.transmission.opened_at >= since || retained.contains(&r.id()));
    for record in in_scope {
        if let Some(confirmed) = record.confirmed() {
            for matched in confirmed.content().iter() {
                readers.insert(matched.reader_exchange(), matched.reader());
                if dropped.contains(&record.id()) {
                    dropped_readers.insert(matched.reader_exchange());
                }
            }
        }
    }
    assert!(readers.len() > 300, "{}", readers.len());
    assert!(!dropped_readers.is_empty());
    let mut placed = BTreeMap::new();
    let ids: Vec<ExchangeId> = readers.keys().copied().collect();
    for chunk in ids.chunks(IdBatch::<ExchangeId>::MAX) {
        let got = surface
            .exchange_turns(&all(), &batch(chunk.iter().copied()))
            .await
            .unwrap_or_else(|e| panic!("exchange_turns: {e:?}"));
        placed.extend(got);
    }
    assert_eq!(
        placed.len(),
        readers.len(),
        "every reader exchange is threaded"
    );
    for (exchange, placement) in &placed {
        let reader = readers
            .get(exchange)
            .copied()
            .unwrap_or_else(|| panic!("reader"));
        assert_eq!(
            placement.agent,
            AgentDirectory::canonical(directory, reader)
        );
    }

    // A reader turn the world dropped the copy of: its input shows the
    // body dropped, while the scan (at capture) indexed it.
    let dropped_exchange = dropped_readers
        .first()
        .copied()
        .unwrap_or_else(|| panic!("dropped"));
    let at = placed
        .get(&dropped_exchange)
        .unwrap_or_else(|| panic!("placed"));
    let page = surface
        .conversation_turns(&all(), at.conversation, &window(at.turn.0, 1))
        .await
        .unwrap_or_else(|e| panic!("turns: {e:?}"))
        .unwrap_or_else(|| panic!("turns"));
    let turn = page.turns.first().unwrap_or_else(|| panic!("the turn"));
    assert_eq!(turn.exchange, dropped_exchange);
    assert!(
        turn.inputs
            .iter()
            .chain(turn.output.iter())
            .any(|m| matches!(m.parts, MessageParts::BodyDropped(_)))
    );

    // Inbound marks: L4's matches, each origin a span point that agrees
    // with span_points, exchange_turns and span_readers.
    let mut checked = 0;
    let mut failed_turns = 0;
    let mut sample: Vec<&ConversationRow> = rows.iter().filter(|r| r.turns >= 3).collect();
    sample.truncate(120);
    let mut text_checked = false;
    for row in sample {
        let turns = every_turn(&world, row.id).await;
        failed_turns += turns
            .iter()
            .filter(|t| matches!(t.outcome, TurnOutcome::Failed { .. }))
            .count();
        for turn in &turns {
            for message in &turn.inputs {
                let MessageParts::Shown(parts) = &message.parts else {
                    continue;
                };
                for part in parts {
                    for inbound in &part.inbound {
                        let origin = &inbound.origin;
                        assert_ne!(origin.agent, turn.agent, "a match within one agent");
                        let points = surface
                            .span_points(&all(), &batch([origin.span]))
                            .await
                            .unwrap_or_else(|e| panic!("span_points: {e:?}"));
                        assert_eq!(points.get(&origin.span), Some(origin));
                        let sent = origin
                            .turn
                            .unwrap_or_else(|| panic!("the origin is threaded"));
                        let located = surface
                            .exchange_turns(&all(), &batch([origin.exchange]))
                            .await
                            .unwrap_or_else(|e| panic!("exchange_turns: {e:?}"));
                        assert_eq!(located.get(&origin.exchange).map(|p| p.point()), Some(sent));
                        let read_by = surface
                            .span_readers(
                                &all(),
                                origin.span,
                                &PageRequest::<SpanReaderList> {
                                    size: size(200),
                                    after: None,
                                },
                            )
                            .await
                            .unwrap_or_else(|e| panic!("span_readers: {e:?}"))
                            .unwrap_or_else(|| panic!("no readers page"));
                        assert!(read_by.items().iter().any(|r| r.exchange == turn.exchange));
                        if !text_checked {
                            text_checked = true;
                            check_text(&world, row.id, turn, message.hash, part.index).await;
                        }
                        checked += 1;
                    }
                }
            }
        }
        if checked > 40 {
            break;
        }
    }
    assert!(checked > 0, "no inbound mark in the sampled conversations");
    assert!(text_checked);
    let _ = failed_turns;

    // Permissions, unchanged: View for the structure, Content for text.
    let no_view = caller(
        OPERATOR_ONCALL,
        PermissionSet::of([Permission::Content, Permission::Triage]),
    );
    let no_content = caller(OPERATOR_ONCALL, PermissionSet::of([Permission::View]));
    let first_page = PageRequest::<ConversationList> {
        size: size(10),
        after: None,
    };
    let forbidden = |missing| QueryError::Forbidden { missing };
    assert_eq!(
        surface
            .conversations(&no_view, &ConversationFilter::default(), &first_page)
            .await
            .err(),
        Some(forbidden(Permission::View))
    );
    assert_eq!(
        surface.conversation(&no_view, fork.id).await.err(),
        Some(forbidden(Permission::View))
    );
    assert_eq!(
        surface
            .conversation_turns(&no_view, fork.id, &window(0, 5))
            .await
            .err(),
        Some(forbidden(Permission::View))
    );
    assert!(
        surface
            .conversation_turns(&no_content, fork.id, &window(0, 5))
            .await
            .is_ok()
    );
    assert_eq!(
        surface
            .conversation_text(&no_content, fork.id, &window(0, 5), TextLimit::DEFAULT)
            .await
            .err(),
        Some(forbidden(Permission::Content))
    );
    assert_eq!(
        surface
            .exchange_turns(&no_view, &batch([dropped_exchange]))
            .await
            .err(),
        Some(forbidden(Permission::View))
    );
    assert_eq!(
        surface
            .span_points(&no_view, &batch([SpanId::from_ulid(1)]))
            .await
            .err(),
        Some(forbidden(Permission::View))
    );

    world.in_process.shutdown().await;
}

/// The text of `turn` (Content) holds the part the inbound mark sits in.
async fn check_text(
    world: &SeededWorld,
    conversation: ConversationId,
    turn: &Turn,
    message: crosstalk_spec::ids::MessageHash,
    part: u16,
) {
    let surface = &world.in_process.surface;
    let text = surface
        .conversation_text(
            &all(),
            conversation,
            &window(turn.index.0, 1),
            TextLimit::DEFAULT,
        )
        .await
        .unwrap_or_else(|e| panic!("conversation_text: {e:?}"))
        .unwrap_or_else(|| panic!("no text"));
    let turn_text = text
        .turns
        .first()
        .unwrap_or_else(|| panic!("the turn's text"));
    assert_eq!(turn_text.index, turn.index);
    assert_eq!(turn_text.inputs.len(), turn.inputs.len());
    let shown = turn_text
        .inputs
        .iter()
        .find(|m| m.hash == message)
        .unwrap_or_else(|| panic!("the message's text"));
    let BodyText::Shown(parts) = &shown.body else {
        panic!("the body is shown");
    };
    let slice = parts
        .get(usize::from(part))
        .and_then(Option::as_ref)
        .unwrap_or_else(|| panic!("the part's text"));
    assert!(!slice.text().is_empty());
    let whole = surface
        .part_text(
            &all(),
            PartRef {
                message,
                index: part,
            },
            TextSlice {
                from: 0,
                limit: TextLimit::DEFAULT,
            },
        )
        .await
        .unwrap_or_else(|e| panic!("part_text: {e:?}"))
        .unwrap_or_else(|| panic!("no part text"));
    assert_eq!(whole.text(), slice.text());
    let no_content = caller(OPERATOR_ONCALL, PermissionSet::of([Permission::View]));
    assert_eq!(
        surface
            .part_text(
                &no_content,
                PartRef {
                    message,
                    index: part
                },
                TextSlice {
                    from: 0,
                    limit: TextLimit::DEFAULT,
                },
            )
            .await
            .err(),
        Some(QueryError::Forbidden {
            missing: Permission::Content
        })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_same_seed_records_the_same_conversations() {
    let (a, b) = (seeded().await, seeded().await);
    assert_eq!(a.recorded, b.recorded);
    let (rows_a, rows_b) = (
        every_row(&a, &ConversationFilter::default()).await,
        every_row(&b, &ConversationFilter::default()).await,
    );
    assert_eq!(rows_a, rows_b);
    for row in rows_a.iter().filter(|r| r.turns > 2).take(10) {
        assert_eq!(every_turn(&a, row.id).await, every_turn(&b, row.id).await);
    }
    a.in_process.shutdown().await;
    b.in_process.shutdown().await;
}
