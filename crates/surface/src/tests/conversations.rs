//! The conversation reads (`surface.conversation.*`, INV-1000..1029) over
//! the reference stores and the conversation fakes: one story of agents,
//! conversations, turns, spans, matches and transmissions, read back
//! through `QueryApi`.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::transmission::{
    Confirmed, DelegationDirection, Route, Transmission, TransmissionState,
};
use crosstalk_spec::derived::provenance::matching::{Carrier, ContentMatch, MatchKind};
use crosstalk_spec::derived::provenance::span::{RelaySource, Span, SpanLocation, SpanState};
use crosstalk_spec::ids::{
    AgentId, ChannelId, ConversationId, ExchangeId, MergeId, MessageHash, SpanId, TransmissionId,
};
use crosstalk_spec::interfaces::l1_canonical::exchanges::StoredExchange;
use crosstalk_spec::interfaces::l2_transport::BlobStore;
use crosstalk_spec::interfaces::l3_reconstruction::conversations::{
    StoredConversation, StoredTurn, ThreadOutcomeKind, TranscriptEntry, TurnIndex, TurnPoint,
    TurnWindow,
};
use crosstalk_spec::interfaces::l4_provenance::reads::{ForwardStatus, ScanStatus, StoredSpan};
use crosstalk_spec::interfaces::l8_surface::conversation::text::{
    BodyText, PartText, TextLimit, TextSlice,
};
use crosstalk_spec::interfaces::l8_surface::conversation::turn::{
    IncrementHistory, MessageParts, MessagePlacement, OriginatedStatus, PartKind, RelayedFrom,
    SpanOrigin, Turn, TurnContinuation, TurnMessage, TurnOutcome, TurnPage,
};
use crosstalk_spec::interfaces::l8_surface::conversation::{
    ConversationFilter, ConversationHead, ConversationRow, ExchangePlacement, OriginLink,
    ReplayFilter, SpanPoint, SuccessorKind,
};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionStateKind;
use crosstalk_spec::interfaces::l8_surface::{
    ActionOutcome, ActionRequest, Caller, InputError, OperatorAction, OperatorActions, Permission,
    QueryApi, QueryError,
};
use crosstalk_spec::observed::client::TrafficSource;
use crosstalk_spec::observed::conversation::{Conversation, ConversationOrigin};
use crosstalk_spec::observed::exchange::{ConnectionId, ExchangeFailure};
use crosstalk_spec::observed::message::{MessageBody, PartRef, Role, ToolCallId, encoding};
use crosstalk_spec::paging::PageSize;
use crosstalk_spec::support::{ByteRange, NonEmpty, Timestamp};
use crosstalk_testkit::build::ExchangeBuilder;
use crosstalk_testkit::build::message::{
    assistant, assistant_text, system_text, tool_call, tool_result, user_text,
};
use crosstalk_testkit::ids::Ids;

use super::page;
use super::world::{Fixture, Who, minute};

fn window(from: u32, size: u16) -> TurnWindow {
    TurnWindow {
        from: TurnIndex(from),
        size: PageSize::new(size).unwrap_or_else(|error| panic!("{error:?}")),
    }
}

fn range(start: u32, end: u32) -> ByteRange {
    ByteRange::new(start, end).unwrap_or_else(|_| panic!("range {start}..{end}"))
}

fn located(message: MessageHash, index: u16, start: u32, end: u32) -> SpanLocation {
    SpanLocation {
        part: PartRef { message, index },
        range: range(start, end),
    }
}

const PLAN: &str = "release plan: ship on friday";
const ANSWER: &str = "the plan says friday, I will tag it";

/// Who and what the story holds.
struct Story {
    planner: AgentId,
    coder: AgentId,
    alias: AgentId,
    child: AgentId,
    /// The planner's conversation: one turn writing the plan.
    plans: ConversationId,
    /// The coder's: turn 0 reads the plan through the wiki and answers,
    /// turn 1 (attributed to the coder's alias) has a mid-conversation
    /// system turn, a dropped body and fails.
    work: ConversationId,
    /// The child's, delegated from the coder's answer.
    task: ConversationId,
    /// A compaction of `work`, carrying its first user turn over.
    resumed: ConversationId,
    /// A fork of `work` after its first turn.
    branch: ConversationId,
    /// A root opened by an increment whose previous response was unseen.
    unseen: ConversationId,
    /// The alias's own conversation.
    aliased: ConversationId,
    plan_exchange: ExchangeId,
    work_exchanges: [ExchangeId; 2],
    task_exchange: ExchangeId,
    /// The planner's originated span over the plan, its forwarded span and
    /// its relayed one; the coder's originated answer span; the coder's
    /// span on its tool call.
    plan_span: SpanId,
    forwarded: SpanId,
    relayed: SpanId,
    answer_span: SpanId,
    call_span: SpanId,
    /// Messages by name.
    messages: BTreeMap<&'static str, MessageHash>,
    /// The wiki transmission (planner to coder) and the delegation (coder
    /// to child).
    wiki: TransmissionId,
    delegation: TransmissionId,
    /// The coder's read of the plan.
    plan_read: ContentMatch,
}

async fn put(fixture: &Fixture, body: &MessageBody) -> MessageHash {
    fixture
        .world
        .blobs
        .put(&encoding::encode(body))
        .await
        .unwrap_or_else(|error| panic!("blob: {error:?}"))
}

fn entry(
    ordinal: u32,
    message: MessageHash,
    role: Role,
    exchange: ExchangeId,
    history_index: Option<u32>,
    output: bool,
) -> TranscriptEntry {
    TranscriptEntry {
        ordinal,
        message,
        role,
        exchange,
        history_index,
        output,
        carried_over: false,
    }
}

fn turn(
    index: u32,
    exchange: ExchangeId,
    agent: AgentId,
    started_at: Timestamp,
    outcome: ThreadOutcomeKind,
    history_end: u32,
    entries: Vec<TranscriptEntry>,
) -> StoredTurn {
    StoredTurn {
        index: TurnIndex(index),
        exchange,
        agent,
        started_at,
        outcome,
        history_end,
        entries,
    }
}

fn conversation(
    id: ConversationId,
    agent: AgentId,
    origin: ConversationOrigin,
    turns: &[StoredTurn],
    source: TrafficSource,
) -> StoredConversation {
    let messages = turns
        .iter()
        .flat_map(|turn| turn.entries.iter())
        .filter(|entry| entry.history_index.is_some())
        .map(|entry| entry.message)
        .collect();
    StoredConversation {
        conversation: Conversation {
            id,
            agent,
            messages,
            origin,
        },
        source,
        started_at: turns.first().map_or(minute(0), |turn| turn.started_at),
        last_turn_at: turns.last().map_or(minute(0), |turn| turn.started_at),
        turns: u32::try_from(turns.len()).unwrap_or(u32::MAX),
    }
}

fn exchange_record(
    ids: &mut Ids,
    id: ExchangeId,
    at: Timestamp,
    request: Vec<MessageHash>,
    adjust: impl FnOnce(ExchangeBuilder) -> ExchangeBuilder,
) -> StoredExchange {
    StoredExchange {
        exchange: adjust(
            ExchangeBuilder::new(ids)
                .with_id(id)
                .started_at(at)
                .request(request),
        )
        .build(),
        warnings: Vec::new(),
    }
}

fn content(
    origin: SpanId,
    origin_agent: AgentId,
    reader: AgentId,
    exchange: ExchangeId,
    read_at: SpanLocation,
    carrier: Carrier,
) -> ContentMatch {
    let bytes = NonZeroU32::new(read_at.range.len().get()).unwrap_or(NonZeroU32::MIN);
    ContentMatch::new(
        origin,
        origin_agent,
        reader,
        exchange,
        read_at,
        carrier,
        MatchKind::Exact,
        bytes,
    )
    .unwrap_or_else(|error| panic!("match: {error:?}"))
}

fn confirmed_transmission(
    id: TransmissionId,
    to: AgentId,
    route: Route,
    at: Timestamp,
    matches: Vec<ContentMatch>,
) -> Transmission {
    let mut matches = matches.into_iter();
    let first = matches.next().unwrap_or_else(|| panic!("a match"));
    let mut confirmed = Confirmed::new(NonEmpty::new(first), Vec::new(), at)
        .unwrap_or_else(|error| panic!("{error:?}"));
    for more in matches {
        confirmed
            .extend(more)
            .unwrap_or_else(|error| panic!("{error:?}"));
    }
    Transmission {
        id,
        to,
        route,
        opened_at: at,
        state: TransmissionState::Confirmed(confirmed),
    }
}

impl Story {
    async fn build(fixture: &Fixture) -> Self {
        let mut ids = Ids::new();
        let (planner, coder, alias, child) = (ids.agent(), ids.agent(), ids.agent(), ids.agent());
        for (agent, at) in [(planner, 0), (coder, 1), (alias, 2), (child, 3)] {
            fixture.agent(agent, minute(at)).await;
        }
        let world = &fixture.world;
        // Messages.
        let system = put(fixture, &system_text("you are a planner")).await;
        let ask = put(fixture, &user_text("write the release plan")).await;
        let plan = put(fixture, &assistant_text(PLAN)).await;
        let coder_system = put(fixture, &system_text("you are a coder")).await;
        let task = put(fixture, &user_text("read the plan on the wiki")).await;
        let fetched = put(fixture, &tool_result("call_wiki", PLAN)).await;
        let answer = put(
            fixture,
            &assistant(vec![
                crosstalk_spec::observed::message::AssistantPart::Text(
                    crosstalk_spec::observed::message::Text(ANSWER.to_owned()),
                ),
                tool_call(
                    "call_task",
                    "Task",
                    &serde_json::json!({"prompt": "tag friday"}),
                ),
            ]),
        )
        .await;
        let reminder = put(fixture, &system_text("reminder: run the tests")).await;
        // Never stored: its body is dropped.
        let dropped = crosstalk_testkit::build::message::content_hash(&user_text("tag it now"));
        let child_task = put(fixture, &user_text("{\"prompt\": \"tag friday\"}")).await;
        let child_done = put(fixture, &assistant_text("tagged")).await;
        let summary = put(fixture, &user_text("Summary: the plan says friday.")).await;
        let resumed_answer = put(fixture, &assistant_text("resuming")).await;
        let branch_tool = put(fixture, &tool_result("call_wiki", "page not found")).await;
        let branch_answer = put(fixture, &assistant_text("no plan yet")).await;
        let increment = put(fixture, &user_text("continue")).await;
        let increment_answer = put(fixture, &assistant_text("continuing")).await;
        let alias_ask = put(fixture, &user_text("alias work")).await;
        let alias_answer = put(fixture, &assistant_text("alias answer")).await;
        // Exchanges.
        let (plan_exchange, work0, work1, task_exchange) = (
            ids.exchange(),
            ids.exchange(),
            ids.exchange(),
            ids.exchange(),
        );
        let (resumed_exchange, branch_exchange, unseen_exchange, alias_exchange) = (
            ids.exchange(),
            ids.exchange(),
            ids.exchange(),
            ids.exchange(),
        );
        let records = [
            exchange_record(&mut ids, plan_exchange, minute(1), vec![system, ask], |b| {
                b.response(plan)
            }),
            exchange_record(
                &mut ids,
                work0,
                minute(2),
                vec![coder_system, task, fetched],
                |b| b.response(answer),
            ),
            exchange_record(
                &mut ids,
                work1,
                minute(3),
                vec![coder_system, task, fetched, answer, reminder, dropped],
                |b| b.failed(ExchangeFailure::UpstreamUnreachable),
            ),
            exchange_record(&mut ids, task_exchange, minute(4), vec![child_task], |b| {
                b.response(child_done)
            }),
            exchange_record(
                &mut ids,
                resumed_exchange,
                minute(5),
                vec![coder_system, summary, task],
                |b| b.response(resumed_answer),
            ),
            exchange_record(
                &mut ids,
                branch_exchange,
                minute(6),
                vec![coder_system, task, branch_tool],
                |b| b.response(branch_answer),
            ),
            exchange_record(&mut ids, unseen_exchange, minute(7), vec![increment], |b| {
                b.increment(
                    "resp_unseen",
                    Some(ConnectionId(0x0192_5f3e_0000_0000_0000_0000_0000_0001)),
                )
                .response(increment_answer)
            }),
            exchange_record(&mut ids, alias_exchange, minute(8), vec![alias_ask], |b| {
                b.response(alias_answer)
            }),
        ];
        for record in records {
            world.exchanges.put(record);
        }
        // Conversations and turns.
        let (plans, work, task_conversation, resumed, branch, unseen, aliased) = (
            ConversationId::from_ulid(0xC0_01),
            ConversationId::from_ulid(0xC0_02),
            ConversationId::from_ulid(0xC0_03),
            ConversationId::from_ulid(0xC0_04),
            ConversationId::from_ulid(0xC0_05),
            ConversationId::from_ulid(0xC0_06),
            ConversationId::from_ulid(0xC0_07),
        );
        let plan_turns = vec![turn(
            0,
            plan_exchange,
            planner,
            minute(1),
            ThreadOutcomeKind::Starts,
            2,
            vec![
                entry(0, system, Role::System, plan_exchange, None, false),
                entry(1, ask, Role::User, plan_exchange, Some(0), false),
                entry(2, plan, Role::Assistant, plan_exchange, Some(1), true),
            ],
        )];
        let work_turns = vec![
            turn(
                0,
                work0,
                coder,
                minute(2),
                ThreadOutcomeKind::Starts,
                3,
                vec![
                    entry(0, coder_system, Role::System, work0, None, false),
                    entry(1, task, Role::User, work0, Some(0), false),
                    entry(2, fetched, Role::Tool, work0, Some(1), false),
                    entry(3, answer, Role::Assistant, work0, Some(2), true),
                ],
            ),
            turn(
                1,
                work1,
                alias,
                minute(3),
                ThreadOutcomeKind::Extends,
                4,
                vec![
                    entry(4, reminder, Role::System, work1, None, false),
                    entry(5, dropped, Role::User, work1, Some(3), false),
                ],
            ),
        ];
        let task_turns = vec![turn(
            0,
            task_exchange,
            child,
            minute(4),
            ThreadOutcomeKind::Starts,
            2,
            vec![
                entry(0, child_task, Role::User, task_exchange, Some(0), false),
                entry(1, child_done, Role::Assistant, task_exchange, Some(1), true),
            ],
        )];
        let mut carried = entry(2, task, Role::User, resumed_exchange, Some(1), false);
        carried.carried_over = true;
        let resumed_turns = vec![turn(
            0,
            resumed_exchange,
            coder,
            minute(5),
            ThreadOutcomeKind::Compacts,
            3,
            vec![
                entry(0, coder_system, Role::System, resumed_exchange, None, false),
                entry(1, summary, Role::User, resumed_exchange, Some(0), false),
                carried,
                entry(
                    3,
                    resumed_answer,
                    Role::Assistant,
                    resumed_exchange,
                    Some(2),
                    true,
                ),
            ],
        )];
        let branch_turns = vec![turn(
            0,
            branch_exchange,
            coder,
            minute(6),
            ThreadOutcomeKind::Forks,
            4,
            vec![
                entry(2, branch_tool, Role::Tool, branch_exchange, Some(1), false),
                entry(
                    3,
                    branch_answer,
                    Role::Assistant,
                    branch_exchange,
                    Some(2),
                    true,
                ),
            ],
        )];
        let unseen_turns = vec![turn(
            0,
            unseen_exchange,
            coder,
            minute(7),
            ThreadOutcomeKind::Starts,
            2,
            vec![
                entry(0, increment, Role::User, unseen_exchange, Some(0), false),
                entry(
                    1,
                    increment_answer,
                    Role::Assistant,
                    unseen_exchange,
                    Some(1),
                    true,
                ),
            ],
        )];
        let aliased_turns = vec![turn(
            0,
            alias_exchange,
            alias,
            minute(8),
            ThreadOutcomeKind::Starts,
            2,
            vec![
                entry(0, alias_ask, Role::User, alias_exchange, Some(0), false),
                entry(
                    1,
                    alias_answer,
                    Role::Assistant,
                    alias_exchange,
                    Some(1),
                    true,
                ),
            ],
        )];
        let live = TrafficSource::Live;
        let conversations = &world.conversations;
        conversations.put(
            conversation(
                plans,
                planner,
                ConversationOrigin::Root,
                &plan_turns,
                live.clone(),
            ),
            plan_turns,
        );
        conversations.put(
            conversation(
                work,
                coder,
                ConversationOrigin::Root,
                &work_turns,
                live.clone(),
            ),
            work_turns,
        );
        conversations.put(
            conversation(
                task_conversation,
                child,
                ConversationOrigin::Root,
                &task_turns,
                live.clone(),
            ),
            task_turns,
        );
        conversations.put(
            conversation(
                resumed,
                coder,
                ConversationOrigin::Compaction { predecessor: work },
                &resumed_turns,
                live.clone(),
            ),
            resumed_turns,
        );
        conversations.put(
            conversation(
                branch,
                coder,
                ConversationOrigin::Fork {
                    parent: work,
                    shared_prefix: 3,
                },
                &branch_turns,
                live.clone(),
            ),
            branch_turns,
        );
        conversations.put(
            conversation(
                unseen,
                coder,
                ConversationOrigin::Root,
                &unseen_turns,
                live.clone(),
            ),
            unseen_turns,
        );
        conversations.put(
            conversation(
                aliased,
                alias,
                ConversationOrigin::Root,
                &aliased_turns,
                TrafficSource::Replay {
                    corpus: crosstalk_spec::observed::client::CorpusId("salt-nlp".into()),
                },
            ),
            aliased_turns,
        );
        // Spans and matches.
        let (plan_span, forwarded, relayed, answer_span, call_span, common) = (
            ids.span(),
            ids.span(),
            ids.span(),
            ids.span(),
            ids.span(),
            ids.span(),
        );
        let planner_spans = vec![
            StoredSpan {
                span: Span {
                    id: plan_span,
                    location: located(plan, 0, 0, 12),
                    agent: planner,
                    exchange: plan_exchange,
                    state: SpanState::Propagated {
                        indexed_at: minute(1),
                        first_hit_at: minute(2),
                        hits: NonZeroU32::MIN,
                    },
                },
                forward: None,
            },
            StoredSpan {
                span: Span {
                    id: relayed,
                    location: located(plan, 0, 22, 28),
                    agent: planner,
                    exchange: plan_exchange,
                    state: SpanState::Relayed {
                        source: RelaySource::Span(answer_span),
                    },
                },
                forward: None,
            },
            StoredSpan {
                span: Span {
                    id: forwarded,
                    location: located(plan, 0, 14, 20),
                    agent: planner,
                    exchange: plan_exchange,
                    state: SpanState::Relayed {
                        source: RelaySource::Input(ask),
                    },
                },
                forward: Some(ForwardStatus::Indexed { at: minute(1) }),
            },
        ];
        let _ = common;
        let coder_spans = vec![
            StoredSpan {
                span: Span {
                    id: answer_span,
                    location: located(answer, 0, 0, 8),
                    agent: coder,
                    exchange: work0,
                    state: SpanState::Indexed { at: minute(2) },
                },
                forward: None,
            },
            StoredSpan {
                span: Span {
                    id: call_span,
                    location: located(answer, 1, 0, 10),
                    agent: coder,
                    exchange: work0,
                    state: SpanState::Indexed { at: minute(2) },
                },
                forward: None,
            },
        ];
        let tool = || Carrier::ToolResult(ToolCallId("call_wiki".into()));
        let plan_read = content(
            plan_span,
            planner,
            coder,
            work0,
            located(fetched, 0, 0, 12),
            tool(),
        );
        let forwarded_read = content(
            forwarded,
            planner,
            coder,
            work0,
            located(fetched, 0, 14, 20),
            tool(),
        );
        let delegated = content(
            call_span,
            coder,
            child,
            task_exchange,
            located(child_task, 0, 0, 10),
            Carrier::UserTurn,
        );
        let provenance = &world.provenance;
        provenance.put(
            plan_exchange,
            minute(1),
            ScanStatus::Indexed { at: minute(1) },
            planner_spans,
            Vec::new(),
        );
        provenance.put(
            work0,
            minute(2),
            ScanStatus::Indexed { at: minute(2) },
            coder_spans,
            vec![forwarded_read.clone(), plan_read.clone()],
        );
        provenance.put(
            work1,
            minute(3),
            ScanStatus::Failed {
                at: minute(3),
                failure:
                    crosstalk_spec::interfaces::l4_provenance::reads::ScanFailureKind::BodyMissing,
            },
            Vec::new(),
            Vec::new(),
        );
        provenance.put(
            task_exchange,
            minute(4),
            ScanStatus::Scanned { at: minute(4) },
            Vec::new(),
            vec![delegated.clone()],
        );
        // Transmissions: the wiki one holds both of the coder's reads.
        let (wiki, delegation) = (ids.transmission(), ids.transmission());
        fixture
            .transmission(&confirmed_transmission(
                wiki,
                coder,
                Route::Channel(ChannelId::from_ulid(0xA1)),
                minute(2),
                vec![plan_read.clone(), forwarded_read],
            ))
            .await;
        fixture
            .transmission(&confirmed_transmission(
                delegation,
                child,
                Route::Delegation(DelegationDirection::ParentToChild),
                minute(4),
                vec![delegated],
            ))
            .await;
        let messages = BTreeMap::from([
            ("system", system),
            ("ask", ask),
            ("plan", plan),
            ("coder_system", coder_system),
            ("task", task),
            ("fetched", fetched),
            ("answer", answer),
            ("reminder", reminder),
            ("dropped", dropped),
            ("summary", summary),
        ]);
        Self {
            planner,
            coder,
            alias,
            child,
            plans,
            work,
            task: task_conversation,
            resumed,
            branch,
            unseen,
            aliased,
            plan_exchange,
            work_exchanges: [work0, work1],
            task_exchange,
            plan_span,
            forwarded,
            relayed,
            answer_span,
            call_span,
            messages,
            wiki,
            delegation,
            plan_read,
        }
    }
}

async fn story() -> (Fixture, Story) {
    let fixture = Fixture::new().await;
    let story = Story::build(&fixture).await;
    (fixture, story)
}

async fn turns(fixture: &Fixture, caller: &Caller, id: ConversationId) -> TurnPage {
    match fixture
        .surface
        .conversation_turns(caller, id, &window(0, 20))
        .await
    {
        Ok(Some(page)) => page,
        other => panic!("turns of {id:?}: {other:?}"),
    }
}

async fn head(fixture: &Fixture, caller: &Caller, id: ConversationId) -> ConversationHead {
    match fixture.surface.conversation(caller, id).await {
        Ok(Some(head)) => head,
        other => panic!("head of {id:?}: {other:?}"),
    }
}

async fn listed(
    fixture: &Fixture,
    caller: &Caller,
    filter: &ConversationFilter,
) -> Vec<ConversationRow> {
    match fixture
        .surface
        .conversations(caller, filter, &page(50))
        .await
    {
        Ok(page) => page.into_parts().0,
        Err(error) => panic!("list: {error:?}"),
    }
}

fn ids_of(rows: &[ConversationRow]) -> BTreeSet<ConversationId> {
    rows.iter().map(|row| row.id).collect()
}

async fn merge(fixture: &Fixture, from: AgentId, into: AgentId) -> MergeId {
    let admin = fixture.caller(Who::Admin).await;
    match fixture
        .surface
        .request(&admin, ActionRequest::MergeAgents { from, into })
        .await
    {
        Ok(ActionOutcome::Merged(merge)) => merge,
        other => panic!("merge: {other:?}"),
    }
}

async fn unmerge(fixture: &Fixture, merge: MergeId) {
    let admin = fixture.caller(Who::Admin).await;
    let outcome = fixture
        .surface
        .act(&admin, OperatorAction::Unmerge { merge })
        .await;
    assert!(outcome.is_ok(), "unmerge: {outcome:?}");
}

fn shown(
    message: &TurnMessage,
) -> &[crosstalk_spec::interfaces::l8_surface::conversation::turn::PartShape] {
    match &message.parts {
        MessageParts::Shown(parts) => parts,
        MessageParts::BodyDropped(_) => panic!("{:?} is dropped", message.hash),
    }
}

/// INV-1000 `surface.conversation.list-canonical`: an agent's list holds
/// its cluster's conversations, an alias's after a merge and not after an
/// unmerge, each row naming the canonical agent.
#[tokio::test]
async fn list_follows_merges_and_unmerges() {
    let (fixture, story) = story().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let of = |agent| ConversationFilter {
        agent: Some(agent),
        ..ConversationFilter::default()
    };
    let coder_own = BTreeSet::from([story.work, story.resumed, story.branch, story.unseen]);
    assert_eq!(
        ids_of(&listed(&fixture, &viewer, &of(story.coder)).await),
        coder_own
    );
    let merged = merge(&fixture, story.alias, story.coder).await;
    let rows = listed(&fixture, &viewer, &of(story.coder)).await;
    let mut with_alias = coder_own.clone();
    with_alias.insert(story.aliased);
    assert_eq!(ids_of(&rows), with_alias);
    assert!(
        rows.iter().all(|row| row.agent == story.coder),
        "canonical agents"
    );
    assert_eq!(
        ids_of(&listed(&fixture, &viewer, &of(story.alias)).await),
        with_alias,
        "an alias lists its canonical agent's conversations"
    );
    unmerge(&fixture, merged).await;
    assert_eq!(
        ids_of(&listed(&fixture, &viewer, &of(story.coder)).await),
        coder_own
    );
    let rows = listed(&fixture, &viewer, &of(story.alias)).await;
    assert_eq!(ids_of(&rows), BTreeSet::from([story.aliased]));
    assert_eq!(rows[0].agent, story.alias);
    // Newest first, every agent without a filter; an unknown agent none.
    let every = listed(&fixture, &viewer, &ConversationFilter::default()).await;
    let order: Vec<ConversationId> = every.iter().map(|row| row.id).collect();
    let mut newest_first = order.clone();
    newest_first.sort_unstable_by(|a, b| b.cmp(a));
    assert_eq!(order, newest_first);
    assert_eq!(every.len(), 7);
    let unknown = AgentId::from_ulid(0xDEAD_BEEF);
    assert!(listed(&fixture, &viewer, &of(unknown)).await.is_empty());
    let replayed = ConversationFilter {
        replay: ReplayFilter::Only { corpus: None },
        ..ConversationFilter::default()
    };
    assert_eq!(
        ids_of(&listed(&fixture, &viewer, &replayed).await),
        BTreeSet::from([story.aliased])
    );
}

/// INV-1003 `surface.conversation.turns-window`.
#[tokio::test]
async fn turns_window_is_the_index_range() {
    let (fixture, story) = story().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let read = async |from, size| match fixture
        .surface
        .conversation_turns(&viewer, story.work, &window(from, size))
        .await
    {
        Ok(Some(page)) => (
            page.total,
            page.turns
                .iter()
                .map(|turn| turn.index.0)
                .collect::<Vec<_>>(),
        ),
        other => panic!("{other:?}"),
    };
    assert_eq!(read(0, 20).await, (2, vec![0, 1]));
    assert_eq!(read(1, 1).await, (2, vec![1]));
    assert_eq!(read(2, 20).await, (2, Vec::new()));
    assert_eq!(read(99, 20).await, (2, Vec::new()));
    let mut ids = Ids::new();
    let unknown = fixture
        .surface
        .conversation_turns(
            &viewer,
            ConversationId::from_ulid(ids.agent().as_ulid()),
            &window(0, 20),
        )
        .await;
    assert_eq!(unknown, Ok(None));
}

/// INV-1007 `surface.conversation.inputs-are-transcript`: a turn's inputs
/// then output are its exchange's entries in ordinal order, system turns
/// where the request put them.
#[tokio::test]
async fn turn_messages_are_the_turn_entries() {
    let (fixture, story) = story().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let page = turns(&fixture, &viewer, story.work).await;
    let m = |name: &str| story.messages[name];
    let hashes = |turn: &Turn| {
        turn.inputs
            .iter()
            .chain(turn.output.iter())
            .map(|message| (message.hash, message.role))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        hashes(&page.turns[0]),
        vec![
            (m("coder_system"), Role::System),
            (m("task"), Role::User),
            (m("fetched"), Role::Tool),
            (m("answer"), Role::Assistant)
        ]
    );
    assert_eq!(
        hashes(&page.turns[1]),
        vec![(m("reminder"), Role::System), (m("dropped"), Role::User)]
    );
    assert_eq!(page.turns[0].exchange, story.work_exchanges[0]);
}

/// INV-1009 `surface.conversation.output-is-response`: a completed
/// turn's output is its response; a failed one with no partial has none.
#[tokio::test]
async fn output_is_the_response_or_partial() {
    let (fixture, story) = story().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let page = turns(&fixture, &viewer, story.work).await;
    let output = page.turns[0]
        .output
        .as_ref()
        .unwrap_or_else(|| panic!("an output"));
    assert_eq!(output.hash, story.messages["answer"]);
    assert_eq!(output.placement, MessagePlacement::Output);
    assert!(matches!(
        page.turns[0].outcome,
        TurnOutcome::Completed { .. }
    ));
    assert_eq!(page.turns[1].output, None);
    assert!(matches!(
        page.turns[1].outcome,
        TurnOutcome::Failed {
            failure: ExchangeFailure::UpstreamUnreachable,
            ..
        }
    ));
    let placements: Vec<MessagePlacement> = page
        .turns
        .iter()
        .flat_map(|turn| turn.inputs.iter().map(|message| message.placement))
        .collect();
    assert!(
        placements
            .iter()
            .all(|placement| *placement == MessagePlacement::New)
    );
}

/// INV-1008 `surface.conversation.carried-over`.
#[tokio::test]
async fn carried_over_messages_are_placed_and_counted() {
    let (fixture, story) = story().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let page = turns(&fixture, &viewer, story.resumed).await;
    let placements: Vec<(MessageHash, MessagePlacement)> = page.turns[0]
        .inputs
        .iter()
        .map(|message| (message.hash, message.placement))
        .collect();
    assert_eq!(
        placements,
        vec![
            (story.messages["coder_system"], MessagePlacement::New),
            (story.messages["summary"], MessagePlacement::New),
            (story.messages["task"], MessagePlacement::CarriedOver),
        ]
    );
    let row = head(&fixture, &viewer, story.resumed).await.row;
    assert_eq!(
        row.origin,
        OriginLink::Compaction {
            predecessor: story.work,
            predecessor_agent: story.coder,
            carried_over: 1
        }
    );
}

/// INV-1010 `surface.conversation.origin-resolved` and INV-1011
/// `surface.conversation.successors-complete`.
#[tokio::test]
async fn origin_links_are_resolved() {
    let (fixture, story) = story().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let branch = head(&fixture, &viewer, story.branch).await;
    assert_eq!(
        branch.row.origin,
        OriginLink::Fork {
            parent: story.work,
            parent_agent: story.coder,
            shared_prefix: 3,
            branch_turn: Some(TurnIndex(0)),
        }
    );
    let work = head(&fixture, &viewer, story.work).await;
    assert_eq!(work.row.origin, OriginLink::Root);
    let successors: Vec<(ConversationId, SuccessorKind)> = work
        .successors
        .iter()
        .map(|successor| (successor.conversation, successor.kind))
        .collect();
    assert_eq!(
        successors,
        vec![
            (story.resumed, SuccessorKind::Compaction),
            (
                story.branch,
                SuccessorKind::Fork {
                    shared_prefix: 3,
                    branch_turn: Some(TurnIndex(0))
                }
            ),
        ]
    );
    assert_eq!(
        fixture
            .surface
            .conversation(&viewer, ConversationId::from_ulid(1))
            .await,
        Ok(None)
    );
}

/// INV-1012 `surface.conversation.inbound-are-matches`: each match read in
/// the turn sits on its part, ordered by range start.
#[tokio::test]
async fn inbound_marks_are_the_matches_read_in_the_turn() {
    let (fixture, story) = story().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let page = turns(&fixture, &viewer, story.work).await;
    let fetched = &page.turns[0].inputs[2];
    let parts = shown(fetched);
    assert_eq!(parts.len(), 1);
    let starts: Vec<u32> = parts[0]
        .inbound
        .iter()
        .map(|mark| mark.range.start())
        .collect();
    assert_eq!(starts, vec![0, 14], "both matches, by range start");
    let origins: Vec<SpanId> = parts[0]
        .inbound
        .iter()
        .map(|mark| mark.origin.span)
        .collect();
    assert_eq!(origins, vec![story.plan_span, story.forwarded]);
    let origin = &parts[0].inbound[0].origin;
    assert_eq!(origin.agent, story.planner);
    assert_eq!(origin.exchange, story.plan_exchange);
    assert_eq!(
        origin.turn,
        Some(TurnPoint {
            conversation: story.plans,
            turn: TurnIndex(0)
        })
    );
    // No other message of the turn carries a mark.
    let others: usize = page.turns[0]
        .inputs
        .iter()
        .filter(|message| message.hash != story.messages["fetched"])
        .map(|message| {
            shown(message)
                .iter()
                .map(|part| part.inbound.len())
                .sum::<usize>()
        })
        .sum();
    assert_eq!(others, 0);
}

/// INV-1013 `surface.conversation.inbound-transmission`.
#[tokio::test]
async fn inbound_marks_carry_the_holding_transmission() {
    let (fixture, story) = story().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let page = turns(&fixture, &viewer, story.work).await;
    let parts = shown(&page.turns[0].inputs[2]);
    for mark in &parts[0].inbound {
        let transmission = mark.transmission.as_ref().unwrap_or_else(|| panic!("held"));
        assert_eq!(transmission.id, story.wiki);
        assert_eq!(
            transmission.route,
            Route::Channel(ChannelId::from_ulid(0xA1))
        );
        assert_eq!(transmission.state, TransmissionStateKind::Confirmed);
    }
}

/// INV-1014 `surface.conversation.output-spans`: every non-common span of
/// the output, on its part, with its origin and state.
#[tokio::test]
async fn output_spans_are_the_exchanges_spans() {
    let (fixture, story) = story().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let page = turns(&fixture, &viewer, story.plans).await;
    let output = page.turns[0]
        .output
        .as_ref()
        .unwrap_or_else(|| panic!("output"));
    let parts = shown(output);
    let spans: Vec<(SpanId, u32)> = parts[0]
        .spans
        .iter()
        .map(|span| (span.span, span.range.start()))
        .collect();
    assert_eq!(
        spans,
        vec![
            (story.plan_span, 0),
            (story.forwarded, 14),
            (story.relayed, 22)
        ],
        "by range start"
    );
    assert!(matches!(
        &parts[0].spans[0].origin,
        SpanOrigin::Originated {
            status: OriginatedStatus::Propagated { .. },
            ..
        }
    ));
    assert!(matches!(
        &parts[0].spans[1].origin,
        SpanOrigin::Forwarded { input, status: ForwardStatus::Indexed { .. }, .. } if *input == story.messages["ask"]
    ));
    match &parts[0].spans[2].origin {
        SpanOrigin::Relayed(RelayedFrom::Span(point)) => {
            assert_eq!(point.span, story.answer_span);
            assert_eq!(point.agent, story.coder);
        }
        other => panic!("{other:?}"),
    }
    // Inputs carry no spans.
    assert!(
        page.turns[0]
            .inputs
            .iter()
            .flat_map(|message| shown(message).iter())
            .all(|part| part.spans.is_empty())
    );
}

/// INV-1015 `surface.conversation.read-by`: inline readers and their
/// total, and `span_readers` listing all of them.
#[tokio::test]
async fn read_by_is_the_newest_readers_and_their_count() {
    let (fixture, story) = story().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let page = turns(&fixture, &viewer, story.work).await;
    let output = page.turns[0]
        .output
        .as_ref()
        .unwrap_or_else(|| panic!("output"));
    let parts = shown(output);
    assert_eq!(
        parts[1].kind,
        PartKind::ToolCall {
            call: ToolCallId("call_task".into()),
            name: crosstalk_spec::observed::message::ToolName("Task".into()),
            execution: crosstalk_spec::observed::message::ToolExecution::Client,
        }
    );
    let SpanOrigin::Originated { read_by, .. } = &parts[1].spans[0].origin else {
        panic!("originated");
    };
    assert_eq!(read_by.total(), 1);
    let reader = &read_by.first()[0];
    assert_eq!(reader.agent, story.child);
    assert_eq!(
        reader.turn,
        Some(TurnPoint {
            conversation: story.task,
            turn: TurnIndex(0)
        })
    );
    let mark = reader
        .transmission
        .as_ref()
        .unwrap_or_else(|| panic!("held"));
    assert_eq!(mark.id, story.delegation);
    assert_eq!(
        mark.route,
        Route::Delegation(DelegationDirection::ParentToChild)
    );
    let readers = fixture
        .surface
        .span_readers(&viewer, story.call_span, &super::page(20))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"))
        .unwrap_or_else(|| panic!("a kept span"));
    assert_eq!(readers.items(), read_by.first());
    let none = fixture
        .surface
        .span_readers(&viewer, SpanId::from_ulid(7), &super::page(20))
        .await;
    assert_eq!(none, Ok(None));
}

/// INV-1017 `surface.conversation.view-no-text`: no View read's JSON
/// holds any message text.
#[tokio::test]
async fn view_reads_carry_no_text() {
    let (fixture, story) = story().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let mut json = String::new();
    for id in [
        story.plans,
        story.work,
        story.task,
        story.resumed,
        story.branch,
    ] {
        let turns = turns(&fixture, &viewer, id).await;
        json.push_str(&serde_json::to_string(&turns).unwrap_or_default());
        json.push_str(
            &serde_json::to_string(&head(&fixture, &viewer, id).await).unwrap_or_default(),
        );
    }
    let rows = listed(&fixture, &viewer, &ConversationFilter::default()).await;
    json.push_str(&serde_json::to_string(&rows).unwrap_or_default());
    for text in [
        PLAN,
        ANSWER,
        "release plan",
        "tag friday",
        "read the plan on the wiki",
    ] {
        assert!(!json.contains(text), "View read leaks {text:?}");
    }
}

/// INV-1018 `surface.conversation.text-content`: the text reads need
/// Content, and a View-only caller reads nothing.
#[tokio::test]
async fn text_reads_need_content_and_read_nothing_without_it() {
    let (fixture, story) = story().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let forbidden = Err(QueryError::Forbidden {
        missing: Permission::Content,
    });
    assert_eq!(
        fixture
            .surface
            .conversation_text(&viewer, story.work, &window(0, 20), TextLimit::DEFAULT)
            .await
            .map(|_| ()),
        forbidden
    );
    let part = PartRef {
        message: story.messages["plan"],
        index: 0,
    };
    let slice = TextSlice {
        from: 0,
        limit: TextLimit::DEFAULT,
    };
    assert_eq!(
        fixture
            .surface
            .part_text(&viewer, part, slice)
            .await
            .map(|_| ()),
        forbidden
    );
    let auditor = fixture.caller(Who::Auditor).await;
    assert_eq!(
        fixture
            .surface
            .conversation_turns(&auditor, story.work, &window(0, 20))
            .await
            .map(|_| ()),
        Err(QueryError::Forbidden {
            missing: Permission::View
        })
    );
}

/// INV-1019 `surface.conversation.text-aligns`, and INV-1020 on the
/// window's text: each part's text from its start, clipped.
#[tokio::test]
async fn text_aligns_with_turns() {
    let (fixture, story) = story().await;
    let reader = fixture.caller(Who::Reader).await;
    let structure = turns(&fixture, &reader, story.work).await;
    let limit = TextLimit::new(12).unwrap_or_else(|error| panic!("{error:?}"));
    let text = fixture
        .surface
        .conversation_text(&reader, story.work, &window(0, 20), limit)
        .await
        .unwrap_or_else(|error| panic!("{error:?}"))
        .unwrap_or_else(|| panic!("a conversation"));
    assert_eq!(text.turns.len(), structure.turns.len());
    for (shape, words) in structure.turns.iter().zip(&text.turns) {
        assert_eq!(shape.index, words.index);
        let shapes: Vec<MessageHash> = shape.inputs.iter().map(|m| m.hash).collect();
        let texts: Vec<MessageHash> = words.inputs.iter().map(|m| m.hash).collect();
        assert_eq!(shapes, texts);
        assert_eq!(
            shape.output.as_ref().map(|m| m.hash),
            words.output.as_ref().map(|m| m.hash)
        );
        for (message, written) in shape
            .inputs
            .iter()
            .chain(shape.output.iter())
            .zip(words.inputs.iter().chain(words.output.iter()))
        {
            match (&message.parts, &written.body) {
                (MessageParts::Shown(parts), BodyText::Shown(texts)) => {
                    assert_eq!(parts.len(), texts.len());
                    for (part, text) in parts.iter().zip(texts) {
                        assert_eq!(part.text_bytes.is_some(), text.is_some());
                    }
                }
                (MessageParts::BodyDropped(_), BodyText::BodyDropped) => {}
                other => panic!("misaligned: {other:?}"),
            }
        }
    }
    let BodyText::Shown(fetched) = &text.turns[0].inputs[2].body else {
        panic!("shown");
    };
    let clipped = fetched[0].as_ref().unwrap_or_else(|| panic!("text"));
    assert_eq!(clipped.text(), &PLAN[..12]);
    assert_eq!(clipped.remaining() as usize, PLAN.len() - 12);
}

/// INV-1021 `surface.conversation.body-dropped`.
#[tokio::test]
async fn a_dropped_body_is_reported_and_the_rest_returned() {
    let (fixture, story) = story().await;
    let reader = fixture.caller(Who::Reader).await;
    let page = turns(&fixture, &reader, story.work).await;
    let dropped = &page.turns[1].inputs[1];
    assert_eq!(dropped.hash, story.messages["dropped"]);
    assert_eq!(dropped.parts, MessageParts::BodyDropped(Vec::new()));
    assert!(matches!(
        page.turns[1].inputs[0].parts,
        MessageParts::Shown(_)
    ));
    let text = fixture
        .surface
        .conversation_text(&reader, story.work, &window(0, 20), TextLimit::DEFAULT)
        .await
        .unwrap_or_else(|error| panic!("{error:?}"))
        .unwrap_or_else(|| panic!("a conversation"));
    assert_eq!(text.turns[1].inputs[1].body, BodyText::BodyDropped);
    assert!(matches!(text.turns[1].inputs[0].body, BodyText::Shown(_)));
    let part = PartRef {
        message: story.messages["dropped"],
        index: 0,
    };
    let slice = TextSlice {
        from: 0,
        limit: TextLimit::DEFAULT,
    };
    assert_eq!(
        fixture.surface.part_text(&reader, part, slice).await,
        Ok(None)
    );
}

/// INV-1020 on `part_text`: slices on character boundaries; refusals for a
/// part without text and a start outside the text.
#[tokio::test]
async fn part_text_slices_and_refuses() {
    let (fixture, story) = story().await;
    let reader = fixture.caller(Who::Reader).await;
    let at = |message: &str, index| PartRef {
        message: story.messages[message],
        index,
    };
    let slice = |from, limit| TextSlice {
        from,
        limit: TextLimit::new(limit).unwrap_or_else(|error| panic!("{error:?}")),
    };
    let read = fixture
        .surface
        .part_text(&reader, at("plan", 0), slice(14, 7))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"))
        .unwrap_or_else(|| panic!("stored"));
    assert_eq!(
        read,
        PartText::cut(PLAN, 14, TextLimit::new(7).unwrap_or(TextLimit::DEFAULT))
            .unwrap_or_else(|e| panic!("{e:?}"))
    );
    assert_eq!(read.text(), "ship on");
    assert_eq!(
        fixture
            .surface
            .part_text(&reader, at("plan", 3), slice(0, 10))
            .await,
        Err(QueryError::InvalidInput(InputError::PartWithoutText {
            index: 3
        }))
    );
    assert_eq!(
        fixture
            .surface
            .part_text(&reader, at("plan", 0), slice(999, 10))
            .await,
        Err(QueryError::InvalidInput(InputError::SliceOutsideText {
            from: 999,
            part_len: PLAN.len() as u32
        }))
    );
}

/// INV-1022 `surface.conversation.locate`.
#[tokio::test]
async fn span_points_locate_spans_and_resolve_authors() {
    let (fixture, story) = story().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let unknown = SpanId::from_ulid(3);
    let batch = IdBatch::new([story.plan_span, story.relayed, unknown])
        .unwrap_or_else(|error| panic!("{error:?}"));
    let points = fixture
        .surface
        .span_points(&viewer, &batch)
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        points.len(),
        1,
        "a relayed span and an unknown id have no record"
    );
    assert_eq!(
        points[&story.plan_span],
        SpanPoint {
            span: story.plan_span,
            agent: story.planner,
            exchange: story.plan_exchange,
            turn: Some(TurnPoint {
                conversation: story.plans,
                turn: TurnIndex(0)
            }),
            location: located(story.messages["plan"], 0, 0, 12),
        }
    );
    let exchanges = IdBatch::new([
        story.work_exchanges[1],
        story.task_exchange,
        ExchangeId::from_ulid(5),
    ])
    .unwrap_or_else(|error| panic!("{error:?}"));
    let located = fixture
        .surface
        .exchange_turns(&viewer, &exchanges)
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        located,
        BTreeMap::from([
            (
                story.work_exchanges[1],
                ExchangePlacement {
                    agent: story.alias,
                    conversation: story.work,
                    turn: TurnIndex(1)
                }
            ),
            (
                story.task_exchange,
                ExchangePlacement {
                    agent: story.child,
                    conversation: story.task,
                    turn: TurnIndex(0)
                }
            ),
        ])
    );
    // The turn's agent is resolved at the read: a merge shows at once.
    let merged = merge(&fixture, story.alias, story.coder).await;
    let located = fixture
        .surface
        .exchange_turns(&viewer, &exchanges)
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(located[&story.work_exchanges[1]].agent, story.coder);
    unmerge(&fixture, merged).await;
}

/// INV-1025 `surface.conversation.merge-split`: turn agents and span
/// authors follow a merge and its unmerge on the next read.
#[tokio::test]
async fn conversation_reads_follow_merges_and_unmerges() {
    let (fixture, story) = story().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let agents = async || {
        turns(&fixture, &viewer, story.work)
            .await
            .turns
            .iter()
            .map(|turn| turn.agent)
            .collect::<Vec<_>>()
    };
    assert_eq!(agents().await, vec![story.coder, story.alias]);
    let merged = merge(&fixture, story.alias, story.coder).await;
    assert_eq!(agents().await, vec![story.coder, story.coder]);
    let merged_planner = merge(&fixture, story.planner, story.child).await;
    let page = turns(&fixture, &viewer, story.work).await;
    let origin = &shown(&page.turns[0].inputs[2])[0].inbound[0].origin;
    assert_eq!(origin.agent, story.child, "the author resolved now");
    unmerge(&fixture, merged).await;
    unmerge(&fixture, merged_planner).await;
    assert_eq!(agents().await, vec![story.coder, story.alias]);
    let page = turns(&fixture, &viewer, story.work).await;
    assert_eq!(
        shown(&page.turns[0].inputs[2])[0].inbound[0].origin.agent,
        story.planner
    );
}

/// INV-1026 `surface.conversation.claims-only`.
#[tokio::test]
async fn claims_come_from_the_turns_harness_claims() {
    let (fixture, story) = story().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let page = turns(&fixture, &viewer, story.work).await;
    let claims = head(&fixture, &viewer, story.work).await.claims;
    let harness = page.turns[0]
        .harness
        .clone()
        .unwrap_or_else(|| panic!("a claim"));
    assert_eq!(claims.entries().len(), 1);
    assert_eq!(claims.entries()[0].claim, harness);
    assert_eq!(
        claims.last_seen(&harness),
        Some(minute(3)),
        "the latest turn's start"
    );
}

/// INV-1027 `surface.conversation.increment-unseen`.
#[tokio::test]
async fn an_unseen_increment_is_marked() {
    let (fixture, story) = story().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let page = turns(&fixture, &viewer, story.unseen).await;
    assert!(matches!(
        page.turns[0].continuation,
        TurnContinuation::Increment {
            connection: Some(_),
            history: IncrementHistory::Unseen
        }
    ));
    let work = turns(&fixture, &viewer, story.work).await;
    assert_eq!(work.turns[0].continuation, TurnContinuation::FullHistory);
}

/// INV-1028 `surface.conversation.delegated-from`.
#[tokio::test]
async fn delegated_from_is_the_earliest_delegation_read() {
    let (fixture, story) = story().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let task = head(&fixture, &viewer, story.task).await;
    let link = task.delegated_from.unwrap_or_else(|| panic!("delegated"));
    assert_eq!(link.transmission, story.delegation);
    assert_eq!(link.parent.span, story.call_span);
    assert_eq!(link.parent.agent, story.coder);
    assert_eq!(
        link.parent.turn,
        Some(TurnPoint {
            conversation: story.work,
            turn: TurnIndex(0)
        })
    );
    assert_eq!(
        link.child,
        TurnPoint {
            conversation: story.task,
            turn: TurnIndex(0)
        }
    );
    assert_eq!(
        head(&fixture, &viewer, story.work).await.delegated_from,
        None
    );
}

/// INV-1029 `surface.conversation.traffic-counts`: the wiki transmission
/// holds two of the coder's reads but counts once.
#[tokio::test]
async fn traffic_counts_each_transmission_once() {
    let (fixture, story) = story().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let work = head(&fixture, &viewer, story.work).await;
    assert_eq!(work.traffic.received, 1, "the wiki transmission, once");
    assert_eq!(work.traffic.sent, 1, "the delegation");
    let plans = head(&fixture, &viewer, story.plans).await;
    assert_eq!(plans.traffic.received, 0);
    assert_eq!(
        plans.traffic.sent, 1,
        "the wiki transmission, once over two spans"
    );
    let _ = story.plan_read;
}

/// Turn headers come from the exchange record, and a turn's scan status
/// from L4 (`Pending` when it holds none).
#[tokio::test]
async fn turn_headers_come_from_the_exchange_record() {
    let (fixture, story) = story().await;
    let viewer = fixture.caller(Who::Viewer).await;
    let page = turns(&fixture, &viewer, story.work).await;
    assert_eq!(
        page.turns[0].provenance,
        ScanStatus::Indexed { at: minute(2) }
    );
    assert!(matches!(
        page.turns[1].provenance,
        ScanStatus::Failed { .. }
    ));
    let branch = turns(&fixture, &viewer, story.branch).await;
    assert_eq!(branch.turns[0].provenance, ScanStatus::Pending);
    assert_eq!(page.turns[0].started_at, minute(2));
    assert!(!page.turns[0].model.0.is_empty());
}
