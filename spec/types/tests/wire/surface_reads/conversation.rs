//! The conversation view on the wire (`interfaces::l8_surface::conversation`):
//! its requests (`ConversationFilter`, `TurnWindow`, `TextLimit`,
//! `TextSlice`, `PartTextBody`), the list row and head, one turn page with
//! every kind of mark, the text of a window, the batch locates and the
//! readers page. Goldens are under `golden/surface_reads/conversation/`.

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use serde_json::json;

use super::super::harness::{assert_golden, assert_rejected, assert_request_golden};
use super::super::{ULID_A, ULID_B, ULID_C, id, ts};
use super::fixtures::{
    ULID_D, ULID_E, ULID_F, ULID_G, ULID_H, coder, content_match, edited, field, message, planner,
    read_at, tx, wiki,
};
use crate::derived::flow::transmission::{DelegationDirection, Route};
use crate::derived::provenance::matching::{Carrier, Codec, MatchKind};
use crate::derived::provenance::span::SpanLocation;
use crate::ids::{ConversationId, ExchangeId, SpanId, TransmissionId};
use crate::interfaces::l4_provenance::reads::{ForwardStatus, ScanFailureKind, ScanStatus};
use crate::interfaces::l8_surface::conversation::text::{
    BodyText, ConversationText, MessageText, PartText, TextLimit, TextSlice, TurnText,
};
use crate::interfaces::l8_surface::conversation::turn::{
    Inbound, IncrementHistory, MessageParts, MessagePlacement, OriginatedStatus, OutputSpan,
    PartKind, PartMarks, PartShape, ReadBy, Reader, RelayedFrom, SpanOrigin, TransmissionMark,
    Turn, TurnContinuation, TurnMessage, TurnOutcome, TurnPage,
};
use crate::interfaces::l8_surface::conversation::{
    ConversationFilter, ConversationHead, ConversationRow, ConversationTraffic, CorpusId,
    DelegationLink, ExchangePlacement, OriginKind, OriginLink, ReplayFilter, SpanPoint, Successor,
    SuccessorKind, TrafficSource, TurnIndex, TurnPoint, TurnWindow,
};
use crate::interfaces::l8_surface::http::bodies::PartTextBody;
use crate::interfaces::l8_surface::summary::TransmissionStateKind;
use crate::observed::agent::ClaimSet;
use crate::observed::client::{HarnessClaim, HarnessFamily, IngressMode, RouteName};
use crate::observed::exchange::{
    ConnectionId, ExchangeFailure, ModelName, StopReason, TokenCounts, TokenUsage, Transport,
    WireProtocol,
};
use crate::observed::message::{
    MediaKind, PartRef, Role, ToolCallId, ToolExecution, ToolName, ToolOutcome,
};
use crate::paging::{Page, PageSize, SpanReaderList};
use crate::support::{ByteRange, NonEmpty};

const AREA: &str = "surface_reads/conversation";

fn conversation(text: &str) -> ConversationId {
    id(ConversationId::from_ulid_text, text)
}

fn exchange(text: &str) -> ExchangeId {
    id(ExchangeId::from_ulid_text, text)
}

fn span(text: &str) -> SpanId {
    id(SpanId::from_ulid_text, text)
}

fn range(start: u32, end: u32) -> ByteRange {
    ByteRange::new(start, end).expect("not empty")
}

fn bytes(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).expect("non-zero")
}

fn window(from: u32, size: u16) -> TurnWindow {
    TurnWindow {
        from: TurnIndex(from),
        size: PageSize::new(size).expect("in range"),
    }
}

fn claim() -> HarnessClaim {
    HarnessClaim {
        family: HarnessFamily::ClaudeCode,
        version: Some("2.1.4".into()),
        user_agent: "claude-cli/2.1.4 (external, cli)".into(),
    }
}

/// The coder's conversation, its turn 7 and where the planner's span sits.
fn coder_turn() -> TurnPoint {
    TurnPoint {
        conversation: conversation(ULID_B),
        turn: TurnIndex(7),
    }
}

fn planner_span() -> SpanPoint {
    SpanPoint {
        span: span(ULID_C),
        agent: planner(),
        exchange: exchange(ULID_A),
        turn: Some(TurnPoint {
            conversation: conversation(ULID_A),
            turn: TurnIndex(3),
        }),
        location: SpanLocation {
            part: PartRef {
                message: message(0x10),
                index: 1,
            },
            range: range(40, 87),
        },
    }
}

fn wiki_mark() -> TransmissionMark {
    TransmissionMark {
        id: tx(),
        route: Route::Channel(wiki()),
        state: TransmissionStateKind::Confirmed,
    }
}

fn row(origin: OriginLink, source: TrafficSource) -> ConversationRow {
    ConversationRow {
        id: conversation(ULID_B),
        agent: coder(),
        origin,
        started_at: ts("2026-10-04T09:01:12.000000Z"),
        last_turn_at: ts("2026-10-04T09:40:55.250000Z"),
        turns: 84,
        source,
    }
}

#[test]
fn conversation_filter_goldens() {
    assert_request_golden(AREA, "filter_default", &ConversationFilter::default());
    assert_request_golden(
        AREA,
        "filter_agent_forks_one_corpus",
        &ConversationFilter {
            agent: Some(coder()),
            origins: vec![OriginKind::Fork, OriginKind::Compaction],
            replay: ReplayFilter::Only {
                corpus: Some(CorpusId("salt-nlp".into())),
            },
        },
    );
    assert_request_golden(
        AREA,
        "filter_live_only",
        &ConversationFilter {
            agent: None,
            origins: vec![OriginKind::Root],
            replay: ReplayFilter::Exclude,
        },
    );
}

#[test]
fn turn_window_and_text_request_goldens() {
    assert_request_golden(AREA, "turn_window", &window(20, 20));
    assert_request_golden(AREA, "text_limit_default", &TextLimit::DEFAULT);
    assert_request_golden(
        AREA,
        "part_text_body",
        &PartTextBody {
            part: PartRef {
                message: message(0x20),
                index: 2,
            },
            slice: TextSlice {
                from: 8192,
                limit: TextLimit::DEFAULT,
            },
        },
    );
}

#[test]
fn requests_refuse_what_their_constructors_refuse() {
    assert_rejected::<TextLimit>("0", "invalid text limit");
    assert_rejected::<TextLimit>("65537", "invalid text limit");
    assert_rejected::<TurnWindow>(r#"{"from": 0, "size": 0}"#, "invalid page size");
    assert_rejected::<TurnWindow>(r#"{"from": 0, "size": 20, "to": 40}"#, "unknown field `to`");
    assert_rejected::<ConversationFilter>(
        r#"{"agent": null, "origins": ["merge"], "replay": {"type": "include"}}"#,
        "unknown variant `merge`",
    );
    assert_rejected::<ReplayFilter>(r#"{"type": "replayed"}"#, "unknown variant `replayed`");
}

#[test]
fn conversation_row_goldens() {
    assert_golden(
        AREA,
        "row_root_live",
        &row(OriginLink::Root, TrafficSource::Live),
    );
    assert_golden(
        AREA,
        "row_fork_replayed",
        &row(
            OriginLink::Fork {
                parent: conversation(ULID_D),
                parent_agent: coder(),
                shared_prefix: 23,
                branch_turn: Some(TurnIndex(11)),
            },
            TrafficSource::Replay {
                corpus: CorpusId("salt-nlp".into()),
            },
        ),
    );
    assert_golden(
        AREA,
        "row_compaction",
        &row(
            OriginLink::Compaction {
                predecessor: conversation(ULID_E),
                predecessor_agent: coder(),
                carried_over: 31,
            },
            TrafficSource::Live,
        ),
    );
}

#[test]
fn conversation_head_golden() {
    let mut claims = ClaimSet::default();
    claims.observe(claim(), ts("2026-10-04T09:40:55.250000Z"));
    let head = ConversationHead {
        row: row(
            OriginLink::Fork {
                parent: conversation(ULID_D),
                parent_agent: coder(),
                shared_prefix: 23,
                branch_turn: None,
            },
            TrafficSource::Live,
        ),
        traffic: ConversationTraffic {
            received: 3,
            sent: 5,
        },
        successors: vec![
            Successor {
                conversation: conversation(ULID_F),
                agent: coder(),
                kind: SuccessorKind::Fork {
                    shared_prefix: 120,
                    branch_turn: Some(TurnIndex(60)),
                },
                started_at: ts("2026-10-04T09:41:02.000000Z"),
            },
            Successor {
                conversation: conversation(ULID_G),
                agent: coder(),
                kind: SuccessorKind::Compaction,
                started_at: ts("2026-10-04T09:41:30.000000Z"),
            },
        ],
        delegated_from: Some(DelegationLink {
            transmission: id(TransmissionId::from_ulid_text, ULID_H),
            parent: planner_span(),
            child: TurnPoint {
                conversation: conversation(ULID_B),
                turn: TurnIndex(0),
            },
        }),
        claims,
    };
    assert_golden(AREA, "head", &head);
}

fn reader() -> Reader {
    Reader {
        agent: coder(),
        exchange: exchange(ULID_B),
        turn: Some(coder_turn()),
        read_at: read_at(),
        carrier: Carrier::ToolResult(ToolCallId("toolu_01VfA7wiki".into())),
        kind: MatchKind::Exact,
        transmission: Some(wiki_mark()),
    }
}

fn inbound() -> Inbound {
    Inbound {
        range: range(12, 59),
        matched_bytes: bytes(47),
        kind: MatchKind::Decoded(NonEmpty::new(Codec::JsonString)),
        carrier: Carrier::ToolResult(ToolCallId("toolu_01VfA7wiki".into())),
        origin: planner_span(),
        transmission: Some(wiki_mark()),
    }
}

fn usage() -> TokenUsage {
    TokenUsage::new(TokenCounts {
        input: 18_204,
        output: 911,
        cache_read: 17_002,
        cache_write: None,
        reasoning: None,
    })
    .expect("cached within input")
}

/// A turn with a system turn mid-conversation, a tool result holding the
/// planner's text, and an output with an originated span read later, a
/// forwarded span and a relayed one.
fn full_turn() -> Turn {
    let read_by = ReadBy::new(vec![reader(), reader()], 2).expect("both inline");
    Turn {
        index: TurnIndex(37),
        exchange: exchange(ULID_B),
        agent: coder(),
        started_at: ts("2026-10-04T09:31:04.000000Z"),
        protocol: WireProtocol::AnthropicMessages,
        transport: Transport::Sse,
        model: ModelName("claude-sonnet-4-5".into()),
        harness: Some(claim()),
        ingress: IngressMode::ReverseProxy {
            route: RouteName("anthropic".into()),
        },
        continuation: TurnContinuation::FullHistory,
        outcome: TurnOutcome::Completed {
            finished_at: ts("2026-10-04T09:31:09.500000Z"),
            stop: StopReason::ToolUse,
            usage: Some(usage()),
        },
        inputs: vec![
            TurnMessage {
                hash: message(0x20),
                role: Role::Tool,
                placement: MessagePlacement::New,
                parts: MessageParts::Shown(vec![PartShape {
                    index: 0,
                    kind: PartKind::ToolResult {
                        call: ToolCallId("toolu_01VfA7wiki".into()),
                        outcome: ToolOutcome::Success,
                    },
                    text_bytes: Some(4180),
                    inbound: vec![inbound()],
                    spans: Vec::new(),
                }]),
            },
            TurnMessage {
                hash: message(0x30),
                role: Role::System,
                placement: MessagePlacement::New,
                parts: MessageParts::Shown(vec![PartShape {
                    index: 0,
                    kind: PartKind::Text,
                    text_bytes: Some(312),
                    inbound: Vec::new(),
                    spans: Vec::new(),
                }]),
            },
        ],
        output: Some(TurnMessage {
            hash: message(0x40),
            role: Role::Assistant,
            placement: MessagePlacement::Output,
            parts: MessageParts::Shown(vec![
                PartShape {
                    index: 0,
                    kind: PartKind::Reasoning { visible: false },
                    text_bytes: None,
                    inbound: Vec::new(),
                    spans: Vec::new(),
                },
                PartShape {
                    index: 1,
                    kind: PartKind::Text,
                    text_bytes: Some(1210),
                    inbound: Vec::new(),
                    spans: vec![
                        OutputSpan {
                            span: span(ULID_C),
                            range: range(40, 87),
                            origin: SpanOrigin::Originated {
                                status: OriginatedStatus::Propagated {
                                    indexed_at: ts("2026-10-04T09:31:10.000000Z"),
                                    first_hit_at: ts("2026-10-04T09:35:00.000000Z"),
                                    hits: bytes(4),
                                },
                                read_by,
                            },
                        },
                        OutputSpan {
                            span: span(ULID_D),
                            range: range(90, 300),
                            origin: SpanOrigin::Forwarded {
                                input: message(0x20),
                                status: ForwardStatus::Indexed {
                                    at: ts("2026-10-04T09:31:10.000000Z"),
                                },
                                read_by: ReadBy::none(),
                            },
                        },
                        OutputSpan {
                            span: span(ULID_E),
                            range: range(400, 480),
                            origin: SpanOrigin::Relayed(RelayedFrom::Span(planner_span())),
                        },
                    ],
                },
                PartShape {
                    index: 2,
                    kind: PartKind::ToolCall {
                        call: ToolCallId("toolu_01Task".into()),
                        name: ToolName("Task".into()),
                        execution: ToolExecution::Client,
                    },
                    text_bytes: Some(96),
                    inbound: Vec::new(),
                    spans: vec![OutputSpan {
                        span: span(ULID_F),
                        range: range(0, 96),
                        origin: SpanOrigin::Originated {
                            status: OriginatedStatus::Indexed {
                                at: ts("2026-10-04T09:31:10.000000Z"),
                            },
                            read_by: ReadBy::new(
                                vec![Reader {
                                    transmission: Some(TransmissionMark {
                                        id: id(TransmissionId::from_ulid_text, ULID_H),
                                        route: Route::Delegation(
                                            DelegationDirection::ParentToChild,
                                        ),
                                        state: TransmissionStateKind::Classified,
                                    }),
                                    ..reader()
                                }],
                                1,
                            )
                            .expect("one reader"),
                        },
                    }],
                },
            ]),
        }),
        provenance: ScanStatus::Indexed {
            at: ts("2026-10-04T09:31:10.000000Z"),
        },
    }
}

/// A compaction's first turn: a carried-over message folded, the summary
/// new, a dropped body, a failed exchange on an increment whose history
/// the gateway never saw.
fn boundary_turn() -> Turn {
    Turn {
        index: TurnIndex(0),
        exchange: exchange(ULID_C),
        agent: coder(),
        started_at: ts("2026-10-04T09:41:30.000000Z"),
        protocol: WireProtocol::OpenAiResponses,
        transport: Transport::WebSocket,
        model: ModelName("gpt-5-codex".into()),
        harness: None,
        ingress: IngressMode::Replay {
            corpus: CorpusId("salt-nlp".into()),
        },
        continuation: TurnContinuation::Increment {
            connection: Some(ConnectionId(0x0192_5f3e_8a4c_7b2d_9e1f_0a3b_4c5d_6e7f)),
            history: IncrementHistory::Unseen,
        },
        outcome: TurnOutcome::Failed {
            failed_at: ts("2026-10-04T09:41:31.000000Z"),
            failure: ExchangeFailure::Upstream { status: 529 },
        },
        inputs: vec![
            TurnMessage {
                hash: message(0x50),
                role: Role::User,
                placement: MessagePlacement::CarriedOver,
                parts: MessageParts::BodyDropped(vec![PartMarks {
                    index: 0,
                    inbound: vec![inbound()],
                    spans: Vec::new(),
                }]),
            },
            TurnMessage {
                hash: message(0x60),
                role: Role::User,
                placement: MessagePlacement::New,
                parts: MessageParts::Shown(vec![
                    PartShape {
                        index: 0,
                        kind: PartKind::Text,
                        text_bytes: Some(2048),
                        inbound: Vec::new(),
                        spans: Vec::new(),
                    },
                    PartShape {
                        index: 1,
                        kind: PartKind::Media(MediaKind::Image),
                        text_bytes: None,
                        inbound: Vec::new(),
                        spans: Vec::new(),
                    },
                    PartShape {
                        index: 2,
                        kind: PartKind::Unknown {
                            kind: "container_upload".into(),
                        },
                        text_bytes: None,
                        inbound: Vec::new(),
                        spans: Vec::new(),
                    },
                ]),
            },
        ],
        output: None,
        provenance: ScanStatus::Failed {
            at: ts("2026-10-04T09:41:32.000000Z"),
            failure: ScanFailureKind::BodyMissing,
        },
    }
}

#[test]
fn turn_page_golden() {
    let page = TurnPage {
        conversation: conversation(ULID_B),
        total: 84,
        turns: vec![full_turn()],
    };
    assert_golden(AREA, "turn_page", &page);
    let boundary = TurnPage {
        conversation: conversation(ULID_G),
        total: 1,
        turns: vec![boundary_turn()],
    };
    assert_golden(AREA, "turn_page_boundary", &boundary);
    let past_the_end = TurnPage {
        conversation: conversation(ULID_B),
        total: 84,
        turns: Vec::new(),
    };
    assert_golden(AREA, "turn_page_past_the_end", &past_the_end);
}

#[test]
fn scan_status_goldens() {
    let all = vec![
        ScanStatus::Pending,
        ScanStatus::Scanned {
            at: ts("2026-10-04T09:31:09.900000Z"),
        },
        ScanStatus::Indexed {
            at: ts("2026-10-04T09:31:10.000000Z"),
        },
        ScanStatus::Failed {
            at: ts("2026-10-04T09:31:10.000000Z"),
            failure: ScanFailureKind::BodyUndecodable,
        },
    ];
    assert_golden(AREA, "scan_status_every_state", &all);
    let forwards = vec![
        ForwardStatus::Pending,
        ForwardStatus::Indexed {
            at: ts("2026-10-04T09:31:10.000000Z"),
        },
        ForwardStatus::Expired {
            indexed_at: ts("2026-10-04T09:31:10.000000Z"),
            at: ts("2026-11-03T09:31:10.000000Z"),
        },
    ];
    assert_golden(AREA, "forward_status_every_state", &forwards);
    let originated = vec![
        OriginatedStatus::Pending,
        OriginatedStatus::Expired {
            at: ts("2026-11-03T09:31:10.000000Z"),
        },
    ];
    assert_golden(AREA, "originated_status_pending_expired", &originated);
}

#[test]
fn read_by_refuses_an_inconsistent_count() {
    let one = serde_json::to_value(reader()).expect("a reader encodes");
    assert_rejected::<ReadBy>(
        &json!({"first": vec![one.clone(); 9], "total": 9}).to_string(),
        "TooManyInline",
    );
    assert_rejected::<ReadBy>(
        &json!({"first": [one.clone(), one.clone()], "total": 1}).to_string(),
        "TotalBelowInline",
    );
    assert_rejected::<ReadBy>(
        &json!({"first": [one], "total": 3}).to_string(),
        "InlineShort",
    );
    assert_rejected::<ReadBy>(
        &edited(&ReadBy::none(), |json| {
            json["more"] = json!(0);
        }),
        "unknown field `more`",
    );
}

#[test]
fn text_goldens() {
    let text = ConversationText {
        conversation: conversation(ULID_B),
        turns: vec![TurnText {
            index: TurnIndex(37),
            inputs: vec![
                MessageText {
                    hash: message(0x20),
                    body: BodyText::Shown(vec![Some(
                        PartText::cut("release plan: ship on friday ✓", 0, TextLimit::DEFAULT)
                            .expect("a whole part"),
                    )]),
                },
                MessageText {
                    hash: message(0x50),
                    body: BodyText::BodyDropped,
                },
            ],
            output: Some(MessageText {
                hash: message(0x40),
                body: BodyText::Shown(vec![
                    None,
                    Some(
                        PartText::cut(
                            "the plan says friday",
                            0,
                            TextLimit::new(8).expect("in range"),
                        )
                        .expect("a clipped part"),
                    ),
                ]),
            }),
        }],
    };
    assert_golden(AREA, "conversation_text", &text);
    let slice =
        PartText::cut("ünïcode", 2, TextLimit::new(4).expect("in range")).expect("from a boundary");
    assert_golden(AREA, "part_text_slice", &slice);
}

#[test]
fn part_text_refuses_a_slice_outside_its_part() {
    assert_rejected::<PartText>(
        r#"{"from": 6, "text": "abc", "part_len": 8}"#,
        "invalid part text",
    );
    assert_rejected::<PartText>(
        r#"{"from": 0, "text": "abc", "part_len": 3, "rest": 0}"#,
        "unknown field `rest`",
    );
}

#[test]
fn locate_and_readers_goldens() {
    let turns = BTreeMap::from([(
        exchange(ULID_B),
        ExchangePlacement {
            agent: coder(),
            conversation: coder_turn().conversation,
            turn: coder_turn().turn,
        },
    )]);
    assert_golden(AREA, "exchange_turns", &turns);
    assert_rejected::<ExchangePlacement>(
        r#"{"conversation": "01J9Z3M2C5D6E7F8G9H0J1K2M3", "turn": 3}"#,
        "missing field `agent`",
    );
    let points = BTreeMap::from([(span(ULID_C), planner_span())]);
    assert_golden(AREA, "span_points", &points);
    let unthreaded = SpanPoint {
        turn: None,
        ..planner_span()
    };
    assert_golden(AREA, "span_point_unthreaded", &unthreaded);
    let page: Page<Reader, SpanReaderList> = Page::last(
        PageSize::new(20).expect("in range"),
        vec![
            reader(),
            Reader {
                turn: None,
                transmission: None,
                carrier: Carrier::ReaderOutput,
                kind: MatchKind::Normalized,
                ..reader()
            },
        ],
    )
    .expect("within the size");
    assert_golden(AREA, "span_readers_page", &page);
}

/// The fixture's match reads where the reader turn's mark says it does.
#[test]
fn inbound_range_is_the_match_read_range() {
    let content = content_match();
    assert_eq!(inbound().range, content.read_at().range);
    let mut json = serde_json::to_value(inbound()).expect("encodes");
    assert_eq!(field(&mut json, "range"), &json!({"start": 12, "end": 59}));
}
