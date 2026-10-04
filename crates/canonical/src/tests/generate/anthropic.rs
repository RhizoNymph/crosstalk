//! Generated Anthropic Messages content: blocks, turns, requests and
//! responses, rendered as request bodies, whole response bodies and event
//! streams.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use proptest::prelude::*;

use super::Style;
use super::json::{GenJson, arb_json, arb_object, arb_text};

/// Tool call ids are drawn from a small pool, so results pair with calls
/// before them, after them, of either execution, or with none.
const IDS: [&str; 4] = [
    "toolu_01A",
    "toolu_01B",
    "srvtoolu_01A",
    "chatcmpl-tool-0f1e2d3c",
];

fn arb_id() -> impl Strategy<Value = String> {
    prop_oneof![
        proptest::sample::select(IDS.as_slice()).prop_map(str::to_owned),
        "call_[0-9a-f]{24}",
    ]
}

/// A block of an assistant turn: in a response, or echoed in a request.
#[derive(Debug, Clone, PartialEq)]
pub enum GenBlock {
    Text(String),
    Thinking {
        text: String,
        signature: String,
    },
    Redacted(String),
    ToolUse {
        id: String,
        name: String,
        input: GenJson,
    },
    /// `server_tool_use` or `mcp_tool_use`.
    ServerToolUse {
        kind: &'static str,
        id: String,
        name: String,
        input: GenJson,
    },
    /// `web_search_tool_result` or `mcp_tool_result`, with text content.
    ServerResult {
        kind: &'static str,
        tool_use_id: String,
        texts: Vec<String>,
    },
    Unknown {
        kind: String,
        payload: GenJson,
    },
}

pub fn arb_unknown_kind() -> impl Strategy<Value = String> {
    "zz_[a-z]{1,8}"
}

pub fn arb_block() -> impl Strategy<Value = GenBlock> {
    prop_oneof![
        3 => arb_text().prop_map(GenBlock::Text),
        1 => (arb_text(), "[A-Za-z0-9+/]{0,40}={0,2}")
            .prop_map(|(text, signature)| GenBlock::Thinking { text, signature }),
        1 => any::<String>().prop_map(GenBlock::Redacted),
        2 => (arb_id(), "[A-Za-z_]{1,10}", arb_object())
            .prop_map(|(id, name, input)| GenBlock::ToolUse { id, name, input }),
        1 => (
            proptest::sample::select(&["server_tool_use", "mcp_tool_use"][..]),
            arb_id(),
            "[a-z_]{1,10}",
            arb_object()
        )
            .prop_map(|(kind, id, name, input)| GenBlock::ServerToolUse { kind, id, name, input }),
        1 => (
            proptest::sample::select(&["web_search_tool_result", "mcp_tool_result"][..]),
            arb_id(),
            proptest::collection::vec(arb_text(), 0..3)
        )
            .prop_map(|(kind, tool_use_id, texts)| GenBlock::ServerResult { kind, tool_use_id, texts }),
        1 => (arb_unknown_kind(), arb_json())
            .prop_map(|(kind, payload)| GenBlock::Unknown { kind, payload }),
    ]
}

pub fn arb_blocks() -> impl Strategy<Value = Vec<GenBlock>> {
    proptest::collection::vec(arb_block(), 0..6)
}

fn s(text: &str) -> GenJson {
    GenJson::Str(text.to_owned())
}

fn obj(members: Vec<(&str, GenJson)>) -> GenJson {
    GenJson::Object(
        members
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect(),
    )
}

fn int(value: u64) -> GenJson {
    GenJson::Number(super::json::GenNumber {
        negative: false,
        digits: value.to_string(),
        exponent: 0,
    })
}

fn text_block(text: &str) -> GenJson {
    obj(vec![("type", s("text")), ("text", s(text))])
}

/// A `cache_control` marker, sometimes, for a known block of a request.
fn cache_control(style: &mut Style) -> Option<GenJson> {
    match style.below(4) {
        0 => Some(obj(vec![("type", s("ephemeral"))])),
        1 => Some(obj(vec![("type", s("ephemeral")), ("ttl", s("1h"))])),
        _ => None,
    }
}

fn with_marker(block: GenJson, style: &mut Style) -> GenJson {
    match (block, cache_control(style)) {
        (GenJson::Object(mut members), Some(marker)) => {
            members.push(("cache_control".to_owned(), marker));
            GenJson::Object(members)
        }
        (block, _) => block,
    }
}

impl GenBlock {
    /// The block in its whole (non-streamed, or echoed) shape.
    pub fn whole(&self) -> GenJson {
        match self {
            Self::Text(text) => text_block(text),
            Self::Thinking { text, signature } => obj(vec![
                ("type", s("thinking")),
                ("thinking", s(text)),
                ("signature", s(signature)),
            ]),
            Self::Redacted(data) => obj(vec![("type", s("redacted_thinking")), ("data", s(data))]),
            Self::ToolUse { id, name, input } => obj(vec![
                ("type", s("tool_use")),
                ("id", s(id)),
                ("name", s(name)),
                ("input", input.clone()),
            ]),
            Self::ServerToolUse {
                kind,
                id,
                name,
                input,
            } => obj(vec![
                ("type", s(kind)),
                ("id", s(id)),
                ("name", s(name)),
                ("input", input.clone()),
            ]),
            Self::ServerResult {
                kind,
                tool_use_id,
                texts,
            } => obj(vec![
                ("type", s(kind)),
                ("tool_use_id", s(tool_use_id)),
                (
                    "content",
                    GenJson::Array(texts.iter().map(|text| text_block(text)).collect()),
                ),
            ]),
            Self::Unknown { kind, payload } => {
                obj(vec![("type", s(kind)), ("payload", payload.clone())])
            }
        }
    }

    /// The block as a harness echoes it in a later request: the whole
    /// shape, sometimes carrying a cache marker.
    pub fn echoed(&self, style: &mut Style) -> GenJson {
        with_marker(self.whole(), style)
    }
}

/// A user turn's block.
#[derive(Debug, Clone, PartialEq)]
pub enum GenUserBlock {
    Text(String),
    Image(Vec<u8>),
    ToolResult {
        id: String,
        content: GenResultContent,
        is_error: bool,
    },
    Unknown {
        kind: String,
        payload: GenJson,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum GenResultContent {
    Absent,
    Str(String),
    Items(Vec<GenResultItem>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum GenResultItem {
    Text(String),
    Image(Vec<u8>),
    Unknown { kind: String, payload: GenJson },
}

fn arb_bytes() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..24)
}

pub fn arb_result_content() -> impl Strategy<Value = GenResultContent> {
    prop_oneof![
        Just(GenResultContent::Absent),
        arb_text().prop_map(GenResultContent::Str),
        proptest::collection::vec(
            prop_oneof![
                3 => arb_text().prop_map(GenResultItem::Text),
                1 => arb_bytes().prop_map(GenResultItem::Image),
                1 => (arb_unknown_kind(), arb_json())
                    .prop_map(|(kind, payload)| GenResultItem::Unknown { kind, payload }),
            ],
            0..4
        )
        .prop_map(GenResultContent::Items),
    ]
}

pub fn arb_user_block() -> impl Strategy<Value = GenUserBlock> {
    prop_oneof![
        3 => arb_text().prop_map(GenUserBlock::Text),
        1 => arb_bytes().prop_map(GenUserBlock::Image),
        3 => (arb_id(), arb_result_content(), any::<bool>())
            .prop_map(|(id, content, is_error)| GenUserBlock::ToolResult { id, content, is_error }),
        1 => (arb_unknown_kind(), arb_json())
            .prop_map(|(kind, payload)| GenUserBlock::Unknown { kind, payload }),
    ]
}

fn image(bytes: &[u8]) -> GenJson {
    obj(vec![
        ("type", s("image")),
        (
            "source",
            obj(vec![
                ("type", s("base64")),
                ("media_type", s("image/png")),
                ("data", s(&STANDARD.encode(bytes))),
            ]),
        ),
    ])
}

impl GenResultItem {
    fn render(&self) -> GenJson {
        match self {
            Self::Text(text) => text_block(text),
            Self::Image(bytes) => image(bytes),
            Self::Unknown { kind, payload } => {
                obj(vec![("type", s(kind)), ("payload", payload.clone())])
            }
        }
    }
}

impl GenUserBlock {
    pub fn render(&self, style: &mut Style) -> GenJson {
        match self {
            Self::Text(text) => with_marker(text_block(text), style),
            Self::Image(bytes) => with_marker(image(bytes), style),
            Self::ToolResult {
                id,
                content,
                is_error,
            } => {
                let mut members = vec![("type", s("tool_result")), ("tool_use_id", s(id))];
                match content {
                    GenResultContent::Absent => {}
                    GenResultContent::Str(text) => members.push(("content", s(text))),
                    GenResultContent::Items(items) => members.push((
                        "content",
                        GenJson::Array(items.iter().map(GenResultItem::render).collect()),
                    )),
                }
                if *is_error {
                    members.push(("is_error", GenJson::Bool(true)));
                } else if style.chance(3) {
                    members.push(("is_error", GenJson::Bool(false)));
                }
                with_marker(obj(members), style)
            }
            Self::Unknown { kind, payload } => with_marker(
                obj(vec![("type", s(kind)), ("payload", payload.clone())]),
                style,
            ),
        }
    }
}

/// One entry of a request's `messages`.
#[derive(Debug, Clone, PartialEq)]
pub enum GenTurn {
    User(Vec<GenUserBlock>),
    UserText(String),
    Assistant(Vec<GenBlock>),
}

pub fn arb_turn() -> impl Strategy<Value = GenTurn> {
    prop_oneof![
        3 => proptest::collection::vec(arb_user_block(), 0..6).prop_map(GenTurn::User),
        1 => arb_text().prop_map(GenTurn::UserText),
        3 => arb_blocks().prop_map(GenTurn::Assistant),
    ]
}

impl GenTurn {
    pub fn render(&self, style: &mut Style) -> GenJson {
        let (role, content) = match self {
            Self::User(blocks) => (
                "user",
                GenJson::Array(blocks.iter().map(|block| block.render(style)).collect()),
            ),
            Self::UserText(text) => ("user", s(text)),
            Self::Assistant(blocks) => (
                "assistant",
                GenJson::Array(blocks.iter().map(|block| block.echoed(style)).collect()),
            ),
        };
        obj(vec![("role", s(role)), ("content", content)])
    }
}

/// A request's top-level system prompt.
#[derive(Debug, Clone, PartialEq)]
pub enum GenSystem {
    Absent,
    Str(String),
    Blocks(Vec<GenSystemBlock>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum GenSystemBlock {
    Text(String),
    Unknown { kind: String, payload: GenJson },
}

pub fn arb_system() -> impl Strategy<Value = GenSystem> {
    prop_oneof![
        Just(GenSystem::Absent),
        arb_text().prop_map(GenSystem::Str),
        proptest::collection::vec(
            prop_oneof![
                3 => arb_text().prop_map(GenSystemBlock::Text),
                1 => (arb_unknown_kind(), arb_json())
                    .prop_map(|(kind, payload)| GenSystemBlock::Unknown { kind, payload }),
            ],
            0..4
        )
        .prop_map(GenSystem::Blocks),
    ]
}

#[derive(Debug, Clone, PartialEq)]
pub struct GenRequest {
    pub system: GenSystem,
    pub turns: Vec<GenTurn>,
}

pub fn arb_request() -> impl Strategy<Value = GenRequest> {
    (arb_system(), proptest::collection::vec(arb_turn(), 0..6))
        .prop_map(|(system, turns)| GenRequest { system, turns })
}

impl GenRequest {
    pub fn render(&self, style: &mut Style) -> String {
        let mut members = vec![
            ("model", s("claude-opus-5-5")),
            ("max_tokens", int(32000)),
            (
                "messages",
                GenJson::Array(self.turns.iter().map(|turn| turn.render(style)).collect()),
            ),
            ("stream", GenJson::Bool(true)),
        ];
        match &self.system {
            GenSystem::Absent => {}
            GenSystem::Str(text) => members.push(("system", s(text))),
            GenSystem::Blocks(blocks) => {
                let rendered = blocks
                    .iter()
                    .map(|block| match block {
                        GenSystemBlock::Text(text) => with_marker(text_block(text), style),
                        GenSystemBlock::Unknown { kind, payload } => with_marker(
                            obj(vec![("type", s(kind)), ("payload", payload.clone())]),
                            style,
                        ),
                    })
                    .collect();
                members.push(("system", GenJson::Array(rendered)));
            }
        }
        obj(members).render(style)
    }
}

/// A completed response: its blocks, stop reason and usage.
#[derive(Debug, Clone, PartialEq)]
pub struct GenResponse {
    pub id: String,
    pub blocks: Vec<GenBlock>,
    pub stop_reason: &'static str,
    /// Input, cache creation, cache read and output tokens.
    pub usage: (u16, u16, u16, u16),
}

pub fn arb_response() -> impl Strategy<Value = GenResponse> {
    (
        "msg_[A-Za-z0-9]{24}",
        arb_blocks(),
        proptest::sample::select(
            &[
                "end_turn",
                "tool_use",
                "max_tokens",
                "stop_sequence",
                "refusal",
                "pause_turn",
            ][..],
        ),
        any::<(u16, u16, u16, u16)>(),
    )
        .prop_map(|(id, blocks, stop_reason, usage)| GenResponse {
            id,
            blocks,
            stop_reason,
            usage,
        })
}

impl GenResponse {
    fn usage_json(&self, output: u16) -> GenJson {
        let (input, creation, read, _) = self.usage;
        obj(vec![
            ("input_tokens", int(u64::from(input))),
            ("cache_creation_input_tokens", int(u64::from(creation))),
            ("cache_read_input_tokens", int(u64::from(read))),
            ("output_tokens", int(u64::from(output))),
        ])
    }

    /// The whole (non-streamed) body.
    pub fn whole(&self, style: &mut Style) -> String {
        obj(vec![
            ("id", s(&self.id)),
            ("type", s("message")),
            ("role", s("assistant")),
            ("model", s("claude-opus-5-5")),
            (
                "content",
                GenJson::Array(self.blocks.iter().map(GenBlock::whole).collect()),
            ),
            ("stop_reason", s(self.stop_reason)),
            ("stop_sequence", GenJson::Null),
            ("usage", self.usage_json(self.usage.3)),
        ])
        .render(style)
    }

    /// The event stream: deltas cut at random, pings between events, and
    /// (when `style` chooses) every block started before any delta, with
    /// the blocks' deltas interleaved.
    pub fn stream(&self, style: &mut Style) -> String {
        let crlf = style.chance(4);
        let mut out = String::new();
        let mut events: Vec<GenJson> = Vec::new();
        events.push(obj(vec![
            ("type", s("message_start")),
            (
                "message",
                obj(vec![
                    ("id", s(&self.id)),
                    ("type", s("message")),
                    ("role", s("assistant")),
                    ("model", s("claude-opus-5-5")),
                    ("content", GenJson::Array(Vec::new())),
                    ("stop_reason", GenJson::Null),
                    ("stop_sequence", GenJson::Null),
                    ("usage", self.usage_json(1)),
                ]),
            ),
        ]));
        let mut starts = Vec::new();
        let mut deltas: Vec<Vec<GenJson>> = Vec::new();
        for (index, block) in self.blocks.iter().enumerate() {
            let index = u64::try_from(index).unwrap_or(u64::MAX);
            let (start, block_deltas) = stream_block(block, style);
            starts.push(obj(vec![
                ("type", s("content_block_start")),
                ("index", int(index)),
                ("content_block", start),
            ]));
            deltas.push(
                block_deltas
                    .into_iter()
                    .map(|delta| {
                        obj(vec![
                            ("type", s("content_block_delta")),
                            ("index", int(index)),
                            ("delta", delta),
                        ])
                    })
                    .collect(),
            );
        }
        let stop = |index: usize| {
            obj(vec![
                ("type", s("content_block_stop")),
                ("index", int(u64::try_from(index).unwrap_or(u64::MAX))),
            ])
        };
        if style.chance(3) {
            events.extend(starts);
            let mut queues: Vec<std::collections::VecDeque<GenJson>> =
                deltas.into_iter().map(Into::into).collect();
            loop {
                let open: Vec<usize> = (0..queues.len())
                    .filter(|&at| !queues[at].is_empty())
                    .collect();
                if open.is_empty() {
                    break;
                }
                let pick = open[style.below(open.len())];
                if let Some(delta) = queues[pick].pop_front() {
                    events.push(delta);
                }
            }
            events.extend((0..queues.len()).map(stop));
        } else {
            for (index, (start, block_deltas)) in starts.into_iter().zip(deltas).enumerate() {
                events.push(start);
                events.extend(block_deltas);
                events.push(stop(index));
            }
        }
        events.push(obj(vec![
            ("type", s("message_delta")),
            (
                "delta",
                obj(vec![
                    ("stop_reason", s(self.stop_reason)),
                    ("stop_sequence", GenJson::Null),
                ]),
            ),
            (
                "usage",
                obj(vec![("output_tokens", int(u64::from(self.usage.3)))]),
            ),
        ]));
        events.push(obj(vec![("type", s("message_stop"))]));
        for event in events {
            if style.chance(5) {
                write_event(&mut out, "ping", "{\"type\": \"ping\"}", crlf, style);
            }
            let name = match &event {
                GenJson::Object(members) => match members.first() {
                    Some((_, GenJson::Str(kind))) => kind.clone(),
                    _ => String::new(),
                },
                _ => String::new(),
            };
            let data = event.render(&mut style.single_line());
            write_event(&mut out, &name, &data, crlf, style);
        }
        out
    }
}

/// Writes one event; the data is compact JSON, so it is one line.
pub fn write_event(out: &mut String, name: &str, data: &str, crlf: bool, style: &mut Style) {
    let end = if crlf { "\r\n" } else { "\n" };
    if !name.is_empty() && !style.chance(6) {
        out.push_str("event: ");
        out.push_str(name);
        out.push_str(end);
    }
    if style.chance(8) {
        out.push_str(": keep-alive");
        out.push_str(end);
    }
    out.push_str(if style.chance(4) { "data:" } else { "data: " });
    out.push_str(data);
    out.push_str(end);
    out.push_str(end);
}

/// A block's `content_block_start` shape and its deltas.
fn stream_block(block: &GenBlock, style: &mut Style) -> (GenJson, Vec<GenJson>) {
    let pieces = |text: &str, style: &mut Style, kind: &str, field: &str| -> Vec<GenJson> {
        style
            .split(text)
            .into_iter()
            .map(|piece| obj(vec![("type", s(kind)), (field, GenJson::Str(piece))]))
            .collect()
    };
    match block {
        GenBlock::Text(text) => (text_block(""), pieces(text, style, "text_delta", "text")),
        GenBlock::Thinking { text, signature } => {
            let mut deltas = pieces(text, style, "thinking_delta", "thinking");
            deltas.extend(pieces(signature, style, "signature_delta", "signature"));
            (
                obj(vec![
                    ("type", s("thinking")),
                    ("thinking", s("")),
                    ("signature", s("")),
                ]),
                deltas,
            )
        }
        GenBlock::ToolUse { id, name, input } => {
            let text = input.render(style);
            let mut deltas = Vec::new();
            if style.chance(2) {
                deltas.push(obj(vec![
                    ("type", s("input_json_delta")),
                    ("partial_json", s("")),
                ]));
            }
            deltas.extend(pieces(&text, style, "input_json_delta", "partial_json"));
            (
                obj(vec![
                    ("type", s("tool_use")),
                    ("id", s(id)),
                    ("name", s(name)),
                    ("input", GenJson::Object(Vec::new())),
                ]),
                deltas,
            )
        }
        GenBlock::ServerToolUse {
            kind,
            id,
            name,
            input,
        } => {
            let text = input.render(style);
            (
                obj(vec![
                    ("type", s(kind)),
                    ("id", s(id)),
                    ("name", s(name)),
                    ("input", GenJson::Object(Vec::new())),
                ]),
                pieces(&text, style, "input_json_delta", "partial_json"),
            )
        }
        GenBlock::Redacted(_) | GenBlock::ServerResult { .. } | GenBlock::Unknown { .. } => {
            (block.whole(), Vec::new())
        }
    }
}
