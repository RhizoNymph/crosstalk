//! Claude Code traffic on the wire: request heads and bodies, and the
//! event-stream responses Anthropic answers them with.
//!
//! Shaped like the recorded corpus (`crates/testkit/corpus/anthropic`):
//! the headers Claude Code sends (`user-agent: claude-cli/…`,
//! `x-claude-code-session-id`, `x-api-key` or a subscription's Bearer
//! token, `anthropic-beta`), the full
//! history in every request, the Claude Code system prompt and tool list,
//! `metadata.user_id` naming the session, and a streamed response with one
//! content block per assistant block.

use serde_json::{Value, json};

/// The Anthropic API version every request names.
pub const ANTHROPIC_VERSION: &str = "2023-06-01";

/// The model every scenario exchange asks for.
pub const MODEL: &str = "claude-opus-5-5";

/// The Claude Code version the user agent claims.
pub const CLAUDE_CODE_VERSION: &str = "2.1.282";

/// One HTTP request as the harness sends it to the gateway's route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    pub method: String,
    /// The path under the gateway, route prefix included
    /// (`/anthropic/v1/messages`).
    pub path: String,
    pub query: Option<String>,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// One HTTP response as the upstream sends it back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    /// The concatenated server-sent events.
    pub body: Vec<u8>,
}

/// A content block of a message.
#[derive(Debug, Clone, PartialEq)]
pub enum Block {
    Text(String),
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
    },
}

impl Block {
    fn to_json(&self) -> Value {
        match self {
            Block::Text(text) => json!({ "type": "text", "text": text }),
            Block::ToolUse { id, name, input } => {
                json!({ "type": "tool_use", "id": id, "name": name, "input": input })
            }
            Block::ToolResult {
                tool_use_id,
                content,
            } => json!({
                "type": "tool_result",
                "tool_use_id": tool_use_id,
                "content": content,
            }),
        }
    }
}

/// Who sent a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    User,
    Assistant,
}

impl Role {
    fn as_str(self) -> &'static str {
        match self {
            Role::User => "user",
            Role::Assistant => "assistant",
        }
    }
}

/// One message of a conversation.
#[derive(Debug, Clone, PartialEq)]
pub struct Turn {
    pub role: Role,
    pub blocks: Vec<Block>,
}

impl Turn {
    fn to_json(&self) -> Value {
        json!({
            "role": self.role.as_str(),
            "content": self.blocks.iter().map(Block::to_json).collect::<Vec<_>>(),
        })
    }
}

/// How a session authenticates. Either way the credential is hashed at L0
/// and never leaves the hot path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Credential {
    /// A Console API key, sent as `x-api-key`.
    ApiKey(String),
    /// A Claude Pro/Max login: the OAuth access token as
    /// `Authorization: Bearer`, with the OAuth capability in
    /// `anthropic-beta` that subscription requests require.
    Subscription { access_token: String },
}

/// The `anthropic-beta` values every request carries.
const BETAS: &str = "claude-code-20250219,interleaved-thinking-2025-05-14";
/// The same with the OAuth capability, as Claude Code sends on a claude.ai
/// login.
const OAUTH_BETAS: &str = "claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14";

impl Credential {
    /// The header carrying the credential, and its value.
    fn header(&self) -> (&'static str, String) {
        match self {
            Self::ApiKey(key) => ("x-api-key", key.clone()),
            Self::Subscription { access_token } => {
                ("authorization", format!("Bearer {access_token}"))
            }
        }
    }

    fn betas(&self) -> &'static str {
        match self {
            Self::ApiKey(_) => BETAS,
            Self::Subscription { .. } => OAUTH_BETAS,
        }
    }
}

/// Who a session is on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionHeaders {
    pub credential: Credential,
    /// The `x-claude-code-session-id` value.
    pub session_id: String,
    /// The account part of `metadata.user_id`.
    pub user_hash: String,
    /// The working directory the system prompt names.
    pub cwd: String,
}

/// The tools Claude Code declares, by name, with a minimal input schema
/// each.
fn tool_definitions(tools: &[ToolSpec]) -> Value {
    Value::Array(
        tools
            .iter()
            .map(|tool| {
                let properties: serde_json::Map<String, Value> = tool
                    .fields
                    .iter()
                    .map(|field| ((*field).to_owned(), json!({ "type": "string" })))
                    .collect();
                json!({
                    "name": tool.name,
                    "description": tool.description,
                    "input_schema": {
                        "type": "object",
                        "properties": properties,
                        "required": tool.fields,
                        "additionalProperties": false,
                        "$schema": "http://json-schema.org/draft-07/schema#",
                    },
                })
            })
            .collect(),
    )
}

/// A tool the request declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolSpec {
    pub name: &'static str,
    pub description: &'static str,
    pub fields: &'static [&'static str],
}

/// The request Claude Code sends for `history` (the whole conversation so
/// far, ending in a user turn).
pub fn request(session: &SessionHeaders, history: &[Turn], tools: &[ToolSpec]) -> HttpRequest {
    let body = json!({
        "model": MODEL,
        "messages": history.iter().map(Turn::to_json).collect::<Vec<_>>(),
        "system": [
            {
                "type": "text",
                "text": format!(
                    "x-anthropic-billing-header: cc_version={CLAUDE_CODE_VERSION}.e2e; cc_entrypoint=cli;"
                ),
            },
            {
                "type": "text",
                "text": "You are Claude Code, Anthropic's official CLI for Claude.",
                "cache_control": { "type": "ephemeral" },
            },
            {
                "type": "text",
                "text": format!(
                    "You are an interactive CLI tool that helps users with software engineering tasks.\n\n<env>\nWorking directory: {}\nIs directory a git repo: Yes\nPlatform: linux\n</env>",
                    session.cwd
                ),
                "cache_control": { "type": "ephemeral" },
            },
        ],
        "tools": tool_definitions(tools),
        "metadata": {
            "user_id": format!(
                "user_{}_account__session_{}",
                session.user_hash, session.session_id
            ),
        },
        "max_tokens": 32000,
        "stream": true,
    });
    let credential = session.credential.header();
    let headers = [
        ("accept", "application/json".to_owned()),
        ("anthropic-beta", session.credential.betas().to_owned()),
        (
            "anthropic-dangerous-direct-browser-access",
            "true".to_owned(),
        ),
        ("anthropic-version", ANTHROPIC_VERSION.to_owned()),
        ("content-type", "application/json".to_owned()),
        (
            "user-agent",
            format!("claude-cli/{CLAUDE_CODE_VERSION} (external, cli)"),
        ),
        ("x-app", "cli".to_owned()),
        credential,
        ("x-claude-code-session-id", session.session_id.clone()),
        ("x-stainless-arch", "x64".to_owned()),
        ("x-stainless-lang", "js".to_owned()),
        ("x-stainless-os", "Linux".to_owned()),
        ("x-stainless-package-version", "0.70.0".to_owned()),
        ("x-stainless-retry-count", "0".to_owned()),
        ("x-stainless-runtime", "node".to_owned()),
        ("x-stainless-runtime-version", "v22.19.0".to_owned()),
        ("x-stainless-timeout", "600".to_owned()),
        ("x-stainless-helper-method", "stream".to_owned()),
    ];
    HttpRequest {
        method: "POST".to_owned(),
        path: "/anthropic/v1/messages".to_owned(),
        query: Some("beta=true".to_owned()),
        headers: headers
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect(),
        body: body.to_string().into_bytes(),
    }
}

/// Why the assistant stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    EndTurn,
    ToolUse,
}

impl Stop {
    fn as_str(self) -> &'static str {
        match self {
            Stop::EndTurn => "end_turn",
            Stop::ToolUse => "tool_use",
        }
    }
}

/// Token counts the response reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Usage {
    pub input: u32,
    pub cache_read: u32,
    pub output: u32,
}

/// The streamed response carrying `blocks` (text and tool calls only).
pub fn response(
    message_id: &str,
    request_id: &str,
    blocks: &[Block],
    stop: Stop,
    usage: Usage,
) -> HttpResponse {
    let mut events = Vec::new();
    events.push(sse(
        "message_start",
        &json!({
            "type": "message_start",
            "message": {
                "id": message_id,
                "type": "message",
                "role": "assistant",
                "model": MODEL,
                "content": [],
                "stop_reason": null,
                "stop_sequence": null,
                "usage": {
                    "input_tokens": usage.input,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": usage.cache_read,
                    "cache_creation": {
                        "ephemeral_5m_input_tokens": 0,
                        "ephemeral_1h_input_tokens": 0,
                    },
                    "output_tokens": 2,
                    "service_tier": "standard",
                },
            },
        }),
    ));
    for (index, block) in blocks.iter().enumerate() {
        match block {
            Block::Text(text) => {
                events.push(sse(
                    "content_block_start",
                    &json!({
                        "type": "content_block_start",
                        "index": index,
                        "content_block": { "type": "text", "text": "" },
                    }),
                ));
                for piece in text_pieces(text) {
                    events.push(sse(
                        "content_block_delta",
                        &json!({
                            "type": "content_block_delta",
                            "index": index,
                            "delta": { "type": "text_delta", "text": piece },
                        }),
                    ));
                }
            }
            Block::ToolUse { id, name, input } => {
                events.push(sse(
                    "content_block_start",
                    &json!({
                        "type": "content_block_start",
                        "index": index,
                        "content_block": {
                            "type": "tool_use",
                            "id": id,
                            "name": name,
                            "input": {},
                        },
                    }),
                ));
                events.push(sse(
                    "content_block_delta",
                    &json!({
                        "type": "content_block_delta",
                        "index": index,
                        "delta": { "type": "input_json_delta", "partial_json": input.to_string() },
                    }),
                ));
            }
            // A response never carries a tool result.
            Block::ToolResult { .. } => continue,
        }
        events.push(sse(
            "content_block_stop",
            &json!({ "type": "content_block_stop", "index": index }),
        ));
    }
    events.push(sse(
        "message_delta",
        &json!({
            "type": "message_delta",
            "delta": { "stop_reason": stop.as_str(), "stop_sequence": null },
            "usage": { "output_tokens": usage.output },
        }),
    ));
    events.push(sse("message_stop", &json!({ "type": "message_stop" })));
    HttpResponse {
        status: 200,
        headers: vec![
            (
                "content-type".to_owned(),
                "text/event-stream; charset=utf-8".to_owned(),
            ),
            ("cache-control".to_owned(), "no-cache".to_owned()),
            ("request-id".to_owned(), request_id.to_owned()),
        ],
        body: events.concat().into_bytes(),
    }
}

fn sse(event: &str, data: &Value) -> String {
    format!("event: {event}\ndata: {data}\n\n")
}

/// A text block streamed in word-boundary pieces of a few words each, as
/// the API streams them.
fn text_pieces(text: &str) -> Vec<String> {
    let words: Vec<&str> = text.split_inclusive(' ').collect();
    words.chunks(6).map(|chunk| chunk.concat()).collect()
}
