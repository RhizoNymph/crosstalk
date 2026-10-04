//! One simulated agent: conversations of prompts through the gateway, wiki
//! tool calls executed against the wiki and their results sent back, think
//! time between prompts, until the run stops.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use crosstalk_testkit::client::{BodyEnd, HarnessClient, Next};
use crosstalk_testkit::corpus::http::Headers;
use crosstalk_testkit::corpus::sse::EventStream;
use hyper::Method;
use hyper::header::{HeaderName, HeaderValue};
use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::anthropic::assemble::assemble_stream;
use crate::anthropic::{API_VERSION, AssistantMessage};
use crate::http::request;
use crate::knobs::Rng;
use crate::protocol::{PageSlug, Task, Topic, WIKI_READ, WIKI_WRITE};
use crate::upstream::generate::page_of;
use crate::wiki::{AUTHOR_HEADER, VERSION_HEADER};

use super::config::SwarmConfig;
use super::conversation::{
    Conversation, OrderError, PendingTools, Profile, Step, ToolCall, ToolResult,
};
use super::stats::{Event, Outcome, RequestSample};

/// Tool rounds one prompt may take before the conversation is abandoned.
const MAX_TOOL_ROUNDS: u32 = 4;
/// Attempts per request before the conversation is abandoned.
const ATTEMPTS: u32 = 3;
/// The user agent outside the Claude Code shape.
pub const USER_AGENT: &str = concat!("crosstalk-demo-swarm/", env!("CARGO_PKG_VERSION"));
/// The user agent in the Claude Code shape.
pub const CLAUDE_CODE_USER_AGENT: &str = "claude-cli/2.1.282 (external, cli)";

/// What every agent shares, read-only.
#[derive(Debug)]
pub struct Shared {
    pub config: SwarmConfig,
    pub gateway: HarnessClient,
    pub wiki: HarnessClient,
}

/// One agent's identity.
#[derive(Debug, Clone)]
pub struct Agent {
    pub index: u32,
    pub name: String,
    pub key: String,
    pub profile: Profile,
}

/// The `x-api-key` agents in `group` share: fake, stable per seed.
pub fn api_key(seed: u64, group: u32) -> String {
    let hex = Rng::derive(seed, &[b"api-key", &group.to_le_bytes()]).hex(40);
    format!("sk-ant-demo{group:04}-{hex}")
}

impl Agent {
    pub fn new(config: &SwarmConfig, index: u32) -> Self {
        let name = format!("agent-{index:03}");
        let group = index / config.agents_per_key;
        let focus = Topic::of(index % config.topics);
        let system = format!(
            "You are {name}, a research agent on a team of {} agents. Your focus is {}. \
             The team shares a wiki: read pages with {WIKI_READ} before relying on them and \
             record what you learn with {WIKI_WRITE}. Be concise.",
            config.agents, focus.label
        );
        Self {
            index,
            profile: Profile {
                agent: name.clone(),
                system,
                model: config.model.clone(),
                max_tokens: config.max_tokens.get(),
            },
            name,
            key: api_key(config.seed, group),
        }
    }
}

/// Why a request failed (after it was counted).
#[derive(Debug, thiserror::Error)]
enum ExchangeError {
    #[error("building the request: {0}")]
    Build(String),
    #[error("request failed: {0}")]
    Failed(Outcome),
}

/// Why a conversation was given up.
#[derive(Debug, thiserror::Error)]
enum Abandon {
    #[error(transparent)]
    Order(#[from] OrderError),
    #[error("a request failed {ATTEMPTS} times")]
    Failing,
    #[error("more than {MAX_TOOL_ROUNDS} tool rounds for one prompt")]
    TooManyRounds,
}

/// The wiki listing could not be read.
#[derive(Debug, thiserror::Error)]
#[error("the wiki listing could not be read")]
struct ListingFailed;

/// A stable UUID-shaped session id.
fn session_id(rng: &mut Rng) -> String {
    let hex = rng.hex(32);
    format!(
        "{}-{}-4{}-8{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[13..16],
        &hex[17..20],
        &hex[20..32]
    )
}

/// Sleeps `duration` unless the run stops first; `false` when it stopped.
async fn pause(duration: Duration, stop: &mut watch::Receiver<bool>) -> bool {
    if *stop.borrow() {
        return false;
    }
    tokio::select! {
        () = tokio::time::sleep(duration) => !*stop.borrow(),
        _ = stop.changed() => false,
    }
}

/// Runs `agent` until `stop` turns true.
pub async fn run(
    agent: Agent,
    shared: Arc<Shared>,
    events: mpsc::Sender<Event>,
    mut stop: watch::Receiver<bool>,
) {
    let config = &shared.config;
    let mut rng = Rng::derive(config.seed, &[b"agent", &agent.index.to_le_bytes()]);
    tracing::debug!(agent = %agent.name, "agent starting");
    while !*stop.borrow() {
        let mut conversation = Conversation::new(session_id(&mut rng), config.claude_code_shape);
        let prompts = config.turns.get().draw(&mut rng);
        let mut completed = true;
        while u64::from(conversation.prompts()) < prompts {
            if *stop.borrow() {
                return;
            }
            let task = choose_task(&agent, &shared, &mut rng, &events).await;
            if let Err(why) =
                prompt(&agent, &shared, &mut rng, &events, &mut conversation, task).await
            {
                tracing::debug!(agent = %agent.name, %why, "abandoning the conversation");
                completed = false;
                break;
            }
            if !pause(config.think_ms.draw_ms(&mut rng), &mut stop).await {
                return;
            }
        }
        let _ = events.send(Event::ConversationEnded { completed }).await;
        if !completed && !pause(config.think_ms.draw_ms(&mut rng), &mut stop).await {
            return;
        }
    }
}

/// Picks the next prompt's task.
async fn choose_task(
    agent: &Agent,
    shared: &Shared,
    rng: &mut Rng,
    events: &mpsc::Sender<Event>,
) -> Task {
    let config = &shared.config;
    let draw = rng.unit();
    let random_page = |rng: &mut Rng| {
        let index = u32::try_from(rng.below(u64::from(config.pages.get()))).unwrap_or(0);
        (
            PageSlug::of_page(index, config.topics.get()),
            index % config.topics,
        )
    };
    if draw < config.mix.write().get() {
        let (page, topic) = random_page(rng);
        return Task::Write { page, topic };
    }
    if draw < config.mix.write().get() + config.mix.read().get() {
        // Prefer a page someone else wrote: that is a transmission.
        let others = match list_pages(shared).await {
            Ok(pages) => pages
                .into_iter()
                .filter(|(_, author)| author != &agent.name)
                .map(|(page, _)| page)
                .collect(),
            Err(ListingFailed) => {
                let _ = events.send(Event::WikiError).await;
                Vec::new()
            }
        };
        let page = match rng.pick(&others) {
            Some(page) => page.clone(),
            None => random_page(rng).0,
        };
        return Task::Read { page };
    }
    Task::Chat {
        topic: agent.index % config.topics,
    }
}

/// The wiki's pages and their last writers.
async fn list_pages(shared: &Shared) -> Result<Vec<(PageSlug, String)>, ListingFailed> {
    let request =
        request(Method::GET, "/pages", Headers::new(), Bytes::new()).map_err(|_| ListingFailed)?;
    let response = shared.wiki.send(&request).await.map_err(|error| {
        tracing::debug!(%error, "wiki listing failed");
        ListingFailed
    })?;
    if !response.status.is_success() || response.end != BodyEnd::Complete {
        return Err(ListingFailed);
    }
    let value: Value = serde_json::from_slice(&response.body).map_err(|_| ListingFailed)?;
    Ok(value
        .get("pages")
        .and_then(Value::as_array)
        .map(|pages| {
            pages
                .iter()
                .filter_map(|p| {
                    let page = p.get("page")?.as_str()?.parse().ok()?;
                    let author = p.get("author")?.as_str()?.to_owned();
                    Some((page, author))
                })
                .collect()
        })
        .unwrap_or_default())
}

/// One prompt: ask, then answer tool calls until the model ends its turn.
async fn prompt(
    agent: &Agent,
    shared: &Shared,
    rng: &mut Rng,
    events: &mpsc::Sender<Event>,
    conversation: &mut Conversation,
    task: Task,
) -> Result<(), Abandon> {
    conversation.ask(task.prompt())?;
    for _ in 0..=MAX_TOOL_ROUNDS {
        let answer = send_with_retries(agent, shared, rng, events, conversation).await?;
        let pending = match conversation.receive(answer)? {
            Step::Answered => return Ok(()),
            Step::Tools(pending) => pending,
        };
        let results = execute(agent, shared, events, &pending).await;
        conversation.resolve(pending, results)?;
    }
    Err(Abandon::TooManyRounds)
}

async fn send_with_retries(
    agent: &Agent,
    shared: &Shared,
    rng: &mut Rng,
    events: &mpsc::Sender<Event>,
    conversation: &Conversation,
) -> Result<AssistantMessage, Abandon> {
    let streaming = rng.chance(shared.config.stream_fraction);
    for attempt in 1..=ATTEMPTS {
        match exchange(agent, shared, events, conversation, streaming).await {
            Ok(answer) => return Ok(answer),
            Err(error) => {
                tracing::debug!(agent = %agent.name, attempt, %error, "request failed");
                if attempt < ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(500 * u64::from(attempt))).await;
                }
            }
        }
    }
    Err(Abandon::Failing)
}

fn headers(
    agent: &Agent,
    shared: &Shared,
    conversation: &Conversation,
    streaming: bool,
) -> Result<Headers, ExchangeError> {
    let mut headers = Headers::new();
    let mut push = |name: &'static str, value: &str| -> Result<(), ExchangeError> {
        let value =
            HeaderValue::from_str(value).map_err(|e| ExchangeError::Build(e.to_string()))?;
        headers.push(HeaderName::from_static(name), value);
        Ok(())
    };
    push(
        "accept",
        if streaming {
            "text/event-stream"
        } else {
            "application/json"
        },
    )?;
    push("anthropic-version", API_VERSION)?;
    push("content-type", "application/json")?;
    if shared.config.claude_code_shape {
        push("anthropic-beta", "claude-code-20250219")?;
        push("user-agent", CLAUDE_CODE_USER_AGENT)?;
        push("x-app", "cli")?;
    } else {
        push("user-agent", USER_AGENT)?;
    }
    push("x-api-key", &agent.key)?;
    push("x-claude-code-session-id", conversation.session())?;
    Ok(headers)
}

/// Sends the conversation once and reads the answer; reports the sample.
async fn exchange(
    agent: &Agent,
    shared: &Shared,
    events: &mpsc::Sender<Event>,
    conversation: &Conversation,
    streaming: bool,
) -> Result<AssistantMessage, ExchangeError> {
    let body = serde_json::to_vec(&conversation.body(&agent.profile, streaming))
        .map_err(|e| ExchangeError::Build(e.to_string()))?;
    let request_bytes = body.len();
    let request = request(
        Method::POST,
        "/v1/messages",
        headers(agent, shared, conversation, streaming)?,
        body,
    )
    .map_err(|e| ExchangeError::Build(e.to_string()))?;
    let started = Instant::now();
    let mut ttfb = None;
    let mut received = BytesMut::new();
    let (outcome, answer) = match shared.gateway.open(&request).await {
        Err(error) => {
            tracing::debug!(agent = %agent.name, %error, "no response");
            (Outcome::Transport, None)
        }
        Ok(mut response) => {
            let end = loop {
                match response.next().await {
                    Next::Chunk(chunk) => {
                        ttfb.get_or_insert_with(|| started.elapsed());
                        received.extend_from_slice(&chunk.bytes);
                    }
                    Next::End(end) => break end,
                }
            };
            let status = response.status();
            if !status.is_success() {
                (Outcome::Status(status.as_u16()), None)
            } else if end != BodyEnd::Complete {
                tracing::debug!(agent = %agent.name, ?end, "body did not complete");
                (Outcome::Transport, None)
            } else {
                match decode(received.clone().freeze(), streaming) {
                    Ok(answer) => (Outcome::Ok, Some(answer)),
                    Err(error) => {
                        tracing::warn!(agent = %agent.name, %error, "unreadable answer");
                        (Outcome::Malformed, None)
                    }
                }
            }
        }
    };
    let _ = events
        .send(Event::Request(RequestSample {
            streaming,
            followup: conversation.is_followup(),
            outcome,
            ttfb,
            total: started.elapsed(),
            request_bytes,
            response_bytes: received.len(),
        }))
        .await;
    answer.ok_or(ExchangeError::Failed(outcome))
}

/// Why a 2xx body is not an answer.
#[derive(Debug, thiserror::Error)]
enum DecodeError {
    #[error(transparent)]
    Stream(#[from] crate::anthropic::assemble::AssembleError),
    #[error(transparent)]
    Document(#[from] crate::anthropic::DocumentError),
    #[error(transparent)]
    Sse(#[from] crosstalk_testkit::corpus::sse::SseError),
}

fn decode(body: Bytes, streaming: bool) -> Result<AssistantMessage, DecodeError> {
    if streaming {
        Ok(assemble_stream(&EventStream::parse(body)?)?)
    } else {
        Ok(AssistantMessage::from_document(&body)?)
    }
}

/// Runs every pending tool call against the wiki.
async fn execute(
    agent: &Agent,
    shared: &Shared,
    events: &mpsc::Sender<Event>,
    pending: &PendingTools,
) -> Vec<ToolResult> {
    let mut results = Vec::with_capacity(pending.calls().len());
    for call in pending.calls() {
        results.push(run_tool(agent, shared, events, call).await);
    }
    results
}

async fn run_tool(
    agent: &Agent,
    shared: &Shared,
    events: &mpsc::Sender<Event>,
    call: &ToolCall,
) -> ToolResult {
    let Some(page) = page_of(&call.input) else {
        return call.result("Error: `page` must be a page name.".to_owned(), true);
    };
    match call.name.as_str() {
        WIKI_WRITE => {
            let Some(content) = call.input.get("content").and_then(Value::as_str) else {
                return call.result("Error: `content` must be text.".to_owned(), true);
            };
            write_page(agent, shared, events, call, page, content).await
        }
        WIKI_READ => read_page(agent, shared, events, call, page).await,
        other => call.result(format!("Error: no tool named {other}."), true),
    }
}

async fn write_page(
    agent: &Agent,
    shared: &Shared,
    events: &mpsc::Sender<Event>,
    call: &ToolCall,
    page: PageSlug,
    content: &str,
) -> ToolResult {
    let mut headers = Headers::new();
    if let Ok(author) = HeaderValue::from_str(&agent.name) {
        headers.push(HeaderName::from_static(AUTHOR_HEADER), author);
    }
    headers.push(
        HeaderName::from_static("content-type"),
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    let target = format!("/pages/{page}");
    let Ok(request) = request(Method::PUT, &target, headers, content.to_owned()) else {
        return call.result("Error: bad page name.".to_owned(), true);
    };
    let response = match shared.wiki.send(&request).await {
        Ok(response) if response.status.is_success() => response,
        Ok(response) => {
            let _ = events.send(Event::WikiError).await;
            return call.result(
                format!("Error: the wiki answered {}.", response.status),
                true,
            );
        }
        Err(error) => {
            tracing::debug!(%error, "wiki write failed");
            let _ = events.send(Event::WikiError).await;
            return call.result("Error: the wiki is unreachable.".to_owned(), true);
        }
    };
    let version = serde_json::from_slice::<Value>(&response.body)
        .ok()
        .and_then(|v| v.get("version").and_then(Value::as_u64))
        .unwrap_or(0);
    let _ = events
        .send(Event::WikiWrite {
            author: agent.name.clone(),
            page: page.clone(),
            version,
        })
        .await;
    call.result(
        format!(
            "Saved `{page}` (version {version}, {} bytes).",
            content.len()
        ),
        false,
    )
}

async fn read_page(
    agent: &Agent,
    shared: &Shared,
    events: &mpsc::Sender<Event>,
    call: &ToolCall,
    page: PageSlug,
) -> ToolResult {
    let target = format!("/pages/{page}");
    let Ok(request) = request(Method::GET, &target, Headers::new(), Bytes::new()) else {
        return call.result("Error: bad page name.".to_owned(), true);
    };
    match shared.wiki.send(&request).await {
        Ok(response) if response.status.is_success() => {
            let author = response
                .headers
                .get_str(AUTHOR_HEADER)
                .unwrap_or("anonymous")
                .to_owned();
            let version = response
                .headers
                .get_str(VERSION_HEADER)
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let text = String::from_utf8_lossy(&response.body).into_owned();
            let _ = events
                .send(Event::WikiRead {
                    reader: agent.name.clone(),
                    page,
                    found: Some((author, version)),
                })
                .await;
            call.result(text, false)
        }
        Ok(response) if response.status == hyper::StatusCode::NOT_FOUND => {
            let _ = events
                .send(Event::WikiRead {
                    reader: agent.name.clone(),
                    page: page.clone(),
                    found: None,
                })
                .await;
            call.result(format!("Page `{page}` does not exist yet."), true)
        }
        Ok(response) => {
            let _ = events.send(Event::WikiError).await;
            call.result(
                format!("Error: the wiki answered {}.", response.status),
                true,
            )
        }
        Err(error) => {
            tracing::debug!(%error, "wiki read failed");
            let _ = events.send(Event::WikiError).await;
            call.result("Error: the wiki is unreachable.".to_owned(), true)
        }
    }
}
