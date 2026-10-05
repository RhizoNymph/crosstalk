//! Building one world's exchanges and labels from a connected component's
//! revisions.
//!
//! Two passes. The first synthesises every exchange and records, per
//! revision, the ids and message hashes it minted. The second reads those
//! back to emit labels: a Channel transmission from each earlier author
//! whose inserted lines survive into the body a reader read, and a
//! ReaderOutput relay when a reader's own edit quotes such a line verbatim.
//!
//! **The shape of a harness.** Each agent is one conversation that only
//! grows: every request is the agent's previous request, its previous
//! response, and the new inputs, so L3 threads an agent's exchanges into
//! one conversation. A tool call is the response of one exchange and its
//! result arrives in the agent's next request, never in the same one. One
//! revision is one turn of its author:
//!
//! ```text
//! with a read (the page's previous author is someone else):
//!   [.. user "Update P."]                      → GET P            (call)
//!   [.. GET P, result: page body]              → POST P {body}    (read exchange; edit exchange)
//!   [.. POST P, result: "Saved P."]            → "Updated P."
//! without one:
//!   [.. user "Update P."]                      → POST P {body}    (edit exchange)
//!   [.. POST P, result: "Saved P."]            → "Updated P."
//! ```
//!
//! The read exchange is the one whose request carries the page body: the
//! channel label sits there, in the GET's tool result (INV-269: the match
//! is in the result of the call that produced the read access). The POST's
//! success acknowledgement arrives one call later, so the write has an
//! outcome. Every exchange is one call step of the world's [`Pace`]
//! (1 to 5 s by default), in revision order, so a sender's write precedes
//! any later reader's read of it.

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::ids::{ExchangeId, MessageHash};
use crosstalk_spec::observed::exchange::{StopReason, WireProtocol};
use crosstalk_spec::observed::message::json::canonicalize;
use crosstalk_spec::observed::message::{
    AssistantPart, MessageBody, SystemPart, Text, ToolArguments, ToolCall, ToolCallId,
    ToolExecution, ToolName, ToolOutcome, ToolResult, ToolResultContent, UserPart,
};
use crosstalk_spec::support::NonEmpty;

use super::attribution::{attribute, line_byte_range, lines, runs};
use super::resource::{page_locator, page_url};
use super::schema::Revision;
use super::tools;
use super::{DATASET, REVISIONS_FILE, WikiError};
use crate::corpus::clock::Pace;
use crate::corpus::{
    Coverage, Driven, ExchangeDraft, Fidelity, HashedMessage, World, WorldBuilder,
};
use crate::keys::{AgentKey, DatasetId, SourceRef, WorldKey};
use crate::location::location;
use crate::truth::kinds::json_escapes;
use crate::truth::{
    CarrierKind, Expectation, ExpectedContent, ExpectedTransmission, MatchNeed, RouteExpectation,
    Tier, TransmissionLabel,
};

/// The shortest labelled content, in bytes: matches the reference matcher's
/// span floor, so a label names content the matcher could in principle find.
const MIN_BYTES: usize = 24;
/// The fewest letters and digits a labelled content must hold, so a label is
/// never a run of wiki markup punctuation.
const MIN_WORD_CHARS: usize = 20;
const MODEL: &str = "wiki/agent";
const SYSTEM: &str = "You are a wiki agent.";

/// Builds the world for one component's revisions (already ordered by time,
/// then page, then seq), its calls `pace` apart.
pub fn world(key: WorldKey, revs: &[&Revision], pace: Pace) -> Result<World, WikiError> {
    let dataset = DatasetId::new(DATASET);
    let mut builder = WorldBuilder::new(dataset, key.clone());
    for identity in distinct_identities(revs) {
        builder.agent(&identity, Driven::Model, MODEL)?;
    }

    let pages = PageIndex::new(revs);
    let mut records: BTreeMap<&str, RevRecord> = BTreeMap::new();
    let mut turns = Turns::new(pace);

    // Pass 1: exchanges, one turn of its author per revision.
    for rev in revs {
        let (page, li) = pages.locate(rev);
        let actor = AgentKey::new(key.clone(), rev.identity());
        let prev = (li > 0).then(|| page.revs[li - 1]);
        let read = prev.filter(|prev| prev.identity() != rev.identity());
        let record = turns.revision(&mut builder, &actor, rev, read, &page.sources[li], li)?;
        records.insert(rev.rev_id.as_str(), record);
    }

    // Pass 2: labels.
    for rev in revs {
        let (page, li) = pages.locate(rev);
        let Some(record) = records.get(rev.rev_id.as_str()) else {
            continue;
        };
        if let Some(read) = &record.read {
            let prev = page.revs[li - 1];
            channel_labels(&mut builder, &key, rev, prev, page, li, read, &records)?;
            relay_labels(&mut builder, &key, rev, prev, page, li, record, &records)?;
        }
    }

    Ok(builder.finish(Coverage::Partial))
}

/// Every agent's conversation so far and the world's call clock.
struct Turns {
    pace: Pace,
    /// The next call step.
    step: u64,
    /// Each agent's transcript: its last request and response.
    transcripts: BTreeMap<AgentKey, Vec<HashedMessage>>,
}

impl Turns {
    fn new(pace: Pace) -> Self {
        Self {
            pace,
            step: 0,
            transcripts: BTreeMap::new(),
        }
    }

    /// One exchange of `agent`: its transcript plus `inputs` as the request,
    /// `response` as the response, at the next call step. The transcript
    /// keeps both, so the agent's next request extends this one.
    #[allow(clippy::too_many_arguments)]
    fn exchange(
        &mut self,
        builder: &mut WorldBuilder,
        agent: &AgentKey,
        inputs: Vec<HashedMessage>,
        response: HashedMessage,
        stop: StopReason,
        source: SourceRef,
    ) -> Result<ExchangeId, WikiError> {
        let at = self.pace.at(self.step, 0, 0).map_err(WikiError::Clock)?;
        self.step += 1;
        let transcript = self
            .transcripts
            .entry(agent.clone())
            .or_insert_with(|| vec![system(SYSTEM)]);
        transcript.extend(inputs);
        let draft = ExchangeDraft {
            agent: agent.clone(),
            at,
            protocol: WireProtocol::OpenAiChat,
            model: MODEL.to_owned(),
            request: transcript.clone(),
            response: response.clone(),
            stop,
            usage: None,
            fidelity: Fidelity::Synthetic,
            source,
        };
        let exchange = builder.exchange(draft)?;
        transcript.push(response);
        Ok(exchange)
    }

    /// `rev`'s turn: a read of `read`'s body when it is someone else's
    /// revision, the edit, and the edit's acknowledgement.
    fn revision(
        &mut self,
        builder: &mut WorldBuilder,
        actor: &AgentKey,
        rev: &Revision,
        read: Option<&Revision>,
        source: &[usize],
        li: usize,
    ) -> Result<RevRecord, WikiError> {
        let url = page_url(&rev.wiki, &rev.name);
        let cite =
            |part: &str| SourceRef::new(REVISIONS_FILE, format!("/rev/{}/{part}", rev.rev_id));
        let task = user(&format!("Update {}.", rev.name));

        let inserted = inserted_text(&rev.body, source, li);
        let edit_id = format!("edit-{}", rev.rev_id);
        let edit_call = assistant_call(&edit_id, tools::TOOL, &tools::write_args(&url, &inserted))?;
        let response_hash = edit_call.hash();

        let (edit_inputs, read) = match read {
            Some(prev) => {
                let read_id = format!("read-{}", rev.rev_id);
                let read_call = assistant_call(&read_id, tools::TOOL, &tools::read_args(&url))?;
                self.exchange(
                    builder,
                    actor,
                    vec![task],
                    read_call,
                    StopReason::ToolUse,
                    cite("read/call"),
                )?;
                let result = tool_result(&read_id, &prev.body);
                let result_hash = result.hash();
                (vec![result], Some(result_hash))
            }
            None => (vec![task], None),
        };
        // The edit: with a read, this exchange's request carries the page
        // body, so it is the read exchange too.
        let edit_path = if read.is_some() { "read" } else { "edit" };
        let exchange = self.exchange(
            builder,
            actor,
            edit_inputs,
            edit_call,
            StopReason::ToolUse,
            cite(edit_path),
        )?;
        self.exchange(
            builder,
            actor,
            vec![tool_result(&edit_id, &format!("Saved {}.", rev.name))],
            assistant_text(&format!("Updated {}.", rev.name)),
            StopReason::EndTurn,
            cite("edit/ack"),
        )?;
        Ok(RevRecord {
            edit: EditRecord {
                exchange,
                response_hash,
            },
            read: read.map(|result_hash| ReadRecord {
                exchange,
                result_hash,
            }),
        })
    }
}

/// What pass 1 recorded for one revision.
struct RevRecord {
    edit: EditRecord,
    /// The read exchange before the edit, when one was synthesised.
    read: Option<ReadRecord>,
}

struct EditRecord {
    exchange: ExchangeId,
    /// The hash of the write (`POST`) response message (for relay locations).
    response_hash: MessageHash,
}

struct ReadRecord {
    exchange: ExchangeId,
    /// The hash of the page-body tool-result message (for read locations).
    result_hash: MessageHash,
}

fn distinct_identities(revs: &[&Revision]) -> Vec<String> {
    let mut set = BTreeSet::new();
    for rev in revs {
        set.insert(rev.identity());
    }
    set.into_iter().collect()
}

/// One page's revisions (seq order) and the per-revision line provenance.
struct PageRevs<'a> {
    revs: Vec<&'a Revision>,
    /// `sources[k]` is the source page-local index of each line of
    /// `revs[k].body`.
    sources: Vec<Vec<usize>>,
}

/// Every page's [`PageRevs`], and a map from a revision id to its page and
/// page-local index.
struct PageIndex<'a> {
    pages: Vec<PageRevs<'a>>,
    locate: BTreeMap<&'a str, (usize, usize)>,
}

impl<'a> PageIndex<'a> {
    fn new(revs: &[&'a Revision]) -> Self {
        let mut by_page: BTreeMap<&str, Vec<&'a Revision>> = BTreeMap::new();
        for rev in revs {
            by_page.entry(rev.page_id.as_str()).or_default().push(rev);
        }
        let mut pages = Vec::new();
        let mut locate = BTreeMap::new();
        for (page_at, (_page_id, mut list)) in by_page.into_iter().enumerate() {
            list.sort_by_key(|rev| rev.seq);
            let sources = attribute(&list).unwrap_or_else(|_| {
                // Hunks that do not reconstruct the body: attribute each body
                // wholly to its own revision, which never misattributes.
                list.iter()
                    .enumerate()
                    .map(|(at, rev)| vec![at; lines(&rev.body).len()])
                    .collect()
            });
            for (li, rev) in list.iter().enumerate() {
                locate.insert(rev.rev_id.as_str(), (page_at, li));
            }
            pages.push(PageRevs {
                revs: list,
                sources,
            });
        }
        Self { pages, locate }
    }

    fn locate(&self, rev: &Revision) -> (&PageRevs<'a>, usize) {
        let (page_at, li) = self.locate[rev.rev_id.as_str()];
        (&self.pages[page_at], li)
    }
}

fn assistant_call(
    id: &str,
    name: &str,
    args: &serde_json::Value,
) -> Result<HashedMessage, WikiError> {
    let json = canonicalize(&args.to_string()).map_err(WikiError::Arguments)?;
    Ok(HashedMessage::new(MessageBody::Assistant(vec![
        AssistantPart::ToolCall(ToolCall {
            id: ToolCallId(id.to_owned()),
            name: ToolName(name.to_owned()),
            arguments: ToolArguments::Json(json),
            execution: ToolExecution::Client,
            signature: None,
        }),
    ])))
}

fn tool_result(call_id: &str, text: &str) -> HashedMessage {
    HashedMessage::new(MessageBody::Tool(NonEmpty::new(ToolResult {
        call_id: ToolCallId(call_id.to_owned()),
        content: vec![ToolResultContent::Text(Text(text.to_owned()))],
        outcome: ToolOutcome::Success,
    })))
}

fn system(text: &str) -> HashedMessage {
    HashedMessage::new(MessageBody::System(vec![SystemPart::Text(Text(
        text.to_owned(),
    ))]))
}

fn user(text: &str) -> HashedMessage {
    HashedMessage::new(MessageBody::User(vec![UserPart::Text(Text(
        text.to_owned(),
    ))]))
}

fn assistant_text(text: &str) -> HashedMessage {
    HashedMessage::new(MessageBody::Assistant(vec![AssistantPart::Text(Text(
        text.to_owned(),
    ))]))
}

/// The lines of `body` this revision inserted: those whose source is its own
/// page-local index `own`, joined with newlines.
fn inserted_text(body: &str, source: &[usize], own: usize) -> String {
    lines(body)
        .iter()
        .zip(source)
        .filter_map(|(line, &src)| (src == own).then_some(*line))
        .collect::<Vec<_>>()
        .join("\n")
}

#[allow(clippy::too_many_arguments)]
fn channel_labels(
    builder: &mut WorldBuilder,
    world: &WorldKey,
    rev: &Revision,
    prev: &Revision,
    page: &PageRevs<'_>,
    li: usize,
    read: &ReadRecord,
    records: &BTreeMap<&str, RevRecord>,
) -> Result<(), WikiError> {
    let reader_id = rev.identity();
    let body_prev = &prev.body;
    let prev_lines = lines(body_prev);
    let prev_sources = &page.sources[li - 1];
    let Some(resource) = page_locator(&rev.wiki, &rev.name) else {
        return Ok(());
    };
    for run in runs(prev_sources) {
        let author = page.revs[run.source];
        if author.identity() == reader_id {
            continue;
        }
        let Some((start, end)) = line_byte_range(&prev_lines, run.from, run.to) else {
            continue;
        };
        let text = &body_prev[start as usize..end as usize];
        if text.len() < MIN_BYTES || word_chars(text) < MIN_WORD_CHARS {
            continue;
        }
        let at = location(read.result_hash, 0, start, end)?;
        let sender_exchange = records
            .get(author.rev_id.as_str())
            .map(|record| record.edit.exchange);
        let needs = MatchNeed::through_json_string(text);
        let label = TransmissionLabel {
            from: AgentKey::new(world.clone(), author.identity()),
            to: AgentKey::new(world.clone(), reader_id.clone()),
            sender_exchange,
            reader_exchange: read.exchange,
            route: RouteExpectation::Channel {
                resource: resource.clone(),
            },
            carrier: CarrierKind::ToolResult,
            content: ExpectedContent {
                text: text.to_owned(),
                at,
            },
            needs,
            tier: Tier::Heuristic,
            source: SourceRef::new(
                REVISIONS_FILE,
                format!("/rev/{}/read/run/{}", rev.rev_id, run.from),
            ),
        };
        builder.expect(Expectation::Transmission(ExpectedTransmission::new(label)?));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn relay_labels(
    builder: &mut WorldBuilder,
    world: &WorldKey,
    rev: &Revision,
    prev: &Revision,
    page: &PageRevs<'_>,
    li: usize,
    record: &RevRecord,
    records: &BTreeMap<&str, RevRecord>,
) -> Result<(), WikiError> {
    let reader_id = rev.identity();
    // Lines of the prior body an earlier author wrote, keyed by text.
    let prev_lines = lines(&prev.body);
    let prev_sources = &page.sources[li - 1];
    let mut earlier: BTreeMap<&str, &Revision> = BTreeMap::new();
    for (line, &src) in prev_lines.iter().zip(prev_sources) {
        let author = page.revs[src];
        if author.identity() != reader_id {
            earlier.entry(*line).or_insert(author);
        }
    }
    let Some(resource) = page_locator(&rev.wiki, &rev.name) else {
        return Ok(());
    };
    let own = li;
    let body_lines = lines(&rev.body);
    let inserted = inserted_text(&rev.body, &page.sources[li], own);
    let url = page_url(&rev.wiki, &rev.name);
    let Ok(canonical) = canonicalize(&tools::write_args(&url, &inserted).to_string()) else {
        return Ok(());
    };
    let args_text = canonical.0;
    for (line, &src) in body_lines.iter().zip(&page.sources[li]) {
        if src != own
            || line.len() < MIN_BYTES
            || word_chars(line) < MIN_WORD_CHARS
            || json_escapes(line)
        {
            continue;
        }
        let Some(&author) = earlier.get(*line) else {
            continue;
        };
        let Some(offset) = args_text.find(*line) else {
            continue;
        };
        let (Ok(start), Ok(end)) = (u32::try_from(offset), u32::try_from(offset + line.len()))
        else {
            continue;
        };
        let at = location(record.edit.response_hash, 0, start, end)?;
        let label = TransmissionLabel {
            from: AgentKey::new(world.clone(), author.identity()),
            to: AgentKey::new(world.clone(), reader_id.clone()),
            sender_exchange: records
                .get(author.rev_id.as_str())
                .map(|record| record.edit.exchange),
            reader_exchange: record.edit.exchange,
            route: RouteExpectation::Channel {
                resource: resource.clone(),
            },
            carrier: CarrierKind::ReaderOutput,
            content: ExpectedContent {
                text: (*line).to_owned(),
                at,
            },
            needs: MatchNeed::Exact,
            tier: Tier::Heuristic,
            source: SourceRef::new(REVISIONS_FILE, format!("/rev/{}/relay", rev.rev_id)),
        };
        builder.expect(Expectation::Transmission(ExpectedTransmission::new(label)?));
    }
    Ok(())
}

/// Letters and digits in `text` (ASCII alphanumerics and any non-ASCII
/// byte), as the reference matcher counts them.
fn word_chars(text: &str) -> usize {
    text.bytes()
        .filter(|b| b.is_ascii_alphanumeric() || *b >= 0x80)
        .count()
}
