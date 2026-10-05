//! Building one world's exchanges and labels from a connected component's
//! revisions.
//!
//! Two passes. The first synthesises every exchange (a read before each edit
//! whose author differs from the page's previous author, then the edit) and
//! records, per revision, the ids and message hashes it minted. The second
//! reads those back to emit labels: a Channel transmission from each earlier
//! author whose inserted lines survive into the body a reader read, and a
//! ReaderOutput relay when a reader's own edit quotes such a line verbatim.

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::ids::{ExchangeId, MessageHash};
use crosstalk_spec::observed::exchange::{StopReason, WireProtocol};
use crosstalk_spec::observed::message::json::canonicalize;
use crosstalk_spec::observed::message::{
    AssistantPart, MessageBody, SystemPart, Text, ToolArguments, ToolCall, ToolCallId,
    ToolExecution, ToolName, ToolOutcome, ToolResult, ToolResultContent, UserPart,
};
use crosstalk_spec::support::{NonEmpty, Timestamp};

use super::attribution::{attribute, line_byte_range, lines, runs};
use super::resource::{page_locator, page_url};
use super::schema::Revision;
use super::tools;
use super::{DATASET, REVISIONS_FILE, WikiError};
use crate::corpus::{
    Coverage, Driven, ExchangeDraft, Fidelity, HashedMessage, World, WorldBuilder,
};
use crate::keys::{AgentKey, DatasetId, SourceRef, WorldKey};
use crate::location::location;
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

/// Builds the world for one component's revisions (already ordered by time,
/// then page, then seq).
pub fn world(key: WorldKey, revs: &[&Revision]) -> Result<World, WikiError> {
    let dataset = DatasetId::new(DATASET);
    let mut builder = WorldBuilder::new(dataset, key.clone());
    for identity in distinct_identities(revs) {
        builder.agent(&identity, Driven::Model, MODEL)?;
    }

    let pages = PageIndex::new(revs);
    let mut records: BTreeMap<&str, RevRecord> = BTreeMap::new();
    let mut counter: u64 = 0;

    // Pass 1: exchanges.
    for rev in revs {
        let (page, li) = pages.locate(rev);
        let actor = AgentKey::new(key.clone(), rev.identity());
        let prev = (li > 0).then(|| page.revs[li - 1]);
        let read = match prev {
            Some(prev) if prev.identity() != rev.identity() => {
                Some(build_read(&mut builder, &actor, rev, prev, &mut counter)?)
            }
            _ => None,
        };
        let edit = build_edit(
            &mut builder,
            &actor,
            rev,
            &page.sources[li],
            li,
            &mut counter,
        )?;
        records.insert(rev.rev_id.as_str(), RevRecord { edit, read });
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

/// What pass 1 recorded for one revision.
struct RevRecord {
    edit: EditRecord,
    /// The read exchange before the edit, when one was synthesised.
    read: Option<ReadRecord>,
}

struct EditRecord {
    exchange: ExchangeId,
    /// The hash of the `edit_page` response message (for relay locations).
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

fn next_at(counter: &mut u64) -> Result<Timestamp, WikiError> {
    let at = crate::corpus::clock::ordinal(*counter).map_err(WikiError::Clock)?;
    *counter += 1;
    Ok(at)
}

/// Synthesises the read exchange before `rev`'s edit: a `read_page` call
/// whose result is `prev`'s body.
fn build_read(
    builder: &mut WorldBuilder,
    reader: &AgentKey,
    rev: &Revision,
    prev: &Revision,
    counter: &mut u64,
) -> Result<ReadRecord, WikiError> {
    let url = page_url(&rev.wiki, &rev.name);
    let call_id = format!("read-{}", rev.rev_id);
    let call = assistant_call(&call_id, tools::TOOL, &tools::read_args(&url))?;
    let result = tool_result(&call_id, &prev.body);
    let result_hash = result.hash();
    let at = next_at(counter)?;
    let draft = ExchangeDraft {
        agent: reader.clone(),
        at,
        protocol: WireProtocol::OpenAiChat,
        model: MODEL.to_owned(),
        request: vec![system("You are a wiki agent."), call, result],
        response: assistant_text("Read the page."),
        stop: StopReason::EndTurn,
        usage: None,
        fidelity: Fidelity::Synthetic,
        source: SourceRef::new(REVISIONS_FILE, format!("/rev/{}/read", rev.rev_id)),
    };
    let exchange = builder.exchange(draft)?;
    Ok(ReadRecord {
        exchange,
        result_hash,
    })
}

/// Synthesises the edit exchange: an `edit_page` call whose `text` is the
/// lines this revision inserted (those attributed to its own index `li`).
fn build_edit(
    builder: &mut WorldBuilder,
    author: &AgentKey,
    rev: &Revision,
    source: &[usize],
    li: usize,
    counter: &mut u64,
) -> Result<EditRecord, WikiError> {
    let inserted = inserted_text(&rev.body, source, li);
    let url = page_url(&rev.wiki, &rev.name);
    let call_id = format!("edit-{}", rev.rev_id);
    let call = assistant_call(&call_id, tools::TOOL, &tools::write_args(&url, &inserted))?;
    let response_hash = call.hash();
    let at = next_at(counter)?;
    let draft = ExchangeDraft {
        agent: author.clone(),
        at,
        protocol: WireProtocol::OpenAiChat,
        model: MODEL.to_owned(),
        request: vec![
            system("You are a wiki agent."),
            user(&format!("Update {}.", rev.name)),
        ],
        response: call,
        stop: StopReason::ToolUse,
        usage: None,
        fidelity: Fidelity::Synthetic,
        source: SourceRef::new(REVISIONS_FILE, format!("/rev/{}/edit", rev.rev_id)),
    };
    let exchange = builder.exchange(draft)?;
    Ok(EditRecord {
        exchange,
        response_hash,
    })
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
        let needs = if needs_escape(text) {
            MatchNeed::Normalized
        } else {
            MatchNeed::Exact
        };
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
            || needs_escape(line)
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

/// Whether `text` changes when written as a JSON string: it holds a quote, a
/// backslash or a control character, so it is not byte-identical inside
/// canonical tool-call arguments.
fn needs_escape(text: &str) -> bool {
    text.chars()
        .any(|ch| matches!(ch, '"' | '\\' | '\u{0}'..='\u{1f}'))
}
