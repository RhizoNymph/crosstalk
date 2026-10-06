//! What both conversation stores share to answer the spec's
//! `ConversationReads`: the list's cursors, one page from the rows a store
//! read, and the turn rows' shape.
//!
//! **Cursors** are `<last conversation id>_<tag>`, where the tag is a keyed
//! BLAKE3 over the store's cursor key, the query's binding
//! (`binding`: its agents, origins and replay filter) and the id. A
//! cursor this store did not issue, or issued for another query, fails the
//! tag check and is `InvalidCursor`. A merge or unmerge between two pages
//! changes the agents the surface passes, so the next page is
//! `InvalidCursor` and the client restarts.

use crosstalk_spec::ids::{AgentId, ConversationId, ExchangeId};
use crosstalk_spec::interfaces::l3_reconstruction::conversations::{
    ConversationQuery, ConversationReadError, ReplayFilter, StoredConversation, StoredTurn,
    ThreadOutcomeKind, TranscriptEntry, TurnIndex,
};
use crosstalk_spec::observed::client::TrafficSource;
use crosstalk_spec::observed::conversation::OriginKind;
use crosstalk_spec::paging::{ConversationList, Cursor, Page, PageSize};
use crosstalk_spec::support::{Blake3, NonEmpty, Timestamp};

use crate::agents::codec::{id_of, id_text};

/// The key cursors are tagged with when a store is given none: the list's
/// cursors bind a query, and an operator who forges one only reaches a
/// position in a list View already shows them.
pub const DEFAULT_CURSOR_KEY: [u8; 32] = [0; 32];

/// The query as the text a cursor binds: agents ascending, origins in
/// kind order, the replay filter.
pub(crate) fn binding(query: &ConversationQuery) -> String {
    let agents = match &query.agents {
        None => "*".to_owned(),
        Some(agents) => agents
            .iter()
            .map(|agent| id_text(*agent))
            .collect::<Vec<_>>()
            .join(","),
    };
    let origins = query
        .origins
        .iter()
        .map(|kind| origin_text(*kind))
        .collect::<Vec<_>>()
        .join(",");
    let replay = match &query.replay {
        ReplayFilter::Include => "include".to_owned(),
        ReplayFilter::Exclude => "exclude".to_owned(),
        ReplayFilter::Only { corpus: None } => "only:*".to_owned(),
        ReplayFilter::Only {
            corpus: Some(corpus),
        } => format!("only:{}:{}", corpus.0.len(), corpus.0),
    };
    format!("agents={agents};origins={origins};replay={replay}")
}

pub(crate) fn origin_text(kind: OriginKind) -> &'static str {
    match kind {
        OriginKind::Root => "root",
        OriginKind::Fork => "fork",
        OriginKind::Compaction => "compaction",
    }
}

pub(crate) fn outcome_text(kind: ThreadOutcomeKind) -> &'static str {
    match kind {
        ThreadOutcomeKind::Starts => "starts",
        ThreadOutcomeKind::Extends => "extends",
        ThreadOutcomeKind::Forks => "forks",
        ThreadOutcomeKind::Compacts => "compacts",
    }
}

pub(crate) fn outcome_kind_of(text: &str) -> Option<ThreadOutcomeKind> {
    match text {
        "starts" => Some(ThreadOutcomeKind::Starts),
        "extends" => Some(ThreadOutcomeKind::Extends),
        "forks" => Some(ThreadOutcomeKind::Forks),
        "compacts" => Some(ThreadOutcomeKind::Compacts),
        _ => None,
    }
}

/// The corpus a source names, for the stores' source column.
pub(crate) fn corpus_of(source: &TrafficSource) -> Option<&str> {
    match source {
        TrafficSource::Live => None,
        TrafficSource::Replay { corpus } => Some(corpus.0.as_str()),
    }
}

fn tag(key: &[u8; 32], binding: &str, last: ConversationId) -> String {
    let mut bytes = key.to_vec();
    bytes.extend_from_slice(binding.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(id_text(last).as_bytes());
    Blake3::of(&bytes).to_hex()[..32].to_owned()
}

/// The list's cursors under one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Cursors {
    pub(crate) key: [u8; 32],
}

impl Default for Cursors {
    fn default() -> Self {
        Self {
            key: DEFAULT_CURSOR_KEY,
        }
    }
}

impl Cursors {
    fn issue(
        &self,
        binding: &str,
        last: ConversationId,
    ) -> Result<Cursor<ConversationList>, ConversationReadError> {
        let token = format!("{}_{}", id_text(last), tag(&self.key, binding, last));
        Cursor::from_token(token).map_err(|error| ConversationReadError::Store {
            reason: format!("cursor not issued: {error:?}"),
        })
    }

    /// The last conversation a cursor this store issued for `binding`
    /// served.
    pub(crate) fn resume(
        &self,
        cursor: &Cursor<ConversationList>,
        binding: &str,
    ) -> Result<ConversationId, ConversationReadError> {
        let (id, given) = cursor
            .token()
            .split_once('_')
            .ok_or(ConversationReadError::InvalidCursor)?;
        let last: ConversationId = id_of("cursor", id)
            .map_err(|_: crate::agents::codec::CodecError| ConversationReadError::InvalidCursor)?;
        if tag(&self.key, binding, last) == given {
            Ok(last)
        } else {
            Err(ConversationReadError::InvalidCursor)
        }
    }

    /// One page of `rows`, which hold every admitted conversation after
    /// the cursor (newest first) up to at least `size + 1` of them.
    pub(crate) fn page(
        &self,
        binding: &str,
        size: PageSize,
        mut rows: Vec<StoredConversation>,
    ) -> Result<Page<StoredConversation, ConversationList>, ConversationReadError> {
        let limit = usize::from(size.get().get());
        let overflow = |_| ConversationReadError::Store {
            reason: "page larger than its size".to_owned(),
        };
        if rows.len() <= limit {
            return Page::last(size, rows).map_err(overflow);
        }
        rows.truncate(limit);
        let last = rows.last().map(|row| row.conversation.id).ok_or_else(|| {
            ConversationReadError::Store {
                reason: "empty page with more to follow".to_owned(),
            }
        })?;
        let items = NonEmpty::from_vec(rows).ok_or_else(|| ConversationReadError::Store {
            reason: "empty page with more to follow".to_owned(),
        })?;
        Page::more(size, items, self.issue(binding, last)?).map_err(overflow)
    }
}

/// One turn as both stores keep it: where its entries start in the
/// transcript and how many there are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TurnRow {
    pub(crate) exchange: ExchangeId,
    pub(crate) first_ordinal: u32,
    pub(crate) entries: u32,
    pub(crate) agent: AgentId,
    pub(crate) started_at: Timestamp,
    pub(crate) outcome: ThreadOutcomeKind,
    pub(crate) history_end: u32,
}

impl TurnRow {
    /// The turn at `index` with its `entries`.
    pub(crate) fn turn(&self, index: u32, entries: Vec<TranscriptEntry>) -> StoredTurn {
        StoredTurn {
            index: TurnIndex(index),
            exchange: self.exchange,
            agent: self.agent,
            started_at: self.started_at,
            outcome: self.outcome,
            history_end: self.history_end,
            entries,
        }
    }

    /// The ordinals of its entries.
    pub(crate) fn ordinals(&self) -> std::ops::Range<u32> {
        self.first_ordinal..self.first_ordinal.saturating_add(self.entries)
    }
}
