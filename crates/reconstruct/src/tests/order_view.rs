//! What the order-tolerance tests compare: everything a conversation
//! store holds for a set of threaded exchanges (each conversation with its
//! agent, origin, source, times, history, transcript and turns, and each
//! exchange's recorded outcome), with conversation ids named by the
//! exchange whose turn opened the conversation.
//!
//! Conversation ids are minted when an exchange is first threaded, so two
//! delivery orders draw different ids for the same conversation; the
//! exchange that opens it is a function of the exchanges alone. Every
//! other value is compared as stored.

use std::collections::BTreeMap;

use crosstalk_spec::ids::{AgentId, ConversationId, ExchangeId, MessageHash};
use crosstalk_spec::interfaces::l3_reconstruction::conversations::{
    ConversationReads, StoredTurn, ThreadOutcomeKind, TranscriptEntry, TurnIndex, TurnWindow,
};
use crosstalk_spec::interfaces::l3_reconstruction::{ThreadOutcome, Threader};
use crosstalk_spec::observed::client::TrafficSource;
use crosstalk_spec::observed::conversation::ConversationOrigin;
use crosstalk_spec::observed::exchange::Exchange;
use crosstalk_spec::paging::PageSize;
use crosstalk_spec::support::Timestamp;

use crate::thread::ConversationStore;

/// A conversation's origin, its source conversation named by its opening
/// exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OriginView {
    Root,
    Fork {
        parent: ExchangeId,
        shared_prefix: u32,
    },
    Compaction {
        predecessor: ExchangeId,
    },
}

/// One stored conversation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConversationView {
    pub(crate) agent: AgentId,
    pub(crate) origin: OriginView,
    pub(crate) source: TrafficSource,
    pub(crate) started_at: Timestamp,
    pub(crate) last_turn_at: Timestamp,
    pub(crate) messages: Vec<MessageHash>,
    pub(crate) transcript: Vec<TranscriptEntry>,
    pub(crate) turns: Vec<StoredTurn>,
}

/// One exchange's recorded outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutcomeView {
    pub(crate) kind: ThreadOutcomeKind,
    pub(crate) conversation: ExchangeId,
    pub(crate) origin: OriginView,
    pub(crate) agent: AgentId,
    pub(crate) new_inputs: Vec<MessageHash>,
    pub(crate) new_system: Option<MessageHash>,
    pub(crate) output: Option<MessageHash>,
}

/// Everything the store holds, by opening exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct L3View {
    pub(crate) conversations: BTreeMap<ExchangeId, ConversationView>,
    pub(crate) outcomes: BTreeMap<ExchangeId, OutcomeView>,
}

impl L3View {
    /// How many conversations are stored.
    pub(crate) fn conversation_count(&self) -> usize {
        self.conversations.len()
    }
}

/// Why a view could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ViewError(pub(crate) String);

fn failed(what: &str, error: impl std::fmt::Debug) -> ViewError {
    ViewError(format!("{what}: {error:?}"))
}

fn origin_view(
    origin: ConversationOrigin,
    anchors: &BTreeMap<ConversationId, ExchangeId>,
) -> Result<OriginView, ViewError> {
    let anchor = |id: ConversationId| {
        anchors
            .get(&id)
            .copied()
            .ok_or_else(|| ViewError(format!("origin names unknown conversation {id:?}")))
    };
    Ok(match origin {
        ConversationOrigin::Root => OriginView::Root,
        ConversationOrigin::Fork {
            parent,
            shared_prefix,
        } => OriginView::Fork {
            parent: anchor(parent)?,
            shared_prefix,
        },
        ConversationOrigin::Compaction { predecessor } => OriginView::Compaction {
            predecessor: anchor(predecessor)?,
        },
    })
}

fn every_turn() -> Result<TurnWindow, ViewError> {
    let size = PageSize::new(PageSize::MAX).map_err(|error| failed("page size", error))?;
    Ok(TurnWindow {
        from: TurnIndex(0),
        size,
    })
}

/// The view of `store` after `exchanges` were threaded, each attributed to
/// its agent. Each exchange's recorded outcome is read by threading it
/// again through `threader`, which returns the recorded outcome
/// (`reconstruct.thread.rethread-idempotent`) and changes nothing.
pub(crate) async fn view<S, T>(
    store: &S,
    threader: &mut T,
    exchanges: &[(Exchange, AgentId)],
) -> Result<L3View, ViewError>
where
    S: ConversationStore + ConversationReads,
    T: Threader,
{
    let window = every_turn()?;
    let ids = store
        .conversations()
        .await
        .map_err(|error| failed("conversations", error))?;
    let mut anchors = BTreeMap::new();
    let mut read = Vec::new();
    for id in ids {
        let stored = ConversationReads::conversation(store, id)
            .await
            .map_err(|error| failed("conversation", error))?
            .ok_or_else(|| ViewError(format!("{id:?} listed but not stored")))?;
        let turns = store
            .turns(id, &window)
            .await
            .map_err(|error| failed("turns", error))?
            .ok_or_else(|| ViewError(format!("{id:?} has no turns")))?;
        let opening = turns
            .turns
            .first()
            .map(|turn| turn.exchange)
            .ok_or_else(|| ViewError(format!("{id:?} has no first turn")))?;
        if anchors.insert(id, opening).is_some() {
            return Err(ViewError(format!("{id:?} listed twice")));
        }
        let transcript = store
            .transcript(id)
            .await
            .map_err(|error| failed("transcript", error))?;
        read.push((opening, stored, turns.turns, transcript));
    }
    let mut conversations = BTreeMap::new();
    for (opening, stored, turns, transcript) in read {
        let view = ConversationView {
            agent: stored.conversation.agent,
            origin: origin_view(stored.conversation.origin, &anchors)?,
            source: stored.source,
            started_at: stored.started_at,
            last_turn_at: stored.last_turn_at,
            messages: stored.conversation.messages,
            transcript,
            turns,
        };
        if conversations.insert(opening, view).is_some() {
            return Err(ViewError(format!(
                "two conversations opened by {opening:?}"
            )));
        }
    }
    let mut outcomes = BTreeMap::new();
    for (exchange, agent) in exchanges {
        let outcome = threader
            .thread(exchange, *agent)
            .await
            .map_err(|error| failed("recorded outcome", error))?;
        let delta = outcome.delta();
        let conversation = anchors
            .get(&delta.conversation)
            .copied()
            .ok_or_else(|| ViewError(format!("{:?} names no stored conversation", delta)))?;
        let origin = match &outcome {
            ThreadOutcome::Starts { .. } | ThreadOutcome::Extends { .. } => {
                ConversationOrigin::Root
            }
            ThreadOutcome::Forks {
                parent,
                shared_prefix,
                ..
            } => ConversationOrigin::Fork {
                parent: *parent,
                shared_prefix: *shared_prefix,
            },
            ThreadOutcome::Compacts { predecessor, .. } => ConversationOrigin::Compaction {
                predecessor: *predecessor,
            },
        };
        outcomes.insert(
            exchange.meta.id,
            OutcomeView {
                kind: outcome.kind(),
                conversation,
                origin: origin_view(origin, &anchors)?,
                agent: delta.agent,
                new_inputs: delta.new_inputs.clone(),
                new_system: delta.new_system,
                output: delta.output,
            },
        );
    }
    Ok(L3View {
        conversations,
        outcomes,
    })
}
