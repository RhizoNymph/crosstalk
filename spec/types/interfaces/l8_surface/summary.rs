//! Transmission rows: one transmission as a list shows it, with no message
//! content (`QueryApi::transmissions_by_id`, View).
//!
//! A [`TransmissionSummary`] names the canonical reader and the route with
//! its channel resolved, the opened time, and a [`SummaryState`] holding
//! what that state knows. Its shape follows the state, so a row cannot
//! disagree with it:
//!
//! | State | Sender, confirmation time, matched bytes | Topic | Verdict |
//! | --- | --- | --- | --- |
//! | `Detected`, `AwaitingContent` | no | no | no: not judgeable |
//! | `Suspected`, `Discarded` | no | no | current |
//! | `Confirmed` | yes ([`Delivery`]) | no: not classified yet | current |
//! | `Classified`, `Aggregated` | yes | under the page's version ([`TopicUnder`]) | current |
//!
//! [`TransmissionSummary::of`] is the definition: what the surface computes
//! from a stored transmission, the aliases, its verdict log and its topic
//! assignment under one resolved version. Every row of one page (and one
//! traversal) reads topics under the same version, which the page reports
//! ([`TransmissionPage::topic_version`]).
//!
//! The same row serves every list of transmissions that shows their current
//! state (explore selections, the evidence page's header, export). The rows
//! behind a topology edge are not summaries: they are what the edge
//! counted (`EdgeTransmission`, from L7's stored contributions, so their
//! byte counts sum to the edge's), while a summary reads the transmission's
//! current state, which a later content match can still extend.

use std::num::NonZeroU64;

use crate::aggregates::topic::TopicModelVersion;
use crate::aliases::Aliases;
use crate::derived::flow::transmission::{Confirmed, Route, Transmission, TransmissionState};
use crate::derived::flow::verdict::Verdict;
use crate::ids::{AgentId, TopicId, TransmissionId};
use crate::paging::{Page, TransmissionList};
use crate::support::Timestamp;

/// One transmission as a row: no message content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransmissionSummary {
    pub id: TransmissionId,
    /// The canonical reader: `AgentDirectory::canonical(Transmission::to)`.
    pub to: AgentId,
    /// The route with its channel canonical (`Route::resolved`).
    pub route: Route,
    pub opened_at: Timestamp,
    pub state: SummaryState,
}

/// A transmission's state with what is known in it. The sender, matched
/// bytes and confirmation time exist from `Confirmed` on, the topic from
/// `Classified` on, and a verdict only in the states that take one
/// (`TransmissionState::judgeable`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SummaryState {
    Detected,
    AwaitingContent,
    Suspected {
        verdict: Option<Verdict>,
    },
    Discarded {
        verdict: Option<Verdict>,
    },
    Confirmed {
        delivery: Delivery,
        verdict: Option<Verdict>,
    },
    Classified {
        delivery: Delivery,
        topic: TopicUnder,
        verdict: Option<Verdict>,
    },
    Aggregated {
        delivery: Delivery,
        topic: TopicUnder,
        verdict: Option<Verdict>,
    },
}

/// What content evidence established.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Delivery {
    /// The canonical sender: `AgentDirectory::canonical(Confirmed::from())`.
    pub from: AgentId,
    /// `Confirmed::at`.
    pub confirmed_at: Timestamp,
    /// `Confirmed::matched_bytes`, as it is now.
    pub matched_bytes: NonZeroU64,
}

/// A classified transmission's topic under the page's version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TopicUnder {
    Topic(TopicId),
    /// The version's topic model marks it an outlier.
    Outlier,
    /// The version holds no assignment for it: it was classified under
    /// later versions only.
    Unassigned,
}

/// Which state a transmission is in, without its data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransmissionStateKind {
    Detected,
    AwaitingContent,
    Suspected,
    Confirmed,
    Classified,
    Aggregated,
    Discarded,
}

impl TransmissionStateKind {
    pub fn of(state: &TransmissionState) -> Self {
        match state {
            TransmissionState::Detected => Self::Detected,
            TransmissionState::AwaitingContent { .. } => Self::AwaitingContent,
            TransmissionState::Suspected { .. } => Self::Suspected,
            TransmissionState::Confirmed(_) => Self::Confirmed,
            TransmissionState::Classified { .. } => Self::Classified,
            TransmissionState::Aggregated { .. } => Self::Aggregated,
            TransmissionState::Discarded { .. } => Self::Discarded,
        }
    }
}

impl TransmissionSummary {
    /// The row for `transmission`. `aliases` resolves the reader, the
    /// sender and the route's channel. `verdict` is the current verdict
    /// (`VerdictLog::current`), read only for a judgeable state; `topic` is
    /// the assignment under the page's version, read only for a classified
    /// or aggregated one.
    pub fn of(
        transmission: &Transmission,
        aliases: impl Aliases,
        verdict: impl FnOnce(TransmissionId) -> Option<Verdict>,
        topic: impl FnOnce(TransmissionId) -> TopicUnder,
    ) -> Self {
        let id = transmission.id;
        let delivered = |confirmed: &Confirmed| Delivery {
            from: aliases.agent(confirmed.from()),
            confirmed_at: confirmed.at(),
            matched_bytes: confirmed.matched_bytes(),
        };
        let state = match &transmission.state {
            TransmissionState::Detected => SummaryState::Detected,
            TransmissionState::AwaitingContent { .. } => SummaryState::AwaitingContent,
            TransmissionState::Suspected { .. } => SummaryState::Suspected {
                verdict: verdict(id),
            },
            TransmissionState::Discarded { .. } => SummaryState::Discarded {
                verdict: verdict(id),
            },
            TransmissionState::Confirmed(confirmed) => SummaryState::Confirmed {
                delivery: delivered(confirmed),
                verdict: verdict(id),
            },
            TransmissionState::Classified { confirmed, .. } => SummaryState::Classified {
                delivery: delivered(confirmed),
                topic: topic(id),
                verdict: verdict(id),
            },
            TransmissionState::Aggregated { confirmed, .. } => SummaryState::Aggregated {
                delivery: delivered(confirmed),
                topic: topic(id),
                verdict: verdict(id),
            },
        };
        let to = aliases.agent(transmission.to);
        Self {
            id,
            to,
            route: transmission.route.resolved(aliases),
            opened_at: transmission.opened_at,
            state,
        }
    }
}

impl SummaryState {
    pub fn kind(&self) -> TransmissionStateKind {
        match self {
            Self::Detected => TransmissionStateKind::Detected,
            Self::AwaitingContent => TransmissionStateKind::AwaitingContent,
            Self::Suspected { .. } => TransmissionStateKind::Suspected,
            Self::Discarded { .. } => TransmissionStateKind::Discarded,
            Self::Confirmed { .. } => TransmissionStateKind::Confirmed,
            Self::Classified { .. } => TransmissionStateKind::Classified,
            Self::Aggregated { .. } => TransmissionStateKind::Aggregated,
        }
    }

    /// The sender, confirmation time and matched bytes, once confirmed.
    pub fn delivery(&self) -> Option<&Delivery> {
        match self {
            Self::Confirmed { delivery, .. }
            | Self::Classified { delivery, .. }
            | Self::Aggregated { delivery, .. } => Some(delivery),
            Self::Detected
            | Self::AwaitingContent
            | Self::Suspected { .. }
            | Self::Discarded { .. } => None,
        }
    }

    /// The topic, once classified.
    pub fn topic(&self) -> Option<TopicUnder> {
        match self {
            Self::Classified { topic, .. } | Self::Aggregated { topic, .. } => Some(*topic),
            Self::Detected
            | Self::AwaitingContent
            | Self::Suspected { .. }
            | Self::Discarded { .. }
            | Self::Confirmed { .. } => None,
        }
    }

    /// The current verdict; `None` when never judged, withdrawn, or not
    /// judgeable.
    pub fn verdict(&self) -> Option<Verdict> {
        match self {
            Self::Detected | Self::AwaitingContent => None,
            Self::Suspected { verdict }
            | Self::Discarded { verdict }
            | Self::Confirmed { verdict, .. }
            | Self::Classified { verdict, .. }
            | Self::Aggregated { verdict, .. } => *verdict,
        }
    }
}

/// The ids of a selection to list: at least one, at most
/// [`TransmissionSelection::MAX`] (the largest projection sample), each
/// once, newest id first: the order rows are listed in.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransmissionSelection(Vec<TransmissionId>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidSelection {
    Empty,
    /// More distinct ids than [`TransmissionSelection::MAX`].
    TooMany {
        max: usize,
        got: usize,
    },
}

impl TransmissionSelection {
    /// `ProjectionLimit::MAX`: a lasso over a whole projection fits.
    pub const MAX: usize = 100_000;

    /// Repeated ids count once.
    pub fn new(mut ids: Vec<TransmissionId>) -> Result<Self, InvalidSelection> {
        ids.sort_unstable_by(|a, b| b.cmp(a));
        ids.dedup();
        if ids.is_empty() {
            return Err(InvalidSelection::Empty);
        }
        if ids.len() > Self::MAX {
            return Err(InvalidSelection::TooMany {
                max: Self::MAX,
                got: ids.len(),
            });
        }
        Ok(Self(ids))
    }

    /// Distinct, newest first.
    pub fn ids(&self) -> &[TransmissionId] {
        &self.0
    }
}

/// One page of rows and the topic-model version their topics are under:
/// the one the first page resolved, pinned by the cursor for every later
/// page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransmissionPage {
    pub topic_version: TopicModelVersion,
    pub page: Page<TransmissionSummary, TransmissionList>,
}
