//! Display labels for agents.
//!
//! A label is free text an operator gives the canonical agent so people can
//! recognise it ("release bot", "planner on build-3"). It is shown and
//! searchable, and never identity evidence: it is not an
//! [`IdentityEvidence`](super::IdentityEvidence) variant, and resolution never
//! reads it.
//!
//! Each agent keeps an append-only [`LabelLog`]. A merge or unmerge touches no
//! log: the target keeps its label, a merged agent keeps its own, and
//! [`LabelView::of`] shows the canonical agent's label with the differing
//! labels of the agents merged into it as history.

use crate::ids::{AgentId, OperatorId};
use crate::support::Timestamp;

use super::{Agent, AgentState};

/// A display label: trimmed, non-empty, at most [`AgentLabel::MAX_CHARS`]
/// characters, and free of control characters.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AgentLabel(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidLabel {
    Blank,
    TooLong { max: usize, got: usize },
    ControlCharacter,
}

impl AgentLabel {
    pub const MAX_CHARS: usize = 64;

    pub fn new(text: &str) -> Result<Self, InvalidLabel> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Err(InvalidLabel::Blank);
        }
        let chars = trimmed.chars().count();
        if chars > Self::MAX_CHARS {
            return Err(InvalidLabel::TooLong {
                max: Self::MAX_CHARS,
                got: chars,
            });
        }
        if trimmed.chars().any(char::is_control) {
            return Err(InvalidLabel::ControlCharacter);
        }
        Ok(Self(trimmed.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A label as an operator set it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Labeled {
    pub label: AgentLabel,
    pub by: OperatorId,
    pub at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelChange {
    Set(Labeled),
    Cleared { by: OperatorId, at: Timestamp },
}

impl LabelChange {
    pub fn at(&self) -> Timestamp {
        match self {
            Self::Set(labeled) => labeled.at,
            Self::Cleared { at, .. } => *at,
        }
    }
}

/// An agent's label changes, oldest first. Append-only and in time order,
/// so the current label and the history can never disagree.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LabelLog {
    changes: Vec<LabelChange>,
}

/// A change earlier than the last one recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutOfOrder;

impl LabelLog {
    pub fn record(&mut self, change: LabelChange) -> Result<(), OutOfOrder> {
        if self
            .changes
            .last()
            .is_some_and(|last| change.at() < last.at())
        {
            return Err(OutOfOrder);
        }
        self.changes.push(change);
        Ok(())
    }

    /// The label in force: the last change, if it set one.
    pub fn current(&self) -> Option<&Labeled> {
        match self.changes.last() {
            Some(LabelChange::Set(labeled)) => Some(labeled),
            Some(LabelChange::Cleared { .. }) | None => None,
        }
    }

    pub fn changes(&self) -> &[LabelChange] {
        &self.changes
    }
}

/// A label the canonical agent no longer shows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PastLabel {
    /// One of the canonical agent's own labels, since replaced or cleared.
    Earlier(Labeled),
    /// The label of an agent merged into the canonical one, where it differs
    /// from the canonical agent's current label.
    Alias { agent: AgentId, label: Labeled },
}

impl PastLabel {
    pub fn at(&self) -> Timestamp {
        match self {
            Self::Earlier(labeled) | Self::Alias { label: labeled, .. } => labeled.at,
        }
    }
}

/// What the UI shows for a canonical agent's label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelView {
    /// The canonical agent's own current label. A merged agent's label never
    /// takes its place.
    pub current: Option<Labeled>,
    /// Oldest first.
    pub history: Vec<PastLabel>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidLabelView {
    /// The agent given as canonical is itself merged.
    NotCanonical(AgentId),
    /// An agent given as an alias is not merged into the canonical agent.
    NotAnAlias(AgentId),
}

impl LabelView {
    /// The view of `canonical`'s label, given the agents merged into it.
    pub fn of(canonical: &Agent, aliases: &[Agent]) -> Result<Self, InvalidLabelView> {
        if matches!(canonical.state, AgentState::Merged(_)) {
            return Err(InvalidLabelView::NotCanonical(canonical.id));
        }
        let current = canonical.labels.current().cloned();
        let mut history: Vec<PastLabel> = Vec::new();
        // Every label the canonical agent set, except the one in force (which
        // can only be the last change).
        let own = canonical.labels.changes();
        let superseded = own.len().saturating_sub(1);
        for change in &own[..superseded] {
            if let LabelChange::Set(labeled) = change {
                history.push(PastLabel::Earlier(labeled.clone()));
            }
        }
        for alias in aliases {
            match &alias.state {
                AgentState::Merged(merged) if merged.into == canonical.id => {}
                _ => return Err(InvalidLabelView::NotAnAlias(alias.id)),
            }
            if let Some(labeled) = alias.labels.current() {
                let differs = current
                    .as_ref()
                    .is_none_or(|own| own.label != labeled.label);
                if differs {
                    history.push(PastLabel::Alias {
                        agent: alias.id,
                        label: labeled.clone(),
                    });
                }
            }
        }
        history.sort_by_key(PastLabel::at);
        Ok(Self { current, history })
    }
}
