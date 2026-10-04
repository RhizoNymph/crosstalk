//! Why a builder could not produce a value.
//!
//! Builders of checked spec types build through the spec's constructor, so a
//! value they return always passes it. Their defaults always build; an
//! override that breaks the constructor's invariant (a content match whose
//! sender is its reader, a user rule under a reserved id) is reported as the
//! constructor's own refusal.

use crosstalk_spec::aggregates::alert::ReservedRuleId;
use crosstalk_spec::aggregates::topic::InvalidEmbedding;
use crosstalk_spec::aggregates::topic_history::{InvalidHistory, InvalidVersionInfo};
use crosstalk_spec::derived::flow::evidence::InvalidCoAccess;
use crosstalk_spec::derived::flow::transmission::MixedMatches;
use crosstalk_spec::derived::provenance::matching::InvalidMatch;
use crosstalk_spec::support::{EmptyRange, InvalidQueryText, InvalidText, OutOfRange};

/// A spec constructor refused the builder's values.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum BuildError {
    #[error("byte range refused: it is empty")]
    Range(EmptyRange),
    #[error("content match refused: {0:?}")]
    ContentMatch(InvalidMatch),
    #[error("co-access refused: {0:?}")]
    CoAccess(InvalidCoAccess),
    #[error("confirmed transmission refused: {0:?}")]
    Confirmed(MixedMatches),
    #[error("embedding refused: {0:?}")]
    Embedding(InvalidEmbedding),
    #[error("similarity out of range: {0:?}")]
    Similarity(OutOfRange),
    #[error("display text refused: {0:?}")]
    Text(InvalidText),
    #[error("query text refused: {0:?}")]
    QueryText(InvalidQueryText),
    #[error("user rule refused: {0:?} is reserved for built-in rules")]
    ReservedRuleId(ReservedRuleId),
    #[error("topic version refused: {0:?}")]
    VersionInfo(InvalidVersionInfo),
    #[error("topic version history refused: {0:?}")]
    History(InvalidHistory),
}

macro_rules! from_refusal {
    ($($variant:ident($error:ty)),* $(,)?) => {$(
        impl From<$error> for BuildError {
            fn from(error: $error) -> Self {
                Self::$variant(error)
            }
        }
    )*};
}

from_refusal!(
    Range(EmptyRange),
    ContentMatch(InvalidMatch),
    CoAccess(InvalidCoAccess),
    Confirmed(MixedMatches),
    Embedding(InvalidEmbedding),
    Similarity(OutOfRange),
    Text(InvalidText),
    QueryText(InvalidQueryText),
    ReservedRuleId(ReservedRuleId),
    VersionInfo(InvalidVersionInfo),
    History(InvalidHistory),
);
