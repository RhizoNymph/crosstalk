//! Detection quality: operator verdicts tallied against the detector's calls.
//!
//! [`DetectionQuality`] answers "how often is the detector right, by route
//! and by kind of evidence". It counts every judgeable transmission
//! ([`TransmissionState::judgeable`]) opened in the window once, in the row of
//! its route kind and [`QualityMatch`], under its current verdict:
//!
//! | Detector call (row) | `genuine` | `false_detection` |
//! | --- | --- | --- |
//! | `Content(_)`: confirmed | true positive | false positive |
//! | `Suspected`: access only, undecided | missed so far | correctly not confirmed |
//! | `Discarded`: expired | false negative | true negative |
//!
//! Precision for confirmed evidence of one class is `genuine / (genuine +
//! false_detection)` over its `Content` rows. Recall over what the detector
//! opened is confirmed `genuine` over all `genuine`; communications that
//! never produced a co-access or a content match are invisible to both.
//! `unlabeled` is the sample still to judge.
//!
//! **Which window.** A transmission is in the window when its
//! `Transmission::opened_at` is, a time every state has and none changes, so
//! a transmission keeps its window as it moves from suspected to confirmed
//! or discarded. (The linked views test `Confirmed::at`, which suspected and
//! discarded transmissions lack.)
//!
//! **Which match kind.** A confirmed transmission may hold several content
//! matches. It is counted under its strongest one ([`MatchClass::strongest`]):
//! a transmission is as credible as its best evidence, so a row's
//! `false_detection` count is the false positives of transmissions whose best
//! evidence was of that class, and `Semantic` rows hold only transmissions
//! with nothing but semantic matches.
//!
//! **Which state and verdict.** Both are read at query time: a transmission
//! judged while suspected and confirmed since is counted as confirmed, and a
//! withdrawn verdict counts as `unlabeled`.

use std::collections::BTreeMap;

use crate::aggregates::edge::RouteKind;
use crate::derived::flow::transmission::{Confirmed, Transmission};
use crate::derived::flow::verdict::{Judgeable, Verdict};
use crate::derived::provenance::matching::MatchKind;
use crate::support::TimeWindow;

#[cfg(doc)]
use crate::derived::flow::transmission::TransmissionState;

/// A content match's kind without its parameters, ordered strongest first:
/// the less the reader's text had to be transformed, the stronger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MatchClass {
    Exact,
    Normalized,
    Decoded,
    Semantic,
}

impl From<&MatchKind> for MatchClass {
    fn from(kind: &MatchKind) -> Self {
        match kind {
            MatchKind::Exact => Self::Exact,
            MatchKind::Normalized => Self::Normalized,
            MatchKind::Decoded(_) => Self::Decoded,
            MatchKind::Semantic(_) => Self::Semantic,
        }
    }
}

impl MatchClass {
    /// The strongest class among a confirmed transmission's matches.
    pub fn strongest(confirmed: &Confirmed) -> Self {
        let first = Self::from(confirmed.content().first().kind());
        confirmed
            .content()
            .iter()
            .map(|content| Self::from(content.kind()))
            .fold(first, Self::min)
    }
}

/// The detector's call on a transmission, as a quality row sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum QualityMatch {
    /// Confirmed (or classified, or aggregated), by its strongest match.
    Content(MatchClass),
    /// Access-pattern evidence only.
    Suspected,
    /// Suspected, then discarded.
    Discarded,
}

impl From<Judgeable<'_>> for QualityMatch {
    fn from(judgeable: Judgeable<'_>) -> Self {
        match judgeable {
            Judgeable::Suspected(_) => Self::Suspected,
            Judgeable::Discarded(_) => Self::Discarded,
            Judgeable::Confirmed(confirmed) => Self::Content(MatchClass::strongest(confirmed)),
        }
    }
}

/// The transmissions of one route kind and detector call, by current verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QualityRow {
    pub route_kind: RouteKind,
    pub match_kind: QualityMatch,
    pub genuine: u64,
    pub false_detection: u64,
    /// Never judged, or the verdict was withdrawn.
    pub unlabeled: u64,
}

impl QualityRow {
    pub fn total(&self) -> u64 {
        self.genuine + self.false_detection + self.unlabeled
    }
}

/// The quality rows for one window.
///
/// Built only through [`DetectionQuality::new`] or
/// [`DetectionQuality::tally`]: at most one row per (`route_kind`,
/// `match_kind`), none all zero, ordered by route kind and then match kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectionQuality {
    window: TimeWindow,
    rows: Vec<QualityRow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidQuality {
    DuplicateRow {
        route_kind: RouteKind,
        match_kind: QualityMatch,
    },
    EmptyRow {
        route_kind: RouteKind,
        match_kind: QualityMatch,
    },
}

fn route_order(kind: RouteKind) -> u8 {
    match kind {
        RouteKind::Channel => 0,
        RouteKind::Delegation => 1,
        RouteKind::Direct => 2,
        RouteKind::Unobserved => 3,
    }
}

fn key(row: &QualityRow) -> (u8, QualityMatch) {
    (route_order(row.route_kind), row.match_kind)
}

impl DetectionQuality {
    /// Rows in any order; they are sorted. Rejects a repeated key and an
    /// all-zero row.
    pub fn new(window: TimeWindow, mut rows: Vec<QualityRow>) -> Result<Self, InvalidQuality> {
        if let Some(row) = rows.iter().find(|row| row.total() == 0) {
            return Err(InvalidQuality::EmptyRow {
                route_kind: row.route_kind,
                match_kind: row.match_kind,
            });
        }
        rows.sort_by_key(key);
        if let Some(pair) = rows.windows(2).find(|pair| key(&pair[0]) == key(&pair[1])) {
            return Err(InvalidQuality::DuplicateRow {
                route_kind: pair[0].route_kind,
                match_kind: pair[0].match_kind,
            });
        }
        Ok(Self { window, rows })
    }

    /// The reference tally: each transmission with its current verdict.
    /// Keeps those opened in `window` whose state is judgeable, and counts
    /// each once in its row under its verdict. An implementation's query
    /// returns exactly this for the stored transmissions and verdict logs.
    pub fn tally<'a>(
        window: TimeWindow,
        transmissions: impl IntoIterator<Item = (&'a Transmission, Option<Verdict>)>,
    ) -> Self {
        let mut rows: BTreeMap<(u8, QualityMatch), QualityRow> = BTreeMap::new();
        for (transmission, verdict) in transmissions {
            if !window.contains(transmission.opened_at) {
                continue;
            }
            let Ok(judgeable) = transmission.state.judgeable() else {
                continue;
            };
            let route_kind = RouteKind::from(&transmission.route);
            let match_kind = QualityMatch::from(judgeable);
            let row = rows
                .entry((route_order(route_kind), match_kind))
                .or_insert(QualityRow {
                    route_kind,
                    match_kind,
                    genuine: 0,
                    false_detection: 0,
                    unlabeled: 0,
                });
            match verdict {
                Some(Verdict::Genuine) => row.genuine += 1,
                Some(Verdict::FalseDetection) => row.false_detection += 1,
                None => row.unlabeled += 1,
            }
        }
        Self {
            window,
            rows: rows.into_values().collect(),
        }
    }

    pub fn window(&self) -> TimeWindow {
        self.window
    }

    pub fn rows(&self) -> &[QualityRow] {
        &self.rows
    }
}
