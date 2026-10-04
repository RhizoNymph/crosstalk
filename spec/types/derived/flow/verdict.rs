//! Operator verdicts on transmissions: ground truth beside the detector.
//!
//! A verdict is a separate axis from [`TransmissionState`]. The detector's
//! output is never changed by one, so the state says what the detector
//! decided and the verdict says what an operator judged, and the two
//! together measure the detector (`DetectionQuality`).
//!
//! ```text
//!                 SetVerdict(Some(v))            SetVerdict(None)
//! (no verdict) ───────────────────────▶ v ──────────────────────────▶ (no verdict)
//!                                       │ ▲
//!                                       └─┘ SetVerdict(Some(other))
//! ```
//!
//! **Append-only.** Every change is a [`TransmissionVerdict`] record appended
//! to the transmission's [`VerdictLog`]; withdrawing appends a record whose
//! verdict is `None`. Nothing is ever edited or removed. The latest record
//! (the highest [`VerdictRevision`]) is the current verdict. A request for
//! the verdict already current appends nothing, so retries are idempotent.
//!
//! **Judgeable states.** A verdict needs a detector call to judge:
//! `Suspected`, `Discarded` (the detector's "no", so a `Genuine` verdict
//! there records a false negative) and every state holding a [`Confirmed`].
//! `Detected` and `AwaitingContent` are still collecting evidence
//! ([`TransmissionState::judgeable`]). Transmissions only move forward
//! through their states, and every state after a judgeable one is
//! judgeable, so once a transmission takes a verdict it always does: a
//! verdict never races a state change into an invalid pair, and needs no
//! ordering with the correlator.
//!
//! **Readers.** L5 owns the log and publishes `VerdictSet` once per appended
//! record. Every reader that needs the current verdict (alert triage, the
//! edge store, search and the projection) keeps a [`CurrentVerdict`] per
//! transmission from those events, which keeps the highest revision it has
//! seen, so redelivered or reordered events never roll a verdict back.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::derived::flow::evidence::CoAccess;
use crate::derived::flow::transmission::{Confirmed, Transmission, TransmissionState};
use crate::ids::{OperatorId, TransmissionId};
use crate::support::{NonEmpty, Timestamp};
use crate::wire::Rejected;

/// What an operator judged a transmission to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// A real agent-to-agent communication.
    Genuine,
    /// Not a communication: coincidental text, a shared template, an
    /// unrelated co-access.
    FalseDetection,
}

/// A transmission state the detector has made a call on, borrowed from a
/// [`TransmissionState`]. Only these take a verdict.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Judgeable<'a> {
    /// Access-pattern evidence only, window closed.
    Suspected(&'a NonEmpty<CoAccess>),
    /// Suspected, then discarded: the detector's negative call.
    Discarded(&'a NonEmpty<CoAccess>),
    /// `Confirmed`, `Classified` or `Aggregated`: content evidence.
    Confirmed(&'a Confirmed),
}

/// `Detected` or `AwaitingContent`: the detector has not decided yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotJudgeable;

impl TransmissionState {
    /// The detector's call, when this state takes a verdict. The match is
    /// exhaustive, so a new state does not compile until it is placed.
    pub fn judgeable(&self) -> Result<Judgeable<'_>, NotJudgeable> {
        match self {
            Self::Detected | Self::AwaitingContent { .. } => Err(NotJudgeable),
            Self::Suspected { co_access, .. } => Ok(Judgeable::Suspected(co_access)),
            Self::Discarded { co_access, .. } => Ok(Judgeable::Discarded(co_access)),
            Self::Confirmed(confirmed)
            | Self::Classified { confirmed, .. }
            | Self::Aggregated { confirmed, .. } => Ok(Judgeable::Confirmed(confirmed)),
        }
    }
}

/// The position of a record in its transmission's [`VerdictLog`]: 1 for the
/// first record, one more for each after it. On the wire, the number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct VerdictRevision(NonZeroU32);

impl VerdictRevision {
    pub const FIRST: Self = Self(NonZeroU32::MIN);

    pub const fn new(revision: NonZeroU32) -> Self {
        Self(revision)
    }

    pub const fn get(self) -> NonZeroU32 {
        self.0
    }

    /// The revision after this one. `None` once the counter is exhausted;
    /// the store rejects that record.
    pub fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

/// One operator verdict, or the withdrawal of one.
///
/// Built only through [`TransmissionVerdict::new`], which takes the
/// transmission and rejects one in a state that takes no verdict. The
/// surface stamps `by` and `at` from the authenticated caller and the time
/// it accepted the action; callers supply only the verdict and the note.
///
/// A response, never a request. Decoding cannot rerun
/// [`TransmissionVerdict::new`]: its check reads the transmission's state,
/// which the record names only by id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct TransmissionVerdict {
    transmission: TransmissionId,
    verdict: Option<Verdict>,
    by: OperatorId,
    at: Timestamp,
    note: Option<String>,
}

impl TransmissionVerdict {
    /// `verdict` is `None` for a withdrawal.
    pub fn new(
        transmission: &Transmission,
        verdict: Option<Verdict>,
        by: OperatorId,
        at: Timestamp,
        note: Option<String>,
    ) -> Result<Self, NotJudgeable> {
        transmission.state.judgeable()?;
        Ok(Self {
            transmission: transmission.id,
            verdict,
            by,
            at,
            note,
        })
    }

    pub fn transmission(&self) -> TransmissionId {
        self.transmission
    }

    /// `None` when this record withdraws the verdict before it.
    pub fn verdict(&self) -> Option<Verdict> {
        self.verdict
    }

    pub fn by(&self) -> OperatorId {
        self.by
    }

    pub fn at(&self) -> Timestamp {
        self.at
    }

    pub fn note(&self) -> Option<&str> {
        self.note.as_deref()
    }
}

/// What appending a record did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerdictRecorded {
    /// Appended at this revision; it is now the current verdict.
    Appended(VerdictRevision),
    /// The record's verdict is already current (including a withdrawal with
    /// no verdict in force): nothing was appended.
    Unchanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidVerdictRecord {
    /// The record is about another transmission than the log.
    OtherTransmission,
    /// The log already holds `u32::MAX` records.
    RevisionsExhausted,
}

/// Every verdict record of one transmission, in append order. The record at
/// index `i` has revision `i + 1`, so revisions are consecutive by
/// construction, and the last record is the current verdict.
///
/// On the wire, `{"transmission": .., "records": [{"revision": 1, "record":
/// {..}}, ..]}`: each record with its revision, so a reader can line the log
/// up with the `VerdictSet` events and `VerdictRow`s that carry the same
/// numbers. Decoding goes through [`VerdictLog::from_records`], so a decoded
/// log is one [`VerdictLog::record`] could have built.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    rename_all = "snake_case",
    try_from = "RawVerdictLog",
    into = "RawVerdictLog"
)]
pub struct VerdictLog {
    transmission: TransmissionId,
    records: Vec<TransmissionVerdict>,
}

/// [`VerdictLog`]'s wire form: its records, each with its revision.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawVerdictLog {
    transmission: TransmissionId,
    records: Vec<RawLoggedVerdict>,
}

/// One record of a [`VerdictLog`] on the wire, with its revision.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawLoggedVerdict {
    revision: VerdictRevision,
    record: TransmissionVerdict,
}

impl From<VerdictLog> for RawVerdictLog {
    fn from(log: VerdictLog) -> Self {
        let revisions = std::iter::successors(Some(VerdictRevision::FIRST), |r| r.next());
        Self {
            transmission: log.transmission,
            records: revisions
                .zip(log.records)
                .map(|(revision, record)| RawLoggedVerdict { revision, record })
                .collect(),
        }
    }
}

/// Why records are not a log [`VerdictLog::record`] could have built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidVerdictLog {
    /// Record `index` carries `found` where its position gives `expected`
    /// (`index + 1`): a gap, a repeat or a reordering of revisions.
    UnexpectedRevision {
        index: usize,
        expected: VerdictRevision,
        found: VerdictRevision,
    },
    /// Record `index` was refused.
    Record {
        index: usize,
        error: InvalidVerdictRecord,
    },
    /// Record `index` repeats the verdict current before it, so it would
    /// not have been appended.
    Unchanged { index: usize },
}

impl TryFrom<RawVerdictLog> for VerdictLog {
    type Error = Rejected<InvalidVerdictLog>;

    fn try_from(raw: RawVerdictLog) -> Result<Self, Self::Error> {
        let records = raw
            .records
            .into_iter()
            .map(|logged| (logged.revision, logged.record))
            .collect();
        Self::from_records(raw.transmission, records)
            .map_err(|error| Rejected::new("verdict log", error))
    }
}

impl VerdictLog {
    /// The log of a transmission no operator has judged.
    pub fn new(transmission: TransmissionId) -> Self {
        Self {
            transmission,
            records: Vec::new(),
        }
    }

    /// Rebuild a stored log from its records, oldest first, each with its
    /// revision: starting from [`VerdictLog::new`], each record must carry
    /// the next revision and is appended with [`VerdictLog::record`], which
    /// must append it. The first refusal is the error.
    pub fn from_records(
        transmission: TransmissionId,
        records: Vec<(VerdictRevision, TransmissionVerdict)>,
    ) -> Result<Self, InvalidVerdictLog> {
        let mut log = Self::new(transmission);
        for (index, (found, record)) in records.into_iter().enumerate() {
            let expected = match log.revision() {
                None => VerdictRevision::FIRST,
                Some(last) => last.next().ok_or(InvalidVerdictLog::Record {
                    index,
                    error: InvalidVerdictRecord::RevisionsExhausted,
                })?,
            };
            if found != expected {
                return Err(InvalidVerdictLog::UnexpectedRevision {
                    index,
                    expected,
                    found,
                });
            }
            match log.record(record) {
                Ok(VerdictRecorded::Appended(_)) => {}
                Ok(VerdictRecorded::Unchanged) => {
                    return Err(InvalidVerdictLog::Unchanged { index });
                }
                Err(error) => return Err(InvalidVerdictLog::Record { index, error }),
            }
        }
        Ok(log)
    }

    /// Append `record` unless its verdict is already current. Rejects,
    /// leaving the log unchanged, a record about another transmission.
    pub fn record(
        &mut self,
        record: TransmissionVerdict,
    ) -> Result<VerdictRecorded, InvalidVerdictRecord> {
        if record.transmission != self.transmission {
            return Err(InvalidVerdictRecord::OtherTransmission);
        }
        if record.verdict == self.current() {
            return Ok(VerdictRecorded::Unchanged);
        }
        let revision = match self.revision() {
            None => VerdictRevision::FIRST,
            Some(last) => last
                .next()
                .ok_or(InvalidVerdictRecord::RevisionsExhausted)?,
        };
        self.records.push(record);
        Ok(VerdictRecorded::Appended(revision))
    }

    pub fn transmission(&self) -> TransmissionId {
        self.transmission
    }

    /// Oldest first.
    pub fn records(&self) -> &[TransmissionVerdict] {
        &self.records
    }

    /// The latest record's verdict; `None` when never judged or withdrawn.
    pub fn current(&self) -> Option<Verdict> {
        self.records.last().and_then(TransmissionVerdict::verdict)
    }

    /// The latest record's revision; `None` for an empty log.
    pub fn revision(&self) -> Option<VerdictRevision> {
        u32::try_from(self.records.len())
            .ok()
            .and_then(NonZeroU32::new)
            .map(VerdictRevision)
    }
}

/// A reader's copy of one transmission's current verdict, fed by
/// `VerdictSet` events. A reader holds no `CurrentVerdict` for a
/// transmission it has seen no event for, which means no verdict; the first
/// event it sees becomes the copy, and later ones go through
/// [`CurrentVerdict::observe`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CurrentVerdict {
    pub verdict: Option<Verdict>,
    pub revision: VerdictRevision,
}

/// Whether [`CurrentVerdict::observe`] changed the copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Observed {
    /// The event was newer: the copy now holds it.
    Newer,
    /// The event's revision is not newer than the copy's (a redelivery or a
    /// late arrival): nothing changed.
    Stale,
}

impl CurrentVerdict {
    /// Take the event's verdict if its revision is newer than the copy's.
    /// Whatever order a log's events arrive in, and however often each is
    /// delivered, the copy ends at the log's latest record.
    pub fn observe(&mut self, verdict: Option<Verdict>, revision: VerdictRevision) -> Observed {
        if revision <= self.revision {
            return Observed::Stale;
        }
        *self = Self { verdict, revision };
        Observed::Newer
    }

    /// What [`FilterSubject::false_detection`] holds for the transmission.
    ///
    /// [`FilterSubject::false_detection`]: crate::aggregates::filter::FilterSubject::false_detection
    pub fn is_false_detection(copy: Option<&Self>) -> bool {
        copy.is_some_and(|copy| copy.verdict == Some(Verdict::FalseDetection))
    }
}
