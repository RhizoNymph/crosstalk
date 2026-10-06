//! What the conversation pages render, independent of how the surface
//! shapes its reads: the head, each turn with its messages and parts, and
//! the provenance marks on them, all in words and links.
//!
//! [`segments`] cuts a part's text at the marks' byte ranges, so the text
//! renders with each inbound range, originated span, relayed span and the
//! highlighted span marked. Ranges index `Message::part_text`, as the
//! marks' locations do, and the text shown may be a slice of it.

use std::ops::Range;

use crosstalk_spec::observed::client::HarnessClaim;

use crate::pages::common::transmissions::Named;

/// A link with its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link {
    pub label: String,
    pub url: String,
}

/// The head of the conversation page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadView {
    pub short: String,
    pub full: String,
    pub agent: Named,
    /// Harness claims seen on its turns, shown only as claims.
    pub claims: Vec<HarnessClaim>,
    pub started: String,
    pub last: String,
    pub turns: u32,
    pub received: u32,
    pub sent: u32,
    pub origin: OriginView,
    /// The parent turn whose task started this conversation.
    pub spawned_by: Option<SpawnedBy>,
    pub successors: Vec<SuccessorView>,
    /// The corpus its traffic was replayed from.
    pub replayed: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnedBy {
    pub parent: Named,
    pub turn: Link,
    pub transmission: Link,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OriginView {
    Root,
    Fork {
        parent: Link,
        /// The parent turn the fork branches after, linked.
        branch: Option<Link>,
        shared: u32,
    },
    Compaction {
        predecessor: Link,
        carried: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuccessorView {
    /// "fork at turn 12" or "compaction".
    pub kind: String,
    pub link: Link,
    pub started: String,
}

/// Where the window sits in the conversation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowView {
    pub from: u32,
    /// One past the last turn shown.
    pub to: u32,
    pub total: u32,
    pub earlier: Option<String>,
    pub later: Option<String>,
    /// The turn asked for lies past the last turn.
    pub past_end: Option<u32>,
}

/// A row between turns: where a conversation's history came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundaryView {
    /// A compaction's turn 0: the harness summarised the predecessor.
    Compaction { predecessor: Link, carried: u32 },
    /// A fork's turn 0: the shared prefix came from the parent.
    Fork { parent: Link, branch: Option<Link> },
    /// An increment whose previous response the gateway never saw.
    UnseenHistory { connection: Option<String> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutcomeView {
    Completed { stop: String, finished: String },
    Failed { failure: String, at: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnView {
    pub index: u32,
    pub time: String,
    pub model: String,
    pub transport: String,
    /// The WebSocket connection an increment continued on.
    pub connection: Option<String>,
    pub outcome: OutcomeView,
    pub usage: Option<String>,
    pub claim: Option<HarnessClaim>,
    /// The turn's agent, when it is not the conversation's (a merge threaded
    /// an alias's exchange here).
    pub agent: Option<Named>,
    pub replayed: Option<String>,
    pub boundaries: Vec<BoundaryView>,
    /// The provenance scan has not finished: marks may be missing.
    pub pending: bool,
    pub inputs: Vec<MessageView>,
    /// Carried-over messages, folded under the boundary.
    pub carried: Vec<MessageView>,
    pub output: Option<MessageView>,
}

impl TurnView {
    pub fn anchor(&self) -> String {
        anchor(self.index)
    }
}

/// The fragment of turn `index`.
pub fn anchor(index: u32) -> String {
    format!("turn-{index}")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageView {
    pub role: String,
    /// A system message after the first turn.
    pub system_turn: bool,
    pub parts: Vec<PartView>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartView {
    pub kind: String,
    /// The tool, call or block a part names.
    pub detail: Option<String>,
    pub size: Option<String>,
    pub text: TextView,
    pub marks: Vec<MarkView>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TextView {
    /// The caller lacks `Content`.
    Hidden,
    /// The part has no text (media, opaque reasoning, an unknown block).
    NoText,
    /// Content retention dropped the message body.
    Dropped,
    Shown {
        segments: Vec<Segment>,
        /// Bytes of the part after the slice shown.
        remaining: u32,
    },
}

/// A run of text and the marks over it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    pub text: String,
    pub tones: Vec<Tone>,
}

/// What a marked range is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Tone {
    /// Text another agent originated, read here.
    Inbound,
    /// Text this agent originated.
    Originated,
    /// Text this agent copied.
    Relayed,
    /// The span the URL highlights.
    Highlight,
}

/// A marked byte range of a part's text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Marked {
    pub range: Range<u32>,
    pub tone: Tone,
}

/// The text `text`, which is bytes `from .. from + text.len()` of a part,
/// cut into runs at every mark boundary inside it. Marks outside the slice
/// are dropped and marks crossing its ends are clipped; overlapping marks
/// give a run both tones. Boundaries off a character boundary (which the
/// spec's ranges never are) move back to the previous one.
pub fn segments(from: u32, text: &str, marks: &[Marked]) -> Vec<Segment> {
    let len = u32::try_from(text.len()).unwrap_or(u32::MAX);
    let end = from.saturating_add(len);
    let mut cuts: Vec<u32> = vec![0, len];
    for mark in marks {
        for at in [mark.range.start, mark.range.end] {
            if at > from && at < end {
                cuts.push(at - from);
            }
        }
    }
    cuts.sort_unstable();
    cuts.dedup();
    let floor = |at: u32| {
        let mut at = at as usize;
        while !text.is_char_boundary(at) {
            at -= 1;
        }
        at
    };
    let mut out: Vec<Segment> = Vec::new();
    for pair in cuts.windows(2) {
        let (start, stop) = (floor(pair[0]), floor(pair[1]));
        if start >= stop {
            continue;
        }
        let absolute = from + pair[0];
        let mut tones: Vec<Tone> = marks
            .iter()
            .filter(|m| m.range.start <= absolute && absolute < m.range.end)
            .map(|m| m.tone)
            .collect();
        tones.sort_unstable();
        tones.dedup();
        match out.last_mut() {
            Some(last) if last.tones == tones => last.text.push_str(&text[start..stop]),
            _ => out.push(Segment {
                text: text[start..stop].to_owned(),
                tones,
            }),
        }
    }
    out
}

/// A provenance mark under a part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkView {
    /// Text another agent originated, read in this part.
    Inbound(InboundView),
    /// Text this agent originated, and who read it later.
    Originated(OriginatedView),
    /// Text this agent copied from a span or an input.
    Relayed(RelayedView),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundView {
    pub from: Named,
    /// The route in words ("via wiki.example.org", "sub-agent → parent").
    pub route: Option<String>,
    pub route_url: Option<String>,
    pub kind: String,
    pub carrier: String,
    pub matched: String,
    pub range: String,
    /// The sender's turn holding the span.
    pub sender_turn: Option<String>,
    pub transmission: Option<Link>,
    /// The transmission's state in words.
    pub state: Option<String>,
    /// A delegation: "returned by sub-agent" or "task from parent".
    pub delegation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OriginatedView {
    pub status: String,
    pub range: String,
    pub readers: Vec<ReaderView>,
    /// Readers beyond those listed.
    pub more: u32,
    /// The reader list, on this page.
    pub more_url: Option<String>,
    /// Past retention: no later reader can be detected.
    pub expired: bool,
    pub highlighted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReaderView {
    pub agent: Named,
    pub turn: Option<String>,
    pub transmission: Option<Link>,
    pub carrier: String,
    /// "delegated to sub-agent" when the read was a delegated task.
    pub delegation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayedView {
    pub range: String,
    /// The span copied, linked to its turn; `None` for an input that is not
    /// a span.
    pub source: Option<Link>,
}

/// A byte range in words.
pub fn range_text(range: &Range<u32>) -> String {
    format!("bytes {}–{}", range.start, range.end)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mark(start: u32, end: u32, tone: Tone) -> Marked {
        Marked {
            range: start..end,
            tone,
        }
    }

    fn texts(segments: &[Segment]) -> Vec<(&str, Vec<Tone>)> {
        segments
            .iter()
            .map(|s| (s.text.as_str(), s.tones.clone()))
            .collect()
    }

    #[test]
    fn unmarked_text_is_one_plain_run() {
        assert_eq!(
            texts(&segments(0, "hello world", &[])),
            vec![("hello world", vec![])]
        );
    }

    #[test]
    fn a_mark_splits_the_text_around_it() {
        let marked = segments(0, "say hello world", &[mark(4, 9, Tone::Inbound)]);
        assert_eq!(
            texts(&marked),
            vec![
                ("say ", vec![]),
                ("hello", vec![Tone::Inbound]),
                (" world", vec![]),
            ]
        );
    }

    #[test]
    fn overlapping_marks_give_a_run_both_tones() {
        let marked = segments(
            0,
            "abcdefgh",
            &[mark(1, 5, Tone::Originated), mark(3, 7, Tone::Highlight)],
        );
        assert_eq!(
            texts(&marked),
            vec![
                ("a", vec![]),
                ("bc", vec![Tone::Originated]),
                ("de", vec![Tone::Originated, Tone::Highlight]),
                ("fg", vec![Tone::Highlight]),
                ("h", vec![]),
            ]
        );
    }

    #[test]
    fn a_slice_clips_marks_to_what_it_shows() {
        // Bytes 10..20 of the part are shown.
        let marked = segments(
            10,
            "0123456789",
            &[
                mark(5, 12, Tone::Inbound),
                mark(18, 30, Tone::Relayed),
                mark(25, 40, Tone::Originated),
            ],
        );
        assert_eq!(
            texts(&marked),
            vec![
                ("01", vec![Tone::Inbound]),
                ("234567", vec![]),
                ("89", vec![Tone::Relayed]),
            ]
        );
    }

    #[test]
    fn segments_rejoin_to_the_text() {
        let text = "naïve café — déjà vu";
        let marked = segments(
            0,
            text,
            &[mark(3, 9, Tone::Inbound), mark(7, 16, Tone::Relayed)],
        );
        let joined: String = marked.iter().map(|s| s.text.as_str()).collect();
        assert_eq!(joined, text);
    }

    #[test]
    fn ranges_read_as_bytes() {
        assert_eq!(range_text(&(120..332)), "bytes 120–332");
        assert_eq!(anchor(37), "turn-37");
    }
}
