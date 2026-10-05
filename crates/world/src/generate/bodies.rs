//! Message bodies: what the evidence page cuts its excerpts from, and the
//! span records that locate a sender's text in them.
//!
//! A body is one message whose part `part` holds filler, the generated
//! text and more filler, after short filler parts ([`StoredBody`]). It is
//! built into the spec's [`MessageBody`] and encoded with the spec's
//! canonical encoding, so its [`MessageHash`] is exactly what
//! `BlobStore::put` returns for its bytes and `Message::part_text` indexes
//! the bytes the span and match locations were generated against.
//!
//! Span records ([`Blobs::span`], [`Blobs::spans`]) locate each content
//! match's origin: the seed records them through L4's `SpanIndex` (what
//! the evidence page reads a sender-side excerpt's location from) and uses
//! them to know which bodies a dropped sender side names.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crosstalk_spec::derived::provenance::matching::Carrier;
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{MessageHash, SpanId};
use crosstalk_spec::interfaces::l4_provenance::IndexedSpan;
use crosstalk_spec::observed::message::encoding;
use crosstalk_spec::observed::message::{
    AssistantPart, MessageBody, SystemPart, Text, ToolCallId, ToolOutcome, ToolResult,
    ToolResultContent, UserPart,
};
use crosstalk_spec::support::{NonEmpty, Timestamp};

/// Plain ASCII prose the filler is cut from, so every byte is a character
/// boundary.
const FILLER: &str = "Earlier in this exchange the agent listed the files it had open, \
summarised the last test run and noted two follow-ups for the next session. \
The build passed on the second attempt after the cache was cleared. \
Nothing in this part of the context relates to the matched text. \
The operator asked for a short status update and a list of open questions. \
Logs from the previous step were trimmed to the last hundred lines. ";

/// `len` bytes of filler, starting `seed` bytes into the source.
fn filler(seed: u64, len: u32) -> String {
    let source = FILLER.as_bytes();
    let start = usize::try_from(seed % source.len() as u64).unwrap_or(0);
    let len = usize::try_from(len).unwrap_or(0);
    source
        .iter()
        .cycle()
        .skip(start)
        .take(len)
        .map(|b| char::from(*b))
        .collect()
}

/// Which role's message holds the text, and so which part kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Holder {
    /// The sender's reply, or a reader's own output.
    Assistant,
    /// A relayed user turn.
    User,
    System,
    /// A tool message: the matched part is the result of this call.
    Tool(ToolCallId),
}

impl Holder {
    /// Where a reader's copy arrived.
    pub fn of(carrier: &Carrier) -> Self {
        match carrier {
            Carrier::ToolResult(call) => Self::Tool(call.clone()),
            Carrier::UserTurn => Self::User,
            Carrier::SystemPrompt => Self::System,
            Carrier::ReaderOutput => Self::Assistant,
        }
    }
}

/// One message body: part `part` holds `before` bytes of filler, then
/// `core`, then `after` bytes of filler; every earlier part is a short
/// filler text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredBody {
    pub holder: Holder,
    pub part: u16,
    pub before: u32,
    pub core: Arc<str>,
    pub after: u32,
}

impl StoredBody {
    fn seed(&self) -> u64 {
        u64::from(self.before) ^ (u64::from(self.after) << 16) ^ u64::from(self.part)
    }

    /// The text of part `part`: what a location's range indexes.
    pub fn part_text(&self) -> String {
        let seed = self.seed();
        let mut text = filler(seed, self.before);
        text.push_str(&self.core);
        text.push_str(&filler(seed.wrapping_add(97), self.after));
        text
    }

    /// The canonical message body.
    pub fn body(&self) -> MessageBody {
        let text = self.part_text();
        let earlier = |i: u16| format!("(earlier part {i} of this message)");
        let texts: Vec<Text> = (0..self.part)
            .map(|i| Text(earlier(i)))
            .chain(std::iter::once(Text(text.clone())))
            .collect();
        match &self.holder {
            Holder::Assistant => {
                MessageBody::Assistant(texts.into_iter().map(AssistantPart::Text).collect())
            }
            Holder::User => MessageBody::User(texts.into_iter().map(UserPart::Text).collect()),
            Holder::System => {
                MessageBody::System(texts.into_iter().map(SystemPart::Text).collect())
            }
            Holder::Tool(call) => {
                let result = |i: usize, text: Text| ToolResult {
                    call_id: if i == usize::from(self.part) {
                        call.clone()
                    } else {
                        ToolCallId(format!("{}-earlier-{i}", call.0))
                    },
                    content: vec![ToolResultContent::Text(text)],
                    outcome: ToolOutcome::Success,
                };
                let mut results = texts.into_iter().enumerate().map(|(i, t)| result(i, t));
                let first = results
                    .next()
                    .unwrap_or_else(|| result(0, Text(text.clone())));
                let mut all = NonEmpty::new(first);
                for next in results {
                    all.push(next);
                }
                MessageBody::Tool(all)
            }
        }
    }

    /// The body's canonical encoding and hash.
    pub fn encode(&self) -> (MessageHash, Vec<u8>) {
        let bytes = encoding::encode(&self.body());
        (encoding::hash_bytes(&bytes), bytes)
    }
}

/// One encoded body and the time of the exchange that carried it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Encoded {
    pub at: Timestamp,
    pub bytes: Vec<u8>,
}

/// One originated span as L4 records it, and when its exchange was
/// captured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordedSpan {
    pub indexed: IndexedSpan,
    pub at: Timestamp,
}

/// Span records and encoded message bodies.
#[derive(Debug, Clone, Default)]
pub struct Blobs {
    spans: HashMap<SpanId, RecordedSpan>,
    bodies: BTreeMap<MessageHash, Encoded>,
}

impl Blobs {
    /// Record `span`, written by `indexed.author` in `indexed.exchange`,
    /// captured at `at`.
    pub fn record_span(&mut self, span: SpanId, indexed: IndexedSpan, at: Timestamp) {
        self.spans.insert(span, RecordedSpan { indexed, at });
    }

    /// Every span record, by id.
    pub fn spans(&self) -> BTreeMap<SpanId, RecordedSpan> {
        self.spans
            .iter()
            .map(|(id, recorded)| (*id, *recorded))
            .collect()
    }

    /// Store `body`, carried at `at`, and return its hash.
    pub fn store(&mut self, body: &StoredBody, at: Timestamp) -> MessageHash {
        let (hash, bytes) = body.encode();
        self.bodies.entry(hash).or_insert(Encoded { at, bytes });
        hash
    }

    /// The span's location, as L4 recorded it.
    pub fn span(&self, id: SpanId) -> Option<SpanLocation> {
        self.spans
            .get(&id)
            .map(|recorded| recorded.indexed.location)
    }

    pub fn contains(&self, hash: MessageHash) -> bool {
        self.bodies.contains_key(&hash)
    }

    /// Content retention: the body is never stored. Whether it was held.
    pub fn drop_body(&mut self, hash: MessageHash) -> bool {
        self.bodies.remove(&hash).is_some()
    }

    /// Every body still held, by hash.
    pub fn into_bodies(self) -> BTreeMap<MessageHash, Encoded> {
        self.bodies
    }
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::observed::message::Role;

    use super::*;

    fn body(holder: Holder, part: u16) -> StoredBody {
        StoredBody {
            holder,
            part,
            before: 300,
            core: Arc::from("the matched text"),
            after: 40,
        }
    }

    #[test]
    fn the_part_text_is_filler_core_filler_and_indexes_the_message() {
        let stored = body(Holder::Assistant, 2);
        let text = stored.part_text();
        assert_eq!(text.len(), 300 + "the matched text".len() + 40);
        assert_eq!(text.get(300..316), Some("the matched text"));
        let message = encoding::message(stored.body());
        assert_eq!(message.part_count(), 3);
        assert_eq!(message.part_text(2).as_deref(), Ok(text.as_str()));
    }

    #[test]
    fn each_holder_builds_its_role() {
        let call = ToolCallId("toolu_9".into());
        for (holder, role) in [
            (Holder::Assistant, Role::Assistant),
            (Holder::User, Role::User),
            (Holder::System, Role::System),
            (Holder::Tool(call.clone()), Role::Tool),
        ] {
            for part in [0, 3] {
                let stored = body(holder.clone(), part);
                let message = encoding::message(stored.body());
                assert_eq!(message.body.role(), role);
                assert_eq!(message.part_count(), usize::from(part) + 1);
                assert_eq!(
                    message.part_text(part).as_deref(),
                    Ok(stored.part_text().as_str())
                );
            }
        }
    }

    #[test]
    fn the_hash_is_the_encodings_and_decodes_back() {
        let stored = body(Holder::User, 1);
        let (hash, bytes) = stored.encode();
        assert_eq!(hash, encoding::hash(&stored.body()));
        assert_eq!(encoding::decode(&bytes), Ok(stored.body()));
    }

    #[test]
    fn dropped_bodies_are_gone_and_spans_stay() {
        let mut blobs = Blobs::default();
        let hash = blobs.store(&body(Holder::User, 0), Timestamp::from_micros(1));
        assert!(blobs.contains(hash));
        assert!(blobs.drop_body(hash));
        assert!(!blobs.drop_body(hash));
        assert!(!blobs.contains(hash));
    }
}
