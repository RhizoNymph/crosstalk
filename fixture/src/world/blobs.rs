//! The fixture's blob store and span records: what the evidence page cuts
//! its excerpts from, as the surface reads them (`Span::location`, then
//! `BlobStore::get` by message hash).
//!
//! A body is kept compactly as a [`StoredBody`]: the one part holding the
//! matched text, made of filler before it, the generated text and filler
//! after it, preceded by short filler parts. [`StoredBody::message`] builds
//! the canonical [`Message`] on read, so `Message::part_text` indexes the
//! same bytes the span and match locations were generated against.
//!
//! Content retention is modelled by [`Blobs::drop_body`]: a dropped body is
//! gone from the store, and its span record stays.

use std::collections::HashMap;
use std::sync::Arc;

use crosstalk_spec::derived::provenance::matching::Carrier;
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{MessageHash, SpanId};
use crosstalk_spec::observed::message::{
    AssistantPart, Message, MessageBody, SystemPart, Text, ToolCallId, ToolOutcome, ToolResult,
    ToolResultContent, UserPart,
};
use crosstalk_spec::support::NonEmpty;

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

/// One stored message body: part `part` holds `before` bytes of filler,
/// then `core`, then `after` bytes of filler; every earlier part is a short
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

    /// The canonical message the blob store returns under `hash`.
    pub fn message(&self, hash: MessageHash) -> Message {
        let text = self.part_text();
        let earlier = |i: u16| format!("(earlier part {i} of this message)");
        let texts: Vec<Text> = (0..self.part)
            .map(|i| Text(earlier(i)))
            .chain(std::iter::once(Text(text.clone())))
            .collect();
        let body = match &self.holder {
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
        };
        Message { hash, body }
    }
}

/// Span records and message bodies.
#[derive(Debug, Clone, Default)]
pub struct Blobs {
    spans: HashMap<SpanId, SpanLocation>,
    bodies: HashMap<MessageHash, StoredBody>,
}

impl Blobs {
    pub fn record_span(&mut self, span: SpanId, location: SpanLocation) {
        self.spans.insert(span, location);
    }

    pub fn store(&mut self, hash: MessageHash, body: StoredBody) {
        self.bodies.insert(hash, body);
    }

    /// The span's location, as L4 recorded it.
    pub fn span(&self, id: SpanId) -> Option<SpanLocation> {
        self.spans.get(&id).copied()
    }

    /// `BlobStore::get`: the body under `hash`, `None` once retention
    /// dropped it.
    pub fn body(&self, hash: MessageHash) -> Option<Message> {
        self.bodies.get(&hash).map(|body| body.message(hash))
    }

    /// Content retention: the body is gone. Whether it was held.
    pub fn drop_body(&mut self, hash: MessageHash) -> bool {
        self.bodies.remove(&hash).is_some()
    }
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::observed::message::Role;
    use crosstalk_spec::support::Blake3;

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

    fn hash(n: u8) -> MessageHash {
        MessageHash::from_digest(Blake3::from_bytes([n; 32]))
    }

    #[test]
    fn the_part_text_is_filler_core_filler() {
        let stored = body(Holder::Assistant, 2);
        let text = stored.part_text();
        assert_eq!(text.len(), 300 + "the matched text".len() + 40);
        assert_eq!(&text[300..316], "the matched text");
        let message = stored.message(hash(1));
        assert_eq!(message.part_count(), 3);
        assert_eq!(message.part_text(2).as_deref(), Ok(text.as_str()));
        assert!(message.part_text(0).is_ok());
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
                let message = stored.message(hash(2));
                assert_eq!(message.body.role(), role);
                assert_eq!(message.part_count(), usize::from(part) + 1);
                assert_eq!(
                    message.part_text(part).as_deref(),
                    Ok(stored.part_text().as_str())
                );
            }
        }
        let MessageBody::Tool(results) = body(Holder::Tool(call.clone()), 1).message(hash(3)).body
        else {
            panic!("a tool message");
        };
        assert_eq!(results.iter().nth(1).map(|r| &r.call_id), Some(&call));
    }

    #[test]
    fn dropped_bodies_are_gone_and_spans_stay() {
        let mut blobs = Blobs::default();
        blobs.store(hash(4), body(Holder::User, 0));
        assert!(blobs.body(hash(4)).is_some());
        assert!(blobs.drop_body(hash(4)));
        assert!(!blobs.drop_body(hash(4)));
        assert_eq!(blobs.body(hash(4)), None);
    }
}
