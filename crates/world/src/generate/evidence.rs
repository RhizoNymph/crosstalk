//! Content matches with the text behind them: the sender's paragraph, and
//! the reader's copy of its key sentence as it arrived (exact, re-cased,
//! encoded or paraphrased), each stored as a message body with the span
//! and match locations pointing into it.

use std::num::NonZeroU32;
use std::sync::Arc;

use crosstalk_spec::derived::provenance::matching::{Carrier, Codec, ContentMatch, MatchKind};
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{AgentId, ExchangeId, MessageHash, SpanId};
use crosstalk_spec::interfaces::l4_provenance::IndexedSpan;
use crosstalk_spec::observed::message::PartRef;
use crosstalk_spec::support::{Blake3, ByteRange, NonEmpty, Similarity, Timestamp};

use crate::error::WorldError;
use crate::mint::Mint;
use crate::rng::Rng;
use crate::text::{self, Theme, codec};

use super::bodies::{Blobs, Holder, StoredBody};

/// The text behind one content match: the sender's paragraph and the
/// reader's copy as it arrived.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchText {
    pub origin: Arc<str>,
    pub read: Arc<str>,
}

pub struct BuiltMatch {
    pub content: ContentMatch,
    pub text: MatchText,
}

/// The codec chains decoded matches use, in application order.
const CHAINS: &[&[Codec]] = &[
    &[Codec::Base64],
    &[Codec::Base64, Codec::UrlEncoding],
    &[Codec::Hex],
    &[Codec::UrlEncoding],
    &[Codec::UnicodeNormalization],
];

pub fn pick_kind(rng: &mut Rng) -> Result<MatchKind, WorldError> {
    Ok(match rng.weighted(&[0.5, 0.2, 0.15, 0.15]) {
        Some(1) => MatchKind::Normalized,
        Some(2) => {
            let chain = rng.pick(CHAINS).copied().unwrap_or(&[Codec::Base64]);
            let chain = NonEmpty::from_vec(chain.to_vec())
                .ok_or_else(|| WorldError::missing("codec chain"))?;
            MatchKind::Decoded(chain)
        }
        Some(3) => {
            let score = 0.70 + 0.27 * rng.unit();
            MatchKind::Semantic(
                Similarity::new(score as f32).map_err(|e| WorldError::invalid("Similarity", e))?,
            )
        }
        _ => MatchKind::Exact,
    })
}

/// A random part reference: what an access's call or result names. Its
/// message is not stored.
pub fn part(rng: &mut Rng) -> PartRef {
    PartRef {
        message: MessageHash::from_digest(Blake3::from_bytes(rng.bytes32())),
        index: u16::try_from(rng.below(6)).unwrap_or(0),
    }
}

/// The line a reader's input shows before the copied text.
fn context(carrier: &Carrier) -> &'static str {
    match carrier {
        Carrier::ToolResult(_) => "Tool result:\n",
        Carrier::UserTurn => "Forwarded from another session:\n",
        Carrier::SystemPrompt => "## Team context\n",
        Carrier::ReaderOutput => "Plan for this step: ",
    }
}

fn len_u32(n: usize, what: &'static str) -> Result<u32, WorldError> {
    u32::try_from(n).map_err(|e| WorldError::invalid(what, e))
}

/// Who a match is between and how it arrived.
pub struct Ends {
    pub theme: Theme,
    pub from: AgentId,
    pub to: AgentId,
    pub exchange: ExchangeId,
    pub carrier: Carrier,
}

/// One content match of `kind` read at `at`: a fresh paragraph on the
/// theme for the sender, and the reader's copy of its key sentence. Both
/// bodies go to `blobs`, and the span's location is recorded there.
pub fn build(
    rng: &mut Rng,
    mint: &mut Mint,
    blobs: &mut Blobs,
    ends: Ends,
    kind: MatchKind,
    at: Timestamp,
) -> Result<BuiltMatch, WorldError> {
    let Ends {
        theme,
        from,
        to,
        exchange,
        carrier,
    } = ends;
    let paragraph = text::paragraph(theme, rng);
    let key = paragraph.key_text().to_owned();
    let origin_before = len_u32(rng.below(1500) as usize, "elided")?;
    let origin_after = len_u32(rng.below(800) as usize, "elided")?;
    let origin_range = ByteRange::new(
        origin_before + len_u32(paragraph.key.start, "range")?,
        origin_before + len_u32(paragraph.key.end, "range")?,
    )
    .map_err(|e| WorldError::invalid("ByteRange (origin)", e))?;
    let origin_text: Arc<str> = Arc::from(paragraph.text);

    let body = match &kind {
        MatchKind::Exact => key.clone(),
        MatchKind::Normalized => codec::denormalize(&key),
        MatchKind::Decoded(chain) => codec::encode_chain(&key, chain.iter()),
        MatchKind::Semantic(_) => text::sentence(theme, rng),
    };
    let prefix = context(&carrier);
    let suffix = format!("\n{}", text::sentence(theme, rng));
    let read_text = format!("{prefix}{body}{suffix}");
    let highlight = prefix.len()..prefix.len() + body.len();
    let before = len_u32(rng.below(4000) as usize, "elided")?;
    let after = len_u32(rng.below(600) as usize, "elided")?;
    let read_text: Arc<str> = Arc::from(read_text);

    let start = before + len_u32(highlight.start, "range")?;
    let end = before + len_u32(highlight.end, "range")?;
    let range = ByteRange::new(start, end).map_err(|e| WorldError::invalid("ByteRange", e))?;
    let read_len = range.len().get();
    let matched = match kind {
        MatchKind::Semantic(_) => (read_len * 4 / 5).max(1),
        _ => read_len,
    };
    let matched = NonZeroU32::new(matched).ok_or_else(|| WorldError::missing("matched bytes"))?;
    let span: SpanId = mint.at(at)?;
    let read_part = u16::try_from(rng.below(6)).unwrap_or(0);
    let read = StoredBody {
        holder: Holder::of(&carrier),
        part: read_part,
        before,
        core: Arc::clone(&read_text),
        after,
    };
    let origin = StoredBody {
        holder: Holder::Assistant,
        part: u16::try_from(span.as_ulid() % 3).unwrap_or(0),
        before: origin_before,
        core: Arc::clone(&origin_text),
        after: origin_after,
    };
    let origin_hash = blobs.store(&origin, at);
    let read_hash = blobs.store(&read, at);
    blobs.record_span(
        span,
        IndexedSpan {
            // The sender's exchange is not modelled: the span's own ULID,
            // minted at its time and unique among the world's ids, names
            // the exchange whose response holds it.
            exchange: ExchangeId::from_ulid(span.as_ulid()),
            author: from,
            location: SpanLocation {
                part: PartRef {
                    message: origin_hash,
                    index: origin.part,
                },
                range: origin_range,
            },
        },
        at,
    );
    let content = ContentMatch::new(
        span,
        from,
        to,
        exchange,
        SpanLocation {
            part: PartRef {
                message: read_hash,
                index: read_part,
            },
            range,
        },
        carrier,
        kind,
        matched,
    )
    .map_err(|e| WorldError::invalid("ContentMatch", e))?;
    Ok(BuiltMatch {
        content,
        text: MatchText {
            origin: origin_text,
            read: read_text,
        },
    })
}
