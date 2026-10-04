//! Content matches with the text behind them: the sender's paragraph, and
//! the reader's copy of its key sentence as it arrived (exact, re-cased,
//! encoded or paraphrased), each stored as a message body with the span
//! and match locations pointing into it.

use std::num::NonZeroU32;
use std::sync::Arc;

use crosstalk_spec::derived::provenance::matching::{Carrier, Codec, ContentMatch, MatchKind};
use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{AgentId, ExchangeId, MessageHash, SpanId};
use crosstalk_spec::observed::message::PartRef;
use crosstalk_spec::support::{Blake3, ByteRange, NonEmpty, Similarity, Timestamp};

use crate::clock::Mint;
use crate::rng::Rng;
use crate::text::{self, Theme, codec};

use super::blobs::{Holder, StoredBody};
use super::{GenError, MatchText};

pub struct BuiltMatch {
    pub content: ContentMatch,
    pub text: MatchText,
    /// The sender's originated span: where L4 recorded it, and the body it
    /// indexes.
    pub origin: (SpanLocation, StoredBody),
    /// The body the reader's copy arrived in (`ContentMatch::read_at`).
    pub read: StoredBody,
}

/// The codec chains decoded matches use, in application order.
const CHAINS: &[&[Codec]] = &[
    &[Codec::Base64],
    &[Codec::Base64, Codec::UrlEncoding],
    &[Codec::Hex],
    &[Codec::UrlEncoding],
    &[Codec::UnicodeNormalization],
];

pub fn pick_kind(rng: &mut Rng) -> Result<MatchKind, GenError> {
    Ok(match rng.weighted(&[0.5, 0.2, 0.15, 0.15]) {
        Some(1) => MatchKind::Normalized,
        Some(2) => {
            let chain = rng.pick(CHAINS).copied().unwrap_or(&[Codec::Base64]);
            let chain = NonEmpty::from_vec(chain.to_vec())
                .ok_or_else(|| GenError::Missing("codec chain".to_owned()))?;
            MatchKind::Decoded(chain)
        }
        Some(3) => {
            let score = 0.70 + 0.27 * rng.unit();
            MatchKind::Semantic(
                Similarity::new(score as f32).map_err(|e| GenError::invalid("Similarity", e))?,
            )
        }
        _ => MatchKind::Exact,
    })
}

/// The part of the sender's reply holding a span, derived from the span id
/// so the origin draws nothing from the generator.
fn origin_part(span: SpanId) -> PartRef {
    let raw = span.as_ulid().to_be_bytes();
    let mut bytes = [0u8; 32];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = raw[i % raw.len()] ^ 0x5a;
    }
    PartRef {
        message: MessageHash::from_digest(Blake3::from_bytes(bytes)),
        index: u16::try_from(span.as_ulid() % 3).unwrap_or(0),
    }
}

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

fn len_u32(n: usize, what: &'static str) -> Result<u32, GenError> {
    u32::try_from(n).map_err(|e| GenError::invalid(what, e))
}

/// Who a match is between and how it arrived.
pub struct Ends {
    pub theme: Theme,
    pub from: AgentId,
    pub to: AgentId,
    pub exchange: ExchangeId,
    pub carrier: Carrier,
}

/// One content match of `kind`: a fresh paragraph on the theme for the
/// sender, and the reader's copy of its key sentence.
pub fn build(
    rng: &mut Rng,
    mint: &mut Mint,
    ends: Ends,
    kind: MatchKind,
    at: Timestamp,
) -> Result<BuiltMatch, GenError> {
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
    let key_at = paragraph.key.start;
    let origin_range = ByteRange::new(
        origin_before + len_u32(paragraph.key.start, "range")?,
        origin_before + len_u32(paragraph.key.end, "range")?,
    )
    .map_err(|e| GenError::invalid("ByteRange (origin)", e))?;
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
    let range = ByteRange::new(start, end).map_err(|e| GenError::invalid("ByteRange", e))?;
    let read_len = range.len().get();
    let matched = match kind {
        MatchKind::Semantic(_) => (read_len * 4 / 5).max(1),
        _ => read_len,
    };
    let matched =
        NonZeroU32::new(matched).ok_or_else(|| GenError::Missing("matched bytes".to_owned()))?;
    let span = SpanId::from_ulid(mint.ulid(at));
    let origin_at = origin_part(span);
    let read_at = part(rng);
    let read = StoredBody {
        holder: Holder::of(&carrier),
        part: read_at.index,
        before,
        core: Arc::clone(&read_text),
        after,
    };
    let content = ContentMatch::new(
        span,
        from,
        to,
        exchange,
        SpanLocation {
            part: read_at,
            range,
        },
        carrier,
        kind,
        matched,
    )
    .map_err(|e| GenError::invalid("ContentMatch", e))?;
    let origin = StoredBody {
        holder: Holder::Assistant,
        part: origin_at.index,
        before: origin_before,
        core: Arc::clone(&origin_text),
        after: origin_after,
    };
    Ok(BuiltMatch {
        content,
        text: MatchText {
            origin: origin_text,
            key_at,
            read: read_text,
        },
        origin: (
            SpanLocation {
                part: origin_at,
                range: origin_range,
            },
            origin,
        ),
        read,
    })
}
