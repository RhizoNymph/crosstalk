//! The step's derived values: access ids, delivery digests, part
//! references and the context a system prompt states. The digests are
//! pinned (their domain strings included): changing one would give every
//! redelivered delta new access ids, or read a replayed result again.

use crosstalk_spec::ids::{AccessId, AgentId, ExchangeId, MessageHash};
use crosstalk_spec::observed::message::{
    Message, MessageBody, PartRef, SystemPart, ToolArguments, ToolCall, ToolResult,
};
use crosstalk_spec::support::Timestamp;

use super::ledger::DeliveryKey;
use crate::extract::ConversationContext;

/// `agent`'s delivery of `result` (at `part` of `message`) for `call`: a
/// digest of the call's id, name and arguments and the result's outcome
/// and text (or, for a result with no text, its part).
pub(crate) fn delivery_key(
    agent: AgentId,
    call: &ToolCall,
    result: &ToolResult,
    message: &Message,
    part: PartRef,
) -> DeliveryKey {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"crosstalk.live.extract.delivery.v1/");
    let mut field = |bytes: &[u8]| {
        hasher.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
        hasher.update(bytes);
    };
    field(call.id.0.as_bytes());
    field(call.name.0.as_bytes());
    match &call.arguments {
        ToolArguments::Json(json) => field(json.0.as_bytes()),
        ToolArguments::Invalid(text) => field(text.as_bytes()),
    }
    field(format!("{:?}", result.outcome).as_bytes());
    match message.part_text(part.index) {
        Ok(text) => field(text.as_bytes()),
        Err(_) => {
            field(&message.hash.digest().as_bytes()[..]);
            field(&part.index.to_le_bytes());
        }
    }
    DeliveryKey {
        agent,
        digest: *hasher.finalize().as_bytes(),
    }
}

pub(crate) fn part_ref(message: MessageHash, index: usize) -> PartRef {
    PartRef {
        message,
        index: u16::try_from(index).unwrap_or(u16::MAX),
    }
}

/// The conversation context a system prompt states.
pub(crate) fn context_of(message: &Message) -> ConversationContext {
    let MessageBody::System(parts) = &message.body else {
        return ConversationContext::default();
    };
    let text = parts
        .iter()
        .filter_map(|part| match part {
            SystemPart::Text(text) => Some(text.0.as_str()),
            SystemPart::Unknown(_) => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    ConversationContext::from_system_prompt(&text)
}

/// A ULID stamped at `at`'s millisecond whose random part is a digest of
/// the exchange, the call, the kind and the access's place.
pub(crate) fn access_id(
    exchange: ExchangeId,
    call: &str,
    kind: &str,
    index: usize,
    at: Timestamp,
) -> AccessId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"crosstalk.live.extract.access.v1/");
    hasher.update(&exchange.as_ulid().to_be_bytes());
    hasher.update(&u64::try_from(call.len()).unwrap_or(u64::MAX).to_le_bytes());
    hasher.update(call.as_bytes());
    hasher.update(kind.as_bytes());
    hasher.update(&u64::try_from(index).unwrap_or(u64::MAX).to_le_bytes());
    let digest = hasher.finalize();
    let mut random = [0u8; 16];
    random[6..].copy_from_slice(&digest.as_bytes()[..10]);
    let millis = u128::from(at.as_micros() / 1_000) & ((1 << 48) - 1);
    AccessId::from_ulid((millis << 80) | u128::from_be_bytes(random))
}
