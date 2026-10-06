//! `QueryApi::conversation_text` and `part_text`: the text behind the
//! turns, cut from the bodies with `PartText::cut`.

use crosstalk_spec::ids::{ConversationId, MessageHash};
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::interfaces::l8_surface::conversation::TurnWindow;
use crosstalk_spec::interfaces::l8_surface::conversation::text::{
    BodyText, ConversationText, MessageText, PartText, TextError, TextLimit, TextSlice, TurnText,
};
use crosstalk_spec::observed::message::PartRef;

use super::index;
use super::turns::body;
use crate::backend::Result;
use crate::backend::fixture::queries::Ctx;

fn message_text(ctx: &Ctx, hash: MessageHash, limit: TextLimit) -> Result<MessageText> {
    let body = match body(ctx, hash) {
        None => BodyText::BodyDropped,
        Some(message) => {
            let parts = (0..message.part_count())
                .map(|i| {
                    let at = u16::try_from(i).unwrap_or(u16::MAX);
                    match message.part_text(at) {
                        Ok(text) => PartText::cut(&text, 0, limit).map(Some).map_err(|e| {
                            QueryError::Store {
                                reason: format!("a stored part did not cut: {e:?}"),
                            }
                        }),
                        Err(_) => Ok(None),
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            BodyText::Shown(parts)
        }
    };
    Ok(MessageText { hash, body })
}

/// `QueryApi::conversation_text`: aligned with `conversation_turns` for the
/// same window.
pub fn window(
    ctx: &Ctx,
    id: ConversationId,
    window: &TurnWindow,
    limit: TextLimit,
) -> Result<Option<ConversationText>> {
    let Some(record) = ctx.world.conversations.get(id) else {
        return Ok(None);
    };
    let total = u32::try_from(record.turns.len()).unwrap_or(u32::MAX);
    let mut turns = Vec::new();
    for at in window.range(total) {
        let Some(turn) = record.turns.get(at as usize) else {
            continue;
        };
        turns.push(TurnText {
            index: index(at as usize),
            inputs: turn
                .inputs
                .iter()
                .map(|entry| message_text(ctx, entry.message, limit))
                .collect::<Result<_>>()?,
            output: turn
                .output
                .map(|hash| message_text(ctx, hash, limit))
                .transpose()?,
        });
    }
    Ok(Some(ConversationText {
        conversation: id,
        turns,
    }))
}

/// `QueryApi::part_text`: `None` when no such message is stored; a part
/// with no text or a slice off its text is `InvalidInput`.
pub fn part(ctx: &Ctx, part: PartRef, slice: TextSlice) -> Result<Option<PartText>> {
    let Some(message) = body(ctx, part.message) else {
        return Ok(None);
    };
    let text = message
        .part_text(part.index)
        .map_err(|e| QueryError::from(TextError::Part(e)))?;
    PartText::cut(&text, slice.from, slice.limit)
        .map(Some)
        .map_err(QueryError::from)
}
