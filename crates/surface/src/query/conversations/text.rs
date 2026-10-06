//! The text reads (Content): a window's part text, aligned with its turns
//! (`surface.conversation.text-aligns`), and one slice of one part.

use crosstalk_spec::ids::ConversationId;
use crosstalk_spec::interfaces::l2_transport::BlobStore;
use crosstalk_spec::interfaces::l3_reconstruction::conversations::{
    ConversationReads, TranscriptEntry, TurnWindow,
};
use crosstalk_spec::interfaces::l8_surface::conversation::text::{
    BodyText, ConversationText, MessageText, PartText, TextError, TextLimit, TextSlice, TurnText,
};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, QueryError};
use crosstalk_spec::observed::message::{PartRef, encoding};

use super::turns::{Window, split};
use crate::service::{Surface, require};
use crate::stores::SurfaceStores;

/// A message's text: each part's from its start, clipped to `limit`. A
/// part a read model lists that cannot be cut is a fault in the records.
fn message_text(
    window: &Window,
    entry: &TranscriptEntry,
    limit: TextLimit,
) -> Result<MessageText, QueryError> {
    let body = match window.body(entry.message) {
        None => BodyText::BodyDropped,
        Some(message) => {
            let mut parts = Vec::with_capacity(message.part_count());
            for index in 0..message.part_count() {
                let index = u16::try_from(index).map_err(|_| QueryError::Store {
                    reason: format!(
                        "message {:?} has more parts than a part index",
                        entry.message
                    ),
                })?;
                let text =
                    match message.part_text(index) {
                        Err(_) => None,
                        Ok(text) => Some(PartText::cut(&text, 0, limit).map_err(|error| {
                            QueryError::Store {
                                reason: format!(
                                    "part {index} of {:?} cannot be cut: {error:?}",
                                    entry.message
                                ),
                            }
                        })?),
                    };
                parts.push(text);
            }
            BodyText::Shown(parts)
        }
    };
    Ok(MessageText {
        hash: entry.message,
        body,
    })
}

impl<S: SurfaceStores> Surface<S> {
    pub(crate) async fn conversation_text_query(
        &self,
        caller: &Caller,
        id: ConversationId,
        window: &TurnWindow,
        limit: TextLimit,
    ) -> Result<Option<ConversationText>, QueryError> {
        require(caller, Permission::Content)?;
        let Some(slice) = self.stores.conversations().turns(id, window).await? else {
            return Ok(None);
        };
        let window = self.window(slice.turns).await?;
        let mut turns = Vec::with_capacity(window.turns.len());
        for turn in &window.turns {
            let (inputs, output) = split(turn);
            turns.push(TurnText {
                index: turn.index,
                inputs: inputs
                    .into_iter()
                    .map(|entry| message_text(&window, entry, limit))
                    .collect::<Result<_, _>>()?,
                output: output
                    .map(|entry| message_text(&window, entry, limit))
                    .transpose()?,
            });
        }
        Ok(Some(ConversationText {
            conversation: id,
            turns,
        }))
    }

    pub(crate) async fn part_text_query(
        &self,
        caller: &Caller,
        part: PartRef,
        slice: TextSlice,
    ) -> Result<Option<PartText>, QueryError> {
        require(caller, Permission::Content)?;
        let Some(bytes) = self.stores.blobs().get(part.message).await? else {
            return Ok(None);
        };
        let body = encoding::decode(&bytes).map_err(|error| QueryError::Store {
            reason: format!("body {:?} does not decode: {error:?}", part.message),
        })?;
        let message = encoding::message(body);
        let text = message
            .part_text(part.index)
            .map_err(|error| QueryError::from(TextError::Part(error)))?;
        Ok(Some(PartText::cut(&text, slice.from, slice.limit)?))
    }
}
