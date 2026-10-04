//! Content retention in the generated world: the message bodies of a few
//! of the oldest confirmed transmissions are gone from the blob store, the
//! sender's for some and the reader's for others, while their spans,
//! matches and byte counts stay. The evidence page shows those sides as
//! "body dropped".

use crosstalk_spec::ids::TransmissionId;

use crate::clock::{DAY, START, plus};

use super::{Blobs, TxRecord, states::confirmed};

/// Which side of a transmission's matches lost its bodies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BodySide {
    /// The sender's reply holding the originated span.
    Sender,
    /// The message the reader's copy arrived in.
    Reader,
}

/// How many transmissions lose a side's bodies.
const DROPPED: usize = 6;

/// Drops the bodies of one side of every match of the oldest confirmed
/// transmissions opened in the first day, alternating sender and reader.
/// Returns what was dropped, oldest first.
pub fn drop_old_bodies(
    transmissions: &[TxRecord],
    blobs: &mut Blobs,
) -> Vec<(TransmissionId, BodySide)> {
    let cutoff = plus(START, DAY);
    let mut dropped = Vec::with_capacity(DROPPED);
    let old = transmissions
        .iter()
        .filter(|record| record.transmission.opened_at < cutoff)
        .filter_map(|record| {
            Some((
                record.transmission.id,
                confirmed(&record.transmission.state)?,
            ))
        })
        .take(DROPPED);
    for (i, (id, confirmation)) in old.enumerate() {
        let side = if i.is_multiple_of(2) {
            BodySide::Sender
        } else {
            BodySide::Reader
        };
        for content in confirmation.content().iter() {
            let message = match side {
                BodySide::Sender => blobs.span(content.origin()).map(|at| at.part.message),
                BodySide::Reader => Some(content.read_at().part.message),
            };
            if let Some(message) = message {
                blobs.drop_body(message);
            }
        }
        dropped.push((id, side));
    }
    dropped
}
