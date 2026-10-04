//! Content retention in the generated world: the message bodies of a few
//! of the oldest confirmed transmissions are gone, the sender's for some
//! and the reader's for others, while their spans, matches and byte counts
//! stay. The evidence page shows those sides as "body dropped".
//!
//! The spec's `BlobStore` has no delete, so a dropped body is one the seed
//! never stores: `BlobStore::get` answers `None` for it, which the spec
//! defines as dropped by retention.

use crosstalk_spec::ids::TransmissionId;

use crate::clock::{DAY, plus};
use crate::scenario::BodySide;

use super::bodies::Blobs;
use super::states::TxRecord;
use super::times::Times;

/// How many transmissions lose a side's bodies.
const DROPPED: usize = 6;

/// Drops the bodies of one side of every match of the oldest confirmed
/// transmissions opened in the first day, alternating sender and reader.
/// Returns what was dropped, oldest first.
pub fn drop_old_bodies(
    times: &Times,
    transmissions: &[TxRecord],
    blobs: &mut Blobs,
) -> Vec<(TransmissionId, BodySide)> {
    let cutoff = plus(times.start, DAY);
    let mut dropped = Vec::with_capacity(DROPPED);
    let old = transmissions
        .iter()
        .filter(|record| record.transmission.opened_at < cutoff)
        .filter_map(|record| Some((record.id(), record.confirmed()?)))
        .take(DROPPED);
    for (i, (id, confirmation)) in old.enumerate() {
        let side = if i % 2 == 0 {
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
