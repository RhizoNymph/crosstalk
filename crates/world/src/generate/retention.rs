//! Content retention in the generated world: the message bodies of a few
//! of the oldest confirmed transmissions are gone, the sender's for some
//! and the reader's for others, while their spans, matches and byte counts
//! stay. The evidence page shows those sides as "body dropped".
//!
//! The spec's `BlobStore` has no delete, so a dropped body is one the seed
//! never stores: `BlobStore::get` answers `None` for it, which the spec
//! defines as dropped by retention.

use std::collections::BTreeMap;

use crosstalk_spec::ids::{MessageHash, TransmissionId};

use crate::clock::{DAY, plus};
use crate::scenario::BodySide;

use super::bodies::Blobs;
use super::states::TxRecord;
use super::times::Times;

/// How many transmissions lose a side's bodies.
const DROPPED: usize = 6;

/// Drops the bodies of one side of every match of the oldest confirmed
/// transmissions opened in the first day, alternating sender and reader.
/// Returns what was dropped, oldest first; each dropped body's bytes go to
/// `captured` (the wire traffic carried them before retention ran).
pub fn drop_old_bodies(
    times: &Times,
    transmissions: &[TxRecord],
    blobs: &mut Blobs,
    captured: &mut BTreeMap<MessageHash, Vec<u8>>,
) -> Vec<(TransmissionId, BodySide)> {
    let mut dropped = Vec::with_capacity(DROPPED);
    for (record, side) in chosen(times, transmissions) {
        let Some(confirmation) = record.confirmed() else {
            continue;
        };
        let id = record.id();
        for content in confirmation.content().iter() {
            let message = match side {
                BodySide::Sender => blobs.span(content.origin()).map(|at| at.part.message),
                BodySide::Reader => Some(content.read_at().part.message),
            };
            if let Some(message) = message
                && let Some(body) = blobs.take_body(message)
            {
                captured.insert(message, body.bytes);
            }
        }
        dropped.push((id, side));
    }
    dropped
}

/// The transmissions retention drops a side of, oldest first: the oldest
/// confirmed ones opened in the first day, alternating sender and reader.
pub fn chosen<'a>(times: &Times, transmissions: &'a [TxRecord]) -> Vec<(&'a TxRecord, BodySide)> {
    let cutoff = plus(times.start, DAY);
    transmissions
        .iter()
        .filter(|record| record.transmission.opened_at < cutoff)
        .filter(|record| record.confirmed().is_some())
        .take(DROPPED)
        .enumerate()
        .map(|(i, record)| {
            let side = if i % 2 == 0 {
                BodySide::Sender
            } else {
                BodySide::Reader
            };
            (record, side)
        })
        .collect()
}
