//! One transmission's stored record, the text behind it and its verdicts.
//!
//! The evidence is assembled as the surface assembles it
//! ([`TransmissionEvidence::assemble`]): for each content match the origin
//! span's location from the span records and the read location from the
//! match, each cut from the body the blob store holds ([`Excerpted::of`]),
//! a body retention dropped being [`Excerpted::BodyDropped`]; for each
//! access the co-access records name, the access and its resource with the
//! access's canonical agent. A record the transmission names that is not
//! stored is a `Store` error.

use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::derived::flow::verdict::VerdictLog;
use crosstalk_spec::derived::provenance::matching::ContentMatch;
use crosstalk_spec::ids::{AccessId, TransmissionId};
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::interfaces::l8_surface::evidence::{
    AccessDetail, EvidenceError, EvidenceRecord, MatchQuotes, TransmissionEvidence,
};
use crosstalk_spec::interfaces::l8_surface::excerpt::{ExcerptWindow, Excerpted};

use crate::Result;

use super::Ctx;

/// The stored record, ids as stored.
pub fn transmission(ctx: &Ctx, id: TransmissionId) -> Option<Transmission> {
    ctx.world.tx(id).map(|record| record.transmission.clone())
}

/// Both excerpts of one match, cut with `window`.
fn quote(
    ctx: &Ctx,
    content: &ContentMatch,
    window: ExcerptWindow,
) -> std::result::Result<MatchQuotes, EvidenceError> {
    let blobs = &ctx.world.blobs;
    let span = content.origin();
    let missing = EvidenceError::Missing(EvidenceRecord::Span(span));
    let origin_at = blobs.span(span).ok_or(missing)?;
    let read_at = content.read_at();
    let origin = Excerpted::of(
        origin_at,
        blobs.body(origin_at.part.message).as_ref(),
        window,
    )?;
    let read = Excerpted::of(read_at, blobs.body(read_at.part.message).as_ref(), window)?;
    Ok(MatchQuotes { origin, read })
}

/// The access, its resource and its canonical agent.
fn detail(ctx: &Ctx, id: AccessId) -> std::result::Result<AccessDetail, EvidenceError> {
    let access = ctx
        .world
        .access(id)
        .ok_or(EvidenceError::Missing(EvidenceRecord::Access(id)))?;
    let resource = ctx
        .world
        .resource(access.resource)
        .ok_or(EvidenceError::Missing(EvidenceRecord::Resource(
            access.resource,
        )))?;
    Ok(AccessDetail::new(
        access.clone(),
        resource.clone(),
        ctx.aliases(),
    )?)
}

/// The evidence behind `id`; `None` for an unknown id.
pub fn evidence(
    ctx: &Ctx,
    id: TransmissionId,
    window: ExcerptWindow,
) -> Result<Option<TransmissionEvidence>> {
    let Some(record) = ctx.world.tx(id) else {
        return Ok(None);
    };
    TransmissionEvidence::assemble(
        record.transmission.clone(),
        |content| quote(ctx, content, window),
        |access| detail(ctx, access),
    )
    .map(Some)
    .map_err(QueryError::from)
}

/// Every verdict record of `id`, oldest first: an empty log for one never
/// judged, `None` for an unknown id.
pub fn verdicts(ctx: &Ctx, id: TransmissionId) -> Option<VerdictLog> {
    ctx.world.tx(id)?;
    Some(
        ctx.state
            .verdicts
            .get(&id)
            .cloned()
            .unwrap_or_else(|| VerdictLog::new(id)),
    )
}
