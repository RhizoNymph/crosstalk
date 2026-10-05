//! Lists: the audit log and dead letters. Agents are in [`super::agents`],
//! channels in [`super::channels`], alerts and rules in [`super::alerts`].

use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crosstalk_spec::interfaces::l8_surface::audit::{AuditEntry, AuditFilter};
use crosstalk_spec::paging::{AuditList, DeadLetterList, Page, PageRequest};

use crate::backend::Result;

use super::Ctx;
use super::page::{self, newest_first};

/// `AuditLog::query`: the entries `filter` admits, exactly as
/// `AuditFilter::matches` defines it (authors, the entry's subjects as
/// recorded, the window), newest first by `(at, id)`. The cursor is bound
/// to the filter.
pub fn audit(
    ctx: &Ctx,
    filter: &AuditFilter,
    page: &PageRequest<AuditList>,
) -> Result<Page<AuditEntry, AuditList>> {
    let items = ctx
        .state
        .audit
        .entries()
        .iter()
        .filter(|entry| filter.matches(entry))
        .map(|entry| (newest_first(entry.at, entry.id.as_ulid()), entry.clone()))
        .collect();
    page::paginate("audit", page::digest(filter), items, page)
}

/// `DeadLetterStore::list`: the dead letters of `group`, or of every group,
/// newest envelope first. The cursor is bound to the group.
pub fn dead_letters(
    ctx: &Ctx,
    group: Option<&ConsumerGroup>,
    page: &PageRequest<DeadLetterList>,
) -> Result<Page<DeadLetter, DeadLetterList>> {
    let items = ctx
        .state
        .dead_letters
        .iter()
        .filter(|letter| group.is_none_or(|group| letter.group == *group))
        .map(|letter| {
            (
                newest_first(letter.envelope.at, letter.envelope.id.as_ulid()),
                letter.clone(),
            )
        })
        .collect();
    page::paginate("dead-letters", page::digest(&group), items, page)
}
