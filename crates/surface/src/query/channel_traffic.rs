//! A channel's transmissions (`QueryApi::channel_transmissions`): the
//! registry's page of the cross-agent transmissions routed through the
//! canonical channel, each as `ChannelTransmission::of` with its current
//! verdict and its topic under the version the first page resolved.
//!
//! ```text
//! View ─▶ canonical = ChannelDirectory::canonical(channel)
//!   first page: version = the selector resolved as a linked view resolves it
//!   later page: (version, registry cursor) = CursorKey::unwrap(cursor, digest), version still retained
//!   ChannelReads::transmissions(canonical, filter, registry page) ── UnknownChannel ─▶ NotFound
//!   rows = ChannelTransmission::of(each, aliases at the read) that the filter keeps
//!   next = CursorKey::wrap(version, the registry's next cursor, digest)
//! ```
//!
//! The registry's cursor binds the canonical channel and the filter; the
//! surface's wraps it with the version, under a MAC over the request's
//! digest (the canonical channel, the filter and the version selector), so
//! a cursor presented with another request is `InvalidCursor`.

use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::derived::flow::channel::confirmation::Confirmation;
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelReads;
use crosstalk_spec::interfaces::l6_analysis::TopicCatalog;
use crosstalk_spec::interfaces::l8_surface::channel_traffic::{
    ChannelTransmission, ChannelTransmissionFilter, ChannelTransmissionPage,
};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, QueryError};
use crosstalk_spec::paging::{ChannelTransmissionList, PageRequest};

use super::content::topic_under;
use super::topics::{resolve_in, still_retained};
use crate::cursor::RequestDigest;
use crate::service::{Surface, page_of, require};
use crate::stores::SurfaceStores;

/// The digest a channel-transmissions cursor is bound to.
fn request_digest(
    channel: ChannelId,
    filter: &ChannelTransmissionFilter,
    selector: TopicVersionSelector,
) -> [u8; 32] {
    let digest = RequestDigest::new("channel_transmissions").u128(channel.as_ulid());
    let digest = match filter.confirmation {
        None => digest.tag(0),
        Some(Confirmation::Unconfirmed) => digest.tag(1),
        Some(Confirmation::Confirmed) => digest.tag(2),
    };
    match selector {
        TopicVersionSelector::Current => digest.tag(0),
        TopicVersionSelector::Pinned(version) => digest.tag(1).u32(version.0),
    }
    .finish()
}

impl<S: SurfaceStores> Surface<S> {
    pub(crate) async fn channel_transmissions_query(
        &self,
        caller: &Caller,
        channel: ChannelId,
        filter: &ChannelTransmissionFilter,
        selector: TopicVersionSelector,
        page: &PageRequest<ChannelTransmissionList>,
    ) -> Result<ChannelTransmissionPage, QueryError> {
        require(caller, Permission::View)?;
        let canonical = ChannelDirectory::canonical(self.stores.channels(), channel);
        let digest = request_digest(canonical, filter, selector);
        let history = self.stores.topics().versions().await?;
        let (version, after) = match &page.after {
            None => (resolve_in(&history, selector)?, None),
            Some(cursor) => {
                let (version, inner) = self
                    .cursors
                    .unwrap(cursor, &digest)
                    .ok_or(QueryError::InvalidCursor)?;
                still_retained(&history, version)?;
                (version, Some(inner))
            }
        };
        let request = PageRequest {
            size: page.size,
            after,
        };
        let listed = self
            .stores
            .channels()
            .transmissions(canonical, filter, &request)
            .await?;
        let (transmissions, next) = listed.into_parts();
        let aliases = self.aliases();
        let mut rows = Vec::with_capacity(transmissions.len());
        for transmission in &transmissions {
            let verdict = self.current_verdict(transmission).await?;
            let topic = topic_under(transmission, version);
            let row = ChannelTransmission::of(transmission, aliases, |_| verdict, |_| topic);
            rows.extend(row.filter(|row| filter.matches(row)));
        }
        let next = match next {
            Some(inner) => Some(self.cursors.wrap(version, &inner, &digest).ok_or_else(|| {
                QueryError::Store {
                    reason: "the registry's cursor is too long to wrap".to_owned(),
                }
            })?),
            None => None,
        };
        Ok(ChannelTransmissionPage {
            channel: canonical,
            topic_version: version,
            page: page_of(page.size, rows, next)?,
        })
    }
}
