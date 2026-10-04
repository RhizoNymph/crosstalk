//! `AgentReads` on Postgres: the agents list, one agent's cluster and batch
//! names, each from one `REPEATABLE READ` snapshot of every L3 agent table.
//!
//! The list's cursors are `<last agent id>_<tag>`, where the tag is a keyed
//! BLAKE3 over the store's cursor key, the filter's JSON and the id: a
//! cursor this store did not issue, or issued for another filter, fails
//! the tag check and is `InvalidCursor`.

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::agents::filter::AgentFilter;
use crosstalk_spec::aggregates::agents::{AgentCluster, AgentName, AgentProfile};
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::ids::{AgentId, MergeId};
use crosstalk_spec::interfaces::l3_reconstruction::agents::{AgentReadError, AgentReads};
use crosstalk_spec::paging::{AgentList, Cursor, Page, PageRequest};
use crosstalk_spec::support::{Blake3, NonEmpty};

use super::codec::{id_of, id_text, json};
use super::table::{ReadModelError, Table};
use super::{PgAgents, load};
use crate::error::{StorageFailure, StoreReason};
use crate::ids::IdSource;
use crate::publish::EventSink;

fn read_model(error: ReadModelError) -> AgentReadError {
    AgentReadError::Store {
        reason: format!("agent table invariant broken: {error:?}"),
    }
}

/// The tag binding a cursor to the store's key, a filter and a position.
fn tag(key: &[u8; 32], filter: &str, last: AgentId) -> String {
    let mut bytes = key.to_vec();
    bytes.extend_from_slice(filter.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(id_text(last).as_bytes());
    Blake3::of(&bytes).to_hex()[..32].to_owned()
}

impl<S, M> PgAgents<S, M>
where
    S: EventSink,
    M: IdSource<MergeId> + 'static,
{
    /// Every table, read in one snapshot.
    async fn snapshot(&self) -> Result<Table, StorageFailure> {
        // One statement: one snapshot of every table.
        let read = async {
            let mut conn = self.pool.acquire().await?;
            load::full_table(&mut conn).await
        };
        read.await.map_err(StorageFailure::from)
    }

    fn issue(&self, filter: &str, last: AgentId) -> Result<Cursor<AgentList>, AgentReadError> {
        let token = format!("{}_{}", id_text(last), tag(&self.cursor_key, filter, last));
        Cursor::from_token(token).map_err(|error| AgentReadError::Store {
            reason: format!("cursor not issued: {error:?}"),
        })
    }

    fn resume(&self, cursor: &Cursor<AgentList>, filter: &str) -> Result<AgentId, AgentReadError> {
        let (id, given) = cursor
            .token()
            .split_once('_')
            .ok_or(AgentReadError::InvalidCursor)?;
        let last: AgentId =
            id_of("cursor", id).map_err(|_: super::codec::CodecError| AgentReadError::InvalidCursor)?;
        if tag(&self.cursor_key, filter, last) == given {
            Ok(last)
        } else {
            Err(AgentReadError::InvalidCursor)
        }
    }
}

impl<S, M> AgentReads for PgAgents<S, M>
where
    S: EventSink,
    M: IdSource<MergeId> + 'static,
{
    async fn list(
        &self,
        filter: &AgentFilter,
        page: &PageRequest<AgentList>,
    ) -> Result<Page<AgentProfile, AgentList>, AgentReadError> {
        let binding = json(filter).map_err(|error| {
            AgentReadError::from_failure(&StorageFailure::Codec(error))
        })?;
        let after = page
            .after
            .as_ref()
            .map(|cursor| self.resume(cursor, &binding))
            .transpose()?;
        let table = self
            .snapshot()
            .await
            .map_err(|failure| AgentReadError::from_failure(&failure))?;
        let mut profiles = Vec::new();
        for agent in table.canonical_agents() {
            if after.is_some_and(|after| agent.id >= after) {
                continue;
            }
            let profile = table.profile(agent).map_err(read_model)?;
            if filter.matches(&profile, |id| table.canonical(id)) {
                profiles.push(profile);
            }
        }
        let size = page.size;
        let limit = usize::from(size.get().get());
        let overflow = |_| AgentReadError::Store {
            reason: "page larger than its size".to_owned(),
        };
        if profiles.len() <= limit {
            return Page::last(size, profiles).map_err(overflow);
        }
        profiles.truncate(limit);
        let items = NonEmpty::from_vec(profiles).ok_or(AgentReadError::Store {
            reason: "empty page with more to follow".to_owned(),
        })?;
        let last = items
            .iter()
            .last()
            .map(AgentProfile::id)
            .ok_or(AgentReadError::Store {
                reason: "empty page with more to follow".to_owned(),
            })?;
        let next = self.issue(&binding, last)?;
        Page::more(size, items, next).map_err(overflow)
    }

    async fn cluster(&self, id: AgentId) -> Result<Option<AgentCluster>, AgentReadError> {
        let table = self
            .snapshot()
            .await
            .map_err(|failure| AgentReadError::from_failure(&failure))?;
        table.cluster(id).map_err(read_model)
    }

    async fn names(
        &self,
        ids: &IdBatch<AgentId>,
    ) -> Result<BTreeMap<AgentId, AgentName>, AgentReadError> {
        let table = self
            .snapshot()
            .await
            .map_err(|failure| AgentReadError::from_failure(&failure))?;
        Ok(table.names(ids.ids()))
    }
}
