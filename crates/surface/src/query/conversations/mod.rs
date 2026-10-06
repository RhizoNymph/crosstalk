//! The conversation reads ([`crosstalk_spec::interfaces::l8_surface::conversation`]):
//! the list, one conversation's head, a window of turns ([`turns`]), their
//! text ([`text`]), a span's readers, and where exchanges and spans sit.
//!
//! Every read checks its permission first, then reads L3's
//! `ConversationReads`, L1's `ExchangeReads`, the blob store, L4's
//! `ProvenanceReads` and L5's `TransmissionStore::holding`, and resolves
//! every agent through `AgentDirectory` at the read
//! (`surface.conversation.merge-split`).

mod marks;
mod text;
mod turns;

use crosstalk_spec::interfaces::l8_surface::conversation::ExchangePlacement;
use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::aliases::Aliases;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::transmission::{DelegationDirection, Route};
use crosstalk_spec::ids::{ConversationId, ExchangeId, SpanId, TransmissionId};
use crosstalk_spec::interfaces::l1_canonical::exchanges::ExchangeReads;
use crosstalk_spec::interfaces::l3_reconstruction::agents::AgentReads;
use crosstalk_spec::interfaces::l3_reconstruction::conversations::{
    ConversationQuery, ConversationReads, StoredConversation, StoredTurn, TurnIndex, TurnWindow,
};
use crosstalk_spec::interfaces::l4_provenance::reads::ProvenanceReads;
use crosstalk_spec::interfaces::l5_flow::transmissions::{MatchKey, TransmissionStore};
use crosstalk_spec::interfaces::l8_surface::conversation::turn::{
    OriginatedStatus, Reader, TurnPage,
};
use crosstalk_spec::interfaces::l8_surface::conversation::{
    ConversationFilter, ConversationHead, ConversationRow, ConversationTraffic, DelegationLink,
    OriginLink, SpanPoint, Successor, SuccessorKind, TurnPoint,
};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, QueryError};
use crosstalk_spec::observed::agent::ClaimSet;
use crosstalk_spec::observed::conversation::ConversationOrigin;
use crosstalk_spec::paging::{ConversationList, Page, PageRequest, PageSize, SpanReaderList};

use crate::service::{Surface, largest_page, page_of, require};
use crate::stores::SurfaceStores;

use marks::missing;

impl<S: SurfaceStores> Surface<S> {
    /// Every turn of `id`, window by window of the largest page.
    async fn every_turn(&self, id: ConversationId) -> Result<Vec<StoredTurn>, QueryError> {
        let size = largest_page()?;
        let mut turns = Vec::new();
        loop {
            let from = u32::try_from(turns.len()).map_err(|_| QueryError::Store {
                reason: "more turns than a turn index".to_owned(),
            })?;
            let window = TurnWindow {
                from: TurnIndex(from),
                size,
            };
            let Some(slice) = self.stores.conversations().turns(id, &window).await? else {
                return Ok(turns);
            };
            let read = slice.turns.len();
            turns.extend(slice.turns);
            if read == 0 || turns.len() >= slice.total as usize {
                return Ok(turns);
            }
        }
    }

    /// The conversation stored under `id`; a fault when a link names one
    /// L3 does not keep.
    async fn linked(&self, id: ConversationId) -> Result<StoredConversation, QueryError> {
        self.stores
            .conversations()
            .conversation(id)
            .await?
            .ok_or_else(|| {
                missing(format!(
                    "conversation {} is linked but not stored",
                    id.ulid_text()
                ))
            })
    }

    /// `stored` as a list row: agents canonical now, origin links resolved
    /// (`surface.conversation.origin-resolved`).
    async fn row_of(&self, stored: StoredConversation) -> Result<ConversationRow, QueryError> {
        let aliases = self.aliases();
        let conversation = &stored.conversation;
        let origin = match conversation.origin {
            ConversationOrigin::Root => OriginLink::Root,
            ConversationOrigin::Fork {
                parent,
                shared_prefix,
            } => OriginLink::Fork {
                parent,
                parent_agent: aliases.agent(self.linked(parent).await?.conversation.agent),
                shared_prefix,
                branch_turn: self
                    .stores
                    .conversations()
                    .branch_turn(parent, shared_prefix)
                    .await?,
            },
            ConversationOrigin::Compaction { predecessor } => {
                let first = TurnWindow {
                    from: TurnIndex(0),
                    size: PageSize::new(1).map_err(|error| QueryError::Store {
                        reason: format!("a one-turn window refused: {error:?}"),
                    })?,
                };
                let carried_over = match self
                    .stores
                    .conversations()
                    .turns(conversation.id, &first)
                    .await?
                {
                    Some(slice) => slice.turns.first().map_or(0, |turn| {
                        turn.entries
                            .iter()
                            .filter(|entry| entry.carried_over)
                            .count()
                    }),
                    None => 0,
                };
                OriginLink::Compaction {
                    predecessor,
                    predecessor_agent: aliases
                        .agent(self.linked(predecessor).await?.conversation.agent),
                    carried_over: u32::try_from(carried_over).unwrap_or(u32::MAX),
                }
            }
        };
        Ok(ConversationRow {
            id: conversation.id,
            agent: aliases.agent(conversation.agent),
            origin,
            started_at: stored.started_at,
            last_turn_at: stored.last_turn_at,
            turns: stored.turns,
            source: stored.source,
        })
    }

    pub(crate) async fn conversations_query(
        &self,
        caller: &Caller,
        filter: &ConversationFilter,
        page: &PageRequest<ConversationList>,
    ) -> Result<Page<ConversationRow, ConversationList>, QueryError> {
        require(caller, Permission::View)?;
        let agents = match filter.agent {
            None => None,
            Some(agent) => match self.stores.agents().cluster(agent).await? {
                None => return page_of(page.size, Vec::new(), None),
                Some(cluster) => {
                    let mut members: BTreeSet<_> = cluster.alias_ids().iter().copied().collect();
                    members.insert(cluster.agent().id);
                    Some(members)
                }
            },
        };
        let query = ConversationQuery {
            agents,
            origins: filter.origins.iter().copied().collect(),
            replay: filter.replay.clone(),
        };
        let listed = self.stores.conversations().list(&query, page).await?;
        let (stored, next) = listed.into_parts();
        let mut rows = Vec::with_capacity(stored.len());
        for conversation in stored {
            rows.push(self.row_of(conversation).await?);
        }
        page_of(page.size, rows, next)
    }

    pub(crate) async fn conversation_query(
        &self,
        caller: &Caller,
        id: ConversationId,
    ) -> Result<Option<ConversationHead>, QueryError> {
        require(caller, Permission::View)?;
        let Some(stored) = self.stores.conversations().conversation(id).await? else {
            return Ok(None);
        };
        let row = self.row_of(stored).await?;
        let aliases = self.aliases();
        let mut successors = Vec::new();
        for successor in self.stores.conversations().successors(id).await? {
            let kind = match successor.conversation.origin {
                ConversationOrigin::Fork { shared_prefix, .. } => SuccessorKind::Fork {
                    shared_prefix,
                    branch_turn: self
                        .stores
                        .conversations()
                        .branch_turn(id, shared_prefix)
                        .await?,
                },
                ConversationOrigin::Compaction { .. } | ConversationOrigin::Root => {
                    SuccessorKind::Compaction
                }
            };
            successors.push(Successor {
                conversation: successor.conversation.id,
                agent: aliases.agent(successor.conversation.agent),
                kind,
                started_at: successor.started_at,
            });
        }
        let turns = self.every_turn(id).await?;
        let claims = self.claims_of(&turns).await?;
        let (traffic, delegated_from) = self.traffic_of(id, &turns).await?;
        Ok(Some(ConversationHead {
            row,
            traffic,
            successors,
            delegated_from,
            claims,
        }))
    }

    /// The harness claims on `turns`, each at the latest turn start it was
    /// seen at (`surface.conversation.claims-only`).
    async fn claims_of(&self, turns: &[StoredTurn]) -> Result<ClaimSet, QueryError> {
        let mut claims = ClaimSet::default();
        let ids: Vec<ExchangeId> = turns.iter().map(|turn| turn.exchange).collect();
        for chunk in ids.chunks(IdBatch::<ExchangeId>::MAX) {
            let batch = IdBatch::new(chunk.iter().copied())?;
            for stored in self.stores.exchanges().exchanges(&batch).await?.values() {
                let meta = &stored.exchange.meta;
                if let Some(claim) = &meta.client.harness {
                    claims.observe(claim.clone(), meta.started_at);
                }
            }
        }
        Ok(claims)
    }

    /// The distinct transmissions holding a match read in, or originated
    /// from, `turns` (`surface.conversation.traffic-counts`), and the
    /// delegation that started the conversation
    /// (`surface.conversation.delegated-from`).
    async fn traffic_of(
        &self,
        id: ConversationId,
        turns: &[StoredTurn],
    ) -> Result<(ConversationTraffic, Option<DelegationLink>), QueryError> {
        let ids: Vec<ExchangeId> = turns.iter().map(|turn| turn.exchange).collect();
        let size = largest_page()?;
        let mut read = BTreeMap::new();
        let mut originated: Vec<SpanId> = Vec::new();
        for chunk in ids.chunks(IdBatch::<ExchangeId>::MAX) {
            let batch = IdBatch::new(chunk.iter().copied())?;
            let provenance = self.stores.provenance();
            read.extend(provenance.matches_read_in(&batch).await?);
            for spans in provenance.output_spans(&batch).await?.into_values() {
                originated.extend(
                    spans
                        .into_iter()
                        .filter(|stored| {
                            stored.forward.is_some()
                                || OriginatedStatus::of(&stored.span.state).is_some()
                        })
                        .map(|stored| stored.span.id),
                );
            }
        }
        let mut sent_keys = BTreeSet::new();
        for span in originated {
            let mut page = PageRequest { size, after: None };
            while let Some(readers) = self.reader_page(span, &page).await? {
                let (matches, next) = readers.page.into_parts();
                sent_keys.extend(matches.iter().map(MatchKey::of));
                match next {
                    Some(cursor) => page.after = Some(cursor),
                    None => break,
                }
            }
        }
        let received_keys: BTreeSet<MatchKey> = read.values().flatten().map(MatchKey::of).collect();
        let received = self.holders(&received_keys).await?;
        let sent = self.holders(&sent_keys).await?;
        let count = |held: &BTreeMap<MatchKey, TransmissionId>| {
            let distinct: BTreeSet<TransmissionId> = held.values().copied().collect();
            u32::try_from(distinct.len()).unwrap_or(u32::MAX)
        };
        let traffic = ConversationTraffic {
            received: count(&received),
            sent: count(&sent),
        };
        let mut routes: BTreeMap<TransmissionId, Route> = BTreeMap::new();
        for turn in turns {
            for content in read.get(&turn.exchange).map(Vec::as_slice).unwrap_or(&[]) {
                let Some(transmission) = received.get(&MatchKey::of(content)) else {
                    continue;
                };
                let route = match routes.get(transmission) {
                    Some(route) => route.clone(),
                    None => {
                        let stored = self
                            .stores
                            .transmissions()
                            .transmission(*transmission)
                            .await?
                            .ok_or_else(|| {
                                missing(format!(
                                    "transmission {} holds a match but is not stored",
                                    transmission.ulid_text()
                                ))
                            })?;
                        routes.insert(*transmission, stored.route.clone());
                        stored.route
                    }
                };
                if route == Route::Delegation(DelegationDirection::ParentToChild) {
                    let parent = self
                        .span_points_of(&BTreeSet::from([content.origin()]))
                        .await?
                        .remove(&content.origin())
                        .ok_or_else(|| {
                            missing(format!(
                                "span {} has no record",
                                content.origin().ulid_text()
                            ))
                        })?;
                    return Ok((
                        traffic,
                        Some(DelegationLink {
                            transmission: *transmission,
                            parent,
                            child: TurnPoint {
                                conversation: id,
                                turn: turn.index,
                            },
                        }),
                    ));
                }
            }
        }
        Ok((traffic, None))
    }

    /// The transmission holding each of `keys` that one holds.
    async fn holders(
        &self,
        keys: &BTreeSet<MatchKey>,
    ) -> Result<BTreeMap<MatchKey, TransmissionId>, QueryError> {
        let keys: Vec<MatchKey> = keys.iter().copied().collect();
        let mut held = BTreeMap::new();
        for chunk in keys.chunks(IdBatch::<ExchangeId>::MAX) {
            let chunk: BTreeSet<MatchKey> = chunk.iter().copied().collect();
            held.extend(self.stores.transmissions().holding(&chunk).await?);
        }
        Ok(held)
    }

    pub(crate) async fn conversation_turns_query(
        &self,
        caller: &Caller,
        id: ConversationId,
        window: &TurnWindow,
    ) -> Result<Option<TurnPage>, QueryError> {
        require(caller, Permission::View)?;
        let Some(slice) = self.stores.conversations().turns(id, window).await? else {
            return Ok(None);
        };
        let total = slice.total;
        let window = self.window(slice.turns).await?;
        let turns = self.turns_of(id, &window).await?;
        Ok(Some(TurnPage {
            conversation: id,
            total,
            turns,
        }))
    }

    pub(crate) async fn span_readers_query(
        &self,
        caller: &Caller,
        span: SpanId,
        page: &PageRequest<SpanReaderList>,
    ) -> Result<Option<Page<Reader, SpanReaderList>>, QueryError> {
        require(caller, Permission::View)?;
        let Some(read) = self.reader_page(span, page).await? else {
            return Ok(None);
        };
        let (matches, next) = read.page.into_parts();
        let pointed: Vec<_> = matches.iter().collect();
        let pointers = self.pointers(&pointed, BTreeSet::new()).await?;
        let aliases = self.aliases();
        let readers = matches
            .iter()
            .map(|content| pointers.reader(content, aliases))
            .collect();
        Ok(Some(page_of(page.size, readers, next)?))
    }

    pub(crate) async fn exchange_turns_query(
        &self,
        caller: &Caller,
        ids: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, ExchangePlacement>, QueryError> {
        require(caller, Permission::View)?;
        let aliases = self.aliases();
        Ok(self
            .stores
            .conversations()
            .locate(ids)
            .await?
            .into_iter()
            .map(|(exchange, placement)| {
                (
                    exchange,
                    ExchangePlacement {
                        agent: aliases.agent(placement.agent),
                        ..placement
                    },
                )
            })
            .collect())
    }

    pub(crate) async fn span_points_query(
        &self,
        caller: &Caller,
        ids: &IdBatch<SpanId>,
    ) -> Result<BTreeMap<SpanId, SpanPoint>, QueryError> {
        require(caller, Permission::View)?;
        self.span_points_of(&ids.ids().iter().copied().collect())
            .await
    }
}
