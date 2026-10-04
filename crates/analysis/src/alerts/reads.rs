//! `AlertReads` on [`PgAlertStore`]: each list in one snapshot.
//!
//! - **Rules** are few: one statement reads them all, and the page is cut
//!   in Rust in [`rule_list_order`] (built-in rules first in
//!   `BuiltinRule::ALL` order, then user rules newest id first). The cursor
//!   is the last rule served, bound to the filter.
//! - **Alerts**, newest id first: the states filter runs in SQL, in
//!   batches after the keyset; the channel filter (the listed channel, a
//!   channel subject and a transmission subject's stored route, each
//!   resolved through supersession) and `AlertSubject::shown` run in Rust
//!   through [`SubjectFacts`], so a page holds listed alerts only. The
//!   batches share one `REPEATABLE READ` transaction.

use std::cmp::Ordering;

use crosstalk_spec::aggregates::alert::{Alert, AlertRuleDef, AlertSubject, BuiltinRule};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AlertId, AlertRuleId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l6_analysis::Embedder;
use crosstalk_spec::interfaces::l6_analysis::alerts::{AlertReadError, AlertReads};
use crosstalk_spec::interfaces::l8_surface::AlertFilter;
use crosstalk_spec::interfaces::l8_surface::lists::AlertRuleFilter;
use crosstalk_spec::paging::{AlertList, AlertRuleList, Page, PageRequest, PageSize};
use crosstalk_spec::support::NonEmpty;

use super::store::{self, fail, kind_text};
use super::{PgAlertStore, SubjectFacts};
use crate::pg::EventSink;
use crate::pg::codec::{from_json, id_text, to_json};
use crate::pg::cursor;

/// Alerts fetched per round while a page fills.
const ALERT_BATCH: i64 = 128;

/// The order `AlertReads::rules` lists rules in: every built-in rule before
/// every user rule, built-in rules in [`BuiltinRule::ALL`] order, user
/// rules newest id first.
pub fn rule_list_order(a: AlertRuleId, b: AlertRuleId) -> Ordering {
    match (BuiltinRule::from_id(a), BuiltinRule::from_id(b)) {
        (Some(a), Some(b)) => a.cmp(&b),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => b.cmp(&a),
    }
}

/// Cut `items` (already in list order, after the cursor) into a page,
/// issuing a cursor after its last item when more follow.
fn page_of<T, L>(
    items: Vec<T>,
    size: PageSize,
    key: impl Fn(&T) -> Vec<u8>,
    issue: impl Fn(&[u8]) -> Result<crosstalk_spec::paging::Cursor<L>, cursor::CursorTooLong>,
) -> Result<Page<T, L>, AlertReadError> {
    let wanted = usize::from(size.get().get());
    let page = if items.len() > wanted {
        let mut items = items;
        items.truncate(wanted);
        let last = items.last().map(&key).ok_or_else(|| {
            fail::<AlertReadError>(crate::pg::StorageFailure::Invariant(
                "an empty page with more after it".to_owned(),
            ))
        })?;
        let next = issue(&last).map_err(|error| AlertReadError::Store {
            reason: error.to_string(),
        })?;
        let items = NonEmpty::from_vec(items).ok_or_else(|| AlertReadError::Store {
            reason: "an empty page with more after it".to_owned(),
        })?;
        Page::more(size, items, next)
    } else {
        Page::last(size, items)
    };
    page.map_err(|error| AlertReadError::Store {
        reason: format!("page overflow: {error:?}"),
    })
}

impl<E, D, F, S> PgAlertStore<E, D, F, S>
where
    E: Embedder + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Send + Sync,
    F: SubjectFacts,
    S: EventSink,
{
    /// Whether `alert` passes `filter`'s channel and readers show it.
    async fn listed(&self, filter: &AlertFilter, alert: &Alert) -> Result<bool, AlertReadError> {
        if let Some(listed) = filter.channel {
            let listed = ChannelDirectory::canonical(&self.directory, listed);
            let channel = match alert.subject {
                AlertSubject::Channel(channel) => Some(channel),
                AlertSubject::Transmission(transmission) => {
                    match self.facts.route(transmission).await.map_err(fail_facts)? {
                        Some(Route::Channel(channel)) => Some(channel),
                        Some(_) | None => None,
                    }
                }
                AlertSubject::Agent(_) => None,
            };
            let matches = channel.is_some_and(|channel| {
                ChannelDirectory::canonical(&self.directory, channel) == listed
            });
            if !matches {
                return Ok(false);
            }
        }
        self.facts.shown(alert.subject).await.map_err(fail_facts)
    }
}

fn fail_facts(error: super::FactsError) -> AlertReadError {
    AlertReadError::Store {
        reason: error.to_string(),
    }
}

impl<E, D, F, S> AlertReads for PgAlertStore<E, D, F, S>
where
    E: Embedder + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Send + Sync,
    F: SubjectFacts,
    S: EventSink,
{
    async fn rule(&self, id: AlertRuleId) -> Result<Option<AlertRuleDef>, AlertReadError> {
        let mut conn = self.pool.acquire().await.map_err(fail)?;
        Ok(store::rule(&mut conn, id)
            .await
            .map_err(fail)?
            .map(|(rule, _)| rule))
    }

    async fn rules(
        &self,
        filter: &AlertRuleFilter,
        page: &PageRequest<AlertRuleList>,
    ) -> Result<Page<AlertRuleDef, AlertRuleList>, AlertReadError> {
        const LIST: &str = "alert rules";
        let bound = to_json("rule filter", filter).map_err(fail)?;
        let after = match &page.after {
            None => None,
            Some(token) => Some(
                cursor::resume(&self.cursor_key, LIST, bound.as_bytes(), token)
                    .and_then(|bytes| {
                        Some(AlertRuleId::from_ulid(u128::from_be_bytes(
                            bytes.try_into().ok()?,
                        )))
                    })
                    .ok_or(AlertReadError::InvalidCursor)?,
            ),
        };
        let mut conn = self.pool.acquire().await.map_err(fail)?;
        let mut remaining: Vec<AlertRuleDef> = store::rules(&mut conn)
            .await
            .map_err(fail)?
            .into_iter()
            .map(|(rule, _)| rule)
            .filter(|rule| filter.matches(rule))
            .filter(|rule| {
                after.is_none_or(|after| rule_list_order(rule.id(), after) == Ordering::Greater)
            })
            .collect();
        remaining.sort_by(|a, b| rule_list_order(a.id(), b.id()));
        page_of(
            remaining,
            page.size,
            |rule| rule.id().as_ulid().to_be_bytes().to_vec(),
            |position| cursor::issue(&self.cursor_key, LIST, bound.as_bytes(), position),
        )
    }

    async fn alert(&self, id: AlertId) -> Result<Option<Alert>, AlertReadError> {
        let mut conn = self.pool.acquire().await.map_err(fail)?;
        Ok(store::alert(&mut conn, id)
            .await
            .map_err(fail)?
            .map(|(alert, _)| alert))
    }

    async fn alerts(
        &self,
        filter: &AlertFilter,
        page: &PageRequest<AlertList>,
    ) -> Result<Page<Alert, AlertList>, AlertReadError> {
        const LIST: &str = "alerts";
        let bound = to_json("alert filter", filter).map_err(fail)?;
        let mut after: Option<String> = match &page.after {
            None => None,
            Some(token) => Some(
                cursor::resume(&self.cursor_key, LIST, bound.as_bytes(), token)
                    .and_then(|bytes| {
                        Some(AlertId::from_ulid(u128::from_be_bytes(
                            bytes.try_into().ok()?,
                        )))
                    })
                    .map(id_text)
                    .ok_or(AlertReadError::InvalidCursor)?,
            ),
        };
        let states: Option<Vec<&str>> = (!filter.states.is_empty())
            .then(|| filter.states.iter().copied().map(kind_text).collect());
        let wanted = usize::from(page.size.get().get());
        let mut listed: Vec<Alert> = Vec::new();
        let mut tx = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(fail)?;
        loop {
            let rows: Vec<(String, String)> = sqlx::query_as(
                "SELECT id, alert FROM analysis.alerts \
                  WHERE ($1::text IS NULL OR id < $1::text) AND ($2::text[] IS NULL OR state = ANY ($2::text[])) \
                  ORDER BY id DESC LIMIT $3",
            )
            .bind(after.as_deref())
            .bind(states.as_deref())
            .bind(ALERT_BATCH)
            .fetch_all(&mut *tx)
            .await
            .map_err(fail)?;
            let exhausted = i64::try_from(rows.len()).unwrap_or(i64::MAX) < ALERT_BATCH;
            if let Some((last, _)) = rows.last() {
                after = Some(last.clone());
            }
            for (_, json) in rows {
                let alert: Alert = from_json("alert", &json).map_err(fail)?;
                if self.listed(filter, &alert).await? {
                    listed.push(alert);
                }
                if listed.len() > wanted {
                    break;
                }
            }
            if listed.len() > wanted || exhausted {
                break;
            }
        }
        tx.commit().await.map_err(fail)?;
        page_of(
            listed,
            page.size,
            |alert| alert.id.as_ulid().to_be_bytes().to_vec(),
            |position| cursor::issue(&self.cursor_key, LIST, bound.as_bytes(), position),
        )
    }

    async fn rule_version(&self) -> Result<TopicModelVersion, AlertReadError> {
        let mut conn = self.pool.acquire().await.map_err(fail)?;
        Ok(store::rule_state(&mut conn).await.map_err(fail)?.version)
    }
}
