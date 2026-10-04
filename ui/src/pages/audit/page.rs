//! `/audit`: who did what to which subject, and whether it was applied.

use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::{page, query_params};
use topcoat::view::{View, view};

use super::describe::{describe, note, subject};
use super::query::{AuditQuery, RawAuditQuery};
use super::subject::{subject_code, subject_link};
use crate::app::{backend, caller};
use crate::backend::Backend;
use crate::components::form::{BUTTON, FACET, INPUT, LABEL, LINK};
use crate::components::table::{ROW, TD, TD_MUTED};
use crate::components::{
    PageLinks, data_table, empty_state, error_panel, filter_chip, format_time, href, page_header,
    pagination, state_inputs,
};
use crate::contract::errors::QueryError;
use crate::contract::lists::Cursor;
use crate::contract::research::{AuditEntry, AuditOutcome};
use crate::pages::common::action::{require, status_of};
use crate::pages::common::form::invalid;
use crate::pages::common::lookup::{OperatorNames, operator_names};
use crate::pages::common::paging::page_request;
use crate::pages::view::view_state;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

const PATH: &str = "/audit";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRow {
    pub at: String,
    pub actor: String,
    pub what: String,
    pub note: Option<String>,
    /// The subject's label, its page, and this list filtered to it.
    pub subject: Option<(String, Option<String>, String)>,
    /// `Err` holds why the gateway rejected it.
    pub outcome: std::result::Result<(), String>,
}

pub fn audit_row(
    entry: &AuditEntry,
    operators: &OperatorNames,
    query: &AuditQuery,
    state: &ViewState,
) -> AuditRow {
    AuditRow {
        at: format_time(entry.at),
        actor: operators.actor(entry.by),
        what: describe(&entry.action),
        note: note(&entry.action).map(str::to_owned),
        subject: subject(&entry.action).map(|s| {
            let (label, url) = subject_link(s, state);
            (label, url, list_href(state, &query.with_subject(Some(s))))
        }),
        outcome: match &entry.outcome {
            AuditOutcome::Applied => Ok(()),
            AuditOutcome::Rejected(error) => Err(error.to_string()),
        },
    }
}

fn list_href(state: &ViewState, query: &AuditQuery) -> String {
    let pairs = query.pairs();
    let borrowed: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (*k, v.as_str())).collect();
    href(PATH, state, &borrowed)
}

struct Log {
    rows: Vec<AuditRow>,
    operators: Vec<(String, String)>,
    current: Option<Cursor>,
    next: Option<Cursor>,
}

async fn load(
    cx: &Cx,
    caller: &Caller,
    query: &AuditQuery,
    state: &ViewState,
) -> std::result::Result<Log, QueryError> {
    require(caller, Permission::View)?;
    let request = page_request(cx)?;
    let page = backend(cx)
        .audit(caller, &query.filter(state), &request)
        .await?;
    let names = operator_names(cx, caller).await;
    let operators = match backend(cx).operators(caller).await {
        Ok(list) => list.into_iter().map(|o| (o.id.to_ulid(), o.name)).collect(),
        Err(_) => Vec::new(),
    };
    Ok(Log {
        rows: page
            .items
            .iter()
            .map(|e| audit_row(e, &names, query, state))
            .collect(),
        operators,
        current: request.cursor,
        next: page.next,
    })
}

#[page("/audit")]
async fn audit_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx)?;
    let caller = caller(cx);
    let parsed = query_params::<RawAuditQuery>(cx)
        .map_err(|e| invalid("query", e))
        .and_then(AuditQuery::parse);
    let query = parsed.clone().unwrap_or_default();
    let log = match parsed {
        Ok(query) => load(cx, &caller, &query, &state).await,
        Err(error) => Err(error),
    };
    let empty = log.as_ref().is_ok_and(|l| l.rows.is_empty());
    let window_href = list_href(&state, &query.with_all_time(false));
    let all_href = list_href(&state, &query.with_all_time(true));
    let subject_chip = query.subject.map(|s| {
        (
            format!("{} ×", subject_link(s, &state).0),
            list_href(&state, &query.with_subject(None)),
        )
    });
    let operator_chip = query
        .operator
        .map(|_| list_href(&state, &query.with_operator(None)));
    let selected_operator = query.operator.map(|o| o.to_ulid()).unwrap_or_default();
    let subject_value = query.subject.map(subject_code).unwrap_or_default();
    let span_value = if query.all_time { "all" } else { "" };
    let window_text = format!(
        "{} to {}",
        format_time(state.scope.window.start()),
        format_time(state.scope.window.end())
    );
    let pairs = query.pairs();

    Ok(view! {
        page_header(title: "Audit log", subtitle: "Every operator action and config change, with its outcome. Append-only.")
        <div class="mb-3 flex flex-wrap items-center gap-x-5 gap-y-2 text-xs">
            <div class="flex items-center gap-1.5">
                <span class=(FACET)>"When"</span>
                filter_chip(label: &window_text, href: window_href, active: !query.all_time)
                filter_chip(label: "all time", href: all_href, active: query.all_time)
            </div>
            if let Some((label, clear)) = subject_chip {
                <div class="flex items-center gap-1.5">
                    <span class=(FACET)>"Subject"</span>
                    filter_chip(label: &label, href: clear, active: true)
                </div>
            }
            if let Some(clear) = operator_chip {
                <a class=(LINK) href=(clear)>"any operator"</a>
            }
        </div>
        match log {
            Err(error) => {
                (status_of(&error))
                error_panel(error: &error)
            },
            Ok(log) => {
                let links = PageLinks::new(
                    PATH,
                    &state,
                    &pairs.iter().map(|(k, v)| (*k, v.as_str())).collect::<Vec<_>>(),
                    log.current.as_ref(),
                    log.next.as_ref(),
                );
                let operators = log.operators;
                <form method="get" action=(PATH) class="mb-3 flex items-end gap-2">
                    state_inputs(state: &state)
                    if !subject_value.is_empty() {
                        <input type="hidden" name="subject" value=(subject_value.clone())>
                    }
                    if !span_value.is_empty() {
                        <input type="hidden" name="span" value=(span_value)>
                    }
                    <label class="block">
                        <span class=(LABEL)>"Operator"</span>
                        <select name="op" class=(INPUT)>
                            <option value="">"any operator"</option>
                            for (id, name) in operators {
                                let chosen = id == selected_operator;
                                <option value=(id) selected=(chosen)>(name)</option>
                            }
                        </select>
                    </label>
                    <button type="submit" class=(BUTTON)>"Filter"</button>
                </form>
                if empty {
                    empty_state(message: "No audit entries match these filters.")
                } else {
                    data_table(
                        headers: &["When", "Actor", "Action", "Subject", "Outcome"],
                        for row in log.rows {
                            <tr class=(ROW)>
                                <td class=(TD_MUTED)>(row.at)</td>
                                <td class=(TD)>(row.actor)</td>
                                <td class=(TD)>
                                    (row.what)
                                    if let Some(note) = row.note {
                                        <div class="text-xs italic text-zinc-600 dark:text-zinc-400">"\u{201c}" (note) "\u{201d}"</div>
                                    }
                                </td>
                                <td class=(TD)>
                                    match row.subject {
                                        Some((label, page, filter)) => {
                                            match page {
                                                Some(url) => <a class=(LINK) href=(url)>(label)</a>,
                                                None => <span>(label)</span>,
                                            }
                                            <a class="ml-1.5 text-xs text-zinc-400 hover:text-zinc-700 dark:hover:text-zinc-200" href=(filter) title="Only entries about this subject">"filter"</a>
                                        },
                                        None => <span class="text-xs text-zinc-400">"—"</span>,
                                    }
                                </td>
                                <td class=(TD)>
                                    match row.outcome {
                                        Ok(()) => <span class="text-xs text-emerald-700 dark:text-emerald-400">"applied"</span>,
                                        Err(reason) => <span class="text-xs text-red-700 dark:text-red-400">"rejected: " (reason)</span>,
                                    }
                                </td>
                            </tr>
                        }
                    )
                    pagination(links: links)
                }
            },
        }
    })
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::ids::{ChannelId, OperatorId};
    use crosstalk_spec::interfaces::l8_surface::PolicyKind;
    use crosstalk_spec::support::Timestamp;
    use topcoat::router::StatusCode;

    use super::*;
    use crate::components::href::tests::state;
    use crate::contract::AuditId;
    use crate::contract::actions::OperatorAction;
    use crate::contract::errors::ConflictKind;
    use crate::contract::research::{Actor, AuditedAction};
    use crate::testing::get;

    #[test]
    fn rows_link_and_filter_by_subject() {
        let entry = AuditEntry {
            id: AuditId::from_ulid(1),
            at: Timestamp::from_micros(1_790_985_600_000_000),
            by: Actor::Operator(OperatorId::from_ulid(2)),
            action: AuditedAction::Operator(OperatorAction::SetPolicy {
                channel: ChannelId::from_ulid(3),
                policy: PolicyKind::Unsanctioned,
                note: None,
            }),
            outcome: AuditOutcome::Rejected(QueryError::Conflict(ConflictKind::ChannelSuperseded)),
        };
        let names = OperatorNames::new([(OperatorId::from_ulid(2), "ada".to_owned())]);
        let row = audit_row(&entry, &names, &AuditQuery::default(), &state());
        assert_eq!(row.actor, "ada");
        let (label, page, filter) = row.subject.expect("subject");
        assert_eq!(label, "channel …000003");
        assert!(page.is_some_and(|p| p.starts_with("/channels/")));
        assert!(filter.contains("subject=ch.00000000000000000000000003"));
        assert_eq!(
            row.outcome,
            Err("conflict: the channel is superseded".to_owned())
        );
    }

    #[tokio::test]
    async fn audit_page_renders_and_validates_filters() {
        let q = state().to_query();
        let reply = get(&format!("/audit?{q}")).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert!(reply.body.contains("No audit entries match these filters."));
        let reply = get(&format!(
            "/audit?{q}&span=all&subject=ag.01J9ZQ3W8D0000000000000001"
        ))
        .await;
        assert_eq!(reply.status, StatusCode::OK);
        assert!(reply.body.contains("agent …000001 ×"));
        let reply = get(&format!("/audit?{q}&op=nobody")).await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(reply.body.contains("op: expected 26 characters"));
    }
}
