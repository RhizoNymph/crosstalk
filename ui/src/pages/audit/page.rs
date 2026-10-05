//! `/audit`: who did what to which subjects, and what came of it. Reading
//! the log needs `Audit`.

use crosstalk_spec::interfaces::l8_surface::audit::{AuditAuthor, AuditEntry, AuditSubject};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::{page, query_params};
use topcoat::view::{View, view};

use super::entry::{EntryView, OutcomeView, entry_view};
use super::query::{AuditQuery, CONFIG, RawAuditQuery, author_code};
use super::subject::{subject_code, subject_link};
use crate::app::{backend, caller};
use crate::components::form::{BUTTON, FACET, INPUT, LABEL, LINK};
use crate::components::table::{ROW, TD, TD_MUTED};
use crate::components::{
    PageLinks, data_table, empty_state, error_panel, filter_chip, format_time, href, page_header,
    pagination, state_inputs,
};
use crate::error::{UiError, permission_name};
use crate::pages::common::action::{require, status_of};
use crate::pages::common::form::invalid;
use crate::pages::common::lookup::OperatorNames;
use crate::pages::common::paging::page_request;
use crate::pages::view::view_state;
use crate::url::view_state::ViewState;
use crosstalk_spec::interfaces::l8_surface::QueryApi;
use crosstalk_spec::paging::{AuditList, Cursor, PageRequest};

const PATH: &str = "/audit";

/// A subject: its label, its page (when it has one), and this log filtered
/// to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectCell {
    pub label: String,
    pub page: Option<String>,
    pub filter: String,
}

/// What came of the entry, ready to render.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutcomeCell {
    /// Applied, with what it created: words and where to see it (the log
    /// filtered to it when it has no page).
    Applied {
        created: Vec<(String, String)>,
    },
    Unchanged,
    Rejected(String),
    Forbidden(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRow {
    pub at: String,
    pub actor: String,
    pub what: String,
    pub note: Option<String>,
    pub subjects: Vec<SubjectCell>,
    pub outcome: OutcomeCell,
}

fn subject_cell(
    subject: AuditSubject,
    operators: &OperatorNames,
    query: &AuditQuery,
    state: &ViewState,
) -> SubjectCell {
    let (label, page) = subject_link(subject, operators, state);
    SubjectCell {
        label,
        page,
        filter: list_href(state, &query.with_subject(Some(subject))),
    }
}

pub fn audit_row(
    entry: &AuditEntry,
    operators: &OperatorNames,
    query: &AuditQuery,
    state: &ViewState,
) -> AuditRow {
    let EntryView {
        at,
        actor,
        what,
        note,
        subjects,
        outcome,
    } = entry_view(entry, operators);
    let outcome = match outcome {
        OutcomeView::Applied { created } => OutcomeCell::Applied {
            created: created
                .into_iter()
                .map(|created| {
                    let cell = subject_cell(created.subject, operators, query, state);
                    (
                        format!("{} {}", created.verb, cell.label),
                        cell.page.unwrap_or(cell.filter),
                    )
                })
                .collect(),
        },
        OutcomeView::Unchanged => OutcomeCell::Unchanged,
        OutcomeView::Rejected(reason) => OutcomeCell::Rejected(reason),
        OutcomeView::Forbidden(missing) => {
            OutcomeCell::Forbidden(format!("needs {}", permission_name(missing)))
        }
    };
    AuditRow {
        at,
        actor,
        what,
        note,
        subjects: subjects
            .into_iter()
            .map(|subject| subject_cell(subject, operators, query, state))
            .collect(),
        outcome,
    }
}

fn list_href(state: &ViewState, query: &AuditQuery) -> String {
    let pairs = query.pairs();
    let borrowed: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (*k, v.as_str())).collect();
    href(PATH, state, &borrowed)
}

struct Log {
    rows: Vec<AuditRow>,
    names: OperatorNames,
    /// The author choices: `op` value and label.
    authors: Vec<(String, String)>,
    current: Option<Cursor<AuditList>>,
    next: Option<Cursor<AuditList>>,
}

async fn load(
    cx: &Cx,
    caller: &Caller,
    query: &AuditQuery,
    state: &ViewState,
) -> std::result::Result<Log, UiError> {
    require(caller, Permission::Audit)?;
    load_page(cx, caller, query, state, page_request(cx)?).await
}

/// One page of the log, its rows named, and the author choices.
async fn load_page(
    cx: &Cx,
    caller: &Caller,
    query: &AuditQuery,
    state: &ViewState,
    request: PageRequest<AuditList>,
) -> std::result::Result<Log, UiError> {
    let page = backend(cx)
        .audit(caller, &query.filter(state), &request)
        .await?;
    let operators = match backend(cx).operators(caller).await {
        Ok(operators) => operators,
        Err(error) => {
            tracing::warn!(error = ?error, "operators unavailable");
            Vec::new()
        }
    };
    let names = OperatorNames::of(&operators);
    let mut authors = vec![(CONFIG.to_owned(), "config".to_owned())];
    authors.extend(operators.iter().map(|operator| {
        let former = if operator.permissions.is_empty() {
            " (former)"
        } else {
            ""
        };
        (
            author_code(AuditAuthor::Operator(operator.id)),
            format!("{}{former}", operator.name.as_str()),
        )
    }));
    Ok(Log {
        rows: page
            .items()
            .iter()
            .map(|e| audit_row(e, &names, query, state))
            .collect(),
        names,
        authors,
        current: request.after,
        next: page.next().cloned(),
    })
}

#[page("/audit")]
async fn audit_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx).await?;
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
    let names = log.as_ref().map(|l| l.names.clone()).unwrap_or_default();
    let subject_chip = query.subject.map(|s| {
        (
            format!("{} ×", subject_link(s, &names, &state).0),
            list_href(&state, &query.with_subject(None)),
        )
    });
    let author_chip = query
        .author
        .map(|_| list_href(&state, &query.with_author(None)));
    let selected_author = query.author.map(author_code).unwrap_or_default();
    let subject_value = query.subject.map(subject_code).unwrap_or_default();
    let span_value = if query.all_time { "all" } else { "" };
    let window_text = format!(
        "{} to {}",
        format_time(state.scope.window.start()),
        format_time(state.scope.window.end())
    );
    let pairs = query.pairs();

    Ok(view! {
        page_header(title: "Audit log", subtitle: "Every operator action, refused ones included, every change config made and every export, with what came of it. Append-only.")
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
            if let Some(clear) = author_chip {
                <a class=(LINK) href=(clear)>"any author"</a>
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
                <form method="get" action=(PATH) class="mb-3 flex items-end gap-2">
                    state_inputs(state: &state)
                    if !subject_value.is_empty() {
                        <input type="hidden" name="subject" value=(subject_value.clone())>
                    }
                    if !span_value.is_empty() {
                        <input type="hidden" name="span" value=(span_value)>
                    }
                    <label class="block">
                        <span class=(LABEL)>"Author"</span>
                        <select name="op" class=(INPUT)>
                            <option value="">"any author"</option>
                            for (code, name) in log.authors {
                                let chosen = code == selected_author;
                                <option value=(code) selected=(chosen)>(name)</option>
                            }
                        </select>
                    </label>
                    <button type="submit" class=(BUTTON)>"Filter"</button>
                </form>
                if empty {
                    empty_state(message: "No audit entries match these filters.")
                } else {
                    data_table(
                        headers: &["When", "Actor", "Action", "Subjects", "Outcome"],
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
                                    if row.subjects.is_empty() {
                                        <span class="text-xs text-zinc-400">"—"</span>
                                    }
                                    for subject in row.subjects {
                                        <div>
                                            match subject.page {
                                                Some(url) => <a class=(LINK) href=(url)>(subject.label)</a>,
                                                None => <span>(subject.label)</span>,
                                            }
                                            <a class="ml-1.5 text-xs text-zinc-400 hover:text-zinc-700 dark:hover:text-zinc-200" href=(subject.filter) title="Only entries about this subject">"filter"</a>
                                        </div>
                                    }
                                </td>
                                <td class=(TD)>
                                    match row.outcome {
                                        OutcomeCell::Applied { created } => {
                                            <span class="text-xs text-emerald-700 dark:text-emerald-400">"applied"</span>
                                            for (label, url) in created {
                                                <div class="text-xs"><a class=(LINK) href=(url)>(label)</a></div>
                                            }
                                        },
                                        OutcomeCell::Unchanged => <span class="text-xs text-zinc-500">"unchanged"</span>,
                                        OutcomeCell::Rejected(reason) => <span class="text-xs text-red-700 dark:text-red-400">"rejected: " (reason)</span>,
                                        OutcomeCell::Forbidden(missing) => <span class="text-xs text-red-700 dark:text-red-400">"forbidden: " (missing)</span>,
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
    use crosstalk_spec::ids::{AlertRuleId, ChannelId, MergeId, OperatorId};
    use crosstalk_spec::interfaces::l8_surface::{
        ActionError, ActionOutcome, ConflictKind, OperatorAction,
    };
    use topcoat::router::StatusCode;

    use super::super::entry::tests::{ADA, names, operator_entry, policy};
    use super::*;
    use crate::components::href::tests::state;
    use crate::testing::{caller_of, cx, get};

    #[test]
    fn rows_link_and_filter_by_subject() {
        let superseded = ActionError::Conflict(ConflictKind::ChannelSuperseded {
            channel: ChannelId::from_ulid(3),
            by: ChannelId::from_ulid(4),
        });
        let entry = operator_entry(&[Permission::Govern], policy(3), Err(superseded));
        let row = audit_row(&entry, &names(), &AuditQuery::default(), &state());
        assert_eq!(row.actor, "ada");
        let [subject] = &row.subjects[..] else {
            panic!("one subject: {:?}", row.subjects);
        };
        assert_eq!(subject.label, "channel …000003");
        assert!(
            subject
                .page
                .as_ref()
                .is_some_and(|p| p.starts_with("/channels/"))
        );
        assert!(
            subject
                .filter
                .contains("subject=ch.00000000000000000000000003")
        );
        assert_eq!(
            row.outcome,
            OutcomeCell::Rejected("the channel is superseded: 00000000000000000000000003 resolves to 00000000000000000000000004; act on that channel instead".to_owned())
        );
        let forbidden = operator_entry(
            &[Permission::View],
            policy(3),
            Err(ActionError::Forbidden {
                missing: Permission::Govern,
            }),
        );
        let row = audit_row(&forbidden, &names(), &AuditQuery::default(), &state());
        assert_eq!(
            row.outcome,
            OutcomeCell::Forbidden("needs Govern".to_owned())
        );
    }

    #[test]
    fn applied_rows_link_what_they_created() {
        let rule = AlertRuleId::from_ulid(7);
        let toggle = OperatorAction::SetRuleEnabled {
            id: rule,
            enabled: false,
        };
        let entry = operator_entry(&[Permission::Govern], toggle, Ok(ActionOutcome::Applied));
        let row = audit_row(
            &entry,
            &OperatorNames::default(),
            &AuditQuery::default(),
            &state(),
        );
        assert_eq!(
            row.outcome,
            OutcomeCell::Applied {
                created: Vec::new()
            }
        );
        assert_eq!(row.subjects[0].label, "rule …000007");
        assert!(
            row.subjects[0]
                .page
                .as_ref()
                .is_some_and(|p| p.starts_with("/alerts/rules/"))
        );

        let request = crosstalk_spec::observed::agent::MergeRequest::new(
            crosstalk_spec::ids::AgentId::from_ulid(1),
            crosstalk_spec::ids::AgentId::from_ulid(2),
            crosstalk_spec::observed::agent::MergeAuthor::Operator(ADA),
        )
        .expect("request");
        let merged = operator_entry(
            &[Permission::Govern],
            OperatorAction::MergeAgents(request),
            Ok(ActionOutcome::Merged(MergeId::from_ulid(9))),
        );
        let row = audit_row(
            &merged,
            &OperatorNames::default(),
            &AuditQuery::default(),
            &state(),
        );
        let labels: Vec<_> = row.subjects.iter().map(|s| s.label.as_str()).collect();
        assert_eq!(labels, ["agent …000001", "agent …000002", "merge …000009"]);
        let OutcomeCell::Applied { created } = row.outcome else {
            panic!("applied");
        };
        let [(label, url)] = &created[..] else {
            panic!("one created: {created:?}");
        };
        assert_eq!(label, "recorded merge …000009");
        assert!(
            url.contains("subject=mg.00000000000000000000000009"),
            "merges link to the log"
        );
    }

    #[tokio::test]
    async fn reading_the_log_needs_audit() {
        let viewer = caller_of(
            OperatorId::from_ulid(1),
            &[Permission::View, Permission::Govern],
        );
        let loaded = load(&cx(), &viewer, &AuditQuery::default(), &state()).await;
        assert!(matches!(
            loaded,
            Err(UiError::Query(
                crosstalk_spec::interfaces::l8_surface::QueryError::Forbidden {
                    missing: Permission::Audit
                }
            ))
        ));
        let auditor = caller_of(
            OperatorId::from_ulid(1),
            &[Permission::View, Permission::Audit],
        );
        let all = AuditQuery::default().with_all_time(true);
        let first = crate::pages::common::paging::first(std::num::NonZeroU32::MIN);
        let loaded = match load_page(&cx(), &auditor, &all, &state(), first).await {
            Ok(loaded) => loaded,
            Err(error) => panic!("the log: {error}"),
        };
        assert!(!loaded.rows.is_empty());
        assert!(loaded.authors.iter().any(|(code, _)| code == CONFIG));
        assert!(loaded.authors.iter().any(|(_, name)| name == "oncall"));
    }

    #[tokio::test]
    async fn audit_page_renders_and_validates_filters() {
        let q = state().to_query();
        let reply = get(&format!("/audit?{q}&span=all")).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert!(!reply.body.contains("No audit entries match these filters."));
        let wiki = crate::testing::channel_id(crate::backend::fixture::ChannelKey::HijackedWiki);
        let reply = get(&format!(
            "/audit?{q}&span=all&subject={}",
            subject_code(AuditSubject::Channel(wiki))
        ))
        .await;
        assert!(
            reply.body.contains("forbidden: needs Govern"),
            "the refused policy change"
        );
        let reply = get(&format!(
            "/audit?{q}&span=all&op=01J9ZQ3W8D00000000000000ZZ"
        ))
        .await;
        assert_eq!(reply.status, StatusCode::OK);
        assert!(reply.body.contains("No audit entries match these filters."));
        let reply = get(&format!("/audit?{q}&span=all&op=config")).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert!(reply.body.contains("switched to authenticated access"));
        assert!(
            reply
                .body
                .contains("defined operator \u{201c}oncall\u{201d}")
        );
        assert!(!reply.body.contains("acknowledged alert"));
        let reply = get(&format!(
            "/audit?{q}&span=all&subject=ag.01J9ZQ3W8D0000000000000001"
        ))
        .await;
        assert_eq!(reply.status, StatusCode::OK);
        assert!(reply.body.contains("agent …000001 ×"));
        let reply = get(&format!("/audit?{q}&span=all&subject=tv.1")).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert!(reply.body.contains("topic model v1 ×"));
        let reply = get(&format!("/audit?{q}&op=nobody")).await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(reply.body.contains("op: expected 26 characters"));
    }
}
