//! `/alerts/{id}`: one alert: its rule, subject, state and the audit log of
//! its triage. Posts acknowledge or resolve it and return here.

use crosstalk_spec::ids::AlertId;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::content::Form;
use topcoat::router::error::not_found;
use topcoat::router::{StatusCode, page, path_param};
use topcoat::view::{View, component, view};

use super::inbox::parse_action;
use super::model::AlertRow;
use super::rules::PATH as RULES_PATH;
use crate::app::{backend, caller, can};
use crate::backend::Backend;
use crate::components::form::{BUTTON, INPUT, LABEL, LINK, PANEL, SECTION, SECTION_TITLE};
use crate::components::table::{ROW, TD, TD_MUTED};
use crate::components::{
    data_table, empty_state, error_panel, flash_banner, href, kind_badge, page_header,
};
use crate::contract::errors::QueryError;
use crate::contract::lists::PageRequest;
use crate::contract::research::{AuditFilter, AuditSubject};
use crate::contract::rules::{RuleDef, RuleKind};
use crate::pages::channels::sections::{HistoryRow, history_rows};
use crate::pages::common::action::{Failure, done, perform, require, status_of};
use crate::pages::common::flash::{Flash, flash};
use crate::pages::common::form::FormFields;
use crate::pages::common::links::rule_url;
use crate::pages::common::lookup::{RuleNames, operator_names};
use crate::pages::view::view_state;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

path_param!(alert_ulid);

/// Audit entries shown in the triage history.
const HISTORY: std::num::NonZeroU32 = match std::num::NonZeroU32::new(50) {
    Some(n) => n,
    None => std::num::NonZeroU32::MIN,
};

pub fn alert_path(id: AlertId) -> String {
    format!("/alerts/{}", id.to_ulid())
}

fn alert_id(cx: &Cx) -> Result<AlertId> {
    AlertId::parse_ulid(path_param::<AlertUlid>(cx)).map_err(|_| not_found().into())
}

/// Where the alert's rule is shown: its edit page for an operator rule, the
/// rules list for a built-in one (built-ins have no page of their own).
pub fn rule_link(rule: Option<&RuleDef>, state: &ViewState) -> String {
    match rule {
        Some(def) if matches!(def.rule, RuleKind::User(_)) => rule_url(def.id, state),
        _ => href(RULES_PATH, state, &[]),
    }
}

struct Loaded {
    row: AlertRow,
    rule_url: String,
    history: std::result::Result<Vec<HistoryRow>, QueryError>,
}

async fn load(
    cx: &Cx,
    caller: &Caller,
    id: AlertId,
    state: &ViewState,
) -> std::result::Result<Option<Loaded>, QueryError> {
    require(caller, Permission::View)?;
    let backend = backend(cx);
    let Some(alert) = backend.alert(caller, id).await? else {
        return Ok(None);
    };
    let rules = backend.rules(caller).await?;
    let operators = operator_names(cx, caller).await;
    let names = RuleNames::new(rules.iter().map(|r| (r.id, r.name.as_str().to_owned())));
    let filter = AuditFilter {
        subject: Some(AuditSubject::Alert(id)),
        ..AuditFilter::default()
    };
    let history = backend
        .audit(caller, &filter, &PageRequest::first(HISTORY))
        .await
        .map(|page| history_rows(&page.items, &operators));
    Ok(Some(Loaded {
        row: AlertRow::new(&alert, &names, &operators, state),
        rule_url: rule_link(rules.iter().find(|r| r.id == alert.rule), state),
        history,
    }))
}

#[page("/alerts/{alert_ulid}")]
async fn alert_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx).await?;
    let id = alert_id(cx)?;
    let flash = flash(cx);
    Ok(view! { alert_page(id: id, state: state, flash: flash, failure: None) })
}

#[page(POST "/alerts/{alert_ulid}")]
async fn alert_post(cx: &Cx, Form(fields): Form<FormFields>) -> Result<impl View> {
    let state = view_state(cx).await?;
    let id = alert_id(cx)?;
    let error = match parse_action(id, &fields) {
        Ok((action, flash)) => match perform(cx, action).await {
            Ok(_) => return Err(done(&alert_path(id), &state, &[], flash)),
            Err(error) => error,
        },
        Err(error) => error,
    };
    let failure: Failure<()> = Failure::new(None, error, fields);
    Ok(view! { alert_page(id: id, state: state, flash: None, failure: Some(failure)) })
}

#[component]
async fn alert_page(
    cx: &Cx,
    id: AlertId,
    state: ViewState,
    flash: Option<Flash>,
    failure: Option<Failure<()>>,
) -> Result<impl View> {
    let caller = caller(cx);
    let triage = can(&caller, Permission::Triage);
    let loaded = load(cx, &caller, id, &state).await;
    let failed = failure.map(|f| (f.status(), f.error));
    let action_url = href(&alert_path(id), &state, &[]);
    let inbox_url = href("/alerts", &state, &[]);
    let audit_url = href(
        "/audit",
        &state,
        &[
            ("subject", &format!("al.{}", id.to_ulid())),
            ("span", "all"),
        ],
    );

    Ok(view! {
        <div class="mb-1 text-xs text-zinc-500"><a class=(LINK) href=(inbox_url)>"Alerts"</a> " / alert"</div>
        match loaded {
            Err(error) => {
                (status_of(&error))
                page_header(title: "Alert", subtitle: "")
                error_panel(error: &error)
            },
            Ok(None) => {
                (StatusCode::NOT_FOUND)
                page_header(title: "Alert not found", subtitle: "")
                empty_state(message: "No alert has this id.")
            },
            Ok(Some(loaded)) => {
                let row = loaded.row;
                let title = format!("Alert {}", row.short);
                let empty_history = loaded.history.as_ref().is_ok_and(Vec::is_empty);
                page_header(title: &title, subtitle: "What a rule raised, about which subject, and who has handled it.")
                if let Some(flash) = flash {
                    flash_banner(message: flash.message())
                }
                if let Some((status, error)) = failed {
                    (status)
                    <div class="mb-3">error_panel(error: &error)</div>
                }
                <dl class=(format!("{PANEL} mb-4 grid grid-cols-[8rem_1fr] gap-x-4 gap-y-1.5 text-sm"))>
                    <dt class=(LABEL)>"Rule"</dt>
                    <dd><a class=(LINK) href=(loaded.rule_url)>(row.rule.clone())</a></dd>
                    <dt class=(LABEL)>"Subject"</dt>
                    <dd><a class=(LINK) href=(row.subject_url.clone())>(row.subject_label.clone())</a></dd>
                    <dt class=(LABEL)>"Raised"</dt>
                    <dd>(row.raised.clone())</dd>
                    <dt class=(LABEL)>"Occurrences"</dt>
                    <dd>(row.occurrences)</dd>
                    <dt class=(LABEL)>"State"</dt>
                    <dd>
                        kind_badge(value: row.state)
                        if !row.state_detail.is_empty() {
                            <span class="ml-2 text-xs text-zinc-500">(row.state_detail.clone())</span>
                        }
                        if let Some(note) = row.note.clone() {
                            <div class="mt-0.5 text-xs italic text-zinc-600 dark:text-zinc-400">"\u{201c}" (note) "\u{201d}"</div>
                        }
                    </dd>
                </dl>
                if triage && (row.can_acknowledge() || row.can_resolve()) {
                    <section class=(SECTION)>
                        <h2 class=(SECTION_TITLE)>"Triage"</h2>
                        <div class="flex flex-wrap items-end gap-3">
                            if row.can_acknowledge() {
                                <form method="post" action=(action_url.clone())>
                                    <input type="hidden" name="action" value="acknowledge">
                                    <button type="submit" class=(BUTTON)>"Acknowledge"</button>
                                </form>
                            }
                            if row.can_resolve() {
                                <form method="post" action=(action_url.clone()) class="flex items-end gap-2">
                                    <input type="hidden" name="action" value="resolve">
                                    <label class="block">
                                        <span class=(LABEL)>"Resolution note (optional)"</span>
                                        <input type="text" name="note" maxlength="2000" class=(format!("{INPUT} w-80"))>
                                    </label>
                                    <button type="submit" class=(BUTTON)>"Resolve"</button>
                                </form>
                            }
                        </div>
                    </section>
                }
                <section class=(SECTION)>
                    <h2 class=(SECTION_TITLE)>
                        "History"
                        <a class=(format!("{LINK} ml-2 normal-case tracking-normal font-normal")) href=(audit_url)>"in the audit log"</a>
                    </h2>
                    match loaded.history {
                        Err(error) => error_panel(error: &error),
                        Ok(_) if empty_history => empty_state(message: "No operator has acted on this alert."),
                        Ok(rows) => data_table(
                            headers: &["When", "By", "Action", "Note", "Outcome"],
                            for entry in rows {
                                <tr class=(ROW)>
                                    <td class=(TD_MUTED)>(entry.at)</td>
                                    <td class=(TD)>(entry.by)</td>
                                    <td class=(TD)>(entry.what)</td>
                                    <td class=(TD)>(entry.note.unwrap_or_default())</td>
                                    <td class=(TD)>
                                        match entry.outcome {
                                            Ok(()) => <span class="text-xs text-emerald-700 dark:text-emerald-400">"applied"</span>,
                                            Err(reason) => <span class="text-xs text-red-700 dark:text-red-400">"rejected: " (reason)</span>,
                                        }
                                    </td>
                                </tr>
                            }
                        ),
                    }
                </section>
            },
        }
    })
}

#[cfg(test)]
mod tests {
    use topcoat::router::StatusCode;

    use super::*;
    use crate::components::href::tests::state;
    use crate::contract::lists::PageRequest;
    use crate::pages::alerts::rules::model::tests::watched;
    use crate::testing::{Session, get, operator, world};

    #[test]
    fn builtin_rules_link_to_the_rules_list() {
        let user = watched(4, crate::contract::rules::RuleStatus::Enabled);
        assert!(rule_link(Some(&user), &state()).starts_with("/alerts/rules/"));
        let mut builtin = user.clone();
        builtin.rule = RuleKind::Builtin(crate::contract::rules::BuiltinRule::NewChannel);
        assert!(rule_link(Some(&builtin), &state()).starts_with("/alerts/rules?"));
        assert!(rule_link(None, &state()).starts_with("/alerts/rules?"));
    }

    /// An open alert of the harness world.
    async fn open_alert() -> AlertId {
        let filter = crosstalk_spec::interfaces::l8_surface::AlertFilter {
            states: vec![crosstalk_spec::interfaces::l8_surface::AlertStateKind::Open],
            channel: None,
        };
        let page = world()
            .alerts(
                &operator().caller(),
                &filter,
                &PageRequest::first(std::num::NonZeroU32::MIN),
            )
            .await
            .expect("alerts");
        page.items.first().expect("an open alert").id
    }

    #[tokio::test]
    async fn alert_page_shows_the_alert_and_triages_it() {
        let session = Session::new();
        let id = open_alert().await;
        let url = format!("/alerts/{}?{}", id.to_ulid(), state().to_query());
        let reply = session.get(&url).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert!(reply.body.contains(">Acknowledge</button>"));
        assert!(reply.body.contains("No operator has acted on this alert."));
        let reply = session.post(&url, "action=acknowledge").await;
        assert_eq!(reply.status, StatusCode::SEE_OTHER, "{}", reply.body);
        let back = reply.location.expect("location");
        assert!(back.starts_with(&format!("/alerts/{}?", id.to_ulid())));
        let reply = session.get(&back).await;
        assert!(
            reply.body.contains(">acknowledged</span>"),
            "{}",
            reply.body
        );
        assert!(!reply.body.contains(">Acknowledge</button>"));
        assert!(
            reply.body.contains("acknowledged alert"),
            "history lists it"
        );
        let reply = session.post(&url, "action=resolve&note=handled").await;
        assert_eq!(reply.status, StatusCode::SEE_OTHER);
        let reply = session.get(&url).await;
        assert!(reply.body.contains("handled"));
        assert!(!reply.body.contains(">Resolve</button>"));
        // Resolving again is a conflict, shown on the page.
        let reply = session.post(&url, "action=resolve").await;
        assert_eq!(reply.status, StatusCode::CONFLICT);
        assert!(
            reply
                .body
                .contains("the alert is not in a state that allows this")
        );
    }

    #[tokio::test]
    async fn unknown_or_malformed_alerts_are_not_found() {
        let q = state().to_query();
        let reply = get(&format!("/alerts/01J9ZQ3W8D0000000000000003?{q}")).await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND);
        assert!(reply.body.contains("Alert not found"));
        let reply = get(&format!("/alerts/nope?{q}")).await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND);
        let reply = get(&format!("/alerts/rules?{q}")).await;
        assert_eq!(reply.status, StatusCode::OK, "the rules page still wins");
    }
}
