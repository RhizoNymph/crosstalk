//! `/alerts`: the inbox, one tab per state. Posts acknowledge or resolve an
//! alert and return to the same tab.

use crosstalk_spec::ids::AlertId;
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, AlertStateKind, Caller, Permission};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::content::Form;
use topcoat::router::{page, query_params};
use topcoat::view::{View, component, view};

use super::model::AlertRow;
use crate::app::{backend, caller, can};
use crate::backend::Backend;
use crate::components::badge::Badge;
use crate::components::form::{INPUT, LINK, SMALL_BUTTON};
use crate::components::table::{ROW, TD, TD_MUTED, TD_NUM};
use crate::components::{
    PageLinks, Tab, data_table, empty_state, error_panel, flash_banner, href, kind_badge,
    page_header, pagination, short_id, tabs,
};
use crate::contract::actions::OperatorAction;
use crate::contract::errors::QueryError;
use crate::contract::lists::Cursor;
use crate::pages::common::action::{Failure, done, perform, require, status_of};
use crate::pages::common::flash::{Flash, flash};
use crate::pages::common::form::{FormFields, id, invalid, note};
use crate::pages::common::lookup::{operator_names, rule_names};
use crate::pages::common::paging::page_request;
use crate::pages::view::view_state;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

const PATH: &str = "/alerts";

pub const TABS: [AlertStateKind; 4] = [
    AlertStateKind::Open,
    AlertStateKind::Acknowledged,
    AlertStateKind::Resolved,
    AlertStateKind::Suppressed,
];

pub fn tab_code(state: AlertStateKind) -> &'static str {
    state.label()
}

/// `None` is the default tab, open alerts.
pub fn parse_tab(text: Option<&str>) -> std::result::Result<AlertStateKind, QueryError> {
    match text {
        None => Ok(AlertStateKind::Open),
        Some(text) => TABS
            .into_iter()
            .find(|t| tab_code(*t) == text)
            .ok_or_else(|| invalid("tab", format!("unknown tab {text:?}"))),
    }
}

#[query_params]
struct InboxQuery {
    tab: Option<String>,
}

/// The alert a post is about; its error shows on that row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowForm(pub AlertId);

/// The triage action a form asks for on `alert`: acknowledge, or resolve
/// with an optional note. Shared by the inbox and the alert page.
pub fn parse_action(
    alert: AlertId,
    fields: &FormFields,
) -> std::result::Result<(OperatorAction, Flash), QueryError> {
    match fields.text("action") {
        Some("acknowledge") => Ok((OperatorAction::Acknowledge { alert }, Flash::Acknowledged)),
        Some("resolve") => Ok((
            OperatorAction::Resolve {
                alert,
                note: note(fields, "note")?,
            },
            Flash::Resolved,
        )),
        _ => Err(invalid("action", "unknown action")),
    }
}

/// A validated inbox post: the action, its flash, and the tab to return to.
pub fn parse(
    fields: &FormFields,
) -> std::result::Result<(OperatorAction, Flash, AlertStateKind), (Option<RowForm>, QueryError)> {
    let alert = id::<AlertId>(fields, "alert").map_err(|e| (None, e))?;
    let row = Some(RowForm(alert));
    let tab = parse_tab(fields.text("tab")).map_err(|e| (row, e))?;
    let (action, flash) = parse_action(alert, fields).map_err(|e| (row, e))?;
    Ok((action, flash, tab))
}

struct Inbox {
    rows: Vec<AlertRow>,
    current: Option<Cursor>,
    next: Option<Cursor>,
}

async fn load(
    cx: &Cx,
    caller: &Caller,
    tab: AlertStateKind,
    state: &ViewState,
) -> std::result::Result<Inbox, QueryError> {
    require(caller, Permission::View)?;
    let request = page_request(cx)?;
    // The shared filter narrows the inbox when it names exactly one channel.
    let channel = match state.scope.filter.channels.as_slice() {
        [one] => Some(*one),
        _ => None,
    };
    let filter = AlertFilter {
        states: vec![tab],
        channel,
    };
    let page = backend(cx).alerts(caller, &filter, &request).await?;
    let rules = rule_names(cx, caller).await;
    let operators = operator_names(cx, caller).await;
    Ok(Inbox {
        rows: page
            .items
            .iter()
            .map(|a| AlertRow::new(a, &rules, &operators, state))
            .collect(),
        current: request.cursor,
        next: page.next,
    })
}

#[page("/alerts")]
async fn alerts_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx).await?;
    let tab = query_params::<InboxQuery>(cx)
        .map_err(|e| invalid("query", e))
        .and_then(|q| parse_tab(q.tab.as_deref()));
    let flash = flash(cx);
    Ok(view! { inbox_page(state: state, tab: tab, flash: flash, failure: None) })
}

#[page(POST "/alerts")]
async fn alerts_post(cx: &Cx, Form(fields): Form<FormFields>) -> Result<impl View> {
    let state = view_state(cx).await?;
    let (failure, tab) = match parse(&fields) {
        Ok((action, flash, tab)) => match perform(cx, action).await {
            Ok(_) => return Err(done(PATH, &state, &[("tab", tab_code(tab))], flash)),
            Err(error) => {
                let row = id::<AlertId>(&fields, "alert").ok().map(RowForm);
                (Failure::new(row, error, fields), tab)
            }
        },
        Err((row, error)) => {
            let tab = parse_tab(fields.text("tab")).unwrap_or(AlertStateKind::Open);
            (Failure::new(row, error, fields), tab)
        }
    };
    Ok(view! { inbox_page(state: state, tab: Ok(tab), flash: None, failure: Some(failure)) })
}

#[component]
async fn inbox_page(
    cx: &Cx,
    state: ViewState,
    tab: std::result::Result<AlertStateKind, QueryError>,
    flash: Option<Flash>,
    failure: Option<Failure<RowForm>>,
) -> Result<impl View> {
    let caller = caller(cx);
    let triage = can(&caller, Permission::Triage);
    let current = tab.clone().unwrap_or(AlertStateKind::Open);
    let inbox = match tab {
        Ok(tab) => load(cx, &caller, tab, &state).await,
        Err(error) => Err(error),
    };
    let failed_status = failure.as_ref().map(Failure::status);
    // An error about a listed alert shows on its row; any other at the top.
    let failed_row = failure
        .as_ref()
        .and_then(|f| f.form)
        .map(|RowForm(id)| id.to_ulid());
    let on_row = inbox
        .as_ref()
        .is_ok_and(|i| i.rows.iter().any(|r| Some(&r.id) == failed_row.as_ref()));
    let row_error = failure.as_ref().filter(|_| on_row).map(|f| f.error.clone());
    let top_error = failure
        .as_ref()
        .filter(|_| !on_row)
        .map(|f| f.error.clone());
    let tab_items: Vec<Tab> = TABS
        .iter()
        .map(|t| Tab {
            label: t.label().to_owned(),
            href: href(PATH, &state, &[("tab", tab_code(*t))]),
            active: *t == current,
        })
        .collect();
    let action_url = href(PATH, &state, &[]);
    let rules_url = href("/alerts/rules", &state, &[]);
    let narrowed = match state.scope.filter.channels.as_slice() {
        [one] => Some(short_id(one.to_ulid())),
        _ => None,
    };
    let empty = inbox.as_ref().is_ok_and(|i| i.rows.is_empty());
    let tab_code_now = tab_code(current);

    Ok(view! {
        if let Some(status) = failed_status {
            (status)
        }
        page_header(title: "Alerts", subtitle: "What the rules raised, by state. Acknowledge to claim one; resolve with a note when handled.")
        <div class="mb-2 text-right text-sm">
            <a class=(LINK) href=(rules_url)>"Rules and sinks"</a>
        </div>
        tabs(items: tab_items)
        if let Some(channel) = narrowed {
            <p class="mb-2 text-xs text-zinc-500">"Showing alerts about channel " <span class="font-mono">(channel)</span> " and its transmissions, from the shared filter."</p>
        }
        if let Some(flash) = flash {
            flash_banner(message: flash.message())
        }
        if let Some(error) = top_error {
            <div class="mb-4">error_panel(error: &error)</div>
        }
        match inbox {
            Err(error) => {
                (status_of(&error))
                error_panel(error: &error)
            },
            Ok(_) if empty => empty_state(message: "No alerts in this state."),
            Ok(inbox) => {
                let links = PageLinks::new(PATH, &state, &[("tab", tab_code_now)], inbox.current.as_ref(), inbox.next.as_ref());
                data_table(
                    headers: &["Alert", "Rule", "Subject", "Occurrences", "Raised", "State", ""],
                    for row in inbox.rows {
                        let error_here = row_error.clone().filter(|_| Some(&row.id) == failed_row.as_ref());
                        <tr class=(ROW)>
                            <td class=(TD_MUTED)><a class=(format!("{LINK} font-mono")) href=(row.url.clone())>(row.short.clone())</a></td>
                            <td class=(TD)>(row.rule.clone())</td>
                            <td class=(TD)><a class=(LINK) href=(row.subject_url.clone())>(row.subject_label.clone())</a></td>
                            <td class=(TD_NUM)>(row.occurrences)</td>
                            <td class=(TD_MUTED)>(row.raised.clone())</td>
                            <td class=(TD)>
                                kind_badge(value: row.state)
                                if !row.state_detail.is_empty() {
                                    <div class="mt-0.5 text-xs text-zinc-500">(row.state_detail.clone())</div>
                                }
                                if let Some(note) = row.note.clone() {
                                    <div class="mt-0.5 text-xs italic text-zinc-600 dark:text-zinc-400">"\u{201c}" (note) "\u{201d}"</div>
                                }
                            </td>
                            <td class=(TD)>
                                if triage {
                                    <div class="flex flex-wrap items-center justify-end gap-1.5">
                                        if row.can_acknowledge() {
                                            <form method="post" action=(action_url.clone())>
                                                <input type="hidden" name="action" value="acknowledge">
                                                <input type="hidden" name="alert" value=(row.id.clone())>
                                                <input type="hidden" name="tab" value=(tab_code_now)>
                                                <button type="submit" class=(SMALL_BUTTON)>"Acknowledge"</button>
                                            </form>
                                        }
                                        if row.can_resolve() {
                                            <form method="post" action=(action_url.clone()) class="flex items-center gap-1">
                                                <input type="hidden" name="action" value="resolve">
                                                <input type="hidden" name="alert" value=(row.id.clone())>
                                                <input type="hidden" name="tab" value=(tab_code_now)>
                                                <input type="text" name="note" maxlength="2000" placeholder="Resolution note" class=(format!("{INPUT} w-44 py-0.5 text-xs"))>
                                                <button type="submit" class=(SMALL_BUTTON)>"Resolve"</button>
                                            </form>
                                        }
                                    </div>
                                }
                                if let Some(error) = error_here {
                                    <div class="mt-1">error_panel(error: &error)</div>
                                }
                            </td>
                        </tr>
                    }
                )
                pagination(links: links)
            },
        }
    })
}

#[cfg(test)]
mod tests {
    use topcoat::router::StatusCode;

    use super::*;
    use crate::components::href::tests::state;
    use crate::testing::{get, post};

    const ALERT: &str = "01J9ZQ3W8D0000000000000003";

    #[test]
    fn tabs_parse_from_their_labels() {
        for tab in TABS {
            assert_eq!(parse_tab(Some(tab_code(tab))), Ok(tab));
        }
        assert_eq!(parse_tab(None), Ok(AlertStateKind::Open));
        assert!(parse_tab(Some("muted")).is_err());
    }

    #[test]
    fn posts_parse_into_actions() {
        let fields = FormFields::from_pairs(&[
            ("action", "resolve"),
            ("alert", ALERT),
            ("tab", "acknowledged"),
            ("note", "false alarm"),
        ]);
        let (action, flash, tab) = parse(&fields).expect("valid");
        assert_eq!(flash, Flash::Resolved);
        assert_eq!(tab, AlertStateKind::Acknowledged);
        assert!(matches!(
            action,
            OperatorAction::Resolve { note: Some(ref n), .. } if n == "false alarm"
        ));
        let ack = FormFields::from_pairs(&[("action", "acknowledge"), ("alert", ALERT)]);
        assert_eq!(
            parse(&ack).map(|(_, f, t)| (f, t)),
            Ok((Flash::Acknowledged, AlertStateKind::Open))
        );
    }

    #[test]
    fn bad_posts_say_which_field() {
        let no_alert = FormFields::from_pairs(&[("action", "resolve")]);
        assert_eq!(parse(&no_alert), Err((None, invalid("alert", "required"))));
        let unknown = FormFields::from_pairs(&[("action", "snooze"), ("alert", ALERT)]);
        assert!(matches!(
            parse(&unknown),
            Err((Some(_), QueryError::InvalidInput(_)))
        ));
    }

    #[tokio::test]
    async fn every_tab_renders_alerts() {
        for tab in TABS {
            let reply = get(&format!(
                "/alerts?{}&tab={}",
                state().to_query(),
                tab_code(tab)
            ))
            .await;
            assert_eq!(reply.status, StatusCode::OK, "{tab:?}");
            assert!(
                !reply.body.contains("No alerts in this state."),
                "the fixture has alerts in every state: {tab:?}"
            );
        }
        let reply = get(&format!("/alerts?{}&tab=muted", state().to_query())).await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn posts_validate_then_reach_the_backend() {
        let url = format!("/alerts?{}", state().to_query());
        let reply = post(&url, "action=acknowledge").await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(reply.body.contains("alert: required"));
        let reply = post(&url, &format!("action=acknowledge&alert={ALERT}&tab=open")).await;
        assert_eq!(
            reply.status,
            StatusCode::NOT_FOUND,
            "the stub backend knows no alert"
        );
        assert!(reply.body.contains("not found"));
    }
}
