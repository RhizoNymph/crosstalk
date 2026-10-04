//! `/pipeline`: dead-lettered deliveries, and their replay (`Operate`).
//!
//! A replay re-runs a consumer on an old event, so it can reopen alerts or
//! re-apply stale decisions; the page says so above the list.

use crosstalk_spec::events::BusEvent;
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::content::Form;
use topcoat::router::page;
use topcoat::view::{View, component, view};

use crate::app::{backend, caller};
use crate::backend::Backend;
use crate::components::form::SMALL_BUTTON;
use crate::components::table::{ROW, TD, TD_MUTED, TD_NUM};
use crate::components::{
    PageLinks, data_table, empty_state, error_panel, flash_banner, format_time, href, page_header,
    pagination,
};
use crate::error::UiError;
use crate::pages::common::action::{Failure, done, perform, require, status_of};
use crate::pages::common::flash::{Flash, flash};
use crate::pages::common::form::{FormFields, id, invalid, required};
use crate::pages::common::paging::page_request;
use crate::pages::view::view_state;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;
use crosstalk_spec::interfaces::l8_surface::OperatorAction;
use crosstalk_spec::paging::{Cursor, DeadLetterList};

const PATH: &str = "/pipeline";

/// Consumer group names are short identifiers; this bounds what a form may
/// send.
const GROUP_MAX_CHARS: usize = 200;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LetterRow {
    pub group: String,
    pub event: String,
    pub at: String,
    pub kind: &'static str,
    pub attempts: u32,
    pub last_error: String,
}

pub fn event_kind(event: &BusEvent) -> &'static str {
    match event {
        BusEvent::Ingest(_) => "ingest",
        BusEvent::Detect(_) => "detect",
        BusEvent::Insight(_) => "insight",
        BusEvent::Changed(_) => "changed",
    }
}

pub fn letter_row(letter: &DeadLetter) -> LetterRow {
    LetterRow {
        group: letter.group.0.clone(),
        event: letter.envelope.id.to_ulid(),
        at: format_time(letter.envelope.at),
        kind: event_kind(&letter.envelope.event),
        attempts: letter.attempts.get(),
        last_error: letter.last_error.clone(),
    }
}

/// A validated replay post.
pub fn parse(fields: &FormFields) -> std::result::Result<OperatorAction, UiError> {
    if fields.text("action") != Some("replay") {
        return Err(invalid("action", "unknown action"));
    }
    let group = required(fields, "group")?;
    if group.chars().count() > GROUP_MAX_CHARS {
        return Err(invalid(
            "group",
            format!("longer than {GROUP_MAX_CHARS} characters"),
        ));
    }
    Ok(OperatorAction::ReplayDeadLetter {
        group: ConsumerGroup(group.to_owned()),
        id: id::<EventId>(fields, "event")?,
    })
}

struct Letters {
    rows: Vec<LetterRow>,
    current: Option<Cursor<DeadLetterList>>,
    next: Option<Cursor<DeadLetterList>>,
}

async fn load(cx: &Cx, caller: &Caller) -> std::result::Result<Letters, UiError> {
    require(caller, Permission::Operate)?;
    let request = page_request(cx)?;
    let page = backend(cx).dead_letters(caller, &request).await?;
    Ok(Letters {
        rows: page.items().iter().map(letter_row).collect(),
        current: request.after,
        next: page.next().cloned(),
    })
}

#[page("/pipeline")]
async fn pipeline_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx).await?;
    let flash = flash(cx);
    Ok(view! { pipeline_page(state: state, flash: flash, failure: None) })
}

#[page(POST "/pipeline")]
async fn pipeline_post(cx: &Cx, Form(fields): Form<FormFields>) -> Result<impl View> {
    let state = view_state(cx).await?;
    let error = match parse(&fields) {
        Ok(action) => match perform(cx, action).await {
            Ok(_) => return Err(done(PATH, &state, &[], Flash::Replayed)),
            Err(error) => error,
        },
        Err(error) => error,
    };
    let failure: Failure<()> = Failure::new(None, error, fields);
    Ok(view! { pipeline_page(state: state, flash: None, failure: Some(failure)) })
}

#[component]
async fn pipeline_page(
    cx: &Cx,
    state: ViewState,
    flash: Option<Flash>,
    failure: Option<Failure<()>>,
) -> Result<impl View> {
    let caller = caller(cx);
    let letters = load(cx, &caller).await;
    let failed = failure.map(|f| (f.status(), f.error));
    let action_url = href(PATH, &state, &[]);
    let empty = letters.as_ref().is_ok_and(|l| l.rows.is_empty());

    Ok(view! {
        page_header(
            title: "Pipeline",
            subtitle: "Deliveries that ran out of retries. Replaying re-runs the consumer on the old event, which can reopen alerts or re-apply stale decisions.",
        )
        if let Some((status, error)) = failed {
            (status)
            <div class="mb-4">error_panel(error: &error)</div>
        }
        if let Some(flash) = flash {
            flash_banner(message: flash.message())
        }
        match letters {
            Err(error) => {
                (status_of(&error))
                error_panel(error: &error)
            },
            Ok(_) if empty => empty_state(message: "No dead letters: every delivery succeeded or is still being retried."),
            Ok(letters) => {
                let links = PageLinks::new(PATH, &state, &[], letters.current.as_ref(), letters.next.as_ref());
                data_table(
                    headers: &["Consumer group", "Event", "Kind", "Event time", "Attempts", "Last error", ""],
                    for row in letters.rows {
                        <tr class=(ROW)>
                            <td class=(TD)><span class="font-mono text-xs">(row.group.clone())</span></td>
                            <td class=(TD)><span class="font-mono text-xs">(row.event.clone())</span></td>
                            <td class=(TD_MUTED)>(row.kind)</td>
                            <td class=(TD_MUTED)>(row.at)</td>
                            <td class=(TD_NUM)>(row.attempts)</td>
                            <td class=(TD)>
                                <div class="max-w-md truncate font-mono text-xs text-red-700 dark:text-red-400" title=(row.last_error.clone())>(row.last_error)</div>
                            </td>
                            <td class=(TD)>
                                <form method="post" action=(action_url.clone())>
                                    <input type="hidden" name="action" value="replay">
                                    <input type="hidden" name="group" value=(row.group)>
                                    <input type="hidden" name="event" value=(row.event)>
                                    <button type="submit" class=(SMALL_BUTTON)>"Replay"</button>
                                </form>
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

    const EVENT: &str = "01J9ZQ3W8D0000000000000005";

    #[test]
    fn replays_parse() {
        let fields =
            FormFields::from_pairs(&[("action", "replay"), ("group", "l5-flow"), ("event", EVENT)]);
        assert!(matches!(
            parse(&fields),
            Ok(OperatorAction::ReplayDeadLetter { ref group, .. }) if group.0 == "l5-flow"
        ));
        let no_group = FormFields::from_pairs(&[("action", "replay"), ("event", EVENT)]);
        assert_eq!(parse(&no_group), Err(invalid("group", "required")));
        let long = "g".repeat(GROUP_MAX_CHARS + 1);
        let long_group =
            FormFields::from_pairs(&[("action", "replay"), ("group", &long), ("event", EVENT)]);
        assert!(parse(&long_group).is_err());
    }

    #[tokio::test]
    async fn pipeline_renders_and_validates() {
        let url = format!("/pipeline?{}", state().to_query());
        let reply = get(&url).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert!(!reply.body.contains("No dead letters"));
        let reply = post(&url, "action=replay&group=l5&event=bad").await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(reply.body.contains("event: expected 26 characters"));
        let reply = post(&url, &format!("action=replay&group=l5&event={EVENT}")).await;
        assert_eq!(
            reply.status,
            StatusCode::NOT_FOUND,
            "no dead letter has this event id"
        );
    }
}
