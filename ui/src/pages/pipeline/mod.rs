//! `/pipeline`: dead-lettered deliveries (`dead_letters`, newest envelope
//! first, optionally of one consumer group: `group`), and their replay
//! (`ReplayDeadLetter`). Both need `Operate`.
//!
//! A replay re-runs a consumer on an old event, so it can reopen alerts or
//! re-apply stale decisions; the page says so above the list.

use crosstalk_spec::events::BusEvent;
use crosstalk_spec::ids::EventId;
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, DeadLetter};
use crosstalk_spec::interfaces::l8_surface::{Caller, OperatorAction, Permission};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::content::Form;
use topcoat::router::{page, query_params};
use topcoat::view::{View, component, view};

use crate::app::{backend, caller};
use crate::backend::Backend;
use crate::components::form::{FACET, LINK, SMALL_BUTTON};
use crate::components::table::{ROW, TD, TD_MUTED, TD_NUM};
use crate::components::{
    PageLinks, data_table, empty_state, error_panel, filter_chip, flash_banner, format_time, href,
    page_header, pagination,
};
use crate::error::UiError;
use crate::pages::common::action::{Failure, done, perform, require, status_of};
use crate::pages::common::flash::{Flash, flash};
use crate::pages::common::form::{FormFields, id, invalid, required};
use crate::pages::common::paging::page_request;
use crate::pages::view::view_state;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;
use crosstalk_spec::paging::{Cursor, DeadLetterList};

const PATH: &str = "/pipeline";

/// Consumer group names are short identifiers; this bounds what a form or
/// a query may send.
const GROUP_MAX_CHARS: usize = 200;

#[query_params]
struct PipelineQuery {
    group: Option<String>,
}

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

/// A consumer group named by `field`: trimmed, non-empty, bounded.
fn group(text: &str, field: &'static str) -> std::result::Result<ConsumerGroup, UiError> {
    let text = text.trim();
    if text.is_empty() {
        return Err(invalid(field, "required"));
    }
    if text.chars().count() > GROUP_MAX_CHARS {
        return Err(invalid(
            field,
            format!("longer than {GROUP_MAX_CHARS} characters"),
        ));
    }
    Ok(ConsumerGroup(text.to_owned()))
}

/// The `group` key: the consumer group the list is narrowed to.
pub fn parse_group(text: Option<&str>) -> std::result::Result<Option<ConsumerGroup>, UiError> {
    text.map(|text| group(text, "group")).transpose()
}

/// A validated replay post.
pub fn parse(fields: &FormFields) -> std::result::Result<OperatorAction, UiError> {
    if fields.text("action") != Some("replay") {
        return Err(invalid("action", "unknown action"));
    }
    Ok(OperatorAction::ReplayDeadLetter {
        group: group(required(fields, "group")?, "group")?,
        id: id::<EventId>(fields, "event")?,
    })
}

/// The list's own pairs: the group it is narrowed to.
fn pairs(group: Option<&ConsumerGroup>) -> Vec<(&'static str, &str)> {
    group.map(|g| ("group", g.0.as_str())).into_iter().collect()
}

struct Letters {
    rows: Vec<LetterRow>,
    current: Option<Cursor<DeadLetterList>>,
    next: Option<Cursor<DeadLetterList>>,
}

async fn load(
    cx: &Cx,
    caller: &Caller,
    group: Option<&ConsumerGroup>,
) -> std::result::Result<Letters, UiError> {
    require(caller, Permission::Operate)?;
    let request = page_request(cx)?;
    let page = backend(cx).dead_letters(caller, group, &request).await?;
    Ok(Letters {
        rows: page.items().iter().map(letter_row).collect(),
        current: request.after,
        next: page.next().cloned(),
    })
}

fn query_group(cx: &Cx) -> std::result::Result<Option<ConsumerGroup>, UiError> {
    query_params::<PipelineQuery>(cx)
        .map_err(|e| invalid("query", e))
        .and_then(|q| parse_group(q.group.as_deref()))
}

#[page("/pipeline")]
async fn pipeline_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx).await?;
    let flash = flash(cx);
    let group = query_group(cx);
    Ok(view! { pipeline_page(state: state, group: group, flash: flash, failure: None) })
}

#[page(POST "/pipeline")]
async fn pipeline_post(cx: &Cx, Form(fields): Form<FormFields>) -> Result<impl View> {
    let state = view_state(cx).await?;
    let group = query_group(cx);
    let error = match parse(&fields) {
        Ok(action) => match perform(cx, action).await {
            Ok(_) => {
                let shown = group.as_ref().ok().and_then(Option::as_ref);
                return Err(done(PATH, &state, &pairs(shown), Flash::Replayed));
            }
            Err(error) => error,
        },
        Err(error) => error,
    };
    let failure: Failure<()> = Failure::new(None, error, fields);
    Ok(view! { pipeline_page(state: state, group: group, flash: None, failure: Some(failure)) })
}

#[component]
async fn pipeline_page(
    cx: &Cx,
    state: ViewState,
    group: std::result::Result<Option<ConsumerGroup>, UiError>,
    flash: Option<Flash>,
    failure: Option<Failure<()>>,
) -> Result<impl View> {
    let caller = caller(cx);
    let letters = match &group {
        Ok(group) => load(cx, &caller, group.as_ref()).await,
        Err(error) => Err(error.clone()),
    };
    let group = group.ok().flatten();
    let failed = failure.map(|f| (f.status(), f.error));
    let action_url = href(PATH, &state, &pairs(group.as_ref()));
    let clear_url = group.as_ref().map(|_| href(PATH, &state, &[]));
    let group_label = group.as_ref().map(|g| format!("{} ×", g.0));
    let empty = letters.as_ref().is_ok_and(|l| l.rows.is_empty());
    let page_pairs: Vec<(&'static str, String)> =
        group.iter().map(|g| ("group", g.0.clone())).collect();

    Ok(view! {
        page_header(
            title: "Pipeline",
            subtitle: "Deliveries that ran out of retries, newest first. Replaying re-runs the consumer on the old event, which can reopen alerts or re-apply stale decisions.",
        )
        if let Some((status, error)) = failed {
            (status)
            <div class="mb-4">error_panel(error: &error)</div>
        }
        if let Some(flash) = flash {
            flash_banner(message: flash.message())
        }
        if let (Some(label), Some(clear)) = (group_label, clear_url) {
            <div class="mb-3 flex items-center gap-1.5 text-xs">
                <span class=(FACET)>"Consumer group"</span>
                filter_chip(label: &label, href: clear, active: true)
            </div>
        }
        match letters {
            Err(error) => {
                (status_of(&error))
                error_panel(error: &error)
            },
            Ok(_) if empty => empty_state(message: "No dead letters: every delivery succeeded or is still being retried."),
            Ok(letters) => {
                let links = PageLinks::new(
                    PATH,
                    &state,
                    &page_pairs.iter().map(|(k, v)| (*k, v.as_str())).collect::<Vec<_>>(),
                    letters.current.as_ref(),
                    letters.next.as_ref(),
                );
                data_table(
                    headers: &["Consumer group", "Event", "Kind", "Event time", "Attempts", "Last error", ""],
                    for row in letters.rows {
                        let group_url = href(PATH, &state, &[("group", &row.group)]);
                        <tr class=(ROW)>
                            <td class=(TD)><a class=(format!("{LINK} font-mono text-xs")) href=(group_url) title="Only this group's dead letters">(row.group.clone())</a></td>
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
    use crate::testing::{Session, get, post};

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

    #[test]
    fn the_group_key_names_one_group() {
        assert_eq!(parse_group(None), Ok(None));
        assert_eq!(
            parse_group(Some(" flow ")),
            Ok(Some(ConsumerGroup("flow".to_owned())))
        );
        assert_eq!(parse_group(Some("  ")), Err(invalid("group", "required")));
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

    #[tokio::test]
    async fn one_group_lists_and_replays_its_own() {
        let world = crate::testing::world();
        let caller = crate::testing::operator().caller();
        let letters = world
            .dead_letters(
                &caller,
                None,
                &crate::pages::common::paging::first(std::num::NonZeroU32::new(10).expect("ten")),
            )
            .await
            .expect("letters");
        let letter = letters.items().first().expect("a dead letter").clone();
        let session = Session::new();
        let url = format!("/pipeline?{}&group={}", state().to_query(), letter.group.0);
        let reply = session.get(&url).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert!(reply.body.contains(&format!("{} ×", letter.group.0)));
        let others = letters
            .items()
            .iter()
            .filter(|l| l.group != letter.group)
            .map(|l| l.envelope.id.to_ulid());
        for other in others {
            assert!(!reply.body.contains(&other), "only the group's letters");
        }
        let form = format!(
            "action=replay&group={}&event={}",
            letter.group.0,
            letter.envelope.id.to_ulid()
        );
        let reply = session.post(&url, &form).await;
        assert_eq!(reply.status, StatusCode::SEE_OTHER, "{}", reply.body);
        let back = reply.location.expect("location");
        assert!(
            back.contains(&format!("group={}", letter.group.0)),
            "{back}"
        );
        assert!(back.ends_with("&flash=replayed"), "{back}");
        let reply = session.get(&back).await;
        assert!(
            reply.body.contains("No dead letters"),
            "the group's one letter was replayed"
        );
    }
}
