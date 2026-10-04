//! `/transmissions/{id}`: why the gateway believes a transmission happened.
//!
//! The header (sender → reader, route, state with its data, times, topic
//! under the current topic version) comes from the transmission list, so it
//! renders with `View` alone. The matched text, the co-access timeline and
//! the verdict log come from `transmission(id)`, which needs `Content`;
//! without it those sections show their structure with the text hidden.
//! Posting `set-verdict` records a verdict (`Triage` and `Content`).

pub mod model;
pub mod sections;
pub mod verdict;

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::ids::TransmissionId;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use crosstalk_spec::support::{TimeWindow, Timestamp};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::content::Form;
use topcoat::router::error::not_found;
use topcoat::router::{StatusCode, page, path_param};
use topcoat::view::{View, component, view};

use self::model::{
    CoAccessView, MatchView, Strength, co_access_views, co_accesses, confirmed_at, judgeable,
    kind_text, match_views, named, named_agents, state_text, strength, title_id,
};
use self::sections::{co_access_section, matches_section};
use self::verdict::{FormState, VerdictRow, verdict_rows, verdict_section};
use crate::app::{backend, caller, can};
use crate::backend::Backend;
use crate::components::form::LINK;
use crate::components::{
    content_hidden, empty_state, error_panel, flash_banner, format_bytes, format_time, href,
    kind_badge, route_badge,
};
use crate::contract::graph::{TransmissionSelector, TransmissionStateKind};
use crate::contract::lists::PageRequest;
use crate::contract::scope::{Scope, TopologyFilter};
use crate::contract::verdict::Verdict;
use crate::error::UiError;
use crate::pages::common::action::{
    Failure, done, error_for, fields_for, general_error, perform, require, status_of,
};
use crate::pages::common::flash::{Flash, flash};
use crate::pages::common::form::{FormFields, invalid};
use crate::pages::common::links::channel_url;
use crate::pages::common::lookup::{agent_names, operator_names};
use crate::pages::common::transmissions::{Named, channel_names, route_channel, route_text};
use crate::pages::topology::selection::Selection;
use crate::pages::view::view_state;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;
use crosstalk_spec::interfaces::l8_surface::QueryError;

path_param!(tx_ulid);

pub fn transmission_path(id: TransmissionId) -> String {
    format!("/transmissions/{}", id.to_ulid())
}

/// The transmission named by the path. An id that is not a ULID names
/// nothing.
fn transmission_id(cx: &Cx) -> Result<TransmissionId> {
    TransmissionId::parse_ulid(path_param::<TxUlid>(cx)).map_err(|_| not_found().into())
}

/// The forms on this page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceForm {
    Verdict,
}

/// The topic cell of the header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicCell {
    Label(String),
    /// Hidden without `Content`, or the label is unknown.
    Hidden,
    Outlier,
    /// Not classified (yet) under the version.
    Unclassified,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub short: String,
    pub full: String,
    pub from: Option<Named>,
    pub to: Named,
    pub route_kind: RouteKind,
    pub route: String,
    pub route_url: Option<String>,
    pub state: TransmissionStateKind,
    pub strength: Strength,
    pub state_text: String,
    pub opened: String,
    pub confirmed: Option<String>,
    pub matched: Option<String>,
    pub version: u32,
    pub topic: TopicCell,
    pub verdict: Option<Verdict>,
    /// The topology with this transmission's edge selected.
    pub edge_url: Option<String>,
}

struct Loaded {
    header: Header,
    /// `None` without `Content`.
    matches: Option<Vec<MatchView>>,
    co_access: Option<Vec<CoAccessView>>,
    verdicts: Option<Vec<VerdictRow>>,
    form: FormState,
}

/// Every transmission up to a day after the data's end.
fn all_time(now: Timestamp) -> Option<TimeWindow> {
    let end = Timestamp::from_micros(now.as_micros().saturating_add(86_400_000_000));
    TimeWindow::new(Timestamp::from_micros(0), end).ok()
}

async fn load(
    cx: &Cx,
    caller: &Caller,
    id: TransmissionId,
    state: &ViewState,
) -> std::result::Result<Option<Loaded>, UiError> {
    require(caller, Permission::View)?;
    let backend = backend(cx);
    let content = can(caller, Permission::Content);
    let version = backend.current_topic_version(caller).await?;
    let window = all_time(backend.now(caller).await?).ok_or_else(|| {
        UiError::Query(QueryError::Store {
            reason: "empty window".to_owned(),
        })
    })?;
    let scope = Scope {
        window,
        topic_version: version,
        filter: TopologyFilter::default(),
    };
    let listed = backend
        .transmissions(
            caller,
            &scope,
            &TransmissionSelector::Ids(vec![id]),
            &PageRequest::first(crate::pages::common::paging::PAGE_SIZE),
        )
        .await?;
    let Some(summary) = listed.items.into_iter().find(|s| s.id == id) else {
        return Ok(None);
    };
    let evidence = if content {
        backend.transmission(caller, id).await?
    } else {
        None
    };

    let mut agents: Vec<_> = summary.from.into_iter().chain([summary.to]).collect();
    if let Some(evidence) = &evidence {
        agents.extend(named_agents(&evidence.matches, &evidence.accesses));
    }
    let names = agent_names(cx, caller, agents).await;
    let channels = channel_names(cx, caller, route_channel(&summary.route)).await;
    let topic = match summary.topic {
        None if matches!(
            summary.state,
            TransmissionStateKind::Classified | TransmissionStateKind::Aggregated
        ) =>
        {
            TopicCell::Outlier
        }
        None => TopicCell::Unclassified,
        Some(_) if !content => TopicCell::Hidden,
        Some(topic) => match backend.topics(caller, version).await {
            Ok(topics) => topics
                .into_iter()
                .find(|t| t.id == topic)
                .map_or(TopicCell::Hidden, |t| TopicCell::Label(t.label)),
            Err(error) => {
                tracing::warn!(error = ?error, "topic label unavailable");
                TopicCell::Hidden
            }
        },
    };
    let edge_url = summary.from.map(|from| {
        let sel = Selection::edge(from, summary.to, &summary.route).encode();
        href(crate::pages::topology::PATH, state, &[("sel", &sel)])
    });
    let header = Header {
        short: title_id(id),
        full: id.to_ulid(),
        from: summary.from.map(|f| named(f, &names, state)),
        to: named(summary.to, &names, state),
        route_kind: summary.route_kind,
        route: route_text(&summary.route, &channels),
        route_url: route_channel(&summary.route).map(|c| channel_url(c, state)),
        state: summary.state,
        strength: strength(summary.state),
        state_text: match &evidence {
            Some(evidence) => state_text(&evidence.transmission.state),
            None => kind_text(summary.state).to_owned(),
        },
        opened: format_time(summary.opened_at),
        confirmed: evidence
            .as_ref()
            .and_then(|e| confirmed_at(&e.transmission.state))
            .map(format_time),
        matched: (summary.matched_bytes > 0).then(|| format_bytes(summary.matched_bytes)),
        version: version.0,
        topic,
        verdict: summary.verdict,
        edge_url,
    };
    let form = if !(can(caller, Permission::Triage) && content) {
        FormState::Closed("Recording a verdict needs the Triage and Content permissions.")
    } else if !judgeable(summary.state) {
        FormState::Closed("Nothing to judge yet: the gateway is still gathering evidence.")
    } else {
        FormState::Open {
            action: href(&transmission_path(id), state, &[]),
        }
    };
    let (matches, co_access, verdicts) = match evidence {
        Some(evidence) => {
            let operators = operator_names(cx, caller).await;
            (
                Some(match_views(&evidence.matches, &names, state)),
                Some(co_access_views(
                    &co_accesses(&evidence.transmission.state),
                    &evidence.accesses,
                    &names,
                    state,
                )),
                Some(verdict_rows(&evidence.verdicts, &operators)),
            )
        }
        None => (None, None, None),
    };
    Ok(Some(Loaded {
        header,
        matches,
        co_access,
        verdicts,
        form,
    }))
}

#[page("/transmissions/{tx_ulid}")]
async fn transmission_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx).await?;
    let id = transmission_id(cx)?;
    let flash = flash(cx);
    Ok(view! { evidence_page(id: id, state: state, flash: flash, failure: None) })
}

#[page(POST "/transmissions/{tx_ulid}")]
async fn transmission_post(cx: &Cx, Form(fields): Form<FormFields>) -> Result<impl View> {
    let state = view_state(cx).await?;
    let id = transmission_id(cx)?;
    let failure = match fields.text("action") {
        Some("set-verdict") => {
            let result = match verdict::parse(id, &fields) {
                Ok((action, flash)) => perform(cx, action).await.map(|_| flash),
                Err(error) => Err(error),
            };
            match result {
                Ok(flash) => return Err(done(&transmission_path(id), &state, &[], flash)),
                Err(error) => Failure::new(Some(EvidenceForm::Verdict), error, fields),
            }
        }
        _ => Failure::new(None, invalid("action", "unknown action"), fields),
    };
    Ok(view! { evidence_page(id: id, state: state, flash: None, failure: Some(failure)) })
}

fn strength_classes(strength: Strength) -> &'static str {
    match strength {
        Strength::Content => {
            "border-sky-200 bg-sky-50 text-sky-900 dark:border-sky-900 dark:bg-sky-950/60 dark:text-sky-100"
        }
        Strength::AccessOnly => {
            "border-dashed border-amber-400 bg-amber-50 text-amber-900 dark:border-amber-700 dark:bg-amber-950/60 dark:text-amber-100"
        }
        Strength::Discarded => {
            "border-dashed border-zinc-300 bg-zinc-50 text-zinc-500 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-400"
        }
        Strength::Pending => {
            "border-zinc-200 bg-white text-zinc-700 dark:border-zinc-800 dark:bg-zinc-950 dark:text-zinc-300"
        }
    }
}

fn strength_label(strength: Strength) -> &'static str {
    match strength {
        Strength::Content => "content evidence",
        Strength::AccessOnly => "access pattern only",
        Strength::Discarded => "discarded",
        Strength::Pending => "gathering evidence",
    }
}

#[component]
async fn evidence_page(
    cx: &Cx,
    id: TransmissionId,
    state: ViewState,
    flash: Option<Flash>,
    failure: Option<Failure<EvidenceForm>>,
) -> Result<impl View> {
    let caller = caller(cx);
    let loaded = load(cx, &caller, id, &state).await;
    let failed_status = failure.as_ref().map(Failure::status);
    let verdict_error = error_for(failure.as_ref(), EvidenceForm::Verdict);
    let verdict_fields = fields_for(failure.as_ref(), EvidenceForm::Verdict);
    // A failed post whose form is not shown still shows its error, at the
    // top of the page.
    let form_open = matches!(
        &loaded,
        Ok(Some(Loaded {
            form: FormState::Open { .. },
            ..
        }))
    );
    let (form_error, top_error) = if form_open {
        (verdict_error, general_error(failure.as_ref()))
    } else {
        (None, failure.as_ref().map(|f| f.error.clone()))
    };
    let topology_url = href(crate::pages::topology::PATH, &state, &[]);
    let short = title_id(id);

    Ok(view! {
        if let Some(status) = failed_status {
            (status)
        }
        <div class="mb-1 text-xs text-zinc-500">
            <a class=(LINK) href=(topology_url)>"Topology"</a>
            " / transmission "
            <span class="font-mono">(short)</span>
        </div>
        match loaded {
            Err(error) => {
                (status_of(&error))
                <h1 class="mb-3 text-lg font-semibold">"Transmission"</h1>
                error_panel(error: &error)
            },
            Ok(None) => {
                (StatusCode::NOT_FOUND)
                <h1 class="mb-3 text-lg font-semibold">"Transmission not found"</h1>
                if let Some(error) = top_error {
                    <div class="mb-4">error_panel(error: &error)</div>
                }
                empty_state(message: "No transmission has this id. It may have been mistyped.")
            },
            Ok(Some(loaded)) => {
                let reader = loaded.header.to.name.clone();
                evidence_header(header: loaded.header)
                if let Some(flash) = flash {
                    flash_banner(message: flash.message())
                }
                if let Some(error) = top_error {
                    <div class="mb-4">error_panel(error: &error)</div>
                }
                matches_section(matches: loaded.matches, reader: reader)
                co_access_section(records: loaded.co_access)
                verdict_section(rows: loaded.verdicts, form: loaded.form, retained: verdict_fields, error: form_error)
            },
        }
    })
}

#[component]
async fn evidence_header(header: Header) -> Result<impl View> {
    let callout = format!(
        "mb-4 rounded border px-3 py-2 text-sm {}",
        strength_classes(header.strength)
    );
    let label = strength_label(header.strength);
    let weak = matches!(header.strength, Strength::AccessOnly | Strength::Discarded);
    Ok(view! {
        <header class="mb-3">
            <div class="flex flex-wrap items-center gap-2">
                <h1 class="text-lg font-semibold">"Transmission " <span class="font-mono">(header.short)</span></h1>
                kind_badge(value: header.state)
                if let Some(verdict) = header.verdict {
                    kind_badge(value: verdict)
                }
            </div>
            <p class="font-mono text-xs text-zinc-500">(header.full)</p>
        </header>
        <div class=(callout)>
            <span class="mr-2 text-[11px] font-semibold uppercase tracking-wide opacity-80">(label)</span>
            (header.state_text)
        </div>
        <dl class=(if weak { "mb-6 grid grid-cols-2 gap-x-6 gap-y-2 text-sm opacity-80 md:grid-cols-4" } else { "mb-6 grid grid-cols-2 gap-x-6 gap-y-2 text-sm md:grid-cols-4" })>
            <div class="col-span-2">
                <dt class="text-xs text-zinc-500">"Sender → reader"</dt>
                <dd class="font-medium">
                    match header.from {
                        Some(from) => <a class=(LINK) href=(from.url)>(from.name)</a>,
                        None => <span class="italic text-zinc-500" title="The sender is known once a content match confirms it">"unknown sender"</span>,
                    }
                    <span class="px-1.5 text-zinc-400">"→"</span>
                    <a class=(LINK) href=(header.to.url)>(header.to.name)</a>
                    if let Some(url) = header.edge_url {
                        <a class=(format!("{LINK} ml-2 text-xs font-normal")) href=(url)>"show edge"</a>
                    }
                </dd>
            </div>
            <div class="col-span-2">
                <dt class="text-xs text-zinc-500">"Route"</dt>
                <dd class="flex min-w-0 items-center gap-1.5">
                    route_badge(kind: header.route_kind)
                    match header.route_url {
                        Some(url) => <a class=(format!("{LINK} truncate font-mono text-xs")) href=(url)>(header.route)</a>,
                        None => <span class="truncate text-xs">(header.route)</span>,
                    }
                </dd>
            </div>
            <div>
                <dt class="text-xs text-zinc-500">"Opened"</dt>
                <dd class="tabular-nums">(header.opened)</dd>
            </div>
            <div>
                <dt class="text-xs text-zinc-500">"Confirmed"</dt>
                <dd class="tabular-nums">(header.confirmed.unwrap_or_else(|| "—".to_owned()))</dd>
            </div>
            <div>
                <dt class="text-xs text-zinc-500">"Matched"</dt>
                <dd class="tabular-nums">(header.matched.unwrap_or_else(|| "—".to_owned()))</dd>
            </div>
            <div>
                <dt class="text-xs text-zinc-500">"Topic (v" (header.version) ")"</dt>
                <dd>
                    match header.topic {
                        TopicCell::Label(label) => (label),
                        TopicCell::Hidden => content_hidden(),
                        TopicCell::Outlier => <span class="italic text-zinc-500">"outlier"</span>,
                        TopicCell::Unclassified => <span class="italic text-zinc-500">"not classified"</span>,
                    }
                </dd>
            </div>
        </dl>
    })
}

#[cfg(test)]
mod tests;
