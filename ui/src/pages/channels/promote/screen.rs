//! The promotion page: candidate patterns, the chosen one's coverage, and
//! the confirm form.

use crosstalk_spec::derived::flow::resource::{Locator, ResourcePattern};
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, PolicyKind};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::StatusCode;
use topcoat::view::{View, component, view};

use super::PromoteForm;
use super::patterns::{candidates, pick};
use super::promotable_seed;
use crate::app::{backend, caller};
use crate::backend::Backend;
use crate::components::badge::Badge;
use crate::components::form::{BUTTON_PRIMARY, INPUT, LABEL, LINK, PANEL, SECTION, SECTION_TITLE};
use crate::components::locator::{format_pattern, pattern_kind};
use crate::components::{empty_state, error_panel, href, locator_text, page_header, short_id};
use crate::contract::errors::QueryError;
use crate::data::names::channel_name;
use crate::pages::channels::detail::channel_path;
use crate::pages::channels::model::title;
use crate::pages::common::action::{Failure, error_for, fields_for, require, status_of};
use crate::pages::common::form::{POLICIES, invalid, policy, policy_code};
use crate::pages::common::links::channel_url;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

/// What the chosen pattern would do, display-ready.
struct Preview {
    covered: Vec<Locator>,
    uncovered: Vec<Locator>,
    /// The other channels it supersedes: id, link and name.
    superseded: Vec<(String, String, String)>,
    /// Why promotion would be refused now.
    conflict: Option<QueryError>,
}

struct Promotion {
    title: String,
    seed: Locator,
    options: Vec<ResourcePattern>,
    selected: Option<usize>,
    preview: Option<std::result::Result<Preview, QueryError>>,
}

/// Asks the backend what `pattern` would do, and names the channels it
/// would supersede.
async fn preview(
    cx: &Cx,
    caller: &Caller,
    id: ChannelId,
    pattern: &ResourcePattern,
    state: &ViewState,
) -> std::result::Result<Preview, QueryError> {
    let preview = backend(cx).promotion_preview(caller, id, pattern).await?;
    let names = backend(cx)
        .channel_names(caller, &preview.superseded_channels)
        .await?;
    Ok(Preview {
        covered: preview
            .covered_resources
            .into_iter()
            .map(|r| r.locator)
            .collect(),
        uncovered: preview
            .uncovered_resources
            .into_iter()
            .map(|r| r.locator)
            .collect(),
        superseded: preview
            .superseded_channels
            .iter()
            .map(|other| {
                (
                    short_id(other.to_ulid()),
                    channel_url(*other, state),
                    names
                        .get(other)
                        .map_or_else(|| short_id(other.to_ulid()), channel_name),
                )
            })
            .collect(),
        conflict: preview.conflicts.map(QueryError::Conflict),
    })
}

async fn load(
    cx: &Cx,
    caller: &Caller,
    id: ChannelId,
    state: &ViewState,
    pattern: Option<&str>,
) -> std::result::Result<Option<Promotion>, QueryError> {
    require(caller, Permission::View)?;
    let Some(summary) = backend(cx).channel(caller, id).await? else {
        return Ok(None);
    };
    require(caller, Permission::Govern)?;
    let seed = promotable_seed(&summary)?.clone();
    let options = candidates(&seed);
    let selected = pick(&options, pattern).map_err(|reason| invalid("pattern", reason))?;
    let preview = match selected.and_then(|i| options.get(i)) {
        None => None,
        Some(pattern) => Some(preview(cx, caller, id, pattern, state).await),
    };
    Ok(Some(Promotion {
        title: title(&summary),
        seed,
        options,
        selected,
        preview,
    }))
}

#[component]
pub async fn promote_page(
    cx: &Cx,
    id: ChannelId,
    state: ViewState,
    pattern: Option<String>,
    failure: Option<Failure<PromoteForm>>,
) -> Result<impl View> {
    let caller = caller(cx);
    let loaded = load(cx, &caller, id, &state, pattern.as_deref()).await;
    let failed_status = failure.as_ref().map(Failure::status);
    let error = error_for(failure.as_ref(), PromoteForm::Promote);
    let fields = fields_for(failure.as_ref(), PromoteForm::Promote);
    let chosen_policy = fields
        .as_ref()
        .and_then(|f| policy(f, "policy").ok())
        .unwrap_or(PolicyKind::Sanctioned);
    let note_text = fields
        .as_ref()
        .and_then(|f| f.text("note"))
        .unwrap_or("")
        .to_owned();
    let policies: Vec<_> = POLICIES
        .iter()
        .map(|p| (policy_code(*p), p.label(), *p == chosen_policy))
        .collect();
    let path = format!("{}/promote", channel_path(id));
    let action_url = href(&path, &state, &[]);
    let back_url = channel_url(id, &state);

    Ok(view! {
        if let Some(status) = failed_status {
            (status)
        }
        <div class="mb-1 text-xs text-zinc-500">
            <a class=(LINK) href=(back_url)>"Channel " <span class="font-mono">(short_id(id.to_ulid()))</span></a>
            " / promote"
        </div>
        match loaded {
            Err(error) => {
                (status_of(&error))
                page_header(title: "Promote channel", subtitle: "")
                error_panel(error: &error)
            },
            Ok(None) => {
                (StatusCode::NOT_FOUND)
                page_header(title: "Channel not found", subtitle: "")
                empty_state(message: "No channel has this id.")
            },
            Ok(Some(promotion)) => {
                let options: Vec<_> = promotion
                    .options
                    .iter()
                    .enumerate()
                    .map(|(i, p)| {
                        let index = i.to_string();
                        (
                            href(&path, &state, &[("pattern", &index)]),
                            pattern_kind(p),
                            format_pattern(p),
                            promotion.selected == Some(i),
                        )
                    })
                    .collect();
                let selected_index = promotion.selected.map(|i| i.to_string());
                page_header(
                    title: "Promote to a declared channel",
                    subtitle: "A declared channel is matched by a pattern. Promotion supersedes this channel and every other discovered channel the pattern covers.",
                )
                <div class=(format!("{PANEL} mb-4 text-sm"))>
                    <span class=(LABEL)>"Seed resource"</span>
                    <div class="mt-1">locator_text(locator: &promotion.seed)</div>
                    <p class="mt-1 text-xs text-zinc-500">(promotion.title)</p>
                </div>
                <section class=(SECTION)>
                    <h2 class=(SECTION_TITLE)>"1. Choose a pattern"</h2>
                    <ul class="divide-y divide-zinc-100 rounded border border-zinc-200 text-sm dark:divide-zinc-800 dark:border-zinc-800">
                        for (link, kind, text, active) in options {
                            <li>
                                <a
                                    href=(link)
                                    class=(if active { "flex items-baseline gap-3 bg-sky-50 px-3 py-1.5 dark:bg-sky-950" } else { "flex items-baseline gap-3 px-3 py-1.5 hover:bg-zinc-50 dark:hover:bg-zinc-900" })
                                    aria-current=(active.then_some("true"))
                                >
                                    <span class="w-24 shrink-0 text-[10px] uppercase tracking-wide text-zinc-400">(kind)</span>
                                    <span class="break-all font-mono text-xs">(text)</span>
                                </a>
                            </li>
                        }
                    </ul>
                </section>
                match promotion.preview {
                    None => empty_state(message: "Pick a pattern to see what it covers."),
                    Some(Err(error)) => error_panel(error: &error),
                    Some(Ok(preview)) => {
                        let covered_count = preview.covered.len();
                        let uncovered_count = preview.uncovered.len();
                        let superseded_count = preview.superseded.len();
                        <section class=(SECTION)>
                            <h2 class=(SECTION_TITLE)>"2. Check what it covers"</h2>
                            if let Some(conflict) = preview.conflict {
                                <div class="mb-2">error_panel(error: &conflict)</div>
                            }
                            <p class="mb-2 text-sm">
                                "The declared channel would hold " <strong>(covered_count)</strong>
                                " known resources: every resource of the channels it supersedes that the pattern matches."
                            </p>
                            <ul class="mb-3 space-y-1 text-sm">
                                for locator in preview.covered {
                                    <li class="flex items-center gap-2">
                                        <span class="w-20 text-xs text-emerald-700 dark:text-emerald-400">"covered"</span>
                                        locator_text(locator: &locator)
                                    </li>
                                }
                            </ul>
                            if uncovered_count > 0 {
                                <p class="mb-1 text-sm">(uncovered_count) " resources of these channels fall outside the pattern; they stay with their superseded channel:"</p>
                                <ul class="mb-3 space-y-1 text-sm">
                                    for locator in preview.uncovered {
                                        <li class="flex items-center gap-2">
                                            <span class="w-20 text-xs text-amber-700 dark:text-amber-400">"not covered"</span>
                                            locator_text(locator: &locator)
                                        </li>
                                    }
                                </ul>
                            }
                            if superseded_count == 0 {
                                <p class="text-sm text-zinc-500">"No other discovered channel is covered."</p>
                            } else {
                                <p class="mb-1 text-sm">"Also supersedes " <strong>(superseded_count)</strong> " other discovered channels:"</p>
                                <ul class="space-y-1 text-sm">
                                    for (label, url, name) in preview.superseded {
                                        <li class="flex items-center gap-2">
                                            <a class=(format!("{LINK} font-mono text-xs")) href=(url)>(label)</a>
                                            <span class="break-all font-mono text-xs">(name)</span>
                                        </li>
                                    }
                                </ul>
                            }
                        </section>
                    },
                }
                if let Some(index) = selected_index {
                    <section class=(SECTION)>
                        <h2 class=(SECTION_TITLE)>"3. Declare"</h2>
                        <form method="post" action=(action_url) class=(format!("{PANEL} space-y-2"))>
                            <input type="hidden" name="pattern" value=(index)>
                            <div class="flex flex-wrap items-end gap-3">
                                <label class="block">
                                    <span class=(LABEL)>"Policy"</span>
                                    <select name="policy" class=(INPUT)>
                                        for (code, label, chosen) in policies {
                                            <option value=(code) selected=(chosen)>(label)</option>
                                        }
                                    </select>
                                </label>
                                <label class="block min-w-64 flex-1">
                                    <span class=(LABEL)>"Note (optional)"</span>
                                    <input type="text" name="note" value=(note_text) maxlength="2000" class=(format!("{INPUT} w-full"))>
                                </label>
                                <button type="submit" class=(BUTTON_PRIMARY)>"Promote"</button>
                            </div>
                            if let Some(error) = error {
                                error_panel(error: &error)
                            }
                        </form>
                    </section>
                } else {
                    if let Some(error) = error {
                        error_panel(error: &error)
                    }
                }
            },
        }
    })
}
