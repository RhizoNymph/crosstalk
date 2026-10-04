//! The promotion page: candidate patterns, the chosen one's coverage, and
//! the confirm form.

use std::num::NonZeroU32;

use crosstalk_spec::derived::flow::resource::{Locator, ResourcePattern};
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, PolicyKind};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::StatusCode;
use topcoat::view::{View, component, view};

use super::PromoteForm;
use super::patterns::{Coverage, candidates, pick};
use super::promotable_seed;
use crate::app::{backend, caller};
use crate::backend::Backend;
use crate::components::badge::Badge;
use crate::components::form::{BUTTON_PRIMARY, INPUT, LABEL, LINK, PANEL, SECTION, SECTION_TITLE};
use crate::components::locator::{format_pattern, pattern_kind};
use crate::components::{empty_state, error_panel, href, locator_text, page_header, short_id};
use crate::contract::channels::{ChannelListFilter, OriginKind};
use crate::contract::errors::QueryError;
use crate::contract::lists::PageRequest;
use crate::pages::channels::detail::channel_path;
use crate::pages::channels::model::title;
use crate::pages::common::action::{Failure, error_for, fields_for, require, status_of};
use crate::pages::common::form::{POLICIES, invalid, policy, policy_code};
use crate::pages::common::links::channel_url;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

/// Discovered channels are read this many pages deep to find the ones a
/// pattern would supersede.
const OTHER_PAGES: usize = 5;
const OTHER_PAGE_SIZE: NonZeroU32 = match NonZeroU32::new(100) {
    Some(size) => size,
    None => NonZeroU32::MIN,
};

struct Promotion {
    title: String,
    seed: Locator,
    options: Vec<ResourcePattern>,
    selected: Option<usize>,
    coverage: Option<std::result::Result<Coverage, QueryError>>,
    /// Whether the discovered list was cut short.
    partial: bool,
}

/// The seeds of other live discovered channels.
async fn other_seeds(
    cx: &Cx,
    caller: &Caller,
    except: ChannelId,
) -> std::result::Result<(Vec<(ChannelId, Locator)>, bool), QueryError> {
    let filter = ChannelListFilter {
        origins: vec![OriginKind::Discovered],
        ..ChannelListFilter::default()
    };
    let mut request = PageRequest::first(OTHER_PAGE_SIZE);
    let mut seeds = Vec::new();
    for _ in 0..OTHER_PAGES {
        let page = backend(cx).channels(caller, &filter, &request).await?;
        seeds.extend(
            page.items
                .into_iter()
                .filter(|s| s.channel.id != except && s.superseded.is_none())
                .filter_map(|s| s.seed.map(|seed| (s.channel.id, seed.locator))),
        );
        match page.next {
            Some(next) => request.cursor = Some(next),
            None => return Ok((seeds, false)),
        }
    }
    Ok((seeds, true))
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
    let mut partial = false;
    let coverage = match selected.and_then(|i| options.get(i)) {
        None => None,
        Some(pattern) => Some(
            async {
                let uses = backend(cx)
                    .channel_resources(caller, id, state.scope.window)
                    .await?;
                let mut own: Vec<Locator> = vec![seed.clone()];
                for locator in uses.into_iter().map(|u| u.resource.locator) {
                    if !own.contains(&locator) {
                        own.push(locator);
                    }
                }
                let (others, cut) = other_seeds(cx, caller, id).await?;
                partial = cut;
                Ok(Coverage::new(pattern, &own, &others))
            }
            .await,
        ),
    };
    Ok(Some(Promotion {
        title: title(&summary),
        seed,
        options,
        selected,
        coverage,
        partial,
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
                match promotion.coverage {
                    None => empty_state(message: "Pick a pattern to see what it covers."),
                    Some(Err(error)) => error_panel(error: &error),
                    Some(Ok(coverage)) => {
                        let missed = coverage.missed();
                        let total = coverage.resources.len();
                        let supersedes: Vec<_> = coverage
                            .supersedes
                            .iter()
                            .map(|(other, seed)| (channel_url(*other, &state), short_id(other.to_ulid()), seed.clone()))
                            .collect();
                        let superseded_count = supersedes.len();
                        <section class=(SECTION)>
                            <h2 class=(SECTION_TITLE)>"2. Check what it covers"</h2>
                            <p class="mb-2 text-sm">
                                "Covers " <strong>(total - missed)</strong> " of " (total)
                                " known resources of this channel (the seed and those seen in the selected window)."
                            </p>
                            <ul class="mb-3 space-y-1 text-sm">
                                for (locator, covered) in coverage.resources {
                                    <li class="flex items-center gap-2">
                                        if covered {
                                            <span class="w-20 text-xs text-emerald-700 dark:text-emerald-400">"covered"</span>
                                        } else {
                                            <span class="w-20 text-xs text-amber-700 dark:text-amber-400">"not covered"</span>
                                        }
                                        locator_text(locator: &locator)
                                    </li>
                                }
                            </ul>
                            if superseded_count == 0 {
                                <p class="text-sm text-zinc-500">"No other discovered channel is covered."</p>
                            } else {
                                <p class="mb-1 text-sm">"Also supersedes " <strong>(superseded_count)</strong> " other discovered channels:"</p>
                                <ul class="space-y-1 text-sm">
                                    for (url, label, seed) in supersedes {
                                        <li class="flex items-center gap-2">
                                            <a class=(format!("{LINK} font-mono text-xs")) href=(url)>(label)</a>
                                            locator_text(locator: &seed)
                                        </li>
                                    }
                                </ul>
                            }
                            if promotion.partial {
                                <p class="mt-1 text-xs text-amber-700 dark:text-amber-400">"Only the first discovered channels were checked; more may be covered."</p>
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
