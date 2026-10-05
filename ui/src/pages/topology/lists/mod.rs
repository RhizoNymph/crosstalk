//! The topology page's agent and channel lists: every agent node and every
//! channel carrying traffic in the view, each a button that selects it
//! exactly as clicking it in the graph does.
//!
//! Rows carry no script: each list has one delegated `click` handler (and
//! one pair of hover handlers) on its `<ul>`, which reads the row button's
//! `value` (the item's selection value; the row's children ignore the
//! pointer, so the event's target is the button). A click sets the page's
//! `sel` signal to the value, or clears it when the row is already
//! selected, and resets the drawer's cursor; hovering previews the
//! highlight through the `hover` signal. Everything that follows `sel`
//! outside Topcoat's own bindings (the URL's `sel` key, the rows'
//! `aria-pressed`, scrolling the selected row into view) is one binding on
//! the page ([`selection_sync`]), and each list's filter is one binding on
//! the list ([`list_rows`]), which hides rows by their `data-key` and updates
//! the count. The tab and the filter text are browser state only (signals,
//! not URL keys): neither changes what the view shows.

pub mod model;

use crosstalk_spec::derived::flow::channel::confirmation::Confirmation;
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::runtime::{Event, Signal, signal};
use topcoat::view::{View, component, view};

use self::model::{AgentItem, Carried, ChannelItem, GraphLists};
use crate::components::{Badge, family_name};
use crate::pages::topology::selection::Selection;
use crate::url::view_state::GraphMode;

/// Which list is shown: the `tab` signal's values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListTab {
    Agents,
    Channels,
}

impl ListTab {
    pub fn code(self) -> &'static str {
        match self {
            Self::Agents => "agents",
            Self::Channels => "channels",
        }
    }

    /// The tab that shows the selected item: channels for a channel, else
    /// agents.
    pub fn for_selection(selection: &Selection) -> Self {
        match selection {
            Selection::Channel(_) => Self::Channels,
            Selection::None | Selection::Agent(_) | Selection::Edge { .. } => Self::Agents,
        }
    }
}

const PANEL: &str = "rounded border border-zinc-200 bg-white dark:border-zinc-800 dark:bg-zinc-950";
const TAB: &str = "inline-flex items-center gap-1.5 rounded px-2 py-1 text-xs font-medium text-zinc-600 hover:bg-zinc-100 focus-visible:outline-2 focus-visible:outline-sky-500 dark:text-zinc-400 dark:hover:bg-zinc-800 aria-pressed:bg-zinc-100 aria-pressed:text-zinc-900 dark:aria-pressed:bg-zinc-800 dark:aria-pressed:text-zinc-100";
const CLEAR: &str = "rounded px-1.5 py-0.5 text-[11px] text-sky-700 hover:bg-sky-50 focus-visible:outline-2 focus-visible:outline-sky-500 dark:text-sky-400 dark:hover:bg-sky-950";
const SEARCH: &str = "w-full min-w-0 rounded border border-zinc-300 bg-white px-2 py-0.5 text-xs placeholder:text-zinc-400 focus:border-sky-500 focus:outline-none dark:border-zinc-700 dark:bg-zinc-900";
const SCROLL: &str = "relative max-h-80 overflow-y-auto lg:max-h-[29rem] xl:max-h-80 border-t border-zinc-200 dark:border-zinc-800";
const UL: &str = "divide-y divide-zinc-100 dark:divide-zinc-800/70";
const CAPTION: &str = "mt-1 text-[11px] text-zinc-500";
const NOTE: &str = "px-2 py-3 text-xs text-zinc-500";

fn width(volume: u64, max: u64) -> String {
    let pct = if max == 0 {
        0.0
    } else {
        (volume as f64 / max as f64 * 100.0).clamp(0.0, 100.0)
    };
    format!("width:{pct:.1}%")
}

/// A row's initial `aria-pressed`: the selection the page was loaded with.
fn pressed(code: &str, selected: &str) -> &'static str {
    if code == selected { "true" } else { "false" }
}

fn claim_text(claim: &crosstalk_spec::observed::client::HarnessClaim) -> String {
    match &claim.version {
        Some(version) => format!("{} {version}", family_name(&claim.family)),
        None => family_name(&claim.family).to_owned(),
    }
}

/// The page's one binding that keeps the URL, the rows' `aria-pressed` and
/// their scroll position in step with `sel` (`SYNC_JS`).
#[component]
pub async fn selection_sync(sel: &Signal<String>) -> Result<impl View> {
    let sel = sel.clone();
    Ok(view! {
        <span
            hidden=(true)
            data-sel-sync=""
            :data-sel=$({
                let v = sel.get();
                raw!("((value) => { const v = String(value); if ((new URLSearchParams(location.search).get('sel') ?? '') !== v) { const kept = location.search.slice(1).split('&').filter((p) => p !== '' && p.split('=')[0] !== 'sel'); if (v !== '') kept.push('sel=' + encodeURIComponent(v).replace(/%3A/g, ':')); history.replaceState(history.state, '', location.pathname + '?' + kept.join('&')); } for (const row of document.querySelectorAll('[data-list-item]')) { const on = row.value === v; row.setAttribute('aria-pressed', on ? 'true' : 'false'); if (!on) continue; const box = row.closest('[data-list-scroll]'); requestAnimationFrame(() => { const top = row.offsetTop; if (box && (top < box.scrollTop || top + row.offsetHeight > box.scrollTop + box.clientHeight)) box.scrollTop = Math.max(0, top - box.clientHeight / 2); }); } return value; })(${v})", v)
            })
        ></span>
    })
}

/// The two lists in a tabbed panel. `selected` is the selection the page
/// was loaded with (marked in the markup), `sel` the page's selection signal,
/// `cursor` the drawer's, `hover` the highlight preview and `tab` the
/// shown list ([`ListTab::code`]).
#[component]
pub async fn graph_lists(
    cx: &Cx,
    lists: GraphLists,
    selected: String,
    sel: &Signal<String>,
    cursor: &Signal<String>,
    hover: &Signal<String>,
    tab: &Signal<String>,
) -> Result<impl View> {
    let (sel_clear, cursor_clear, hover_clear) = (sel.clone(), cursor.clone(), hover.clone());
    let tab = tab.clone();
    let agent_query = signal(cx, String::new);
    let channel_query = signal(cx, String::new);
    let agent_count = lists.agents.len().to_string();
    let channel_count = lists.channels.len().to_string();
    let mode = lists.mode;
    let (agent_caption, channel_caption, no_channels) = match mode {
        GraphMode::Agents => (
            "Agent nodes, by transmissions in and out",
            "Channels behind channel-routed edges, by transmissions",
            "No channel-routed transmissions in this window and filter.",
        ),
        GraphMode::Channels => (
            "Agent nodes, by transmissions in and out (not accesses)",
            "Channel nodes, by reads and writes",
            "No channels in this window and filter.",
        ),
    };
    let agent_total = lists.agents.len();
    let channel_total = lists.channels.len();
    Ok(view! {
        <section class=(PANEL) aria-label="Agents and channels in this view">
            <div class="flex items-center gap-1 px-1.5 py-1">
                <button
                    type="button"
                    class=(TAB)
                    data-list-tab="agents"
                    :aria-pressed=$(if tab.get() == "agents" { "true" } else { "false" })
                    @click=$(|_e| tab.set("agents".to_owned()))
                >"Agents" <span class="tabular-nums text-zinc-500">(agent_count)</span></button>
                <button
                    type="button"
                    class=(TAB)
                    data-list-tab="channels"
                    :aria-pressed=$(if tab.get() == "channels" { "true" } else { "false" })
                    @click=$(|_e| tab.set("channels".to_owned()))
                >"Channels" <span class="tabular-nums text-zinc-500">(channel_count)</span></button>
                <span class="flex-1"></span>
                <button
                    type="button"
                    class=(CLEAR)
                    data-list-clear=""
                    :hidden=$(sel_clear.get().is_empty())
                    @click=$(|_e| {
                        sel_clear.set("".to_owned());
                        cursor_clear.set("".to_owned());
                        hover_clear.set("".to_owned());
                    })
                >"Clear selection"</button>
            </div>
            <div :hidden=$(tab.get() != "agents")>
                list_head(name: "agents", caption: agent_caption, total: agent_total, query: &agent_query)
                list_rows(name: "agents", none: "No agents in this window and filter.", rows: Rows::Agents(lists.agents), selected: selected.clone(), query: &agent_query, sel: sel, cursor: cursor, hover: hover)
            </div>
            <div :hidden=$(tab.get() != "channels")>
                list_head(name: "channels", caption: channel_caption, total: channel_total, query: &channel_query)
                list_rows(name: "channels", none: no_channels, rows: Rows::Channels(lists.channels), selected: selected, query: &channel_query, sel: sel, cursor: cursor, hover: hover)
            </div>
        </section>
    })
}

/// A list's rows, by kind.
enum Rows {
    Agents(Vec<AgentItem>),
    Channels(Vec<ChannelItem>),
}

impl Rows {
    fn is_empty(&self) -> bool {
        match self {
            Self::Agents(items) => items.is_empty(),
            Self::Channels(items) => items.is_empty(),
        }
    }
}

/// The filter box and the count above a list. The count is static text
/// that [`list_rows`]' filter binding rewrites.
#[component]
async fn list_head(
    name: &str,
    caption: &str,
    total: usize,
    query: &Signal<String>,
) -> Result<impl View> {
    let query = query.clone();
    let placeholder = format!("Filter {name}");
    Ok(view! {
        <div class="px-1.5 pb-1.5">
            <div class="flex items-center gap-2">
                <input
                    type="search"
                    class=(SEARCH)
                    placeholder=(placeholder.clone())
                    aria-label=(placeholder)
                    autocomplete="off"
                    spellcheck="false"
                    @input=$(|e: Event| query.set(e.target.value))
                >
                <span class="shrink-0 text-[11px] tabular-nums text-zinc-500" aria-live="polite" data-list-count=(name)>(total)</span>
            </div>
            <p class=(CAPTION)>(caption)</p>
        </div>
    })
}

/// A list's rows, with its one click handler and one pair of hover
/// handlers (reading the row button's `value`), and its one filter
/// binding, which hides the rows whose `data-key` misses the filter text,
/// rewrites the count and shows the no-match note.
#[component]
async fn list_rows(
    name: &str,
    none: &str,
    rows: Rows,
    selected: String,
    query: &Signal<String>,
    sel: &Signal<String>,
    cursor: &Signal<String>,
    hover: &Signal<String>,
) -> Result<impl View> {
    let (query, sel, cursor, hover) = (query.clone(), sel.clone(), cursor.clone(), hover.clone());
    let hover_out = hover.clone();
    let list = name.to_owned();
    let empty = rows.is_empty();
    let no_match = format!("No {name} match the filter.");
    Ok(view! {
        <div
            class=(SCROLL)
            data-list-scroll=(name)
            :data-filter=$({
                let q = query.get();
                raw!("((list, value) => { const name = String(list); const q = String(value).trim().toLowerCase(); const box = document.querySelector('[data-list-scroll=' + JSON.stringify(name) + ']'); if (!box) return value; let shown = 0; let total = 0; for (const row of box.querySelectorAll('[data-list-item]')) { total += 1; const show = (row.dataset.key ?? '').includes(q); row.parentElement.hidden = !show; if (show) shown += 1; } const count = document.querySelector('[data-list-count=' + JSON.stringify(name) + ']'); if (count) count.textContent = q === '' ? String(total) : shown + ' of ' + total; const none = box.querySelector('[data-list-none]'); if (none) none.hidden = total === 0 || shown > 0; return value; })(${list}, ${q})", q)
            })
        >
            if empty {
                <p class=(NOTE)>(none)</p>
            } else {
                <ul
                    class=(UL)
                    @click=$(|e: Event| {
                        let v = e.target.value;
                        if v.contains(":") {
                            let next = if sel.get() == v { "".to_owned() } else { v.to_owned() };
                            sel.set(next);
                            cursor.set("".to_owned());
                            hover.set("".to_owned());
                        }
                    })
                    @mouseover=$(|e: Event| {
                        let v = e.target.value;
                        if v.contains(":") {
                            hover.set(v.to_owned());
                        }
                    })
                    @mouseleave=$(|_e| hover_out.set("".to_owned()))
                >
                    match rows {
                        Rows::Agents(items) => {
                            let max = items.iter().map(AgentItem::volume).max().unwrap_or(0);
                            for item in items {
                                agent_row(bar: width(item.volume(), max), pressed: pressed(&item.code(), &selected), item: item)
                            }
                        },
                        Rows::Channels(items) => {
                            let max = items.iter().map(|c| c.carried.volume()).max().unwrap_or(0);
                            for item in items {
                                channel_row(bar: width(item.carried.volume(), max), pressed: pressed(&item.code(), &selected), item: item)
                            }
                        },
                    }
                </ul>
                <p class=(NOTE) data-list-none="" hidden=(true)>(no_match)</p>
            }
        </div>
    })
}

#[component]
async fn agent_row(item: AgentItem, bar: String, pressed: &'static str) -> Result<impl View> {
    let code = item.code();
    let key = item.search_key();
    let volume = item.volume();
    let flow = format!(
        "{} in · {} out",
        item.transmissions_in, item.transmissions_out
    );
    let state = item.state;
    let provisional = state != crosstalk_spec::aggregates::node::CanonicalStateKind::Established;
    let first_claim = item.claims.first().map(claim_text);
    let more_claims = item.claims.len().saturating_sub(1);
    let parent = item.parent.clone();
    let title = match &item.parent {
        Some(parent) => format!("{} (sub-agent of {parent})", item.name),
        None => item.name.clone(),
    };
    Ok(view! {
        <li>
            <button type="button" class="ct-row" value=(code.clone()) data-list-item=(code) data-key=(key) title=(title) aria-pressed=(pressed)>
                <span class="ct-row-main">
                    <span class="flex min-w-0 items-center gap-1.5">
                        <span class="truncate font-medium">(item.name)</span>
                        if provisional {
                            <span class=(format!("ct-row-badge {}", state.tone().classes()))>(state.label())</span>
                        }
                    </span>
                    <span class="ct-row-meta">
                        if let Some(parent) = parent {
                            <span class="truncate">"↳ sub-agent of " (parent)</span>
                        }
                        if let Some(claim) = first_claim {
                            <span class="ct-row-claim"><span class="text-zinc-400">"claims"</span>(claim)</span>
                        }
                        if more_claims > 0 {
                            <span>(format!("+{more_claims}"))</span>
                        }
                    </span>
                </span>
                <span class="ct-row-num"><span class="block">(volume)</span><span>(flow)</span></span>
                <span class="ct-row-bar" style=(bar)></span>
            </button>
        </li>
    })
}

#[component]
async fn channel_row(item: ChannelItem, bar: String, pressed: &'static str) -> Result<impl View> {
    let code = item.code();
    let key = item.search_key();
    let volume = item.carried.volume();
    let detail = match item.carried {
        Carried::Transmissions { edges, .. } => {
            format!("{edges} edge{}", if edges == 1 { "" } else { "s" })
        }
        Carried::Accesses { writes, reads } => format!("{writes} w · {reads} r"),
    };
    let policy = item.policy;
    let unconfirmed = item.is_unconfirmed().then_some(Confirmation::Unconfirmed);
    Ok(view! {
        <li>
            <button type="button" class="ct-row" value=(code.clone()) data-list-item=(code) data-key=(key) title=(item.name.clone()) aria-pressed=(pressed)>
                <span class="ct-row-main">
                    <span class="truncate font-mono text-[11px] font-medium">(item.name)</span>
                    if policy.is_some() || unconfirmed.is_some() {
                        <span class="ct-row-meta">
                            if let Some(policy) = policy {
                                <span class=(format!("ct-row-badge {}", policy.tone().classes()))>(policy.label())</span>
                            }
                            if let Some(unconfirmed) = unconfirmed {
                                <span class=(format!("ct-row-badge {}", unconfirmed.tone().classes()))>(unconfirmed.label())</span>
                            }
                        </span>
                    }
                </span>
                <span class="ct-row-num"><span class="block">(volume)</span><span>(detail)</span></span>
                <span class="ct-row-bar" style=(bar)></span>
            </button>
        </li>
    })
}
