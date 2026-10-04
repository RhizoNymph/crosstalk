//! The topology page's agent and channel lists: every agent node and every
//! channel carrying traffic in the view, each a button that selects it
//! exactly as clicking it in the graph does.
//!
//! A list item's click handler sets the page's `sel` signal to the item's
//! selection value (or clears it when the item is already selected), resets
//! the drawer's cursor and rewrites the URL's `sel` key, as the graph's
//! `change` handler does; the graph's `data-highlight`, the drawer shard
//! and the item's `aria-pressed` all follow the signal. Hovering an item
//! previews its highlight through the `hover` signal. The tab and the
//! filter text are browser state only (signals, not URL keys): neither
//! changes what the view shows.
//!
//! Everything here re-renders in the browser; nothing is read back on the
//! server, so the lists are rendered once with the page.

pub mod model;

use topcoat::Result;
use topcoat::context::Cx;
use topcoat::runtime::{Event, Signal, signal};
use topcoat::view::{View, component, view};

use self::model::{AgentItem, Carried, ChannelItem, GraphLists, matches, shown_label};
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
const ITEM: &str = "relative flex w-full items-center gap-2 px-2 py-1 text-left text-xs hover:bg-zinc-50 focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-sky-500 dark:hover:bg-zinc-900 aria-pressed:bg-sky-50 aria-pressed:shadow-[inset_3px_0_0_var(--color-sky-600)] dark:aria-pressed:bg-sky-950/60 dark:aria-pressed:shadow-[inset_3px_0_0_var(--color-sky-400)]";
const BAR: &str =
    "pointer-events-none absolute bottom-0 left-0 h-px bg-sky-500/50 dark:bg-sky-400/40";
const META: &str = "flex min-w-0 flex-wrap items-center gap-1 text-[11px] text-zinc-500";
const NUM: &str = "shrink-0 text-right tabular-nums";
const CAPTION: &str = "mt-1 text-[11px] text-zinc-500";
const MINI: &str = "inline-flex items-center whitespace-nowrap rounded border px-1 text-[10px] leading-4 font-medium";
const CLAIM: &str = "inline-flex min-w-0 items-center gap-1 rounded border border-dashed border-zinc-400 px-1 text-[10px] leading-4 text-zinc-600 dark:border-zinc-600 dark:text-zinc-300";

fn width(volume: u64, max: u64) -> String {
    let pct = if max == 0 {
        0.0
    } else {
        (volume as f64 / max as f64 * 100.0).clamp(0.0, 100.0)
    };
    format!("width: {pct:.1}%")
}

fn claim_text(claim: &crosstalk_spec::observed::client::HarnessClaim) -> String {
    match &claim.version {
        Some(version) => format!("{} {version}", family_name(&claim.family)),
        None => family_name(&claim.family).to_owned(),
    }
}

/// The two lists in a tabbed panel. `sel` is the page's selection signal,
/// `cursor` the drawer's, `hover` the highlight preview and `tab` the
/// shown list ([`ListTab::code`]).
#[component]
pub async fn graph_lists(
    cx: &Cx,
    lists: GraphLists,
    sel: &Signal<String>,
    cursor: &Signal<String>,
    hover: &Signal<String>,
    tab: &Signal<String>,
) -> Result<impl View> {
    let sel = sel.clone();
    let tab = tab.clone();
    let cursor_clear = cursor.clone();
    let hover_clear = hover.clone();
    let agent_query = signal(cx, String::new);
    let channel_query = signal(cx, String::new);
    let agent_count = lists.agents.len().to_string();
    let channel_count = lists.channels.len().to_string();
    let mode = lists.mode;
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
                    :hidden=$(sel.get().is_empty())
                    @click=$(|_e| {
                        sel.set("".to_owned());
                        cursor_clear.set("".to_owned());
                        hover_clear.set("".to_owned());
                        raw!("(() => { const kept = location.search.slice(1).split('&').filter((p) => p !== '' && p.split('=')[0] !== 'sel'); history.replaceState(history.state, '', location.pathname + '?' + kept.join('&')); })()");
                    })
                >"Clear selection"</button>
            </div>
            <div :hidden=$(tab.get() != "agents")>
                agent_list(items: lists.agents, mode: mode, query: &agent_query, sel: &sel, cursor: cursor, hover: hover)
            </div>
            <div :hidden=$(tab.get() != "channels")>
                channel_list(items: lists.channels, mode: mode, query: &channel_query, sel: &sel, cursor: cursor, hover: hover)
            </div>
        </section>
    })
}

/// The filter box and the reactive count above a list.
#[component]
async fn list_head(
    noun: &str,
    caption: &str,
    keys: String,
    total: usize,
    query: &Signal<String>,
) -> Result<impl View> {
    let query = query.clone();
    let placeholder = format!("Filter {noun}");
    let total_text = total.to_string();
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
                <span class="shrink-0 text-[11px] tabular-nums text-zinc-500" aria-live="polite">
                    $({
                        let q = query.get();
                        raw!("((all, value, total) => { const q = String(value).trim().toLowerCase(); if (q === '') return String(total); return String(String(all).split('\\n').filter((k) => k.includes(q)).length) + ' of ' + String(total); })(${keys}, ${q}, ${total_text})", shown_label(&keys, total, &q))
                    })
                </span>
            </div>
            <p class=(CAPTION)>(caption)</p>
        </div>
    })
}

/// Shown when the filter text matches nothing in a non-empty list.
#[component]
async fn no_match(noun: &str, keys: String, query: &Signal<String>) -> Result<impl View> {
    let query = query.clone();
    let text = format!("No {noun} match the filter.");
    Ok(view! {
        <p
            class="px-2 py-3 text-xs text-zinc-500"
            :hidden=$({
                let q = query.get();
                raw!("String(${keys}).split('\\n').some((k) => k.includes(String(${q}).trim().toLowerCase()))", keys.split('\n').any(|k| matches(k, &q)))
            })
        >(text)</p>
    })
}

fn joined(keys: impl Iterator<Item = String>) -> String {
    keys.collect::<Vec<_>>().join("\n")
}

#[component]
async fn agent_list(
    items: Vec<AgentItem>,
    mode: GraphMode,
    query: &Signal<String>,
    sel: &Signal<String>,
    cursor: &Signal<String>,
    hover: &Signal<String>,
) -> Result<impl View> {
    let keys = joined(items.iter().map(AgentItem::search_key));
    let total = items.len();
    let empty = items.is_empty();
    let max = items.iter().map(AgentItem::volume).max().unwrap_or(0);
    let caption = match mode {
        GraphMode::Agents => "Agent nodes, by transmissions in and out",
        GraphMode::Channels => "Agent nodes, by transmissions in and out (not accesses)",
    };
    Ok(view! {
        list_head(noun: "agents", caption: caption, keys: keys.clone(), total: total, query: query)
        <div class=(SCROLL) data-list-scroll="agents">
            if empty {
                <p class="px-2 py-3 text-xs text-zinc-500">"No agents in this window and filter."</p>
            } else {
                <ul class=(UL)>
                    for item in items {
                        agent_row(bar: width(item.volume(), max), item: item, query: query, sel: sel, cursor: cursor, hover: hover)
                    }
                </ul>
                no_match(noun: "agents", keys: keys, query: query)
            }
        </div>
    })
}

#[component]
async fn channel_list(
    items: Vec<ChannelItem>,
    mode: GraphMode,
    query: &Signal<String>,
    sel: &Signal<String>,
    cursor: &Signal<String>,
    hover: &Signal<String>,
) -> Result<impl View> {
    let keys = joined(items.iter().map(ChannelItem::search_key));
    let total = items.len();
    let empty = items.is_empty();
    let max = items.iter().map(|c| c.carried.volume()).max().unwrap_or(0);
    let (caption, none) = match mode {
        GraphMode::Agents => (
            "Channels behind channel-routed edges, by transmissions",
            "No channel-routed transmissions in this window and filter.",
        ),
        GraphMode::Channels => (
            "Channel nodes, by reads and writes",
            "No channels in this window and filter.",
        ),
    };
    Ok(view! {
        list_head(noun: "channels", caption: caption, keys: keys.clone(), total: total, query: query)
        <div class=(SCROLL) data-list-scroll="channels">
            if empty {
                <p class="px-2 py-3 text-xs text-zinc-500">(none)</p>
            } else {
                <ul class=(UL)>
                    for item in items {
                        channel_row(bar: width(item.carried.volume(), max), item: item, query: query, sel: sel, cursor: cursor, hover: hover)
                    }
                </ul>
                no_match(noun: "channels", keys: keys, query: query)
            }
        </div>
    })
}

#[component]
async fn agent_row(
    item: AgentItem,
    bar: String,
    query: &Signal<String>,
    sel: &Signal<String>,
    cursor: &Signal<String>,
    hover: &Signal<String>,
) -> Result<impl View> {
    let (query, sel, cursor, hover) = (query.clone(), sel.clone(), cursor.clone(), hover.clone());
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
    let claim_detail = item.claims.first().map(|c| c.user_agent.clone());
    let more_claims = item.claims.len().saturating_sub(1);
    let parent = item.parent.clone();
    let title = match &item.parent {
        Some(parent) => format!("{} (sub-agent of {parent})", item.name),
        None => item.name.clone(),
    };
    Ok(view! {
        <li
            :hidden=$({
                let q = query.get();
                raw!("!String(${key}).includes(String(${q}).trim().toLowerCase())", !matches(&key, &q))
            })
        >
            <button
                type="button"
                class=(ITEM)
                data-list-item=(code.clone())
                title=(title)
                :aria-pressed=$(if sel.get() == code { "true" } else { "false" })
                @click=$(|_e| {
                    let next = if sel.get() == code { "".to_owned() } else { code.to_owned() };
                    sel.set(next.to_owned());
                    cursor.set("".to_owned());
                    hover.set("".to_owned());
                    raw!("((value) => { const v = String(value); const kept = location.search.slice(1).split('&').filter((p) => p !== '' && p.split('=')[0] !== 'sel'); if (v !== '') kept.push('sel=' + encodeURIComponent(v).replace(/%3A/g, ':')); history.replaceState(history.state, '', location.pathname + '?' + kept.join('&')); })(${next})");
                })
                @mouseenter=$(|_e| hover.set(code.to_owned()))
                @mouseleave=$(|_e| hover.set("".to_owned()))
            >
                <span class="flex min-w-0 flex-1 flex-col gap-0.5">
                    <span class="flex min-w-0 items-center gap-1.5">
                        <span class="truncate font-medium">(item.name)</span>
                        if provisional {
                            <span class=(format!("{MINI} {}", state.tone().classes()))>(state.label())</span>
                        }
                    </span>
                    <span class=(META)>
                        if let Some(parent) = parent {
                            <span class="truncate">"↳ sub-agent of " (parent)</span>
                        }
                        if let Some(claim) = first_claim {
                            <span class=(CLAIM) title=(claim_detail.unwrap_or_default())>
                                <span class="text-zinc-400">"claims"</span>
                                <span class="truncate">(claim)</span>
                            </span>
                        }
                        if more_claims > 0 {
                            <span>(format!("+{more_claims}"))</span>
                        }
                    </span>
                </span>
                <span class=(NUM)>
                    <span class="block">(volume)</span>
                    <span class="block text-[11px] text-zinc-500">(flow)</span>
                </span>
                <span class=(BAR) style=(bar) aria-hidden="true"></span>
            </button>
        </li>
    })
}

#[component]
async fn channel_row(
    item: ChannelItem,
    bar: String,
    query: &Signal<String>,
    sel: &Signal<String>,
    cursor: &Signal<String>,
    hover: &Signal<String>,
) -> Result<impl View> {
    let (query, sel, cursor, hover) = (query.clone(), sel.clone(), cursor.clone(), hover.clone());
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
    Ok(view! {
        <li
            :hidden=$({
                let q = query.get();
                raw!("!String(${key}).includes(String(${q}).trim().toLowerCase())", !matches(&key, &q))
            })
        >
            <button
                type="button"
                class=(ITEM)
                data-list-item=(code.clone())
                title=(item.name.clone())
                :aria-pressed=$(if sel.get() == code { "true" } else { "false" })
                @click=$(|_e| {
                    let next = if sel.get() == code { "".to_owned() } else { code.to_owned() };
                    sel.set(next.to_owned());
                    cursor.set("".to_owned());
                    hover.set("".to_owned());
                    raw!("((value) => { const v = String(value); const kept = location.search.slice(1).split('&').filter((p) => p !== '' && p.split('=')[0] !== 'sel'); if (v !== '') kept.push('sel=' + encodeURIComponent(v).replace(/%3A/g, ':')); history.replaceState(history.state, '', location.pathname + '?' + kept.join('&')); })(${next})");
                })
                @mouseenter=$(|_e| hover.set(code.to_owned()))
                @mouseleave=$(|_e| hover.set("".to_owned()))
            >
                <span class="flex min-w-0 flex-1 flex-col gap-0.5">
                    <span class="truncate font-mono text-[11px] font-medium">(item.name)</span>
                    <span class=(META)>
                        if let Some(policy) = policy {
                            <span class=(format!("{MINI} {}", policy.tone().classes()))>(policy.label())</span>
                        }
                    </span>
                </span>
                <span class=(NUM)>
                    <span class="block">(volume)</span>
                    <span class="block text-[11px] text-zinc-500">(detail)</span>
                </span>
                <span class=(BAR) style=(bar) aria-hidden="true"></span>
            </button>
        </li>
    })
}
