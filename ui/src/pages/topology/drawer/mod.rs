//! The topology drawer: a shard that re-renders on the server whenever the
//! page's selection signal changes.
//!
//! For an edge it shows the route, the edge's stats and share, and the
//! transmissions counted into it (`edge_transmissions`, newest confirmation
//! first, paged with a cursor signal); for an agent
//! or a channel a summary card with its heaviest edges; with nothing
//! selected, the heaviest edges of the view. Edges listed here select
//! themselves on click, updating the signal (and so the graph's highlight
//! and, through the page's one sync binding, the URL's `sel`).
//!
//! Shard endpoints run without the page's guards: the shard builds the
//! caller, checks `View` and validates every argument itself
//! ([`model::load`]).

pub mod model;

use topcoat::Result;
use topcoat::context::Cx;
use topcoat::runtime::{Signal, shard};
use topcoat::view::{View, view};

use self::model::{Drawer, EdgeItem, EdgeRow, load};
use crate::app::caller;
use crate::components::form::{LINK, SMALL_BUTTON};
use crate::components::{
    Tone, claim_badge, empty_state, error_panel, kind_badge, route_badge, state_badge,
};
use crate::pages::channels::list::Activity;

const CARD: &str =
    "rounded border border-zinc-200 bg-white p-3 dark:border-zinc-800 dark:bg-zinc-950";
const HEAD: &str = "mb-1.5 text-[11px] font-semibold uppercase tracking-wide text-zinc-500";
const DL: &str = "grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 text-xs";
const DT: &str = "text-zinc-500";
const DD: &str = "tabular-nums text-zinc-900 dark:text-zinc-100";

#[shard]
pub async fn topology_drawer(
    cx: &Cx,
    state: String,
    sel: Signal<String>,
    cursor: Signal<String>,
) -> Result<impl View> {
    let caller = caller(cx);
    let selected = sel.get();
    let at = cursor.get();
    let loaded = load(cx, &caller, &state, &selected, &at).await;
    Ok(view! {
        <div class="space-y-3" data-drawer=(selected.clone())>
            match loaded {
                Err(error) => error_panel(error: &error),
                Ok((_, Drawer::Missing(message))) => empty_state(message: message),
                Ok((_, Drawer::Empty { heaviest })) => {
                    <div class=(CARD)>
                        <p class="text-sm text-zinc-600 dark:text-zinc-400">
                            "Select an edge, agent or channel in the graph."
                        </p>
                    </div>
                    edge_list(title: "Heaviest edges", items: heaviest, sel: &sel, cursor: &cursor)
                },
                Ok((_, Drawer::Edge(panel))) => {
                    let paged = panel.paged;
                    let next = panel.next.clone();
                    let empty = panel.rows.is_empty();
                    <div class=(CARD)>
                        <div class=(HEAD)>"Edge"</div>
                        <p class="text-sm font-medium">
                            <a class=(LINK) href=(panel.from.url)>(panel.from.name)</a>
                            <span class="px-1 text-zinc-400">"→"</span>
                            <a class=(LINK) href=(panel.to.url)>(panel.to.name)</a>
                        </p>
                        <div class="mt-1 flex min-w-0 items-center gap-1.5 text-xs">
                            route_badge(kind: panel.route_kind)
                            match panel.route_url {
                                Some(url) => <a class=(format!("{LINK} truncate font-mono")) href=(url)>(panel.route)</a>,
                                None => <span class="truncate text-zinc-600 dark:text-zinc-400">(panel.route)</span>,
                            }
                        </div>
                        match panel.stats {
                            Some(stats) => {
                                <dl class=(format!("{DL} mt-2"))>
                                    <dt class=(DT)>"transmissions"</dt><dd class=(DD)>(stats.transmissions)</dd>
                                    <dt class=(DT)>"matched"</dt><dd class=(DD)>(stats.matched)</dd>
                                    <dt class=(DT)>"share"</dt><dd class=(DD)>(stats.share) <span class="text-zinc-500">" of " (stats.of)</span></dd>
                                </dl>
                            },
                            None => <p class="mt-2 text-xs text-zinc-500">"No confirmed transmissions on this edge in the window and filter."</p>,
                        }
                        <a class=(format!("{LINK} mt-2 inline-block text-xs")) href=(panel.focus_url)>"Filter to these two agents"</a>
                    </div>
                    <div>
                        <div class=(HEAD)>"Transmissions on this edge"</div>
                        if empty {
                            empty_state(message: "None in this window and filter.")
                        } else {
                            compact_rows(rows: panel.rows)
                        }
                        <div class="mt-2 flex gap-2">
                            if paged {
                                <button type="button" class=(SMALL_BUTTON) @click=$(|_e| cursor.set("".to_owned()))>"« First"</button>
                            }
                            if let Some(next) = next {
                                <button type="button" class=(SMALL_BUTTON) @click=$(|_e| cursor.set(next.to_owned()))>"Next »"</button>
                            }
                        </div>
                    </div>
                },
                Ok((_, Drawer::Agent(panel))) => {
                    <div class=(CARD)>
                        <div class=(HEAD)>"Agent"</div>
                        <p class="flex items-center gap-2 text-sm font-medium">
                            <a class=(LINK) href=(panel.url.clone())>(panel.name)</a>
                            kind_badge(value: panel.state)
                        </p>
                        if let Some(parent) = panel.parent {
                            <p class="mt-0.5 text-xs text-zinc-500">"sub-agent of " <a class=(LINK) href=(parent.url)>(parent.name)</a></p>
                        }
                        <div class="mt-1.5 flex flex-wrap gap-1">
                            for claim in panel.claims {
                                claim_badge(claim: &claim)
                            }
                        </div>
                        <dl class=(format!("{DL} mt-2"))>
                            <dt class=(DT)>"transmissions in"</dt><dd class=(DD)>(panel.transmissions_in)</dd>
                            <dt class=(DT)>"transmissions out"</dt><dd class=(DD)>(panel.transmissions_out)</dd>
                            <dt class=(DT)>"last seen"</dt><dd class=(DD)>(panel.last_seen)</dd>
                        </dl>
                        <div class="mt-2 flex gap-3 text-xs">
                            <a class=(LINK) href=(panel.url)>"Open agent page"</a>
                            <a class=(LINK) href=(panel.conversations_url) data-conversations="true">"Conversations"</a>
                            <a class=(LINK) href=(panel.focus_url)>"Filter to this agent"</a>
                        </div>
                    </div>
                    edge_list(title: "Its heaviest edges", items: panel.edges, sel: &sel, cursor: &cursor)
                },
                Ok((_, Drawer::Channel(panel))) => {
                    let [writers, readers, transmissions] = panel.activity.cells();
                    let last_activity = panel.activity.last();
                    let superseded = panel.activity == Activity::Superseded;
                    <div class=(CARD)>
                        <div class=(HEAD)>"Channel"</div>
                        <p class="break-all font-mono text-sm font-medium">
                            <a class=(LINK) href=(panel.url.clone())>(panel.name)</a>
                        </p>
                        <div class="mt-1.5 flex flex-wrap gap-1">
                            kind_badge(value: panel.origin)
                            kind_badge(value: panel.detection)
                            if let Some(listing) = panel.listing {
                                kind_badge(value: listing)
                            }
                            kind_badge(value: panel.policy)
                            if superseded {
                                state_badge(label: "superseded", tone: Tone::Muted)
                            }
                        </div>
                        <dl class=(format!("{DL} mt-2"))>
                            <dt class=(DT)>"writers"</dt><dd class=(DD)>(writers)</dd>
                            <dt class=(DT)>"readers"</dt><dd class=(DD)>(readers)</dd>
                            <dt class=(DT)>"transmissions"</dt><dd class=(DD)>(transmissions)</dd>
                            <dt class=(DT)>"last activity"</dt><dd class=(DD)>(last_activity)</dd>
                        </dl>
                        <div class="mt-2 flex gap-3 text-xs">
                            <a class=(LINK) href=(panel.url)>"Open channel page"</a>
                            <a class=(LINK) href=(panel.focus_url)>"Filter to this channel"</a>
                        </div>
                    </div>
                    edge_list(title: "Edges through it", items: panel.edges, sel: &sel, cursor: &cursor)
                },
            }
        </div>
    })
}

/// Edges that select themselves on click.
#[topcoat::view::component]
async fn edge_list(
    title: &str,
    items: Vec<EdgeItem>,
    sel: &Signal<String>,
    cursor: &Signal<String>,
) -> Result<impl View> {
    let empty = items.is_empty();
    let sel = sel.clone();
    let cursor = cursor.clone();
    Ok(view! {
        <div>
            <div class=(HEAD)>(title)</div>
            if empty {
                empty_state(message: "No edges in this window and filter.")
            } else {
                <ul class="divide-y divide-zinc-100 rounded border border-zinc-200 dark:divide-zinc-800 dark:border-zinc-800">
                    #[key(item.code.clone())]
                    for item in items {
                        let code = item.code.clone();
                        <li>
                            <button
                                type="button"
                                class="flex w-full items-center gap-2 px-2 py-1.5 text-left text-xs hover:bg-zinc-50 dark:hover:bg-zinc-900"
                                data-select=(item.code)
                                @click=$(|_e| {
                                    sel.set(code.to_owned());
                                    cursor.set("".to_owned());
                                })
                            >
                                route_badge(kind: item.route_kind)
                                <span class="min-w-0 flex-1 truncate">
                                    (item.from) <span class="text-zinc-400">" → "</span> (item.to)
                                    <span class="block truncate text-[11px] text-zinc-500">(item.route)</span>
                                </span>
                                <span class="shrink-0 text-right tabular-nums">
                                    (item.share)
                                    <span class="block text-[11px] text-zinc-500">(item.transmissions) " tx"</span>
                                </span>
                            </button>
                        </li>
                    }
                </ul>
            }
        </div>
    })
}

/// The edge's transmissions as a narrow table: when each was confirmed
/// (linking to its evidence) and its matched bytes.
#[topcoat::view::component]
async fn compact_rows(rows: Vec<EdgeRow>) -> Result<impl View> {
    Ok(view! {
        <table class="w-full border-collapse overflow-hidden rounded border border-zinc-200 text-xs dark:border-zinc-800">
            <thead class="bg-zinc-50 text-left text-[10px] uppercase tracking-wide text-zinc-500 dark:bg-zinc-900">
                <tr>
                    <th class="px-2 py-1 font-medium">"Confirmed"</th>
                    <th class="px-2 py-1 text-right font-medium">"Matched"</th>
                </tr>
            </thead>
            <tbody class="divide-y divide-zinc-100 dark:divide-zinc-800">
                for row in rows {
                    <tr class="hover:bg-zinc-50 dark:hover:bg-zinc-900/60">
                        <td class="whitespace-nowrap px-2 py-1">
                            <a class=(LINK) href=(row.url)>(row.confirmed)</a>
                        </td>
                        <td class="px-2 py-1 text-right tabular-nums">(row.matched)</td>
                    </tr>
                }
            </tbody>
        </table>
    })
}
