//! The conversation page's pieces: the head, the window links, and each
//! turn with its boundaries, messages, parts, text and provenance marks.
//!
//! Text appears only when the page read it (`Content`); otherwise each part
//! shows the content-hidden marker and its marks give byte ranges. Harness
//! claims are only ever shown through `claim_badge`.

use topcoat::Result;
use topcoat::view::{View, component, view};

use super::model::{
    BoundaryView, HeadView, InboundView, MarkView, MessageView, OriginView, OriginatedView,
    OutcomeView, PartView, ReaderView, RelayedView, Segment, TextView, Tone, TurnView, WindowView,
};
use crate::components::claim_badge;
use crate::components::content_hidden;
use crate::components::form::{LINK, SECTION};

const CARD: &str = "rounded border border-zinc-200 dark:border-zinc-800";
const META: &str = "text-xs text-zinc-500";
const CHIP: &str = "inline-flex items-center rounded border border-zinc-300 px-1.5 py-0.5 text-[11px] dark:border-zinc-700";
const BOUNDARY: &str = "my-3 flex items-center gap-2 text-xs text-zinc-500 before:h-px before:flex-1 before:bg-zinc-300 after:h-px after:flex-1 after:bg-zinc-300 dark:before:bg-zinc-700 dark:after:bg-zinc-700";
const TEXT: &str =
    "max-h-96 overflow-auto whitespace-pre-wrap break-words font-mono text-xs leading-relaxed";
const REPLAYED: &str = "inline-flex items-center rounded border border-violet-300 bg-violet-50 px-1.5 py-0.5 text-[11px] text-violet-800 dark:border-violet-800 dark:bg-violet-950/60 dark:text-violet-200";

fn tone_class(tones: &[Tone]) -> String {
    let mut classes = Vec::new();
    for tone in tones {
        classes.push(match tone {
            Tone::Inbound => {
                "rounded-sm bg-amber-200 text-zinc-950 dark:bg-amber-400/30 dark:text-amber-50"
            }
            Tone::Originated => "rounded-sm bg-sky-100 dark:bg-sky-400/20",
            Tone::Relayed => "rounded-sm bg-violet-100 dark:bg-violet-400/20",
            Tone::Highlight => {
                "outline outline-2 outline-offset-1 outline-sky-600 dark:outline-sky-400"
            }
        });
    }
    classes.join(" ")
}

/// The badge a replayed conversation or turn carries.
#[component]
pub async fn replayed_badge(corpus: String) -> Result<impl View> {
    Ok(view! {
        <span class=(REPLAYED) data-replayed=(corpus.clone())>"replayed: " (corpus)</span>
    })
}

#[component]
pub async fn head_section(head: HeadView) -> Result<impl View> {
    Ok(view! {
        <section class=(format!("{SECTION} {CARD} p-3"))>
            <div class="flex flex-wrap items-baseline gap-x-3 gap-y-1">
                <h1 class="text-lg font-semibold" title=(head.full.clone())>"Conversation " (head.short)</h1>
                <a class=(LINK) href=(head.agent.url)>(head.agent.name)</a>
                for claim in head.claims.iter() {
                    claim_badge(claim: claim)
                }
                if let Some(corpus) = head.replayed {
                    replayed_badge(corpus: corpus)
                }
            </div>
            <dl class="mt-2 grid grid-cols-2 gap-x-6 gap-y-1 text-xs sm:grid-cols-4">
                <div><dt class="text-zinc-500">"Started"</dt><dd>(head.started)</dd></div>
                <div><dt class="text-zinc-500">"Last turn"</dt><dd>(head.last)</dd></div>
                <div><dt class="text-zinc-500">"Turns"</dt><dd class="tabular-nums">(head.turns)</dd></div>
                <div>
                    <dt class="text-zinc-500">"Transmissions"</dt>
                    <dd class="tabular-nums">(head.received) " received · " (head.sent) " sent"</dd>
                </div>
            </dl>
            <ul class="mt-2 space-y-1 text-xs">
                <li data-origin="true">
                    <span class="text-zinc-500">"Origin: "</span>
                    match head.origin {
                        OriginView::Root => "started here",
                        OriginView::Fork { parent, branch, shared } => {
                            "forked from "
                            <a class=(LINK) href=(parent.url)>(parent.label)</a>
                            if let Some(branch) = branch {
                                " after "
                                <a class=(LINK) href=(branch.url)>(branch.label)</a>
                            }
                            " (" (shared) " shared messages)"
                        },
                        OriginView::Compaction { predecessor, carried } => {
                            "compaction of "
                            <a class=(LINK) href=(predecessor.url)>(predecessor.label)</a>
                            " (" (carried) " messages carried over)"
                        },
                    }
                </li>
                if let Some(spawned) = head.spawned_by {
                    <li data-spawned-by="true">
                        <span class="text-zinc-500">"Spawned by: "</span>
                        <a class=(LINK) href=(spawned.parent.url)>(spawned.parent.name)</a>
                        ", "
                        <a class=(LINK) href=(spawned.turn.url)>(spawned.turn.label)</a>
                        " (delegation, "
                        <a class=(LINK) href=(spawned.transmission.url)>(spawned.transmission.label)</a>
                        ")"
                    </li>
                }
                if !head.successors.is_empty() {
                    <li data-successors="true">
                        <span class="text-zinc-500">"Continued in: "</span>
                        for (i, next) in head.successors.into_iter().enumerate() {
                            if i > 0 { " · " }
                            (next.kind) " "
                            <a class=(LINK) href=(next.link.url)>(next.link.label)</a>
                            <span class="text-zinc-500">" (" (next.started) ")"</span>
                        }
                    </li>
                }
            </ul>
        </section>
    })
}

#[component]
pub async fn window_nav(window: WindowView) -> Result<impl View> {
    let shown = if window.to > window.from {
        format!(
            "turns {}–{} of {}",
            window.from,
            window.to - 1,
            window.total
        )
    } else {
        format!("{} turns", window.total)
    };
    Ok(view! {
        <nav class="my-2 flex items-center justify-between gap-2 text-xs" aria-label="Turns">
            match window.earlier {
                Some(url) => <a class=(LINK) href=(url) rel="prev">"« earlier turns"</a>,
                None => <span class="text-zinc-400">"« earlier turns"</span>,
            }
            <span class="text-zinc-500 tabular-nums">(shown)</span>
            match window.later {
                Some(url) => <a class=(LINK) href=(url) rel="next">"later turns »"</a>,
                None => <span class="text-zinc-400">"later turns »"</span>,
            }
        </nav>
        if let Some(asked) = window.past_end {
            <p class="mb-2 text-xs text-zinc-500" data-past-end="true">
                "Turn " (asked) " does not exist yet: this conversation has " (window.total) " turns."
            </p>
        }
    })
}

#[component]
async fn boundary_row(boundary: BoundaryView) -> Result<impl View> {
    Ok(view! {
        <div class=(BOUNDARY) role="separator">
            match boundary {
                BoundaryView::Compaction { predecessor, carried } => {
                    <span data-boundary="compaction">
                        "compaction boundary: " (carried) " messages carried over from "
                        <a class=(LINK) href=(predecessor.url)>(predecessor.label)</a>
                    </span>
                },
                BoundaryView::Fork { parent, branch } => {
                    <span data-boundary="fork">
                        "branches from "
                        <a class=(LINK) href=(parent.url)>(parent.label)</a>
                        if let Some(branch) = branch {
                            " after "
                            <a class=(LINK) href=(branch.url)>(branch.label)</a>
                        }
                    </span>
                },
                BoundaryView::UnseenHistory { connection } => {
                    <span data-boundary="unseen-history">
                        "history before this turn was not seen by the gateway"
                        if let Some(connection) = connection {
                            " (WebSocket connection " (connection) ")"
                        }
                    </span>
                },
            }
        </div>
    })
}

#[component]
pub async fn turn_card(turn: TurnView) -> Result<impl View> {
    let anchor = turn.anchor();
    let outcome = match &turn.outcome {
        OutcomeView::Completed { stop, .. } => format!("completed ({stop})"),
        OutcomeView::Failed { failure, .. } => format!("failed: {failure}"),
    };
    let failed = matches!(turn.outcome, OutcomeView::Failed { .. });
    Ok(view! {
        for b in turn.boundaries {
            boundary_row(boundary: b)
        }
        <article id=(anchor.clone()) class=(format!("{CARD} mb-3 scroll-mt-4")) data-turn=(turn.index)>
            <header class="flex flex-wrap items-center gap-x-3 gap-y-1 border-b border-zinc-200 bg-zinc-50 px-3 py-1.5 text-xs dark:border-zinc-800 dark:bg-zinc-900">
                <a class="font-semibold" href=(format!("#{anchor}"))>"Turn " (turn.index)</a>
                <span class="tabular-nums text-zinc-500">(turn.time)</span>
                <span>(turn.model)</span>
                <span class="text-zinc-500">(turn.transport)</span>
                if let Some(connection) = turn.connection {
                    <span class="text-zinc-500">"connection " (connection)</span>
                }
                <span class=(if failed { "text-red-700 dark:text-red-400" } else { "text-zinc-500" })>(outcome)</span>
                if let Some(usage) = turn.usage {
                    <span class="tabular-nums text-zinc-500">(usage)</span>
                }
                if let Some(agent) = turn.agent {
                    <a class=(LINK) href=(agent.url)>(agent.name)</a>
                }
                if let Some(claim) = turn.claim.as_ref() {
                    claim_badge(claim: claim)
                }
                if let Some(corpus) = turn.replayed {
                    replayed_badge(corpus: corpus)
                }
                <span class="ml-auto">
                    if turn.pending {
                        <span class="italic text-zinc-500" data-provenance="pending">"provenance: scan in progress"</span>
                    } else {
                        <span class="text-zinc-400" data-provenance="scanned">"provenance: scanned"</span>
                    }
                </span>
            </header>
            <div class=(if turn.pending { "space-y-2 p-3 opacity-80" } else { "space-y-2 p-3" })>
                if !turn.carried.is_empty() {
                    <details class="rounded border border-dashed border-zinc-300 p-2 text-xs dark:border-zinc-700" data-carried="true">
                        <summary class="cursor-pointer text-zinc-500">"carried over (" (turn.carried.len()) ")"</summary>
                        <div class="mt-2 space-y-2">
                            for message in turn.carried {
                                message_block(message: message, output: false)
                            }
                        </div>
                    </details>
                }
                for message in turn.inputs {
                    message_block(message: message, output: false)
                }
                if let Some(message) = turn.output {
                    message_block(message: message, output: true)
                }
            </div>
        </article>
    })
}

#[component]
async fn message_block(message: MessageView, output: bool) -> Result<impl View> {
    let arrow = if output { "◂" } else { "▸" };
    Ok(view! {
        <div class=(if output { "border-l-2 border-sky-400 pl-2" } else { "border-l-2 border-zinc-200 pl-2 dark:border-zinc-700" })
             data-role=(message.role.clone())>
            <div class=(META)>
                (arrow) " " (message.role)
                if output { " output" }
                if message.system_turn {
                    " "
                    <span class=(CHIP) data-system-turn="true">"system turn"</span>
                }
            </div>
            for part in message.parts {
                part_block(part: part)
            }
        </div>
    })
}

#[component]
async fn part_block(part: PartView) -> Result<impl View> {
    Ok(view! {
        <div class="mt-1">
            <div class="flex flex-wrap items-center gap-2 text-xs">
                <span class=(CHIP)>(part.kind)</span>
                if let Some(detail) = part.detail {
                    <span class="font-mono">(detail)</span>
                }
                if let Some(size) = part.size {
                    <span class="tabular-nums text-zinc-500">"(" (size) ")"</span>
                }
            </div>
            match part.text {
                TextView::Hidden => <div class="mt-1">content_hidden()</div>,
                TextView::NoText => "",
                TextView::Dropped => {
                    <p class="mt-1 text-xs italic text-zinc-500" data-body-dropped="true">
                        "Body dropped by content retention. Its marks and sizes are still recorded."
                    </p>
                },
                TextView::Shown { segments, remaining } => {
                    <pre class=(format!("{TEXT} mt-1 rounded bg-zinc-50 p-2 dark:bg-zinc-900"))>
                        for segment in segments {
                            text_run(segment: segment)
                        }
                        if remaining > 0 {
                            <span class="select-none italic text-zinc-400">"\n[… " (remaining) " more bytes]"</span>
                        }
                    </pre>
                },
            }
            if !part.marks.is_empty() {
                <ul class="mt-1 space-y-1">
                    for mark in part.marks {
                        mark_row(mark: mark)
                    }
                </ul>
            }
        </div>
    })
}

#[component]
async fn text_run(segment: Segment) -> Result<impl View> {
    Ok(view! {
        if segment.tones.is_empty() {
            (segment.text)
        } else {
            <mark class=(tone_class(&segment.tones))>(segment.text)</mark>
        }
    })
}

#[component]
async fn mark_row(mark: MarkView) -> Result<impl View> {
    Ok(view! {
        match mark {
            MarkView::Inbound(inbound) => inbound_row(mark: inbound),
            MarkView::Originated(originated) => originated_row(mark: originated),
            MarkView::Relayed(relayed) => relayed_row(mark: relayed),
        }
    })
}

#[component]
async fn inbound_row(mark: InboundView) -> Result<impl View> {
    Ok(view! {
        <li class="flex flex-wrap items-baseline gap-x-2 rounded bg-amber-50 px-2 py-1 text-xs dark:bg-amber-950/40" data-mark="inbound">
            <span aria-hidden="true">"⟵"</span>
            match mark.delegation {
                Some(delegation) => <span class="font-medium">(delegation)</span>,
                None => <span>"from"</span>,
            }
            <a class=(LINK) href=(mark.from.url)>(mark.from.name)</a>
            if let Some(route) = mark.route {
                match mark.route_url {
                    Some(url) => <span>"via " <a class=(LINK) href=(url)>(route)</a></span>,
                    None => <span class="text-zinc-500">(route)</span>,
                }
            }
            <span class="text-zinc-500">(mark.kind) " · " (mark.matched) " · " (mark.carrier) " · " (mark.range)</span>
            if let Some(url) = mark.sender_turn {
                <a class=(LINK) href=(url)>"sender's turn"</a>
            }
            match mark.transmission {
                Some(link) => {
                    <a class=(LINK) href=(link.url) data-evidence="true">(link.label) " ↗"</a>
                    if let Some(state) = mark.state {
                        <span class="text-zinc-500">(state)</span>
                    }
                },
                None => <span class="italic text-zinc-500">"content match, no transmission"</span>,
            }
        </li>
    })
}

#[component]
async fn originated_row(mark: OriginatedView) -> Result<impl View> {
    let class = if mark.highlighted {
        "rounded bg-sky-50 px-2 py-1 text-xs outline outline-2 outline-sky-600 dark:bg-sky-950/40 dark:outline-sky-400"
    } else {
        "rounded bg-sky-50 px-2 py-1 text-xs dark:bg-sky-950/40"
    };
    let none = mark.readers.is_empty();
    Ok(view! {
        <li class=(class) data-mark="originated">
            <div class="flex flex-wrap items-baseline gap-x-2">
                <span aria-hidden="true">"⟶"</span>
                <span>"originated"</span>
                <span class="text-zinc-500">(mark.status) " · " (mark.range)</span>
                if none {
                    <span class="text-zinc-500">
                        if mark.expired { "no reader detected before it expired" } else { "not read by another agent yet" }
                    </span>
                } else {
                    <span>"read later by"</span>
                }
            </div>
            if !none {
                <ul class="mt-1 flex flex-wrap gap-x-3 gap-y-1">
                    for reader in mark.readers {
                        reader_item(reader: reader)
                    }
                    if mark.more > 0 {
                        <li>
                            match mark.more_url {
                                Some(url) => <a class=(LINK) href=(url)>"+" (mark.more) " more"</a>,
                                None => <span class="text-zinc-500">"+" (mark.more) " more"</span>,
                            }
                        </li>
                    }
                </ul>
            }
        </li>
    })
}

#[component]
async fn reader_item(reader: ReaderView) -> Result<impl View> {
    Ok(view! {
        <li data-reader="true">
            if let Some(delegation) = reader.delegation {
                <span class="font-medium">(delegation) " "</span>
            }
            <a class=(LINK) href=(reader.agent.url)>(reader.agent.name)</a>
            if let Some(url) = reader.turn {
                " (" <a class=(LINK) href=(url)>"turn"</a> ")"
            }
            if let Some(link) = reader.transmission {
                " " <a class=(LINK) href=(link.url)>(link.label) " ↗"</a>
            }
            <span class="text-zinc-500">" " (reader.carrier)</span>
        </li>
    })
}

#[component]
async fn relayed_row(mark: RelayedView) -> Result<impl View> {
    Ok(view! {
        <li class="flex flex-wrap items-baseline gap-x-2 rounded bg-violet-50 px-2 py-1 text-xs dark:bg-violet-950/40" data-mark="relayed">
            <span aria-hidden="true">"⟳"</span>
            <span>"relayed"</span>
            match mark.source {
                Some(link) => <span>"from " <a class=(LINK) href=(link.url)>(link.label)</a></span>,
                None => <span class="text-zinc-500">"from an input that is not another agent's span"</span>,
            }
            <span class="text-zinc-500">(mark.range)</span>
        </li>
    })
}
