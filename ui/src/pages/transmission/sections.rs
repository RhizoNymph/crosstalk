//! The evidence page's sections: matched text side by side and the
//! co-access timeline. Both need `Content`; without it they render their
//! structure with the text replaced by the content-hidden marker. A side
//! whose body content retention dropped says so instead of showing text.

use topcoat::Result;
use topcoat::view::{View, component, view};

use super::model::{CoAccessView, MatchView, QuoteView};
use crate::components::form::{LINK, SECTION, SECTION_TITLE};
use crate::components::{content_hidden, empty_state, locator_text};

const PANE: &str = "min-w-0 rounded border border-zinc-200 dark:border-zinc-800";
const PANE_HEAD: &str = "flex items-baseline justify-between gap-2 border-b border-zinc-200 bg-zinc-50 px-2 py-1 text-[11px] text-zinc-500 dark:border-zinc-800 dark:bg-zinc-900";
const TEXT: &str =
    "max-h-72 overflow-auto whitespace-pre-wrap break-words p-2 font-mono text-xs leading-relaxed";
const MARK: &str =
    "rounded-sm bg-amber-200 px-0.5 text-zinc-950 dark:bg-amber-400/30 dark:text-amber-50";
const ELIDED: &str = "select-none italic text-zinc-400";

#[component]
async fn excerpt(title: &str, who: String, quote: QuoteView) -> Result<impl View> {
    Ok(view! {
        <div class=(PANE)>
            <div class=(PANE_HEAD)>
                <span class="font-semibold uppercase tracking-wide">(title)</span>
                <span class="truncate">(who)</span>
            </div>
            match quote {
                QuoteView::Shown(view) => {
                    <pre class=(TEXT)>
                        if let Some(cut) = view.elided_before {
                            <span class=(ELIDED)>"[… " (cut) "]\n"</span>
                        }
                        (view.before)
                        <mark class=(MARK)>(view.matched)</mark>
                        if let Some(cut) = view.highlight_cut {
                            <span class=(ELIDED)>" [… " (cut) " of the match]"</span>
                        }
                        (view.after)
                        if let Some(cut) = view.elided_after {
                            <span class=(ELIDED)>"\n[" (cut) " …]"</span>
                        }
                    </pre>
                },
                QuoteView::BodyDropped => {
                    <p class="p-2 text-xs italic text-zinc-500" data-body-dropped="true">
                        "Body dropped: content retention removed this message's text. The match, its location and its size are still recorded."
                    </p>
                },
            }
        </div>
    })
}

/// Each match: its kind, carrier and size, then the sender's originated
/// text and the reader's input with the matched part highlighted.
#[component]
pub async fn matches_section(matches: Option<Vec<MatchView>>, reader: String) -> Result<impl View> {
    let none = matches.as_ref().is_some_and(Vec::is_empty);
    Ok(view! {
        <section class=(SECTION)>
            <h2 class=(SECTION_TITLE)>"Content matches"</h2>
            match matches {
                None => {
                    <div class="grid gap-3 md:grid-cols-2">
                        <div class=(format!("{PANE} p-3"))>
                            <div class="mb-1 text-[11px] font-semibold uppercase tracking-wide text-zinc-500">"Sender originated"</div>
                            content_hidden()
                        </div>
                        <div class=(format!("{PANE} p-3"))>
                            <div class="mb-1 text-[11px] font-semibold uppercase tracking-wide text-zinc-500">"Reader read"</div>
                            content_hidden()
                        </div>
                    </div>
                    <p class="mt-2 text-xs text-zinc-500">"Matched text, match kinds and co-accesses need the Content permission."</p>
                },
                Some(_) if none => empty_state(message: "No content match: this transmission rests on the access pattern alone."),
                Some(matches) => {
                    <div class="space-y-4">
                        for m in matches {
                            <article>
                                <div class="mb-1.5 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs">
                                    <span class="font-semibold">"Match " (m.number)</span>
                                    <span class="rounded border border-zinc-300 px-1.5 py-0.5 font-mono text-[11px] dark:border-zinc-700">(m.kind)</span>
                                    <span class="text-zinc-500">(m.carrier)</span>
                                    <span class="ml-auto tabular-nums text-zinc-500">(m.matched) " matched"</span>
                                    <a class=(LINK) href=(m.sender_turn) data-sender-turn="true">"in sender's conversation"</a>
                                    <a class=(LINK) href=(m.reader_turn) data-reader-turn="true">"in reader's conversation"</a>
                                </div>
                                <div class="grid gap-3 md:grid-cols-2">
                                    excerpt(title: "Sender originated", who: m.sender.name, quote: m.origin)
                                    excerpt(title: "Reader read", who: reader.clone(), quote: m.read)
                                </div>
                            </article>
                        }
                    </div>
                },
            }
        </section>
    })
}

/// Write → read pairs with the lag between them.
#[component]
pub async fn co_access_section(records: Option<Vec<CoAccessView>>) -> Result<impl View> {
    let none = records.as_ref().is_some_and(Vec::is_empty);
    Ok(view! {
        <section class=(SECTION)>
            <h2 class=(SECTION_TITLE)>"Co-access timeline"</h2>
            match records {
                None => <p class="text-xs text-zinc-500">"Hidden without the Content permission."</p>,
                Some(_) if none => <p class="text-xs text-zinc-500">"No co-access: the text did not travel through a resource both agents touched."</p>,
                Some(records) => {
                    <ol class="space-y-2">
                        for record in records {
                            <li class="grid grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)] items-center gap-3 rounded border border-zinc-200 p-2 text-xs dark:border-zinc-800">
                                access_side(label: "write", side: record.write)
                                <div class="flex min-w-36 flex-col items-center text-zinc-500">
                                    <span class="tabular-nums">(record.lag)</span>
                                    <span class="flex w-full items-center" aria-hidden="true">
                                        <span class="h-2 w-2 rounded-full bg-route-channel"></span>
                                        <span class="h-px flex-1 bg-zinc-300 dark:bg-zinc-700"></span>
                                        <span class="text-zinc-400">"▶"</span>
                                        <span class="h-2 w-2 rounded-full bg-route-channel"></span>
                                    </span>
                                    <span class="text-[10px] uppercase tracking-wide">"lag"</span>
                                </div>
                                access_side(label: "read", side: record.read)
                            </li>
                        }
                    </ol>
                },
            }
        </section>
    })
}

#[component]
async fn access_side(label: &str, side: Option<super::model::AccessSide>) -> Result<impl View> {
    Ok(view! {
        <div class="min-w-0">
            <div class="text-[10px] font-semibold uppercase tracking-wide text-zinc-500">(label)</div>
            match side {
                Some(side) => {
                    <div><a class=(LINK) href=(side.agent.url)>(side.agent.name)</a> <span class="text-zinc-500">" at " (side.at)</span></div>
                    if let Some(locator) = side.resource {
                        <div class="truncate">locator_text(locator: &locator)</div>
                    }
                },
                None => <span class="italic text-zinc-400">"access not available"</span>,
            }
        </div>
    })
}
