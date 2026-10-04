//! In-page navigation: tabs and filter chips. Both are plain links, so the
//! URL always holds the current choice.

use topcoat::Result;
use topcoat::view::{View, component, view};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tab {
    pub label: String,
    pub href: String,
    pub active: bool,
}

fn tab_classes(active: bool) -> &'static str {
    if active {
        "-mb-px border-b-2 border-sky-600 px-3 py-1.5 font-medium text-zinc-900 dark:border-sky-400 dark:text-zinc-100"
    } else {
        "-mb-px border-b-2 border-transparent px-3 py-1.5 text-zinc-500 hover:text-zinc-800 dark:hover:text-zinc-200"
    }
}

#[component]
pub async fn tabs(items: Vec<Tab>) -> Result<impl View> {
    Ok(view! {
        <nav class="mb-4 flex gap-1 border-b border-zinc-200 text-sm dark:border-zinc-800">
            for tab in items {
                <a
                    href=(tab.href)
                    class=(tab_classes(tab.active))
                    aria-current=(tab.active.then_some("page"))
                >(tab.label)</a>
            }
        </nav>
    })
}

fn chip_classes(active: bool) -> &'static str {
    if active {
        "inline-flex items-center rounded-full border border-sky-600 bg-sky-50 px-2 py-0.5 text-xs text-sky-800 dark:border-sky-500 dark:bg-sky-950 dark:text-sky-200"
    } else {
        "inline-flex items-center rounded-full border border-zinc-300 px-2 py-0.5 text-xs text-zinc-600 hover:border-zinc-400 dark:border-zinc-700 dark:text-zinc-400"
    }
}

/// A link that toggles one filter value.
#[component]
pub async fn filter_chip(label: &str, href: String, active: bool) -> Result<impl View> {
    Ok(view! {
        <a href=(href) class=(chip_classes(active)) aria-pressed=(if active { "true" } else { "false" })>
            (label)
        </a>
    })
}

fn segment_classes(active: bool) -> &'static str {
    if active {
        "px-2 py-0.5 bg-zinc-800 text-white dark:bg-zinc-200 dark:text-zinc-900"
    } else {
        "px-2 py-0.5 text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-800"
    }
}

/// A segmented control: mutually exclusive choices as links, the current
/// one filled.
#[component]
pub async fn segmented(label: &str, items: Vec<Tab>) -> Result<impl View> {
    Ok(view! {
        <div class="inline-flex overflow-hidden rounded border border-zinc-300 text-xs dark:border-zinc-700" role="group" aria-label=(label)>
            for item in items {
                <a
                    href=(item.href)
                    class=(segment_classes(item.active))
                    aria-current=(item.active.then_some("true"))
                >(item.label)</a>
            }
        </div>
    })
}
