//! Form controls. Classes are literal strings so Tailwind finds them.

use topcoat::Result;
use topcoat::view::{View, component, view};

use super::href::state_pairs;
use crate::url::view_state::ViewState;

pub const INPUT: &str = "rounded border border-zinc-300 bg-white px-2 py-1 text-sm focus:border-sky-500 focus:outline-none dark:border-zinc-700 dark:bg-zinc-900";
pub const LABEL: &str = "block text-xs font-medium text-zinc-500";
pub const BUTTON: &str = "inline-flex items-center rounded border border-zinc-300 bg-white px-2.5 py-1 text-sm hover:bg-zinc-50 dark:border-zinc-700 dark:bg-zinc-900 dark:hover:bg-zinc-800";
pub const BUTTON_PRIMARY: &str = "inline-flex items-center rounded border border-sky-700 bg-sky-700 px-2.5 py-1 text-sm font-medium text-white hover:bg-sky-800 dark:border-sky-600 dark:bg-sky-600 dark:hover:bg-sky-500";
pub const SMALL_BUTTON: &str = "inline-flex items-center rounded border border-zinc-300 bg-white px-1.5 py-0.5 text-xs hover:bg-zinc-50 dark:border-zinc-700 dark:bg-zinc-900 dark:hover:bg-zinc-800";
pub const LINK: &str = "text-sky-700 hover:underline dark:text-sky-400";
pub const SECTION: &str = "mb-6";
pub const SECTION_TITLE: &str = "mb-2 text-sm font-semibold uppercase tracking-wide text-zinc-500";
/// The label in front of a row of filter chips.
pub const FACET: &str = "text-[11px] font-semibold uppercase tracking-wide text-zinc-500";
pub const PANEL: &str = "rounded border border-zinc-200 p-3 dark:border-zinc-800";

/// The view state as hidden inputs, for `GET` forms.
#[component]
pub async fn state_inputs(state: &ViewState) -> Result<impl View> {
    let pairs = state_pairs(state);
    Ok(view! {
        for (name, value) in pairs {
            <input type="hidden" name=(name) value=(value)>
        }
    })
}
