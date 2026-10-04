//! The outcome of an action, shown after the redirect back to the page.

use topcoat::Result;
use topcoat::view::{View, component, view};

#[component]
pub async fn flash_banner(message: &str) -> Result<impl View> {
    Ok(view! {
        <div
            role="status"
            class="mb-4 rounded border border-emerald-300 bg-emerald-50 px-3 py-2 text-sm text-emerald-800 dark:border-emerald-800 dark:bg-emerald-950 dark:text-emerald-200"
        >
            (message)
        </div>
    })
}
