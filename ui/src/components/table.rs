//! Dense data tables. Rows are written by the page; cells use the class
//! constants here so every table reads the same.

use topcoat::Result;
use topcoat::view::{Child, View, component, view};

pub const ROW: &str = "hover:bg-zinc-50 dark:hover:bg-zinc-900/60";
pub const TD: &str = "px-3 py-1.5 align-top";
pub const TD_NUM: &str = "px-3 py-1.5 text-right align-top tabular-nums";
pub const TD_MUTED: &str = "px-3 py-1.5 align-top whitespace-nowrap text-xs text-zinc-500";

/// A table with a header row; `child` holds the body rows.
#[component]
pub async fn data_table(
    headers: &[&'static str],
    #[default] child: Child<'_>,
) -> Result<impl View> {
    let headers = headers.to_vec();
    Ok(view! {
        <div class="overflow-x-auto rounded border border-zinc-200 dark:border-zinc-800">
            <table class="w-full border-collapse text-sm">
                <thead class="bg-zinc-50 text-left text-xs uppercase tracking-wide text-zinc-500 dark:bg-zinc-900">
                    <tr>
                        for header in headers {
                            <th class="px-3 py-2 font-medium">(header)</th>
                        }
                    </tr>
                </thead>
                <tbody class="divide-y divide-zinc-100 dark:divide-zinc-800">
                    (child)
                </tbody>
            </table>
        </div>
    })
}
