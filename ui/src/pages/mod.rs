//! One module per screen, plus the root layout.

pub mod agents;
pub mod alerts;
pub mod audit;
pub mod channels;
pub mod common;
pub mod explore;
pub mod export;
pub mod gateway;
pub mod overview;
pub mod pipeline;
#[cfg(test)]
mod present_tests;
#[cfg(test)]
mod replay_tests;
pub mod topics;
pub mod topology;
pub mod transmission;
pub mod view;

use crosstalk_spec::interfaces::l8_surface::Permission;
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::request::uri;
use topcoat::router::{Slot, layout};
use topcoat::tailwind;
use topcoat::view::{View, view};

use crate::app::{access, caller, can};
use crate::data::elements::LIVE_JS;
use crate::pages::view::current_state;

/// The live feed's route (`data::live`).
const LIVE_PATH: &str = "/data/live";

/// Navigation sections: path prefix and label.
const SECTIONS: [(&str, &str); 9] = [
    ("/topology", "Topology"),
    ("/explore", "Explore"),
    ("/topics", "Topics"),
    ("/channels", "Channels"),
    ("/agents", "Agents"),
    ("/alerts", "Alerts"),
    ("/export", "Export"),
    ("/audit", "Audit"),
    ("/pipeline", "Pipeline"),
];

fn nav_classes(active: bool) -> &'static str {
    if active {
        "block rounded px-2 py-1 bg-zinc-200 font-medium dark:bg-zinc-800"
    } else {
        "block rounded px-2 py-1 text-zinc-600 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:bg-zinc-900"
    }
}

#[layout("/")]
async fn root_layout(cx: &Cx, slot: Slot<'_>) -> Result<impl View> {
    let path = uri(cx).path().to_owned();
    let operator_name = access(cx).name().to_owned();
    // The gateway page (`gateway`) is rendered because the gateway failed:
    // it neither asks it again for the view state nor opens the live feed.
    let gateway_down = gateway::carried(cx).is_some();
    // Live updates need View, as `/data/live` does.
    let live = !gateway_down && can(&caller(cx), Permission::View);
    // Navigation carries the current view state, so the filter follows the
    // user between sections.
    let query = if gateway_down {
        None
    } else {
        current_state(cx).await.map(|state| state.to_query())
    };
    let sections = SECTIONS.map(|(prefix, label)| {
        let href = match &query {
            Some(query) => format!("{prefix}?{query}"),
            None => prefix.to_owned(),
        };
        (href, label, path.starts_with(prefix))
    });
    Ok(view! {
        <!DOCTYPE html>
        <html lang="en">
            <head>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1">
                <title>"crosstalk"</title>
                <link rel="icon" href="data:,">
                <link rel="stylesheet" href=(tailwind::stylesheet!())>
                topcoat::runtime::script()
                if live {
                    <script type="module" src=(LIVE_JS)></script>
                }
            </head>
            <body class="bg-white text-zinc-900 antialiased dark:bg-zinc-950 dark:text-zinc-100">
                <div class="flex min-h-screen">
                    <nav class="w-44 shrink-0 border-r border-zinc-200 p-3 text-sm dark:border-zinc-800">
                        <a href="/" class="mb-4 block font-mono text-base font-semibold">"crosstalk"</a>
                        <ul class="space-y-0.5">
                            for (href, label, active) in sections {
                                <li>
                                    <a href=(href) class=(nav_classes(active))>(label)</a>
                                </li>
                            }
                        </ul>
                        <p class="mt-6 text-xs text-zinc-500">"signed in as " (operator_name.clone())</p>
                    </nav>
                    <main class="min-w-0 flex-1 p-6">
                        if live {
                            <ct-live data-src=(LIVE_PATH) class="block"></ct-live>
                        }
                        <div data-live-region="page">
                            (slot)
                        </div>
                    </main>
                </div>
            </body>
        </html>
    })
}
