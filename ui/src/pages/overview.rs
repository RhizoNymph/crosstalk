//! The landing page: what needs attention, and where to go.

use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::page;
use topcoat::view::{View, view};

use crate::components::page_header;
use crate::pages::view::view_state;

#[page("/")]
async fn overview(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx)?;
    let topology = format!("/topology?{}", state.to_query());
    Ok(view! {
        page_header(title: "Overview", subtitle: "Agent-to-agent communication seen by the gateway.")
        <p class="text-sm"><a class="underline" href=(topology)>"Open the topology"</a></p>
    })
}
