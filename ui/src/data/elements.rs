//! The bundled custom elements, as Topcoat assets.
//!
//! A page loads an element with
//! `<script type="module" src=(TOPOLOGY_JS)></script>` and renders its tag
//! with inputs as `data-*` attributes and its `/data/` URL in `data-src`.
//! Each bundle is a self-contained ES module that defines its element once.
//!
//! Build order: `pnpm --dir ui/elements build` writes `ui/elements/dist/`,
//! then `topcoat asset bundle` (from `ui/`) copies the files into the asset
//! bundle. Only assets whose handles are used by the binary are bundled, so
//! an element appears in the bundle once a page renders its constant.

use topcoat::asset::{Asset, asset};

/// `<ct-topology>`: the agent/channel graph.
pub const TOPOLOGY_JS: Asset = asset!("../../elements/dist/ct-topology.js");
/// `<ct-projection>`: the UMAP scatterplot with lasso.
pub const PROJECTION_JS: Asset = asset!("../../elements/dist/ct-projection.js");
/// `<ct-timebrush>`: transmissions per bucket with a window brush.
pub const TIMEBRUSH_JS: Asset = asset!("../../elements/dist/ct-timebrush.js");
/// `<ct-live>`: subscribes to `/data/live` and refreshes the page's
/// `data-live-region` when an event matches its `data-live-watch` tokens.
pub const LIVE_JS: Asset = asset!("../../elements/dist/ct-live.js");
