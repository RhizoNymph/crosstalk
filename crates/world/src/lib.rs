//! The synthetic crosstalk world: one deterministic week of agent traffic,
//! generated from a seed and written through the spec's write traits into
//! any stores that implement them. The operator UI and the gateway's tests
//! share it: data plus a clock.
//!
//! ```text
//! World::new(seed, at)        config, clock, embedder (pure)
//!   └─ seed(&mut stores)       declare config's channels   (ChannelRegistry::declare)
//!        ├─ generate           cast, channels, topics, traffic, bodies (no store)
//!        ├─ assemble           every write as a timed step  (Script)
//!        └─ run                each step through the write traits, in time order
//!   → Scenario                 the ids every role got
//! ```
//!
//! - [`World`] holds the seed and the anchor (the world's present, on a
//!   five-minute boundary). Every time in the world is an offset back from
//!   the anchor and every write is given its time: no store reads a clock.
//! - [`WorldConfig`] is what a host configures its stores with before
//!   seeding (operators, sinks, built-in rules, the embedding model, the
//!   catalog's retention, bucket width and correlator timing), and
//!   [`WorldEmbedder`] the embedder the alert store needs.
//! - [`WorldStores`] names the stores and the write traits the seed calls.
//! - [`Scenario`] names what the world holds: agents by their fixture keys,
//!   channels, merges, rules, projection jobs, dropped bodies.
//!
//! The world is ported from the UI fixture (`fixture/` on the UI branch).
//! Where today's spec differs from the UI's, the world follows today's;
//! `docs/features/world.md` lists the differences and the reads the UI
//! fixture answered that no store or spec trait answers yet.
//!
//! A test-support crate: other crates take it as a dev-dependency only.

pub mod assemble;
pub mod clock;
pub mod config;
pub mod embed;
pub mod error;
pub mod generate;
pub mod mint;
pub mod rng;
mod run;
pub mod scenario;
pub mod script;
mod seed;
pub mod stores;
pub mod text;

pub use clock::{Anchor, UI_ANCHOR, WorldClock};
pub use config::WorldConfig;
pub use embed::WorldEmbedder;
pub use error::{StoreError, WorldError};
pub use generate::wire::{Wire, WireExchange, WireScope};
pub use scenario::{BodySide, ChannelKey, JobKey, MergeKey, RuleKey, Scenario};
pub use seed::World;
pub use stores::WorldStores;
