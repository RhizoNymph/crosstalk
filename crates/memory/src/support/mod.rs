//! Building blocks every reference store shares: the locks around its
//! state, the outbox it publishes to, the sequence it draws ids from, and
//! the cursor book it pages with.
//!
//! None of these is a spec trait. They are what an in-memory store needs to
//! stand in for a database: a place for state, a transaction boundary, ids,
//! an outbox and cursors. Time is not among them: every store takes it as
//! an argument (`crosstalk_spec::interfaces`, "Time is an argument"). The
//! one clock here, [`ManualClock`], implements the spec's `Clock` for the
//! computation doubles that stamp their output.

mod clock;
mod ids;
mod outbox;
mod paging;
mod state;

pub use clock::ManualClock;
pub use ids::IdSequence;
pub use outbox::{Outbox, drain};
pub use paging::{CursorBook, PageError, page_after};
pub use state::{State, lock};
