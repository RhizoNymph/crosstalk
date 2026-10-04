//! The seed script: every write the world makes, in time order.
//!
//! Assembly turns the generated data into [`Step`]s, each a time and an
//! [`Op`]; [`Script::into_steps`] orders them by time, keeping the order
//! they were added in among equal times, so an op added after another at
//! the same instant runs after it (a channel is discovered before the
//! access that discovered it is recorded).

pub mod op;

use crosstalk_spec::support::Timestamp;

pub use op::{AlertKey, Op, RuleRef, Step};

/// Steps in the order they were added.
#[derive(Debug, Default)]
pub struct Script {
    steps: Vec<Step>,
}

impl Script {
    pub fn push(&mut self, at: Timestamp, op: Op) {
        self.steps.push(Step { at, op });
    }

    pub fn len(&self) -> usize {
        self.steps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// Every step, by time; equal times keep the order they were added in.
    pub fn into_steps(mut self) -> Vec<Step> {
        self.steps.sort_by_key(|step| step.at);
        self.steps
    }
}
