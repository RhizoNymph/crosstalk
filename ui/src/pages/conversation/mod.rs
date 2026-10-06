//! The agent conversation view: one conversation turn by turn, with where
//! each turn's text came from and where it went
//! (`docs/features/conversation_view.md`).
//!
//! The pages read the spec's conversation reads (INV-1000..1029), which
//! land on staging separately; until then this module holds what does not
//! depend on their shapes: the query keys and windows ([`query`]), the
//! view model ([`model`]) and the components that render it
//! ([`sections`]).

pub mod model;
pub mod query;
pub mod sections;

#[cfg(test)]
mod tests;
