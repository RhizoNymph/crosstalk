//! Provenance: who wrote a piece of text first, and where it went.
//!
//! Assistant output is cut into [`span::Span`]s. Each span is classified by
//! [`span::Origin`]; only originated spans are fingerprinted and indexed.
//! When another agent's input contains an indexed span's fingerprints, the
//! result is a [`matching::ContentMatch`].

pub mod fingerprint;
pub mod matching;
pub mod span;
