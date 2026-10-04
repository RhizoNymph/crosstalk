//! L0 ingress for crosstalk: the reverse and forward proxy, upstream routing,
//! credential hashing, provider adapters, response framing and the SSE tee.
//!
//! Implements [`crosstalk_spec::interfaces::l0_ingress`].
//!
//! Roadmap: P2.4 (L0 ingress: reverse proxy for Anthropic). A layer crate: it
//! depends on the spec, never on another layer crate.

// Every crate builds on the spec; the dependency is declared before any
// code uses it.
use crosstalk_spec as _;

#[cfg(test)]
mod tests {}
