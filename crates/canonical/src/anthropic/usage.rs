//! Anthropic `usage` to the spec's [`TokenUsage`].
//!
//! Anthropic splits the prompt into three disjoint counts: `input_tokens`
//! (after the last cache breakpoint), `cache_creation_input_tokens` (written
//! to the cache) and `cache_read_input_tokens` (read from it). The spec's
//! `input` is every prompt token, as OpenAI's `prompt_tokens` is, with
//! `cache_read` and `cache_write` the parts of it read from and written to
//! the cache:
//!
//! | `TokenUsage` | Anthropic |
//! | --- | --- |
//! | `input` | `input_tokens + cache_creation_input_tokens + cache_read_input_tokens` |
//! | `output` | `output_tokens` (the last value: `message_delta` counts are cumulative) |
//! | `cache_read` | `cache_read_input_tokens` |
//! | `cache_write` | `Some(cache_creation_input_tokens)`: the protocol reports cache writes |
//! | `reasoning` | `None`: thinking tokens are inside `output_tokens` and not reported apart |
//!
//! A missing or null cache count is 0. Usage is `None` when `input_tokens`
//! or `output_tokens` is missing, a count is not a non-negative integer,
//! or a value (the prompt total included) does not fit a `u32`.
//!
//! A stream's usage is `message_start`'s `message.usage` with each later
//! `message_delta`'s `usage` fields laid over it, so it equals the whole
//! response's `usage` (`canonical.normalize.stream-independent`).

use crosstalk_spec::observed::exchange::{TokenCounts, TokenUsage};

use crosstalk_spec::observed::message::json::Json;

/// The usage counts seen so far.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Usage {
    input: Count,
    output: Count,
    cache_creation: Count,
    cache_read: Count,
}

/// One count: never given (or null), a count, or a value that is not one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Count {
    #[default]
    Absent,
    Valid(u64),
    Invalid,
}

impl Count {
    fn merge(&mut self, value: Option<&Json>) {
        match value {
            None | Some(Json::Null) => {}
            Some(value) => {
                *self = value.as_u64().map_or(Count::Invalid, Count::Valid);
            }
        }
    }
}

impl Usage {
    /// Lays the counts present in `usage` over these.
    pub(crate) fn merge(&mut self, usage: &Json) {
        if !usage.is_object() {
            return;
        }
        self.input.merge(usage.get("input_tokens"));
        self.output.merge(usage.get("output_tokens"));
        self.cache_creation
            .merge(usage.get("cache_creation_input_tokens"));
        self.cache_read.merge(usage.get("cache_read_input_tokens"));
    }

    /// The spec's usage (module docs).
    pub(crate) fn token_usage(&self) -> Option<TokenUsage> {
        let Count::Valid(input) = self.input else {
            return None;
        };
        let Count::Valid(output) = self.output else {
            return None;
        };
        let optional = |count: Count| match count {
            Count::Absent => Some(0),
            Count::Valid(value) => Some(value),
            Count::Invalid => None,
        };
        let cache_creation = optional(self.cache_creation)?;
        let cache_read = optional(self.cache_read)?;
        let total = input.checked_add(cache_creation)?.checked_add(cache_read)?;
        // The cache counts are parts of `total`, so the check always
        // passes once every count fits.
        TokenUsage::new(TokenCounts {
            input: u32::try_from(total).ok()?,
            output: u32::try_from(output).ok()?,
            cache_read: u32::try_from(cache_read).ok()?,
            cache_write: Some(u32::try_from(cache_creation).ok()?),
            reasoning: None,
        })
        .ok()
    }
}
