//! New inputs: what an exchange's request adds to the agent's previous one.
//!
//! Requests carry the whole history, so most of a request was already read
//! by the agent's previous exchange. The new inputs are the request's
//! messages beyond those, as a multiset: a message whose hash occurs `n`
//! times in the previous request has its first `n` occurrences here counted
//! as old. Comparing by hash rather than by prefix keeps a truncated or
//! rewritten history (sliding-window memory, summaries) from counting
//! everything it kept as new.

use std::collections::HashMap;

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::observed::message::Message;

use super::exchange::CorpusExchange;

/// The positions (in `current`'s request) and messages that are new relative
/// to `previous`, the same agent's preceding exchange.
pub fn new_inputs<'a>(
    previous: Option<&CorpusExchange>,
    current: &'a CorpusExchange,
) -> Vec<(usize, &'a Message)> {
    let mut seen: HashMap<MessageHash, usize> = HashMap::new();
    if let Some(previous) = previous {
        for hash in &previous.exchange().request {
            *seen.entry(*hash).or_insert(0) += 1;
        }
        // The previous response is echoed back in this request: not new.
        if let Some(response) = previous.response() {
            *seen.entry(response.hash).or_insert(0) += 1;
        }
    }
    current
        .request()
        .enumerate()
        .filter(|(_, message)| match seen.get_mut(&message.hash) {
            Some(count) if *count > 0 => {
                *count -= 1;
                false
            }
            _ => true,
        })
        .collect()
}
