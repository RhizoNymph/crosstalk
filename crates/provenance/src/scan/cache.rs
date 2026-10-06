//! Bounded caches of what the scanner derives from a message body.
//!
//! Every exchange classifies its output against its whole request history,
//! and a conversation's history grows by a turn each exchange, so the same
//! messages are expanded into decode layers and hashed again and again.
//! Bodies are content-addressed, so what is derived from a message is a
//! function of its hash: it is computed once and kept, oldest evicted
//! first, within a budget of items held.
//!
//! - [`KGramCache`]: a message's k-grams per decode layer, for coverage.
//! - [`TokenCache`]: the token sequences a message gives its reader
//!   (`provenance.match.inherited-fragment-dropped`).

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use crosstalk_spec::derived::provenance::fingerprint::Fingerprint;
use crosstalk_spec::ids::MessageHash;

use crate::segment::MessageKGrams;

/// How many items (k-grams or tokens, 8 bytes each) a cache holds at most.
pub const DEFAULT_BUDGET: usize = 4 << 20;

/// What a cached value weighs against the budget.
pub trait Weighed {
    fn weight(&self) -> usize;
}

impl Weighed for MessageKGrams {
    fn weight(&self) -> usize {
        self.iter().map(Vec::len).sum::<usize>().max(1)
    }
}

/// Values derived from messages, by hash, within a budget.
#[derive(Debug)]
pub struct Bounded<V> {
    entries: HashMap<MessageHash, Arc<V>>,
    order: VecDeque<MessageHash>,
    held: usize,
    budget: usize,
}

/// Messages' k-grams by hash.
pub type KGramCache = Bounded<MessageKGrams>;

/// The token sequences of the parts each message gives its reader, by
/// hash (the same shape as a message's k-grams: one list per part).
pub type TokenCache = Bounded<Vec<Vec<Fingerprint>>>;

impl<V: Weighed> Bounded<V> {
    pub fn new(budget: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            held: 0,
            budget,
        }
    }

    pub fn get(&self, hash: MessageHash) -> Option<Arc<V>> {
        self.entries.get(&hash).cloned()
    }

    /// Keep `value` for `hash`, evicting the oldest entries over budget.
    pub fn put(&mut self, hash: MessageHash, value: Arc<V>) {
        let added = value.weight();
        if added > self.budget {
            return;
        }
        if self.entries.insert(hash, value).is_some() {
            return;
        }
        self.order.push_back(hash);
        self.held += added;
        while self.held > self.budget {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(evicted) = self.entries.remove(&oldest) {
                self.held -= evicted.weight();
            }
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
