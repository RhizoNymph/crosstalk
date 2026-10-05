//! A bounded cache of input messages' k-grams.
//!
//! Every exchange classifies its output against its whole request history,
//! and a conversation's history grows by a turn each exchange, so the same
//! messages are expanded into decode layers and hashed again and again.
//! Bodies are content-addressed, so a message's k-grams are a function of
//! its hash: they are computed once and kept, oldest evicted first, within
//! a budget of k-grams held.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use crosstalk_spec::ids::MessageHash;

use crate::segment::MessageKGrams;

/// How many k-grams the cache holds at most (8 bytes each).
pub const DEFAULT_BUDGET: usize = 4 << 20;

/// Messages' k-grams by hash.
#[derive(Debug, Default)]
pub struct KGramCache {
    entries: HashMap<MessageHash, Arc<MessageKGrams>>,
    order: VecDeque<MessageHash>,
    held: usize,
    budget: usize,
}

fn size(kgrams: &MessageKGrams) -> usize {
    kgrams.iter().map(Vec::len).sum::<usize>().max(1)
}

impl KGramCache {
    pub fn new(budget: usize) -> Self {
        Self {
            budget,
            ..Self::default()
        }
    }

    pub fn get(&self, hash: MessageHash) -> Option<Arc<MessageKGrams>> {
        self.entries.get(&hash).cloned()
    }

    /// Keep `kgrams` for `hash`, evicting the oldest entries over budget.
    pub fn put(&mut self, hash: MessageHash, kgrams: Arc<MessageKGrams>) {
        let added = size(&kgrams);
        if added > self.budget {
            return;
        }
        if self.entries.insert(hash, kgrams).is_some() {
            return;
        }
        self.order.push_back(hash);
        self.held += added;
        while self.held > self.budget {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(evicted) = self.entries.remove(&oldest) {
                self.held -= size(&evicted);
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
