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
//! - [`CoverageCache`]: whole coverages of recent input lists. The next
//!   exchange of a conversation lists the same inputs and a few more, so
//!   its coverage is a kept one extended by the new messages, instead of
//!   every k-gram of the history added again.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use crosstalk_spec::derived::provenance::fingerprint::Fingerprint;
use crosstalk_spec::ids::MessageHash;

use crate::segment::{Coverage, MessageKGrams};

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

/// How many k-gram positions the kept coverages hold at most.
pub const COVERAGE_BUDGET: usize = 8 << 20;

/// How many coverages are kept at most.
pub const COVERAGE_ENTRIES: usize = 64;

/// Coverages of input lists, by the lists' message hashes in input order,
/// least recently kept evicted first within a budget of k-gram positions.
///
/// A coverage is a function of its input list ([`Coverage::add_kgrams`]
/// only appends: input and layer numbers continue, and a k-gram's first
/// occurrences stay first), so a kept coverage of a prefix of a list,
/// extended by the rest, is the coverage of the list.
#[derive(Debug)]
pub struct CoverageCache {
    /// Oldest first.
    entries: VecDeque<(Vec<MessageHash>, Coverage)>,
    held: usize,
    budget: usize,
    max_entries: usize,
}

impl CoverageCache {
    pub fn new(budget: usize, max_entries: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            held: 0,
            budget,
            max_entries,
        }
    }

    /// Take out the kept coverage of the longest prefix of `inputs`, with
    /// that prefix's length.
    pub fn take_prefix(&mut self, inputs: &[MessageHash]) -> Option<(usize, Coverage)> {
        let (position, length) = self
            .entries
            .iter()
            .enumerate()
            .filter(|(_, (key, _))| inputs.starts_with(key))
            .map(|(position, (key, _))| (position, key.len()))
            .max_by_key(|(position, length)| (*length, *position))?;
        let (_, coverage) = self.entries.remove(position)?;
        self.held -= coverage.positions();
        Some((length, coverage))
    }

    /// Keep `coverage`, the coverage of `inputs`, evicting the oldest over
    /// budget. A coverage over the whole budget is not kept.
    pub fn keep(&mut self, inputs: Vec<MessageHash>, coverage: Coverage) {
        let added = coverage.positions();
        if added > self.budget || self.max_entries == 0 {
            return;
        }
        self.held += added;
        self.entries.push_back((inputs, coverage));
        while self.held > self.budget || self.entries.len() > self.max_entries {
            let Some((_, evicted)) = self.entries.pop_front() else {
                break;
            };
            self.held -= evicted.positions();
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
