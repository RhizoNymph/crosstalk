//! A request as threading sees it: its messages with their roles, the
//! non-system history they spell, and that history's chain hashes.
//!
//! **Chain hashes.** `chain(0)` is a fixed seed and `chain(k)` is the
//! BLAKE3 of `chain(k - 1)` and the k-th non-system message's hash, so
//! `chain(k)` names the whole history through its k-th message. Two
//! histories share their first k messages exactly when their `chain(k)`
//! agree. A stored conversation keeps the chain of each history position
//! and of its whole history (its head), so "the longest stored history that
//! is a prefix of this request" and "the longest common prefix with any
//! stored history" are equality lookups, whatever the conversations'
//! lengths.
//!
//! **System messages** are left out of the history wherever they appear
//! (a leading system prompt, or a `system` turn mid-conversation): they
//! never take part in prefix matching, so a changed system prompt
//! continues the conversation (`reconstruct.thread.system-change-continues`).
//! The request's system message, for `new_system`, is its last one in
//! request order: the system context in force when the request was sent.

use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::observed::message::Role;
use crosstalk_spec::support::Blake3;

/// A message as threading sees it: its hash and its role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Entry {
    pub message: MessageHash,
    pub role: Role,
}

/// The chain hash of a non-system history prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChainHash(pub Blake3);

impl ChainHash {
    /// The chain of the empty history.
    pub fn empty() -> Self {
        Self(Blake3::of(b"crosstalk.reconstruct.history-chain.v1"))
    }

    /// The chain of this history followed by `message`.
    pub fn then(&self, message: &MessageHash) -> Self {
        let mut bytes = [0u8; 64];
        bytes[..32].copy_from_slice(self.0.as_bytes());
        bytes[32..].copy_from_slice(message.digest().as_bytes());
        Self(Blake3::of(&bytes))
    }
}

/// A request's messages, analysed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct History {
    /// Every message, in request order.
    pub(crate) entries: Vec<Entry>,
    /// Where each non-system message is in `entries`.
    pub(crate) positions: Vec<usize>,
    /// `chains[k]` is the chain of the first `k + 1` non-system messages.
    pub(crate) chains: Vec<ChainHash>,
    /// The last system message, if any.
    pub(crate) system: Option<MessageHash>,
    /// The first assistant message's place in the non-system history.
    pub(crate) first_assistant: Option<usize>,
}

impl History {
    pub(crate) fn of(entries: Vec<Entry>) -> Self {
        let mut positions = Vec::new();
        let mut chains = Vec::new();
        let mut system = None;
        let mut first_assistant = None;
        let mut chain = ChainHash::empty();
        for (position, entry) in entries.iter().enumerate() {
            match entry.role {
                Role::System => system = Some(entry.message),
                Role::User | Role::Assistant | Role::Tool => {
                    if entry.role == Role::Assistant && first_assistant.is_none() {
                        first_assistant = Some(positions.len());
                    }
                    chain = chain.then(&entry.message);
                    positions.push(position);
                    chains.push(chain);
                }
            }
        }
        Self {
            entries,
            positions,
            chains,
            system,
            first_assistant,
        }
    }

    /// The number of non-system messages.
    pub(crate) fn len(&self) -> usize {
        self.positions.len()
    }

    /// The non-system messages, in order.
    pub(crate) fn non_system(&self) -> impl Iterator<Item = MessageHash> + '_ {
        self.positions
            .iter()
            .filter_map(|position| self.entries.get(*position))
            .map(|entry| entry.message)
    }

    /// The entries after the first `k` non-system messages (every entry
    /// when `k` is zero), system messages included.
    pub(crate) fn after(&self, k: usize) -> &[Entry] {
        match k.checked_sub(1).and_then(|last| self.positions.get(last)) {
            Some(position) => self.entries.get(position + 1..).unwrap_or(&[]),
            None => &self.entries,
        }
    }

    /// The chain of the first `k` non-system messages.
    pub(crate) fn chain(&self, k: usize) -> ChainHash {
        k.checked_sub(1)
            .and_then(|last| self.chains.get(last))
            .copied()
            .unwrap_or_else(ChainHash::empty)
    }
}
