//! Decode-chain statistics for a swarm-traces run.
//!
//! The payloads are real attack content, so a report carries only codec
//! chains, counts and byte lengths: never a token, a plaintext or a payload
//! id.

use std::collections::BTreeMap;
use std::fmt;

use super::codec::Decoded;

/// Counts for one codec chain.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChainStats {
    /// Distinct tokens (per payload) that decode through this chain.
    pub tokens: u64,
    /// Of those, tokens whose payload has a recovered-text or response child.
    pub construction: u64,
    /// Of those, tokens whose payload has none (decode-only).
    pub structural: u64,
    /// Total encoded length, in bytes.
    pub encoded_bytes: u64,
    /// Total decoded length, in bytes.
    pub plaintext_bytes: u64,
}

/// Decode-chain counts over a run's payloads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChainTally {
    /// Payload rows read.
    pub payloads: u64,
    /// Distinct candidate tokens extracted from them.
    pub candidates: u64,
    /// Chains every layer of which maps to a spec `Codec`: each token is a
    /// labelled world. Keyed by dotted chain name, outermost first.
    pub labelled: BTreeMap<String, ChainStats>,
    /// Chains with a layer no spec `Codec` names (`\x..` byte escapes): a
    /// reported gap, not converted.
    pub unmapped: BTreeMap<String, ChainStats>,
}

impl ChainTally {
    /// Counts one decoded token of `encoded_len` bytes.
    pub fn add(&mut self, decoded: &Decoded, encoded_len: usize, corroborated: bool) {
        let bucket = if decoded.codecs().is_some() {
            &mut self.labelled
        } else {
            &mut self.unmapped
        };
        let stats = bucket.entry(decoded.chain_name()).or_default();
        stats.tokens += 1;
        if corroborated {
            stats.construction += 1;
        } else {
            stats.structural += 1;
        }
        stats.encoded_bytes += u64::try_from(encoded_len).unwrap_or(u64::MAX);
        stats.plaintext_bytes += u64::try_from(decoded.text.len()).unwrap_or(u64::MAX);
    }

    /// Labelled tokens, over every chain.
    pub fn labelled_tokens(&self) -> u64 {
        self.labelled.values().map(|stats| stats.tokens).sum()
    }
}

impl fmt::Display for ChainTally {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "swarm decode chains: {} payloads, {} candidate tokens, {} labelled",
            self.payloads,
            self.candidates,
            self.labelled_tokens()
        )?;
        writeln!(
            f,
            "{:<8} {:<32} {:>8} {:>12} {:>10} {:>12} {:>12}",
            "kind", "chain", "tokens", "construction", "structural", "encoded_b", "plain_b"
        )?;
        for (kind, chains) in [("labelled", &self.labelled), ("gap", &self.unmapped)] {
            for (chain, stats) in chains {
                writeln!(
                    f,
                    "{:<8} {:<32} {:>8} {:>12} {:>10} {:>12} {:>12}",
                    kind,
                    chain,
                    stats.tokens,
                    stats.construction,
                    stats.structural,
                    stats.encoded_bytes,
                    stats.plaintext_bytes
                )?;
            }
        }
        Ok(())
    }
}
