//! The reader's nearer source wins: index hits on another agent's span
//! that the reader's own path to the same text explains are not counted.
//!
//! Two rules, both applied to each hit of a read (one k-gram, or one
//! short-span run, on one span), so a match loses only the runs its reader
//! already had and keeps the rest:
//!
//! - **Own output** (`provenance.match.own-output-replay`). A hit is not
//!   counted when the reader's own earlier output holds the same k-gram
//!   (or short-span run): any text, reasoning or tool-call argument of an
//!   assistant message in the exchange's request (its history and new
//!   inputs, so written before this read), through every decode layer. A
//!   tool result that replays the reader's call (a `get_log`, a
//!   `send_money` confirmation) or a message quoting the reader back is
//!   the reader's own relay, not a delivery. Server tool results inside
//!   assistant messages are reads, not output, and do not count. The
//!   reader's own output holding the text is the whole condition: whoever
//!   wrote it first, the reader cannot receive from a peer what it already
//!   wrote itself. A transmission is never lost to this rule, because the
//!   read that first brought the text to the reader came before the
//!   reader's own copy and was matched then.
//! - **Direct read of a forward's source** (`provenance.match.forward-direct-read`,
//!   only with forwarding on). A hit on a forwarded span (text the
//!   forwarder relayed from its input message `m`) is not counted when one
//!   of the reader's own reads holding that k-gram is a direct read of
//!   `m`: a layer of a non-assistant input of the exchange (the read being
//!   scanned included) that holds at least `w` (the winnowing window)
//!   k-grams of `m` that the forwarder's output message does not hold. A
//!   peer's delivery of the forward carries only what the forwarder wrote,
//!   so only a reader with its own copy of the source sees the source's
//!   text around the forward.
//!
//! **Tradeoffs.** A forward that holds the whole source (nothing of `m`
//! left outside it) cannot be told from the source, so a peer's own read of
//! it still matches. A peer message repeating text the reader wrote is no
//! delivery of that text, even when the peer had it first.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crosstalk_spec::derived::provenance::fingerprint::{Fingerprint, FingerprintHit};
use crosstalk_spec::derived::provenance::span::{RelaySource, SpanState};
use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::interfaces::l4_provenance::{FingerprintIndex, SemanticMatcher};
use crosstalk_spec::observed::message::{Message, MessageBody};

use super::cache::{Bounded, Weighed};
use super::messages::MessageSource;
use super::{Loaded, ScanError, Scanner, Session};
use crate::fingerprint::short;
use crate::segment::{MessageKGrams, PartKind, message_kgrams, text_parts, view};
use crate::store::ProvenanceStore;
use crate::text::normalize;

/// A set of k-gram and short-span fingerprints.
pub type FingerprintSet = HashSet<Fingerprint>;

impl Weighed for FingerprintSet {
    fn weight(&self) -> usize {
        self.len().max(1)
    }
}

/// The agents' own output messages' fingerprint sets, by hash.
pub type OwnCache = Bounded<FingerprintSet>;

/// A forward, by its source message and the forwarder's output message.
type ForwardKey = (MessageHash, MessageHash);

/// The reader's reads (non-assistant inputs), layer by layer, and which
/// layers hold each fingerprint.
#[derive(Debug, Default)]
struct Reads {
    layers: Vec<Arc<MessageKGrams>>,
    /// Global layer number → (read, layer within it).
    places: Vec<(usize, usize)>,
    holding: HashMap<Fingerprint, Vec<usize>>,
}

impl Reads {
    fn new(layers: Vec<Arc<MessageKGrams>>) -> Self {
        let mut places = Vec::new();
        let mut holding: HashMap<Fingerprint, Vec<usize>> = HashMap::new();
        for (read, kgrams) in layers.iter().enumerate() {
            for (local, fingerprints) in kgrams.iter().enumerate() {
                let layer = places.len();
                places.push((read, local));
                for fingerprint in fingerprints {
                    let entry = holding.entry(*fingerprint).or_default();
                    if entry.last() != Some(&layer) {
                        entry.push(layer);
                    }
                }
            }
        }
        Self {
            layers,
            places,
            holding,
        }
    }

    fn layer(&self, layer: usize) -> &[Fingerprint] {
        self.places
            .get(layer)
            .and_then(|(read, local)| self.layers.get(*read)?.get(*local))
            .map_or(&[], Vec::as_slice)
    }
}

/// What one scan knows of the reader's own paths to text.
#[derive(Debug, Default)]
pub struct Nearer {
    /// The fingerprints of each assistant message in the request.
    own: Vec<Arc<FingerprintSet>>,
    /// The non-assistant inputs' k-grams (forwarding on only), indexed on
    /// first use.
    pending: Vec<Arc<MessageKGrams>>,
    reads: Option<Reads>,
    /// Per forward: the source's k-grams the forwarder's output lacks.
    source_only: HashMap<ForwardKey, Arc<FingerprintSet>>,
    /// Per (layer, forward): whether that layer is a direct read of the
    /// forward's source.
    direct: HashMap<(usize, ForwardKey), bool>,
}

impl Nearer {
    fn own_holds(&self, fingerprint: Fingerprint) -> bool {
        self.own.iter().any(|set| set.contains(&fingerprint))
    }
}

impl Scanner {
    /// A message's k-grams per layer, through the input cache.
    pub(crate) fn cached_kgrams(&self, message: &Message) -> Arc<MessageKGrams> {
        let cached = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(message.hash);
        if let Some(kgrams) = cached {
            return kgrams;
        }
        let computed = Arc::new(message_kgrams(
            self.winnowing(),
            self.pipeline(),
            message,
            |_| true,
        ));
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .put(message.hash, Arc::clone(&computed));
        computed
    }

    /// Every k-gram and short-span run, through every layer, of an
    /// assistant message's own parts (server tool results left out).
    fn own_fingerprints(&self, message: &Message) -> Arc<FingerprintSet> {
        let cached = self
            .own_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(message.hash);
        if let Some(set) = cached {
            return set;
        }
        let mut set = FingerprintSet::new();
        for part in text_parts(message) {
            if part.kind == PartKind::ToolResult {
                continue;
            }
            let base = view(&part.text, part.kind);
            for layer in self.pipeline().layers(base.text()) {
                let normalized = normalize(layer.text.text());
                set.extend(
                    self.winnowing()
                        .kgrams_of(&normalized)
                        .into_iter()
                        .map(|kgram| kgram.fingerprint),
                );
                set.extend(
                    short::token_runs(&normalized, self.short_spans())
                        .into_iter()
                        .map(|run| run.fingerprint),
                );
            }
        }
        let set = Arc::new(set);
        self.own_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .put(message.hash, Arc::clone(&set));
        set
    }

    /// The nearer-source state for a scan of `loaded`.
    pub(crate) fn nearer(&self, loaded: &Loaded) -> Nearer {
        let mut nearer = Nearer::default();
        for message in loaded.inputs() {
            if matches!(message.body, MessageBody::Assistant(_)) {
                nearer.own.push(self.own_fingerprints(message));
            } else if self.forwarding {
                nearer.pending.push(self.cached_kgrams(message));
            }
        }
        nearer
    }

    /// `hits` without those the reader's nearer source explains (see the
    /// module documentation). Hits on the reader's own spans and on spans
    /// not live are kept; the caller skips them.
    pub(crate) async fn nearer_hits<I, S, M, L>(
        &self,
        session: &mut Session<'_, I, S, M, L>,
        hits: &[FingerprintHit],
    ) -> Result<Vec<FingerprintHit>, ScanError>
    where
        I: FingerprintIndex + Sync,
        S: ProvenanceStore + Sync,
        M: SemanticMatcher + Sync,
        L: MessageSource + Sync,
    {
        let mut kept = Vec::with_capacity(hits.len());
        let (mut own, mut direct) = (0_usize, 0_usize);
        for hit in hits {
            let Some(record) = session.live.get(hit.span) else {
                kept.push(*hit);
                continue;
            };
            if record.span.agent == session.reader {
                kept.push(*hit);
                continue;
            }
            if session.nearer.own_holds(hit.fingerprint) {
                own += 1;
                continue;
            }
            let forward = match (&record.span.state, record.forward) {
                (
                    SpanState::Relayed {
                        source: RelaySource::Input(source),
                    },
                    Some(_),
                ) => Some((*source, record.span.location.part.message)),
                _ => None,
            };
            if let Some(key) = forward
                && self.read_directly(session, key, hit.fingerprint).await?
            {
                direct += 1;
                continue;
            }
            kept.push(*hit);
        }
        if own + direct > 0 {
            tracing::debug!(
                exchange = ?session.exchange,
                own_output = own,
                direct_read = direct,
                "hits explained by the reader's nearer source"
            );
        }
        Ok(kept)
    }

    /// Whether a read of the reader holding `fingerprint` is a direct read
    /// of the forward's source.
    async fn read_directly<I, S, M, L>(
        &self,
        session: &mut Session<'_, I, S, M, L>,
        key: ForwardKey,
        fingerprint: Fingerprint,
    ) -> Result<bool, ScanError>
    where
        I: FingerprintIndex + Sync,
        S: ProvenanceStore + Sync,
        M: SemanticMatcher + Sync,
        L: MessageSource + Sync,
    {
        if session.nearer.reads.is_none() {
            let pending = std::mem::take(&mut session.nearer.pending);
            session.nearer.reads = Some(Reads::new(pending));
        }
        let holding: Vec<usize> = session
            .nearer
            .reads
            .as_ref()
            .and_then(|reads| reads.holding.get(&fingerprint))
            .cloned()
            .unwrap_or_default();
        if holding.is_empty() {
            return Ok(false);
        }
        let source_only = self.source_only(session, key).await?;
        if source_only.is_empty() {
            return Ok(false);
        }
        let floor = self.winnowing().w();
        for layer in holding {
            if let Some(known) = session.nearer.direct.get(&(layer, key)) {
                if *known {
                    return Ok(true);
                }
                continue;
            }
            let held = session.nearer.reads.as_ref().map_or(0, |reads| {
                reads
                    .layer(layer)
                    .iter()
                    .filter(|fingerprint| source_only.contains(fingerprint))
                    .count()
            });
            let is_direct = held >= floor;
            session.nearer.direct.insert((layer, key), is_direct);
            if is_direct {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The k-grams of the forward's source message that the forwarder's
    /// output message does not hold; empty when either body is gone.
    async fn source_only<I, S, M, L>(
        &self,
        session: &mut Session<'_, I, S, M, L>,
        key: ForwardKey,
    ) -> Result<Arc<FingerprintSet>, ScanError>
    where
        I: FingerprintIndex + Sync,
        S: ProvenanceStore + Sync,
        M: SemanticMatcher + Sync,
        L: MessageSource + Sync,
    {
        if let Some(set) = session.nearer.source_only.get(&key) {
            return Ok(Arc::clone(set));
        }
        let (source, forwarder) = key;
        let source = session
            .body(source)
            .await?
            .map(|message| self.cached_kgrams(message));
        let forwarder = session
            .body(forwarder)
            .await?
            .map(|message| self.cached_kgrams(message));
        let set = match (source, forwarder) {
            (Some(source), Some(forwarder)) => {
                let written: FingerprintSet = forwarder.iter().flatten().copied().collect();
                source
                    .iter()
                    .flatten()
                    .filter(|fingerprint| !written.contains(fingerprint))
                    .copied()
                    .collect()
            }
            _ => FingerprintSet::new(),
        };
        let set = Arc::new(set);
        session.nearer.source_only.insert(key, Arc::clone(&set));
        Ok(set)
    }
}
