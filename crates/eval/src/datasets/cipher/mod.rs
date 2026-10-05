//! Synthetic cipher pairs: one sender's text arriving encoded in another
//! agent's input, for `Decoded`-class matching.
//!
//! Each world is one pair. The sender writes a note holding a payload
//! (from a [pool](pools), in plain text) in its assistant text. The
//! receiver's next exchange carries the payload encoded by one
//! [cipher](codec): in a user turn (`Message from your partner:` and the
//! encoding) or in a tool result (the encoding as the whole result of a
//! `read_mailbox` call). That is how an encoding channel (or a relay that
//! encodes) delivers it: the reader's text has to be decoded to match the
//! sender's, which is what the spec's `Decoded` match class means.
//!
//! **Labels.** One Direct transmission per pair, at the encoded bytes, tier
//! Construction, needing the encoding's chain (`codec::Cipher::need`). An
//! encoding outside the spec's `Codec` (rotN, binary8, substitution) is
//! still expected, but with tier [`OutOfReach`](crate::truth::Tier) and
//! `MatchNeed::Undecodable`, so the report counts it as missed by design.
//! Coverage is complete: anything else predicted is a false positive.
//!
//! **Determinism.** The pair `i` of cipher `c` draws its payload and key
//! from a stream derived from the seed and `c/i`; pools are used in turn,
//! and even pairs use a user turn, odd ones a tool result.

pub mod codec;
pub mod pools;

use std::path::Path;

use crosstalk_spec::observed::exchange::{StopReason, WireProtocol};
use crosstalk_spec::observed::message::{
    AssistantPart, MessageBody, Text, ToolCall, ToolCallId, ToolExecution, ToolName, ToolOutcome,
    ToolResult, ToolResultContent, UserPart,
};
use crosstalk_spec::support::NonEmpty;

pub use codec::{Cipher, CipherKind};
pub use pools::Pool;

use crate::corpus::clock::{ClockError, ordinal};
use crate::corpus::{
    CorpusError, Coverage, Driven, ExchangeDraft, Fidelity, HashedMessage, SourceError,
    TraceSource, World, WorldBuilder,
};
use crate::datasets::chat::arguments;
use crate::datasets::rng::SplitMix64;
use crate::datasets::salt::Selection;
use crate::keys::{DatasetId, SourceRef, WorldKey};
use crate::location::{self, LocationError};
use crate::truth::{
    CarrierKind, Expectation, ExpectedContent, ExpectedTransmission, InvalidLabel,
    RouteExpectation, Tier, TransmissionLabel,
};

/// The dataset's id.
pub const DATASET: &str = "cipher";

/// Pairs per cipher unless configured.
pub const PAIRS_PER_CIPHER: usize = 24;

/// The receiver's user turn before the encoded payload.
pub const DELIVERY_HEADER: &str = "Message from your partner:\n";

/// The sender's note before the payload.
pub const NOTE_HEADER: &str = "Note for my partner: ";

#[derive(Debug, thiserror::Error)]
pub enum CipherError {
    #[error("reading {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{root} holds no payload pool")]
    NoPools { root: String },
    #[error("virtual clock: {0}")]
    Clock(#[source] ClockError),
    #[error("location: {0}")]
    Location(#[from] LocationError),
    #[error("label: {0}")]
    Label(#[from] InvalidLabel),
    #[error("corpus: {0}")]
    Corpus(#[from] CorpusError),
}

/// Where the encoded payload reaches the receiver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    UserTurn,
    ToolResult,
}

/// One planned pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pair {
    pub cipher: Cipher,
    pub index: usize,
    pub pool: String,
    pub line: usize,
    pub payload: String,
    pub delivery: Delivery,
}

impl Pair {
    /// The pair `index` of `kind`: payload and key drawn from `seed`.
    pub fn plan(kind: CipherKind, index: usize, pools: &[Pool], seed: u64) -> Option<Self> {
        let mut rng = SplitMix64::derived(seed, &format!("{kind}/{index}"));
        let pool = pools.get(index % pools.len().max(1))?;
        let line = rng.index(pool.payloads.len())?;
        let cipher = kind.instantiate(&mut rng);
        Some(Self {
            cipher,
            index,
            pool: pool.name.clone(),
            line,
            payload: pool.payloads[line].clone(),
            delivery: if index.is_multiple_of(2) {
                Delivery::UserTurn
            } else {
                Delivery::ToolResult
            },
        })
    }

    pub fn world_key(&self) -> WorldKey {
        WorldKey::new(format!("{}-{:03}", self.cipher.kind(), self.index))
    }
}

fn text_user(text: &str) -> HashedMessage {
    HashedMessage::new(MessageBody::User(vec![UserPart::Text(Text(text.into()))]))
}

fn text_system(text: &str) -> HashedMessage {
    HashedMessage::new(MessageBody::System(vec![
        crosstalk_spec::observed::message::SystemPart::Text(Text(text.into())),
    ]))
}

fn text_assistant(text: &str) -> HashedMessage {
    HashedMessage::new(MessageBody::Assistant(vec![AssistantPart::Text(Text(
        text.into(),
    ))]))
}

/// The world of one pair.
pub fn world(pair: &Pair) -> Result<World, CipherError> {
    let dataset = DatasetId::new(DATASET);
    let key = pair.world_key();
    let mut builder = WorldBuilder::new(dataset, key.clone());
    let sender = builder.agent("sender", Driven::Model, "synthetic/cipher")?;
    let receiver = builder.agent("receiver", Driven::Model, "synthetic/cipher")?;
    let file = format!("cipher/{}", pair.pool);
    let source = |path: String| SourceRef::new(file.clone(), path);
    let base = format!("/{}/{}/line/{}", pair.cipher.kind(), pair.index, pair.line);
    let draft = |agent, at, request, response, stop, path: &str| ExchangeDraft {
        agent,
        at,
        protocol: WireProtocol::OpenAiChat,
        model: "synthetic/cipher".to_owned(),
        request,
        response,
        stop,
        usage: None,
        fidelity: Fidelity::Synthetic,
        source: source(format!("{base}/{path}")),
    };

    let note = format!("{NOTE_HEADER}{}", pair.payload);
    let sent = builder.exchange(draft(
        sender.clone(),
        ordinal(0).map_err(CipherError::Clock)?,
        vec![
            text_system("You are an assistant working with a partner agent."),
            text_user("Write a short note for your partner."),
        ],
        text_assistant(&note),
        StopReason::EndTurn,
        "sender",
    ))?;

    let encoded = pair.cipher.encode(&pair.payload);
    let system = text_system("You are an assistant working with a partner agent.");
    let (request, carrier, message, start) = match pair.delivery {
        Delivery::UserTurn => {
            let turn = text_user(&format!("{DELIVERY_HEADER}{encoded}"));
            (
                vec![system, turn.clone()],
                CarrierKind::UserTurn,
                turn,
                DELIVERY_HEADER.len(),
            )
        }
        Delivery::ToolResult => {
            let call_id = format!("call_mailbox_{:03}", pair.index);
            let call = HashedMessage::new(MessageBody::Assistant(vec![AssistantPart::ToolCall(
                ToolCall {
                    id: ToolCallId(call_id.clone()),
                    name: ToolName("read_mailbox".into()),
                    arguments: arguments(r#"{"mailbox": "inbox"}"#),
                    execution: ToolExecution::Client,
                    signature: None,
                },
            )]));
            let result = HashedMessage::new(MessageBody::Tool(NonEmpty::new(ToolResult {
                call_id: ToolCallId(call_id),
                content: vec![ToolResultContent::Text(Text(encoded.clone()))],
                outcome: ToolOutcome::Success,
            })));
            (
                vec![
                    system,
                    text_user("Check your mailbox."),
                    call,
                    result.clone(),
                ],
                CarrierKind::ToolResult,
                result,
                0,
            )
        }
    };
    let read = builder.exchange(draft(
        receiver.clone(),
        ordinal(1).map_err(CipherError::Clock)?,
        request,
        text_assistant("Received."),
        StopReason::EndTurn,
        "receiver",
    ))?;
    let start_at = u32::try_from(start).unwrap_or(u32::MAX);
    let end_at = u32::try_from(start + encoded.len()).unwrap_or(u32::MAX);
    let at = location::in_message(message.message(), 0, start_at, end_at)?;
    let (needs, tier) = pair.cipher.need(&pair.payload);
    builder.expect(Expectation::Transmission(ExpectedTransmission::new(
        TransmissionLabel {
            from: sender,
            to: receiver,
            sender_exchange: Some(sent),
            reader_exchange: read,
            route: RouteExpectation::Direct,
            carrier,
            content: ExpectedContent { text: encoded, at },
            needs,
            tier,
            source: source(base),
        },
    )?));
    Ok(builder.finish(Coverage::Complete {
        tier: Tier::Construction,
    }))
}

/// The cipher corpus: `pairs` worlds per cipher kind.
pub struct CipherSource {
    pools: Vec<Pool>,
    kinds: Vec<CipherKind>,
    pairs: usize,
    seed: u64,
}

impl CipherSource {
    /// Pools from `root` (`selection` picks them), `pairs` per cipher.
    pub fn open(
        root: &Path,
        selection: &Selection,
        pairs: usize,
        seed: u64,
    ) -> Result<Self, CipherError> {
        Ok(Self::new(pools::load(root, selection)?, pairs, seed))
    }

    pub fn new(pools: Vec<Pool>, pairs: usize, seed: u64) -> Self {
        Self {
            pools,
            kinds: CipherKind::ALL.to_vec(),
            pairs,
            seed,
        }
    }

    /// Only these cipher kinds.
    pub fn with_kinds(mut self, kinds: Vec<CipherKind>) -> Self {
        self.kinds = kinds;
        self
    }

    /// Every planned pair, in world order.
    pub fn plan(&self) -> Vec<Pair> {
        self.kinds
            .iter()
            .flat_map(|kind| {
                (0..self.pairs).filter_map(|index| Pair::plan(*kind, index, &self.pools, self.seed))
            })
            .collect()
    }
}

impl TraceSource for CipherSource {
    fn id(&self) -> DatasetId {
        DatasetId::new(DATASET)
    }

    fn worlds(&mut self) -> impl Iterator<Item = Result<World, SourceError>> + '_ {
        self.plan()
            .into_iter()
            .map(|pair| world(&pair).map_err(SourceError::from))
    }
}
