//! The evidence page's view models: the transmission's state in words, its
//! matches with their kind, carrier and excerpts, and its co-accesses.

use crosstalk_spec::derived::flow::evidence::CoAccess;
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::derived::flow::transmission::{Confirmed, TransmissionState};
use crosstalk_spec::derived::provenance::matching::{Carrier, Codec, MatchKind};
use crosstalk_spec::ids::AgentId;

use crate::components::{format_bytes, format_duration, format_time, short_id};
use crate::contract::evidence::{AccessDetail, Excerpt, MatchEvidence};
use crate::contract::graph::TransmissionStateKind;
use crate::pages::common::links::agent_url;
use crate::pages::common::lookup::AgentNames;
use crate::pages::common::transmissions::Named;
use crate::url::view_state::ViewState;

/// How strong the evidence behind a transmission is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strength {
    /// Not judged yet: detected or waiting for content.
    Pending,
    /// Backed by content matches.
    Content,
    /// Only the access pattern links the two agents.
    AccessOnly,
    /// Was suspected and expired without content evidence.
    Discarded,
}

pub fn strength(kind: TransmissionStateKind) -> Strength {
    match kind {
        TransmissionStateKind::Detected | TransmissionStateKind::AwaitingContent => {
            Strength::Pending
        }
        TransmissionStateKind::Suspected => Strength::AccessOnly,
        TransmissionStateKind::Confirmed
        | TransmissionStateKind::Classified
        | TransmissionStateKind::Aggregated => Strength::Content,
        TransmissionStateKind::Discarded => Strength::Discarded,
    }
}

/// Whether an operator can record a verdict: suspected, discarded and every
/// state holding a confirmation can be judged; nothing else has evidence.
pub fn judgeable(kind: TransmissionStateKind) -> bool {
    strength(kind) != Strength::Pending
}

fn plural(n: usize, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

fn confirmed_text(c: &Confirmed) -> String {
    format!(
        "Confirmed at {}: {} carried {} of the sender's text to the reader.",
        format_time(c.at()),
        plural(
            c.content().iter().count(),
            "content match",
            "content matches"
        ),
        format_bytes(c.matched_bytes().get())
    )
}

/// The state with its data, as one or two sentences.
pub fn state_text(state: &TransmissionState) -> String {
    match state {
        TransmissionState::Detected => {
            "Detected: the gateway saw a possible transmission and is gathering evidence."
                .to_owned()
        }
        TransmissionState::AwaitingContent {
            window_closes_at, ..
        } => format!(
            "Awaiting content: the reader read a resource the sender wrote. The gateway looks for a content match until {}.",
            format_time(*window_closes_at)
        ),
        TransmissionState::Suspected { co_access, since } => format!(
            "Suspected since {}: only the access pattern links the two agents ({}). No content match was found, so this is weaker evidence than a confirmed transmission.",
            format_time(*since),
            plural(co_access.iter().count(), "co-access", "co-accesses")
        ),
        TransmissionState::Confirmed(c) => confirmed_text(c),
        TransmissionState::Classified {
            confirmed,
            classification,
        } => format!(
            "{} Classified under topic model v{}.",
            confirmed_text(confirmed),
            classification.version.0
        ),
        TransmissionState::Aggregated {
            confirmed,
            classification,
        } => format!(
            "{} Classified under topic model v{} and counted into its edge.",
            confirmed_text(confirmed),
            classification.version.0
        ),
        TransmissionState::Discarded { at, co_access } => format!(
            "Discarded at {}: it was suspected from {}, but no content evidence arrived before expiry. It is not counted anywhere.",
            format_time(*at),
            plural(co_access.iter().count(), "co-access", "co-accesses")
        ),
    }
}

/// The state in words without its data, for callers without `Content`.
pub fn kind_text(kind: TransmissionStateKind) -> &'static str {
    match kind {
        TransmissionStateKind::Detected => "Detected: evidence is being gathered.",
        TransmissionStateKind::AwaitingContent => "Awaiting a content match.",
        TransmissionStateKind::Suspected => {
            "Suspected: only the access pattern links the two agents; weaker evidence."
        }
        TransmissionStateKind::Confirmed => "Confirmed by content matches.",
        TransmissionStateKind::Classified => "Confirmed by content matches and classified.",
        TransmissionStateKind::Aggregated => "Confirmed, classified and counted into its edge.",
        TransmissionStateKind::Discarded => {
            "Discarded: suspected, but no content evidence arrived before expiry."
        }
    }
}

/// When the transmission was confirmed, if it was.
pub fn confirmed_at(state: &TransmissionState) -> Option<crosstalk_spec::support::Timestamp> {
    match state {
        TransmissionState::Confirmed(c)
        | TransmissionState::Classified { confirmed: c, .. }
        | TransmissionState::Aggregated { confirmed: c, .. } => Some(c.at()),
        _ => None,
    }
}

/// The co-access records a state carries.
pub fn co_accesses(state: &TransmissionState) -> Vec<CoAccess> {
    match state {
        TransmissionState::Detected => Vec::new(),
        TransmissionState::AwaitingContent { co_access, .. } => vec![*co_access],
        TransmissionState::Suspected { co_access, .. }
        | TransmissionState::Discarded { co_access, .. } => co_access.iter().copied().collect(),
        TransmissionState::Confirmed(c)
        | TransmissionState::Classified { confirmed: c, .. }
        | TransmissionState::Aggregated { confirmed: c, .. } => c.co_access().to_vec(),
    }
}

pub fn codec_name(codec: Codec) -> &'static str {
    match codec {
        Codec::Base64 => "base64",
        Codec::Hex => "hex",
        Codec::UrlEncoding => "url",
        Codec::UnicodeNormalization => "unicode normalization",
    }
}

/// A match kind in words, with its decode chain or score.
pub fn kind_label(kind: &MatchKind) -> String {
    match kind {
        MatchKind::Exact => "exact".to_owned(),
        MatchKind::Normalized => "normalized (case and whitespace)".to_owned(),
        MatchKind::Decoded(chain) => format!(
            "decoded {}",
            chain
                .iter()
                .map(|c| codec_name(*c))
                .collect::<Vec<_>>()
                .join(" → ")
        ),
        MatchKind::Semantic(score) => format!("semantic, similarity {:.2}", score.get()),
    }
}

/// Where the matched text sat in the reader's exchange.
pub fn carrier_label(carrier: &Carrier) -> String {
    match carrier {
        Carrier::ToolResult(call) => format!("in a tool result (call {})", call.0),
        Carrier::UserTurn => "in a user turn (relayed)".to_owned(),
        Carrier::SystemPrompt => "in the system prompt".to_owned(),
        Carrier::ReaderOutput => {
            "in the reader's own output (its input was not visible)".to_owned()
        }
    }
}

/// An excerpt split around its highlight, with what was cut.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExcerptView {
    pub before: String,
    pub matched: String,
    pub after: String,
    pub elided_before: Option<String>,
    pub elided_after: Option<String>,
}

fn elided(bytes: u32) -> Option<String> {
    (bytes > 0).then(|| format!("{} not shown", format_bytes(u64::from(bytes))))
}

impl ExcerptView {
    pub fn new(excerpt: &Excerpt) -> Self {
        let (before, after) = excerpt.elided();
        Self {
            before: excerpt.before().to_owned(),
            matched: excerpt.matched().to_owned(),
            after: excerpt.after().to_owned(),
            elided_before: elided(before),
            elided_after: elided(after),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchView {
    pub number: usize,
    pub kind: String,
    pub carrier: String,
    pub matched: String,
    pub sender: Named,
    pub origin: ExcerptView,
    pub read: ExcerptView,
}

pub fn match_views(
    matches: &[MatchEvidence],
    names: &AgentNames,
    state: &ViewState,
) -> Vec<MatchView> {
    matches
        .iter()
        .enumerate()
        .map(|(i, m)| {
            let sender = m.content_match.origin_agent();
            MatchView {
                number: i + 1,
                kind: kind_label(m.content_match.kind()),
                carrier: carrier_label(m.content_match.carrier()),
                matched: format_bytes(u64::from(m.content_match.matched_bytes().get())),
                sender: named(sender, names, state),
                origin: ExcerptView::new(&m.origin),
                read: ExcerptView::new(&m.read),
            }
        })
        .collect()
}

pub fn named(id: AgentId, names: &AgentNames, state: &ViewState) -> Named {
    Named {
        url: agent_url(id, state),
        name: names.name(id),
    }
}

/// One side of a co-access: who, when, what.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessSide {
    pub agent: Named,
    pub at: String,
    pub resource: Option<Locator>,
}

/// A write followed by a read of the same resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoAccessView {
    pub write: Option<AccessSide>,
    pub read: Option<AccessSide>,
    pub lag: String,
}

fn side(
    id: crosstalk_spec::ids::AccessId,
    accesses: &[AccessDetail],
    names: &AgentNames,
    state: &ViewState,
) -> Option<AccessSide> {
    accesses
        .iter()
        .find(|a| a.access.id == id)
        .map(|a| AccessSide {
            agent: named(a.access.agent, names, state),
            at: format_time(a.access.at),
            resource: Some(a.resource.locator.clone()),
        })
}

pub fn co_access_views(
    records: &[CoAccess],
    accesses: &[AccessDetail],
    names: &AgentNames,
    state: &ViewState,
) -> Vec<CoAccessView> {
    records
        .iter()
        .map(|c| CoAccessView {
            write: side(c.write(), accesses, names, state),
            read: side(c.read(), accesses, names, state),
            lag: format_duration(c.lag()),
        })
        .collect()
}

/// The agents an evidence page names: matches' senders and every access's
/// agent.
pub fn named_agents(matches: &[MatchEvidence], accesses: &[AccessDetail]) -> Vec<AgentId> {
    matches
        .iter()
        .map(|m| m.content_match.origin_agent())
        .chain(accesses.iter().map(|a| a.access.agent))
        .collect()
}

/// The short id shown in the title.
pub fn title_id(id: crosstalk_spec::ids::TransmissionId) -> String {
    use crate::url::ulid::UlidId;
    short_id(id.to_ulid())
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::support::{NonEmpty, Similarity, Timestamp};

    use super::*;

    #[test]
    fn decode_chains_are_spelled_out() {
        let chain = NonEmpty::from_vec(vec![Codec::Base64, Codec::UrlEncoding]).expect("chain");
        assert_eq!(
            kind_label(&MatchKind::Decoded(chain)),
            "decoded base64 → url"
        );
        let score = Similarity::new(0.834).expect("score");
        assert_eq!(
            kind_label(&MatchKind::Semantic(score)),
            "semantic, similarity 0.83"
        );
        assert_eq!(kind_label(&MatchKind::Exact), "exact");
    }

    #[test]
    fn carriers_say_where_the_text_arrived() {
        use crosstalk_spec::observed::message::ToolCallId;
        assert_eq!(
            carrier_label(&Carrier::ToolResult(ToolCallId("toolu_1".into()))),
            "in a tool result (call toolu_1)"
        );
        assert!(carrier_label(&Carrier::ReaderOutput).contains("not visible"));
    }

    #[test]
    fn excerpts_split_around_the_highlight() {
        let excerpt = Excerpt::new("say hello world".into(), 4..9, (12, 0)).expect("excerpt");
        let view = ExcerptView::new(&excerpt);
        assert_eq!(
            (
                view.before.as_str(),
                view.matched.as_str(),
                view.after.as_str()
            ),
            ("say ", "hello", " world")
        );
        assert_eq!(view.elided_before.as_deref(), Some("12 B not shown"));
        assert_eq!(view.elided_after, None);
    }

    #[test]
    fn strength_orders_the_states() {
        assert_eq!(
            strength(TransmissionStateKind::Suspected),
            Strength::AccessOnly
        );
        assert_eq!(
            strength(TransmissionStateKind::Discarded),
            Strength::Discarded
        );
        assert_eq!(
            strength(TransmissionStateKind::Aggregated),
            Strength::Content
        );
        assert!(!judgeable(TransmissionStateKind::AwaitingContent));
        assert!(judgeable(TransmissionStateKind::Discarded));
    }

    #[test]
    fn detected_states_have_no_co_access_or_confirmation() {
        assert!(co_accesses(&TransmissionState::Detected).is_empty());
        assert_eq!(confirmed_at(&TransmissionState::Detected), None);
        let _ = Timestamp::from_micros(0);
    }
}
