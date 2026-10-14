//! The scenario's whole answer set, read through the same calls in every
//! run, and the renaming that makes two runs comparable when their minted
//! ids differ.
//!
//! ```text
//! answers(surface, caller, transmissions, scenario, watermark)
//!   agents (A and B by session) ─▶ topology edges ─▶ the A→B channel edge's transmissions
//!   every stored transmission ─▶ their summaries and evidence pages ─▶ channels ─▶ watermark
//! Answers::renamed(): every `<Kind>Id(<ulid>)` ─▶ `<Kind>Id(@<millisecond>#<n>)`
//! ```
//!
//! Two runs of the same traffic agree exactly on everything a detection
//! decides, but not on the ids drawn from a random source: agents, merges,
//! conversations and captured envelopes (and every id derived from those),
//! which `serve` draws from OS entropy and a restarted test process draws
//! again from its seed. Each such id is a ULID whose top 48 bits are the
//! millisecond it was minted at, which the traffic fixes. [`Answers::renamed`]
//! keeps that millisecond and replaces the random bits by the order in
//! which the id first appears in the answers, per kind and millisecond: A
//! is the first agent named at its first exchange's millisecond in both
//! runs, whatever bits its id drew. Ids that are already functions of the
//! traffic (exchanges the scenario names, content hashes, derived
//! transmission ids) rename the same in both runs, so renaming hides only
//! where the bits came from, never a difference in what was decided: two
//! distinct ids stay distinct, an id repeated stays repeated, and every
//! other byte of the answers is compared as read.

use std::collections::HashMap;
use std::time::Duration;

use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l8_surface::{Caller, QueryApi};
use crosstalk_spec::support::Timestamp;

use crate::read::{self, ReadError};
use crate::scenario::Scenario;

/// The bits of a ULID below its millisecond.
const RANDOM_BITS: u32 = 80;

/// Everything the comparison reads, as debug text per answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answers {
    pub agents: String,
    pub edges: String,
    pub edge_transmissions: String,
    pub summaries: String,
    pub evidence: String,
    pub channels: String,
    pub transmissions: String,
    pub watermark: u64,
}

/// Read the whole answer set: the scenario's agents, the topology graph,
/// the A→B channel edge's transmissions, every stored transmission with
/// its summary and evidence page, the channels and the watermark.
pub async fn answers<Q, T>(
    surface: &Q,
    caller: &Caller,
    transmissions: &T,
    scenario: &Scenario,
    watermark: Timestamp,
) -> Result<Answers, ReadError>
where
    Q: QueryApi + Sync,
    T: TransmissionStore + Sync,
{
    let window = read::window(scenario, Duration::from_secs(300))?;
    let agents = read::agents(surface, caller, scenario, window).await?;
    let edges = read::edges(surface, caller, window).await?;
    let edge_transmissions = match agents
        .agents
        .and_then(|agents| read::channel_edge(&edges, agents))
    {
        Some(edge) => format!(
            "{:?}",
            read::edge_transmissions(surface, caller, edge, window).await?
        ),
        None => "no channel edge".to_owned(),
    };
    let stored = read::all_transmissions(transmissions).await?;
    let ids: Vec<_> = stored.iter().map(|transmission| transmission.id).collect();
    let summaries = match ids.is_empty() {
        true => "none".to_owned(),
        false => format!("{:?}", read::summaries(surface, caller, ids.clone()).await?),
    };
    let mut evidence = Vec::new();
    for id in &ids {
        evidence.push(format!("{:?}", read::evidence(surface, caller, *id).await?));
    }
    Ok(Answers {
        agents: format!("{agents:?}"),
        edges: format!("{edges:?}"),
        edge_transmissions,
        summaries,
        evidence: evidence.join("\n"),
        channels: format!("{:?}", read::channels(surface, caller).await?),
        transmissions: format!("{stored:?}"),
        watermark: watermark.as_micros(),
    })
}

impl Answers {
    /// The answers with every entity id renamed (see the module docs), one
    /// renaming across all of them, the fields read in declaration order.
    pub fn renamed(&self) -> Answers {
        let mut names = Renaming::default();
        Answers {
            agents: names.rename(&self.agents),
            edges: names.rename(&self.edges),
            edge_transmissions: names.rename(&self.edge_transmissions),
            summaries: names.rename(&self.summaries),
            evidence: names.rename(&self.evidence),
            channels: names.rename(&self.channels),
            transmissions: names.rename(&self.transmissions),
            watermark: self.watermark,
        }
    }

    /// Whether the answers hold a channel transmission: the relay was
    /// detected.
    pub fn detected(&self) -> bool {
        self.transmissions.contains("Channel")
    }
}

/// The names given so far: an id's name, and how many ids of a kind were
/// named at a millisecond.
#[derive(Debug, Default)]
struct Renaming {
    names: HashMap<(String, u128), String>,
    counts: HashMap<(String, u128), u32>,
}

impl Renaming {
    /// `text` with each `<Kind>Id(<decimal>)` renamed.
    fn rename(&mut self, text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        let mut rest = text;
        while let Some(found) = next_id(rest) {
            out.push_str(&rest[..found.start]);
            let name = self.name(found.kind, found.value);
            out.push_str(found.kind);
            out.push('(');
            out.push_str(&name);
            out.push(')');
            rest = &rest[found.end..];
        }
        out.push_str(rest);
        out
    }

    fn name(&mut self, kind: &str, value: u128) -> String {
        let key = (kind.to_owned(), value);
        if let Some(name) = self.names.get(&key) {
            return name.clone();
        }
        let millis = value >> RANDOM_BITS;
        let count = self.counts.entry((kind.to_owned(), millis)).or_insert(0);
        *count += 1;
        let name = format!("@{millis}#{count}");
        self.names.insert(key, name.clone());
        name
    }
}

/// One `<Kind>Id(<decimal>)` in a text: where it starts and ends, its
/// kind and its value.
struct Found<'t> {
    start: usize,
    end: usize,
    kind: &'t str,
    value: u128,
}

/// The first `<Kind>Id(<decimal>)` in `text`: an identifier starting with
/// an upper-case letter and ending in `Id`, an opening parenthesis, decimal
/// digits that fit a `u128`, a closing parenthesis (how an entity id's
/// `Debug` writes it).
fn next_id(text: &str) -> Option<Found<'_>> {
    let mut from = 0;
    while let Some(at) = text[from..].find("Id(") {
        let open = from + at + 2;
        from = open + 1;
        let start = text[..open - 2]
            .char_indices()
            .rev()
            .take_while(|(_, c)| c.is_ascii_alphanumeric() || *c == '_')
            .last()
            .map_or(open - 2, |(index, _)| index);
        let kind = &text[start..open];
        if kind.len() <= 2 || !kind.starts_with(|c: char| c.is_ascii_uppercase()) {
            continue;
        }
        let digits = text[open + 1..]
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(text.len() - open - 1);
        let close = open + 1 + digits;
        if digits == 0 || !text[close..].starts_with(')') {
            continue;
        }
        let Ok(value) = text[open + 1..close].parse::<u128>() else {
            continue;
        };
        return Some(Found {
            start,
            end: close + 1,
            kind,
            value,
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ulid(millis: u128, random: u128) -> u128 {
        (millis << RANDOM_BITS) | random
    }

    #[test]
    fn ids_keep_their_millisecond_and_are_numbered_by_first_appearance() {
        let text = format!(
            "AgentRead {{ listed: [AgentId({}), AgentId({})], a: AgentId({}) }} ConversationId({})",
            ulid(7, 99),
            ulid(9, 3),
            ulid(7, 99),
            ulid(7, 5),
        );
        let mut names = Renaming::default();
        assert_eq!(
            names.rename(&text),
            "AgentRead { listed: [AgentId(@7#1), AgentId(@9#1)], a: AgentId(@7#1) } ConversationId(@7#1)"
        );
    }

    #[test]
    fn runs_differing_only_in_drawn_bits_rename_alike() {
        let one = format!("[AgentId({}), AgentId({})]", ulid(7, 1), ulid(8, 40));
        let two = format!("[AgentId({}), AgentId({})]", ulid(7, 77), ulid(8, 2));
        assert_eq!(
            Renaming::default().rename(&one),
            Renaming::default().rename(&two)
        );
    }

    #[test]
    fn distinct_ids_stay_distinct_and_other_text_is_kept() {
        let text = format!(
            "x: AgentId({}), y: AgentId({}), Id(5), aId(6), FooId(), BarId(12x)",
            ulid(7, 1),
            ulid(7, 2)
        );
        assert_eq!(
            Renaming::default().rename(&text),
            "x: AgentId(@7#1), y: AgentId(@7#2), Id(5), aId(6), FooId(), BarId(12x)"
        );
    }

    #[test]
    fn a_duplicated_id_shows_after_renaming() {
        let once = format!("[TransmissionId({})]", ulid(7, 1));
        let twice = format!(
            "[TransmissionId({}), TransmissionId({})]",
            ulid(7, 1),
            ulid(7, 2)
        );
        assert_ne!(
            Renaming::default().rename(&once),
            Renaming::default().rename(&twice)
        );
    }
}
