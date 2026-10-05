//! Transmissions in every evidence state the verdict rules distinguish,
//! and operator verdicts on two of them (one withdrawn).

use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::support::NonEmpty;

use super::super::{
    AgentFact, AgentRole, Evidence, Scenario, ScenarioError, TransmissionFact, TransmissionRole,
    VerdictFact,
};

pub const NAME: &str = "verdicts";

/// Confirmed, judged a false detection.
pub const JUDGED_FALSE: TransmissionRole = TransmissionRole::new(NAME, "judged_false");
/// Confirmed, judged genuine, then withdrawn.
pub const WITHDRAWN: TransmissionRole = TransmissionRole::new(NAME, "withdrawn");
/// Confirmed, never judged.
pub const UNJUDGED: TransmissionRole = TransmissionRole::new(NAME, "unjudged");
/// Detected: no evidence names a sender. Not judgeable.
pub const DETECTED: TransmissionRole = TransmissionRole::new(NAME, "detected");
/// A co-access whose content window is still open. Not judgeable.
pub const AWAITING: TransmissionRole = TransmissionRole::new(NAME, "awaiting");
/// Suspected: judgeable.
pub const SUSPECTED: TransmissionRole = TransmissionRole::new(NAME, "suspected");
/// Suspected and expired without content: judgeable.
pub const DISCARDED: TransmissionRole = TransmissionRole::new(NAME, "discarded");

const fn agent(name: &'static str) -> AgentRole {
    AgentRole::new(NAME, name)
}

pub fn scenario() -> Result<Scenario, ScenarioError> {
    let pairs = [
        (JUDGED_FALSE, agent("false_writer"), agent("false_reader")),
        (
            WITHDRAWN,
            agent("withdrawn_writer"),
            agent("withdrawn_reader"),
        ),
        (UNJUDGED, agent("unjudged_writer"), agent("unjudged_reader")),
        (AWAITING, agent("awaiting_writer"), agent("awaiting_reader")),
        (
            SUSPECTED,
            agent("suspected_writer"),
            agent("suspected_reader"),
        ),
        (
            DISCARDED,
            agent("discarded_writer"),
            agent("discarded_reader"),
        ),
    ];
    let mut builder = Scenario::build(NAME);
    for (role, writer, reader) in pairs {
        let state = match role {
            r if r == AWAITING => Evidence::AwaitingContent { writer },
            r if r == SUSPECTED => Evidence::Suspected { writer },
            r if r == DISCARDED => Evidence::Discarded { writer },
            _ => Evidence::Confirmed {
                writer,
                timing: super::super::Timing::Settled,
            },
        };
        builder = builder
            .fact(AgentFact::any(writer))
            .fact(AgentFact::any(reader))
            .fact(TransmissionFact::with(role, reader, state));
    }
    let detected_reader = agent("detected_reader");
    let mut withdrawn = NonEmpty::new(Some(Verdict::Genuine));
    withdrawn.push(None);
    builder
        .fact(AgentFact::any(detected_reader))
        .fact(TransmissionFact::with(
            DETECTED,
            detected_reader,
            Evidence::Detected,
        ))
        .fact(VerdictFact {
            transmission: JUDGED_FALSE,
            verdicts: NonEmpty::new(Some(Verdict::FalseDetection)),
        })
        .fact(VerdictFact {
            transmission: WITHDRAWN,
            verdicts: withdrawn,
        })
        .done()
}
