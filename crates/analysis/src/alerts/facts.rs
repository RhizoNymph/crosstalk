//! What the alerts list reads from other layers about an alert's subject:
//! a transmission's stored route (the channel filter) and whether readers
//! show the alert at all (`AlertSubject::shown`: not about a hidden
//! channel, nor about a transmission whose agents have since merged).
//!
//! [`FlowFacts`] answers from the spec's L5 reads (`TransmissionStore`,
//! `ChannelReads`) and both directories; [`NoFacts`] knows no transmission
//! or channel, so it shows every alert and routes nothing.

use crosstalk_spec::aggregates::alert::AlertSubject;
use crosstalk_spec::aliases::Resolve;
use crosstalk_spec::derived::flow::channel::confirmation::Listing;
use crosstalk_spec::derived::flow::transmission::{Crossing, Route};
use crosstalk_spec::ids::{AgentId, ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelReads;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;

/// Why a fact could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("reading a subject's facts: {reason}")]
pub struct FactsError {
    pub reason: String,
}

/// What other layers know about an alert's subject at a read.
pub trait SubjectFacts: Send + Sync {
    /// The route `transmission` is stored with; `None` for an unknown one.
    fn route(
        &self,
        transmission: TransmissionId,
    ) -> impl Future<Output = Result<Option<Route>, FactsError>> + Send;

    /// Whether readers show an alert about `subject` now
    /// (`AlertSubject::shown`).
    fn shown(&self, subject: AlertSubject)
    -> impl Future<Output = Result<bool, FactsError>> + Send;
}

/// No facts: every alert is shown, no transmission has a route.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoFacts;

impl SubjectFacts for NoFacts {
    async fn route(&self, _transmission: TransmissionId) -> Result<Option<Route>, FactsError> {
        Ok(None)
    }

    async fn shown(&self, _subject: AlertSubject) -> Result<bool, FactsError> {
        Ok(true)
    }
}

/// Facts from L5's transmission store and channel reads, resolved through
/// `directory`'s merges and supersessions.
#[derive(Debug, Clone)]
pub struct FlowFacts<T, C, D> {
    pub transmissions: T,
    pub channels: C,
    pub directory: D,
}

fn facts_error(error: impl std::fmt::Debug) -> FactsError {
    FactsError {
        reason: format!("{error:?}"),
    }
}

impl<T, C, D> FlowFacts<T, C, D>
where
    D: AgentDirectory + ChannelDirectory,
{
    fn aliases(
        &self,
    ) -> Resolve<impl Fn(AgentId) -> AgentId + Copy + '_, impl Fn(ChannelId) -> ChannelId + Copy + '_>
    {
        Resolve {
            agents: |agent| AgentDirectory::canonical(&self.directory, agent),
            channels: |channel| ChannelDirectory::canonical(&self.directory, channel),
        }
    }
}

impl<T, C, D> SubjectFacts for FlowFacts<T, C, D>
where
    T: TransmissionStore + Send + Sync,
    C: ChannelReads + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Send + Sync,
{
    async fn route(&self, transmission: TransmissionId) -> Result<Option<Route>, FactsError> {
        Ok(self
            .transmissions
            .transmission(transmission)
            .await
            .map_err(facts_error)?
            .map(|stored| stored.route))
    }

    async fn shown(&self, subject: AlertSubject) -> Result<bool, FactsError> {
        let hidden = match subject.resolved(self.aliases()) {
            AlertSubject::Channel(channel) => self
                .channels
                .channel(channel)
                .await
                .map_err(facts_error)?
                .is_some_and(|stored| stored.listing() == Some(Listing::Hidden)),
            AlertSubject::Transmission(_) | AlertSubject::Agent(_) => false,
        };
        let within = match subject {
            AlertSubject::Transmission(transmission) => self
                .transmissions
                .transmission(transmission)
                .await
                .map_err(facts_error)?
                .is_some_and(|stored| stored.crossing(self.aliases()) == Crossing::WithinOneAgent),
            AlertSubject::Channel(_) | AlertSubject::Agent(_) => false,
        };
        Ok(subject.shown(self.aliases(), |_| hidden, |_| within))
    }
}
