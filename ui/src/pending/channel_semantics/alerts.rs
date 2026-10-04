//! Which alerts the alert list shows. Stand-in for the port's
//! `AlertSubject::shown`.

use crosstalk_spec::aggregates::alert::AlertSubject;
use crosstalk_spec::aliases::Aliases;
use crosstalk_spec::ids::{ChannelId, TransmissionId};

/// Whether an alert about `subject` is shown: not when it is about a hidden
/// channel or a transmission now within one agent, decided at read time
/// after resolving the subject through `aliases`.
pub fn alert_shown(
    subject: AlertSubject,
    aliases: impl Aliases,
    hidden: impl Fn(ChannelId) -> bool,
    within_one_agent: impl Fn(TransmissionId) -> bool,
) -> bool {
    match subject.resolved(aliases) {
        AlertSubject::Channel(channel) => !hidden(channel),
        AlertSubject::Transmission(transmission) => !within_one_agent(transmission),
        AlertSubject::Agent(_) => true,
    }
}
