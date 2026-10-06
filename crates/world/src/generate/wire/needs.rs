//! What the wire traffic must contain for the world's transmissions to
//! have happened: for every content match of a confirmed transmission, a
//! sender exchange whose response is the match's origin body, and a reader
//! exchange that receives the reader's copy the way the match's carrier
//! says.

use std::collections::HashMap;

use crosstalk_spec::derived::flow::access::{Access, AccessOp};
use crosstalk_spec::derived::flow::transmission::{DirectCarrier, Route};
use crosstalk_spec::derived::provenance::matching::Carrier;
use crosstalk_spec::ids::{AccessId, AgentId, ExchangeId, MessageHash};
use crosstalk_spec::observed::message::ToolName;
use crosstalk_spec::support::Timestamp;

use crate::clock::{HOUR, MINUTE, SECOND, minus, plus};
use crate::error::WorldError;
use crate::generate::states::TxRecord;
use crate::generate::traffic::Traffic;
use crate::mint::Mint;
use crate::rng::Rng;
use crate::text::Theme;

/// One exchange an agent's traffic must hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Need {
    /// The response is `output`, a world body holding an origin span.
    Send { output: MessageHash },
    /// The request (or, for `ReaderOutput`, the response) carries `reads`,
    /// world bodies holding the reader's copies, arriving by `carrier`;
    /// a tool result answers a call of `tool`.
    Read {
        reads: Vec<MessageHash>,
        carrier: Carrier,
        tool: ToolName,
    },
}

/// One needed exchange: its id and start, its agent and the theme the
/// prompts around it name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub agent: AgentId,
    pub at: Timestamp,
    pub exchange: ExchangeId,
    pub theme: Theme,
    pub need: Need,
}

/// The tool a reader called to get a tool result over `route`.
fn tool(route: &Route) -> ToolName {
    match route {
        Route::Direct(DirectCarrier::ToolResult(name)) => name.clone(),
        Route::Delegation(_) => ToolName("Task".to_owned()),
        _ => ToolName("read_resource".to_owned()),
    }
}

/// Every needed exchange of the `carried` transmissions, in transmission
/// order.
///
/// - Sender: a channel transmission's first match is sent in the
///   exchange of the sender's write access (its id and time); every other
///   origin body in an exchange minted a second apart after it, or, off a
///   channel, between two minutes and three hours before the read.
/// - Reader: the match's own reader exchange at the transmission's
///   opening (the read access's for a channel), holding every copy; a
///   copy in the system prompt or the reader's output takes an exchange
///   of its own, the later ones minted a second apart.
pub fn events(
    traffic: &Traffic,
    carried: &dyn Fn(&TxRecord) -> bool,
    mint: &mut Mint,
    rng: &mut Rng,
) -> Result<Vec<Event>, WorldError> {
    let accesses: HashMap<AccessId, &Access> = traffic.accesses.iter().map(|a| (a.id, a)).collect();
    let mut events = Vec::new();
    for record in traffic.transmissions.iter().filter(|r| carried(r)) {
        let Some(confirmed) = record.confirmed() else {
            continue;
        };
        let transmission = &record.transmission;
        let (from, to, opened) = (confirmed.from(), transmission.to, transmission.opened_at);
        let write = match transmission.route {
            Route::Channel(_) => record
                .accesses
                .iter()
                .filter_map(|id| accesses.get(id))
                .find(|access| access.agent == from && matches!(access.op, AccessOp::Write { .. }))
                .map(|access| (access.at, access.exchange)),
            _ => None,
        };
        let base = match write {
            Some((at, _)) => at,
            None => minus(opened, rng.between(2 * MINUTE, 3 * HOUR)),
        };
        let content = confirmed.content();
        for (i, matched) in content.iter().enumerate() {
            let Some(origin) = traffic.blobs.span(matched.origin()) else {
                continue;
            };
            let offset = u64::try_from(i).unwrap_or(0) * SECOND;
            let (at, exchange) = match write {
                Some((at, exchange)) if i == 0 => (at, exchange),
                _ => {
                    let at = plus(base, offset);
                    (at, mint.at(at)?)
                }
            };
            events.push(Event {
                agent: from,
                at,
                exchange,
                theme: record.theme,
                need: Need::Send {
                    output: origin.part.message,
                },
            });
        }
        let first = content.first();
        let carrier = first.carrier().clone();
        let reads: Vec<MessageHash> = content.iter().map(|m| m.read_at().part.message).collect();
        let read = |reads: Vec<MessageHash>| Need::Read {
            reads,
            carrier: carrier.clone(),
            tool: tool(&transmission.route),
        };
        match carrier {
            Carrier::ReaderOutput | Carrier::SystemPrompt => {
                for (i, body) in reads.iter().enumerate() {
                    let (at, exchange) = if i == 0 {
                        (opened, first.reader_exchange())
                    } else {
                        let at = plus(opened, u64::try_from(i).unwrap_or(0) * SECOND);
                        (at, mint.at(at)?)
                    };
                    events.push(Event {
                        agent: to,
                        at,
                        exchange,
                        theme: record.theme,
                        need: read(vec![*body]),
                    });
                }
            }
            Carrier::ToolResult(_) | Carrier::UserTurn => events.push(Event {
                agent: to,
                at: opened,
                exchange: first.reader_exchange(),
                theme: record.theme,
                need: read(reads),
            }),
        }
    }
    Ok(events)
}
