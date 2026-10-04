//! Property tests over generated evidence: a few agents writing and
//! reading two resources, and content matches of every carrier.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::transmission::{DirectCarrier, NonChannelRoute};
use crosstalk_spec::derived::provenance::matching::{Carrier, ContentMatch};
use crosstalk_spec::ids::{AgentId, ExchangeId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::TransmissionUpdate;
use crosstalk_spec::support::Timestamp;
use proptest::prelude::*;

use super::fixtures::{Scene, secs, timing, tool_result};
use super::{fold, subject};
use crate::correlate::lifecycle::Stage;
use crate::correlate::pairing;
use crate::correlate::{Decided, Kin, WindowedCorrelator};

/// One input to the correlator.
#[derive(Debug, Clone)]
enum Input {
    Access(Access),
    Match(ContentMatch),
    Exchange(ExchangeId, Timestamp),
}

/// Generated evidence.
#[derive(Debug, Clone)]
struct World {
    agents: Vec<AgentId>,
    /// Agent 1's parent is agent 0.
    parent_link: bool,
    accesses: Vec<Access>,
    matches: Vec<ContentMatch>,
    /// Every reader exchange's start.
    exchanges: BTreeMap<ExchangeId, Timestamp>,
    inputs: Vec<Input>,
}

type Plan = (
    [u8; 4],
    Vec<(u8, u8, u8, u8)>,
    Vec<(u8, u8, u8)>,
    Vec<(u8, u8, u8, u8)>,
    bool,
);

fn plan() -> impl Strategy<Value = Plan> {
    (
        prop::array::uniform4(0_u8..3),
        prop::collection::vec((0_u8..3, 0_u8..2, 0_u8..30, 0_u8..16), 0..5),
        prop::collection::vec((0_u8..3, 0_u8..2, 0_u8..30), 0..5),
        prop::collection::vec((0_u8..5, 0_u8..8, 0_u8..4, 0_u8..30), 0..7),
        any::<bool>(),
    )
}

fn build((owners, writes, reads, matches, parent_link): Plan) -> World {
    let mut scene = Scene::new(77);
    let agents: Vec<AgentId> = (0..3).map(|_| scene.agent()).collect();
    let resources = [scene.resource(), scene.resource()];
    let spans: Vec<_> = (0..4).map(|_| scene.span()).collect();
    let owner = |span: usize| agents[usize::from(owners[span])];
    let mut accesses = Vec::new();
    let mut exchanges = BTreeMap::new();
    for (agent, resource, at, mask) in writes {
        let writer = agents[usize::from(agent)];
        let held = (0..4)
            .filter(|span| mask & (1 << span) != 0 && owner(*span) == writer)
            .map(|span| spans[span])
            .collect();
        let write = scene.write(
            writer,
            resources[usize::from(resource)],
            secs(u64::from(at)),
            held,
        );
        exchanges.insert(write.exchange, write.at);
        accesses.push(write);
    }
    let mut read_accesses = Vec::new();
    for (agent, resource, at) in reads {
        let read = scene.read(
            agents[usize::from(agent)],
            resources[usize::from(resource)],
            secs(u64::from(at)),
        );
        exchanges.insert(read.exchange, read.at);
        read_accesses.push(read.clone());
        accesses.push(read);
    }
    let mut found = Vec::new();
    let mut inputs: Vec<Input> = accesses.iter().cloned().map(Input::Access).collect();
    for (kind, target, span, at) in matches {
        let from = owner(usize::from(span));
        let origin = spans[usize::from(span)];
        let content = if kind == 0 {
            let Some(read) = read_accesses.get(usize::from(target) % read_accesses.len().max(1))
            else {
                continue;
            };
            if read.agent == from {
                continue;
            }
            scene.carried(read, from, origin)
        } else {
            let to = agents[usize::from(target) % 3];
            if to == from {
                continue;
            }
            let exchange = scene.exchange();
            let started = secs(u64::from(at));
            exchanges.insert(exchange, started);
            inputs.push(Input::Exchange(exchange, started));
            let carrier = match kind {
                1 => tool_result(exchange),
                2 => Carrier::UserTurn,
                3 => Carrier::ReaderOutput,
                _ => Carrier::SystemPrompt,
            };
            scene.found(from, to, exchange, origin, carrier)
        };
        inputs.push(Input::Match(content.clone()));
        found.push(content);
    }
    World {
        agents,
        parent_link,
        accesses,
        matches: found,
        exchanges,
        inputs,
    }
}

fn world() -> impl Strategy<Value = World> {
    plan().prop_map(build)
}

impl World {
    fn correlator(&self) -> WindowedCorrelator {
        let mut correlator = WindowedCorrelator::new(timing());
        for (index, agent) in self.agents.iter().enumerate() {
            let parent = (self.parent_link && index == 1).then(|| self.agents[0]);
            correlator.learn_kin(
                *agent,
                Kin {
                    canonical: *agent,
                    parent,
                },
            );
        }
        correlator
    }

    fn related(&self, a: AgentId, b: AgentId) -> bool {
        self.parent_link
            && ((a == self.agents[0] && b == self.agents[1])
                || (a == self.agents[1] && b == self.agents[0]))
    }

    fn reads(&self) -> impl Iterator<Item = &Access> {
        self.accesses
            .iter()
            .filter(|access| crate::correlate::ReadPart::of_read(access).is_some())
    }

    fn writes(&self) -> impl Iterator<Item = &Access> {
        self.accesses
            .iter()
            .filter(|access| crate::correlate::ReadPart::of_read(access).is_none())
    }

    /// The event time of an input: an access's or exchange's time, a
    /// match's reader exchange start.
    fn event_time(&self, input: &Input) -> Option<Timestamp> {
        match input {
            Input::Access(access) => Some(access.at),
            Input::Exchange(_, at) => Some(*at),
            Input::Match(content) => self.exchanges.get(&content.reader_exchange()).copied(),
        }
    }
}

fn feed(correlator: &mut WindowedCorrelator, input: &Input) -> Vec<Decided> {
    match input {
        Input::Access(access) => correlator.access(access, None),
        Input::Match(content) => correlator.content(content),
        Input::Exchange(exchange, at) => correlator.exchange(*exchange, *at),
    }
}

/// Every input in `order`, then ticks at `ticks`.
fn run(world: &World, order: &[usize], ticks: &[Timestamp]) -> Vec<Decided> {
    let mut correlator = world.correlator();
    let mut out = Vec::new();
    for index in order {
        if let Some(input) = world.inputs.get(*index) {
            out.extend(feed(&mut correlator, input));
        }
    }
    for tick in ticks {
        out.extend(correlator.tick(*tick));
    }
    out
}

/// After every window closed (the latest read is at 29 s, windows are
/// 60 s), before any suspicion expires.
const CLOSED: u64 = 91;
/// After every suspicion expired.
const EXPIRED: u64 = 1_000;

fn shuffled() -> impl Strategy<Value = (World, Vec<usize>)> {
    world().prop_flat_map(|world| {
        let order: Vec<usize> = (0..world.inputs.len()).collect();
        (Just(world), Just(order).prop_shuffle())
    })
}

/// Inputs in a random order with ticks at random points between them.
fn interleaved() -> impl Strategy<Value = (World, Vec<usize>, Vec<(usize, u64)>)> {
    world().prop_flat_map(|world| {
        let order: Vec<usize> = (0..world.inputs.len()).collect();
        let len = world.inputs.len();
        (
            Just(world),
            Just(order).prop_shuffle(),
            prop::collection::vec((0..=len, 0_u64..1_200), 0..6),
        )
    })
}

/// Run `order` with ticks inserted before the input at each position
/// (sorted by time), then a final tick; with the event time of each input
/// and the tick before it, per decision.
fn run_interleaved(
    world: &World,
    order: &[usize],
    ticks: &[(usize, u64)],
) -> Vec<(Decided, Option<Timestamp>, Option<Timestamp>)> {
    let mut ticks: Vec<(usize, u64)> = ticks.to_vec();
    ticks.sort_by_key(|(_, at)| *at);
    let mut by_position: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
    for (position, at) in ticks {
        by_position.entry(position).or_default().push(at);
    }
    let mut correlator = world.correlator();
    let mut last_tick: Option<Timestamp> = None;
    let mut out = Vec::new();
    let tick = |correlator: &mut WindowedCorrelator,
                at: Timestamp,
                last_tick: &mut Option<Timestamp>,
                out: &mut Vec<_>| {
        let at = last_tick.map_or(at, |last: Timestamp| last.max(at));
        for decided in correlator.tick(at) {
            out.push((decided, Some(at), *last_tick));
        }
        *last_tick = Some(at);
    };
    for (position, index) in order.iter().enumerate() {
        for at in by_position.get(&position).into_iter().flatten() {
            tick(&mut correlator, secs(*at), &mut last_tick, &mut out);
        }
        if let Some(input) = world.inputs.get(*index) {
            let event = world.event_time(input);
            for decided in feed(&mut correlator, input) {
                out.push((decided, event, last_tick));
            }
        }
    }
    for at in by_position.get(&order.len()).into_iter().flatten() {
        tick(&mut correlator, secs(*at), &mut last_tick, &mut out);
    }
    tick(&mut correlator, secs(5_000), &mut last_tick, &mut out);
    out
}

/// What each opened transmission is: its reader, and for a channel
/// transmission its reader exchange and sender, for a direct one its
/// route.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Opened {
    Channel {
        to: AgentId,
        exchange: ExchangeId,
        sender: AgentId,
    },
    Direct {
        to: AgentId,
        route: NonChannelRoute,
    },
}

fn opened(world: &World, decided: &[Decided]) -> BTreeMap<TransmissionId, Opened> {
    let mut opened = BTreeMap::new();
    for decided in decided {
        match &decided.update {
            TransmissionUpdate::OpenChannel {
                transmission,
                to,
                co_access,
                ..
            } => {
                let exchange = world
                    .accesses
                    .iter()
                    .find(|access| access.id == co_access.read())
                    .map(|read| read.exchange);
                if let Some(exchange) = exchange {
                    opened.insert(
                        *transmission,
                        Opened::Channel {
                            to: *to,
                            exchange,
                            sender: co_access.writer(),
                        },
                    );
                }
            }
            TransmissionUpdate::OpenConfirmed {
                transmission,
                to,
                route,
                ..
            } => {
                opened.insert(
                    *transmission,
                    Opened::Direct {
                        to: *to,
                        route: route.clone(),
                    },
                );
            }
            _ => {}
        }
    }
    opened
}

/// Every content match a decision attaches, with its transmission.
fn attached(decided: &[Decided]) -> Vec<(TransmissionId, ContentMatch)> {
    decided
        .iter()
        .flat_map(|decided| {
            let id = subject(&decided.update);
            let content: Vec<ContentMatch> = match &decided.update {
                TransmissionUpdate::OpenConfirmed { confirmed, .. }
                | TransmissionUpdate::Confirm { confirmed, .. } => {
                    confirmed.content().iter().cloned().collect()
                }
                TransmissionUpdate::Extend { content, .. } => vec![content.clone()],
                _ => Vec::new(),
            };
            content.into_iter().map(move |content| (id, content))
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Every delivery order of evidence within one window gives the same
    /// transmission states once the window closed, and once every
    /// suspicion expired (`flow.correlator.order-insensitive`).
    #[test]
    fn evidence_order_permutations_agree((world, order) in shuffled()) {
        let canonical: Vec<usize> = (0..world.inputs.len()).collect();
        for at in [CLOSED, EXPIRED] {
            let expected = fold(&run(&world, &canonical, &[secs(at)]));
            let actual = fold(&run(&world, &order, &[secs(at)]));
            prop_assert!(expected.is_ok(), "{expected:?}");
            prop_assert_eq!(expected, actual);
        }
    }

    /// Whatever the order of inputs and ticks, every transmission's updates
    /// are a path through the lifecycle: states only move forward, one
    /// confirmation, one suspicion, nothing after a discard.
    #[test]
    fn states_only_move_forward((world, order, ticks) in interleaved()) {
        let decided: Vec<Decided> = run_interleaved(&world, &order, &ticks)
            .into_iter()
            .map(|(decided, _, _)| decided)
            .collect();
        let finals = fold(&decided);
        prop_assert!(finals.is_ok(), "{finals:?}");
        if let Ok(finals) = finals {
            for state in finals.values() {
                prop_assert!(state.stage != Stage::Awaiting, "a window left open after the last tick: {state:?}");
            }
        }
    }

    /// Content joins a channel transmission only when it arrived in the
    /// tool result of a read on that medium (`flow.route.channel-requires-access`)
    /// and a write of its sender there explains it
    /// (`flow.route.shared-upstream-stays-suspected`); never between parent
    /// and child, whose route comes first.
    #[test]
    fn channel_route_requires_access((world, order, ticks) in interleaved()) {
        let decided: Vec<Decided> = run_interleaved(&world, &order, &ticks).into_iter().map(|(d, _, _)| d).collect();
        let opened = opened(&world, &decided);
        for (id, content) in attached(&decided) {
            let Some(Opened::Channel { exchange, sender, .. }) = opened.get(&id) else { continue };
            prop_assert_eq!(content.reader_exchange(), *exchange);
            prop_assert_eq!(content.origin_agent(), *sender);
            let carried: Vec<&Access> = world.reads().filter(|read| pairing::carried_by(&content, read)).collect();
            prop_assert!(!carried.is_empty(), "no read carried {content:?}");
            let explained = world.writes().any(|write| {
                pairing::links(&content, write)
                    && carried.iter().any(|read| pairing::co_access(write, read, timing()).is_ok())
            });
            prop_assert!(explained, "no write of the sender explains {content:?}");
            prop_assert!(!world.related(content.origin_agent(), content.reader()));
        }
    }

    /// Every match a transmission holds was read by the transmission's
    /// reader (`flow.transmission.confirmed-reader-is-to`).
    #[test]
    fn confirmed_reader_is_to((world, order, ticks) in interleaved()) {
        let decided: Vec<Decided> = run_interleaved(&world, &order, &ticks).into_iter().map(|(d, _, _)| d).collect();
        let opened = opened(&world, &decided);
        for (id, content) in attached(&decided) {
            let to = match opened.get(&id) {
                Some(Opened::Channel { to, .. } | Opened::Direct { to, .. }) => *to,
                None => {
                    prop_assert!(false, "content for a transmission never opened: {content:?}");
                    continue;
                }
            };
            prop_assert_eq!(content.reader(), to);
        }
    }

    /// A `Direct` route names the carrier of every match it holds: a user
    /// turn, a system prompt, or a tool result no read carried
    /// (`flow.route.direct-carrier`).
    #[test]
    fn direct_route_carrier((world, order) in shuffled()) {
        let decided = run(&world, &order, &[secs(CLOSED), secs(EXPIRED)]);
        let opened = opened(&world, &decided);
        for (id, content) in attached(&decided) {
            let Some(Opened::Direct { route: NonChannelRoute::Direct(carrier), .. }) = opened.get(&id) else { continue };
            prop_assert!(!world.related(content.origin_agent(), content.reader()));
            match carrier {
                DirectCarrier::ToolResult(_) => {
                    prop_assert!(matches!(content.carrier(), Carrier::ToolResult(_)));
                    prop_assert!(!world.reads().any(|read| pairing::carried_by(&content, read)));
                }
                DirectCarrier::UserTurn => prop_assert_eq!(content.carrier(), &Carrier::UserTurn),
                DirectCarrier::SystemPrompt => prop_assert_eq!(content.carrier(), &Carrier::SystemPrompt),
            }
        }
    }

    /// `Unobserved` holds only matches in the reader's own output, and every
    /// such match between unrelated agents is unobserved
    /// (`flow.route.unobserved-reader-output`).
    #[test]
    fn unobserved_only_for_reader_output((world, order) in shuffled()) {
        let decided = run(&world, &order, &[secs(CLOSED), secs(EXPIRED)]);
        let opened = opened(&world, &decided);
        let attached = attached(&decided);
        for (id, content) in &attached {
            if matches!(opened.get(id), Some(Opened::Direct { route: NonChannelRoute::Unobserved, .. })) {
                prop_assert_eq!(content.carrier(), &Carrier::ReaderOutput);
            }
        }
        for content in &world.matches {
            if content.carrier() == &Carrier::ReaderOutput && !world.related(content.origin_agent(), content.reader()) {
                let unobserved = attached.iter().any(|(id, held)| {
                    held == content && matches!(opened.get(id), Some(Opened::Direct { route: NonChannelRoute::Unobserved, .. }))
                });
                prop_assert!(unobserved, "{content:?}");
            }
        }
    }

    /// Parent and child always take `Delegation`, whatever carried the
    /// match, and nothing else does (`flow.route.precedence`).
    #[test]
    fn route_precedence((world, order) in shuffled()) {
        let decided = run(&world, &order, &[secs(CLOSED), secs(EXPIRED)]);
        let opened = opened(&world, &decided);
        let attached = attached(&decided);
        for (id, content) in &attached {
            let delegated = matches!(opened.get(id), Some(Opened::Direct { route: NonChannelRoute::Delegation(_), .. }));
            prop_assert_eq!(delegated, world.related(content.origin_agent(), content.reader()), "{:?}", content);
        }
        for content in &world.matches {
            if world.related(content.origin_agent(), content.reader()) {
                let delegated = attached.iter().any(|(id, held)| {
                    held == content
                        && matches!(opened.get(id), Some(Opened::Direct { route: NonChannelRoute::Delegation(_), .. }))
                });
                prop_assert!(delegated, "{content:?}");
            }
        }
    }

    /// At most one transmission is live per reader exchange, sender and
    /// route (`flow.transmission.identity`); each holds one sender's
    /// content, so a reader exchange echoing two senders has one each
    /// (`flow.transmission.one-per-sender`).
    #[test]
    fn one_transmission_per_identity((world, order, ticks) in interleaved()) {
        let decided: Vec<Decided> = run_interleaved(&world, &order, &ticks).into_iter().map(|(d, _, _)| d).collect();
        let opened = opened(&world, &decided);
        let finals = fold(&decided);
        prop_assert!(finals.is_ok());
        let finals = finals.unwrap_or_default();
        let mut live: BTreeMap<String, usize> = BTreeMap::new();
        for (id, kind) in &opened {
            if finals.get(id).is_some_and(|state| state.stage == Stage::Discarded) {
                continue;
            }
            let identity = match kind {
                Opened::Channel { exchange, sender, .. } => format!("channel {exchange:?} {sender:?}"),
                Opened::Direct { route, .. } => {
                    let first = attached(&decided).into_iter().find(|(attached, _)| attached == id);
                    match first {
                        Some((_, content)) => format!("{:?} {:?} {route:?}", content.reader_exchange(), content.origin_agent()),
                        None => continue,
                    }
                }
            };
            *live.entry(identity).or_default() += 1;
        }
        for (identity, count) in live {
            prop_assert_eq!(count, 1, "{}", identity);
        }
    }

    /// Every transmission's content has one origin agent
    /// (`flow.transmission.one-per-sender`).
    #[test]
    fn matches_partitioned_by_origin((world, order) in shuffled()) {
        let decided = run(&world, &order, &[secs(CLOSED)]);
        let mut senders: BTreeMap<TransmissionId, BTreeSet<AgentId>> = BTreeMap::new();
        for (id, content) in attached(&decided) {
            senders.entry(id).or_default().insert(content.origin_agent());
        }
        for (id, origins) in senders {
            prop_assert_eq!(origins.len(), 1, "{}", id.ulid_text());
        }
    }

    /// Every confirmation is no older than the earlier of its input's event
    /// time and the previous tick minus `settle_after`
    /// (`flow.timing.confirm-within-settle`).
    #[test]
    fn confirmations_within_settle_bound((world, order, ticks) in interleaved()) {
        let settle = timing().settle_after();
        for (decided, event, previous) in run_interleaved(&world, &order, &ticks) {
            let at = match &decided.update {
                TransmissionUpdate::Confirm { confirmed, .. }
                | TransmissionUpdate::OpenConfirmed { confirmed, .. } => confirmed.at(),
                _ => continue,
            };
            let Some(previous) = previous else { continue };
            let bound_by_tick = Timestamp::from_micros(
                previous.as_micros().saturating_sub(u64::try_from(settle.as_micros()).unwrap_or(u64::MAX)),
            );
            let bound = event.map_or(bound_by_tick, |event| event.min(bound_by_tick));
            prop_assert!(at >= bound, "confirmed at {at:?}, bound {bound:?}");
        }
    }

    /// A tool-result match carried by a read whose sender has no write
    /// explaining it confirms nothing and extends nothing, unless sender
    /// and reader are parent and child
    /// (`flow.route.shared-upstream-stays-suspected`).
    #[test]
    fn match_without_sender_write_confirms_nothing((world, order) in shuffled()) {
        let decided = run(&world, &order, &[secs(CLOSED), secs(EXPIRED)]);
        let attached = attached(&decided);
        for content in &world.matches {
            let carried: Vec<&Access> = world.reads().filter(|read| pairing::carried_by(content, read)).collect();
            if carried.is_empty() || world.related(content.origin_agent(), content.reader()) {
                continue;
            }
            let explained = world.writes().any(|write| {
                pairing::links(content, write)
                    && carried.iter().any(|read| pairing::co_access(write, read, timing()).is_ok())
            });
            if !explained {
                prop_assert!(attached.iter().all(|(_, attached)| attached != content), "{content:?}");
            }
        }
    }
}

/// The generator makes worlds where content does confirm: a sanity check
/// that the properties are not vacuous.
#[test]
fn generated_worlds_confirm_channel_transmissions() {
    let plan: Plan = (
        [0, 1, 2, 0],
        vec![(0, 0, 1, 0b1111)],
        vec![(1, 0, 10)],
        vec![(0, 0, 0, 0), (2, 1, 2, 5)],
        false,
    );
    let world = build(plan);
    let decided = run(
        &world,
        &(0..world.inputs.len()).collect::<Vec<_>>(),
        &[secs(CLOSED)],
    );
    let confirms = decided
        .iter()
        .filter(|decided| matches!(decided.update, TransmissionUpdate::Confirm { .. }))
        .count();
    let direct = decided
        .iter()
        .filter(|decided| matches!(decided.update, TransmissionUpdate::OpenConfirmed { .. }))
        .count();
    assert_eq!((confirms, direct), (1, 1), "{decided:?}");
    let _ = Duration::ZERO;
}
