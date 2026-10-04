//! What counts as a channel: transmissions only between different agents
//! once merges resolve, a channel's confirmation and listing read from its
//! cross-agent traffic, and every reader that counts channels or
//! transmissions honouring both.

use std::time::Duration;

use crate::aggregates::alert::{Alert, AlertState, AlertSubject};
use crate::aggregates::edge::{RouteKind, TopologyFilter};
use crate::aggregates::filter::{AccessSubject, FilterSubject, UnconfirmedChannels};
use crate::aggregates::quality::MatchClass;
use crate::aliases::NoAliases;
use crate::derived::flow::channel::confirmation::{
    Confirmation, CrossTraffic, Listing, ListingKind,
};
use crate::derived::flow::channel::detection::{DeclaredDetection, TrafficDetection};
use crate::derived::flow::channel::policy::{Decision, Policy, PolicyAuthor};
use crate::derived::flow::channel::{
    Channel, ChannelOrigin, Declaration, DeclaredHistory, Seed, Supersession,
};
use crate::derived::flow::evidence::CoAccess;
use crate::derived::flow::resource::{Host, ResourcePattern};
use crate::derived::flow::transmission::{
    Confirmed, Crossing, Route, Transmission, TransmissionState,
};
use crate::ids::{AccessId, AgentId, AlertId, AlertRuleId, ChannelId, OperatorId};
use crate::interfaces::l8_surface::channel_traffic::{
    ChannelTransmission, ChannelTransmissionFilter,
};
use crate::interfaces::l8_surface::channels::{
    ChannelActivity, ChannelRow, ChannelStanding, InvalidChannelRow,
};
use crate::interfaces::l8_surface::export::rows::{InvalidTransmissionRow, TransmissionRow};
use crate::interfaces::l8_surface::lists::{ChannelFilter, OriginFilter};
use crate::interfaces::l8_surface::overview::QueueCounts;
use crate::interfaces::l8_surface::summary::{
    Delivery, SummaryState, TopicUnder, TransmissionSummary,
};
use crate::support::NonEmpty;
use crate::tests::fixtures::{
    agent, at, channel, channel_row, content_match, read_access, resource, transmission,
    write_access,
};

const WINDOW: Duration = Duration::from_secs(3600);

/// Agent 9 merged into agent 2.
fn merged(id: AgentId) -> AgentId {
    if id == agent(9) { agent(2) } else { id }
}

/// Merges only (no channel is superseded): agent 9 into agent 2.
fn merges() -> fn(AgentId) -> AgentId {
    merged
}

/// Write access `n` is by agent `n`.
fn writer(access: AccessId) -> Option<AgentId> {
    Some(agent(access.as_ulid()))
}

fn co_access(writer_agent: u128, reader: u128) -> CoAccess {
    CoAccess::new(
        &write_access(writer_agent, agent(writer_agent), resource(1), 1),
        &read_access(100 + reader, agent(reader), resource(1), 2),
        WINDOW,
    )
    .expect("a write then a read by another agent")
}

/// A transmission to `reader` through channel 1 in `state`.
fn on_channel(n: u128, reader: u128, state: TransmissionState) -> Transmission {
    Transmission {
        id: transmission(n),
        to: agent(reader),
        route: Route::Channel(channel(1)),
        opened_at: at(n as u64),
        state,
    }
}

/// Suspected: written by each of `writers`, read by `reader`.
fn suspected(n: u128, writers: &[u128], reader: u128) -> Transmission {
    let co_access = NonEmpty::from_vec(writers.iter().map(|w| co_access(*w, reader)).collect())
        .expect("at least one writer");
    on_channel(
        n,
        reader,
        TransmissionState::Suspected {
            co_access,
            since: at(10),
        },
    )
}

/// Confirmed: `sender`'s text in `reader`'s input.
fn confirmed(n: u128, sender: u128, reader: u128) -> Transmission {
    let content = NonEmpty::new(content_match(agent(sender), agent(reader), 16));
    let confirmed = Confirmed::new(content, Vec::new(), at(5)).expect("one sender, one reader");
    on_channel(n, reader, TransmissionState::Confirmed(confirmed))
}

fn decision() -> Decision {
    Decision {
        by: PolicyAuthor::Config,
        at: at(0),
        note: None,
    }
}

fn discovered(id: u128) -> Channel {
    Channel {
        id: channel(id),
        origin: ChannelOrigin::Discovered {
            seed: Seed {
                resource: resource(id),
                first_transmission: transmission(id),
            },
            detection: TrafficDetection::Active {
                since: at(1),
                last_transmission: transmission(id),
            },
        },
        resources: Vec::new(),
        policy: Policy::Unreviewed(None),
    }
}

fn declared(id: u128, detection: DeclaredDetection) -> Channel {
    Channel {
        id: channel(id),
        origin: ChannelOrigin::Declared {
            declaration: Declaration {
                pattern: ResourcePattern::Host(Host("wiki.internal".into())),
                by: PolicyAuthor::Config,
                at: at(0),
            },
            history: DeclaredHistory::BeforeTraffic(detection),
        },
        resources: Vec::new(),
        policy: Policy::Sanctioned(decision()),
    }
}

fn promoted(id: u128) -> Channel {
    let mut channel = discovered(id);
    channel.origin = channel
        .origin
        .promoted(Declaration {
            pattern: ResourcePattern::Host(Host("wiki.example".into())),
            by: PolicyAuthor::Operator(OperatorId::from_ulid(7)),
            at: at(50),
        })
        .expect("a discovered channel");
    channel
}

fn superseded(id: u128) -> Channel {
    let mut channel = discovered(id);
    channel.origin = channel
        .origin
        .superseded(Supersession {
            by: crate::tests::fixtures::channel(99),
            at: at(50),
        })
        .expect("a discovered channel");
    channel
}

const UNCONFIRMED: CrossTraffic = CrossTraffic {
    confirmed: 0,
    unconfirmed: 2,
};

const CONFIRMED: CrossTraffic = CrossTraffic {
    confirmed: 1,
    unconfirmed: 2,
};

// Crossing.

#[test]
fn a_confirmed_transmission_crosses_until_its_agents_merge() {
    let sent = confirmed(1, 9, 2);
    assert_eq!(sent.crossing(NoAliases, writer), Crossing::Crosses);
    assert_eq!(sent.crossing(merges(), writer), Crossing::WithinOneAgent);
}

#[test]
fn a_suspected_transmission_crosses_while_any_writer_is_another_agent() {
    let alone = suspected(1, &[9], 2);
    assert_eq!(alone.crossing(NoAliases, writer), Crossing::Crosses);
    assert_eq!(alone.crossing(merges(), writer), Crossing::WithinOneAgent);
    let shared = suspected(2, &[9, 3], 2);
    assert_eq!(shared.crossing(merges(), writer), Crossing::Crosses);
    let unknown = |_: AccessId| None;
    assert_eq!(alone.crossing(NoAliases, unknown), Crossing::WithinOneAgent);
}

#[test]
fn a_detected_transmission_names_no_sender() {
    let detected = on_channel(1, 2, TransmissionState::Detected);
    assert_eq!(detected.crossing(NoAliases, writer), Crossing::Unknown);
}

// Cross traffic and listings.

#[test]
fn cross_traffic_counts_crossing_transmissions_by_confirmation() {
    let transmissions = [
        confirmed(1, 9, 2),
        confirmed(2, 3, 2),
        suspected(3, &[9], 2),
        suspected(4, &[4], 2),
        on_channel(5, 2, TransmissionState::Detected),
    ];
    assert_eq!(
        CrossTraffic::tally(&transmissions, NoAliases, writer),
        CrossTraffic {
            confirmed: 2,
            unconfirmed: 2,
        }
    );
    assert_eq!(
        CrossTraffic::tally(&transmissions, merges(), writer),
        CrossTraffic {
            confirmed: 1,
            unconfirmed: 1,
        }
    );
}

#[test]
fn confirmation_follows_the_counts() {
    assert_eq!(CrossTraffic::NONE.confirmation(), None);
    assert_eq!(UNCONFIRMED.confirmation(), Some(Confirmation::Unconfirmed));
    assert_eq!(CONFIRMED.confirmation(), Some(Confirmation::Confirmed));
}

#[test]
fn listing_follows_origin_and_traffic() {
    let none = CrossTraffic::NONE;
    let cases = [
        (discovered(1).origin, none, Some(Listing::Hidden)),
        (
            declared(2, DeclaredDetection::AwaitingTraffic).origin,
            none,
            Some(Listing::Declaration),
        ),
        (promoted(3).origin, none, Some(Listing::Declaration)),
        (
            discovered(1).origin,
            UNCONFIRMED,
            Some(Listing::Channel(Confirmation::Unconfirmed)),
        ),
        (
            promoted(3).origin,
            CONFIRMED,
            Some(Listing::Channel(Confirmation::Confirmed)),
        ),
        (superseded(4).origin, CONFIRMED, None),
    ];
    for (origin, traffic, listing) in cases {
        assert_eq!(Listing::of(&origin, traffic), listing, "{origin:?}");
    }
    assert_eq!(Listing::Hidden.kind(), None);
    assert_eq!(
        Listing::Channel(Confirmation::Unconfirmed).kind(),
        Some(ListingKind::Unconfirmed)
    );
    assert_eq!(Listing::Declaration.confirmation(), None);
}

#[test]
fn a_merge_hides_a_discovered_channel_and_an_unmerge_lists_it_again() {
    let traffic = [suspected(1, &[9], 2)];
    let hidden = channel_row(
        discovered(1),
        CrossTraffic::tally(&traffic, merges(), writer),
    );
    assert_eq!(hidden.listing(), Some(Listing::Hidden));
    assert!(!ChannelFilter::default().matches(&hidden));
    let back = channel_row(
        discovered(1),
        CrossTraffic::tally(&traffic, NoAliases, writer),
    );
    assert_eq!(
        back.listing(),
        Some(Listing::Channel(Confirmation::Unconfirmed))
    );
    assert!(ChannelFilter::default().matches(&back));
}

#[test]
fn rows_refuse_traffic_their_detection_does_not_have() {
    let channel = declared(2, DeclaredDetection::AwaitingTraffic);
    assert_eq!(
        ChannelRow::new(
            channel,
            None,
            ChannelStanding::InForce {
                traffic: UNCONFIRMED,
                activity: ChannelActivity::Never,
            },
        ),
        Err(InvalidChannelRow::TrafficWithoutDetection)
    );
    let row = channel_row(
        declared(2, DeclaredDetection::AwaitingTraffic),
        CrossTraffic::NONE,
    );
    assert_eq!(row.listing(), Some(Listing::Declaration));
    assert_eq!(row.confirmation(), None);
    let used = channel_row(
        declared(
            3,
            DeclaredDetection::InUse(TrafficDetection::Active {
                since: at(1),
                last_transmission: transmission(1),
            }),
        ),
        UNCONFIRMED,
    );
    assert_eq!(used.confirmation(), Some(Confirmation::Unconfirmed));
}

// The channel list filter.

#[test]
fn the_default_filter_lists_channels_and_declarations_but_nothing_hidden() {
    let filter = ChannelFilter::default();
    assert!(filter.matches(&channel_row(discovered(1), CONFIRMED)));
    assert!(filter.matches(&channel_row(discovered(1), UNCONFIRMED)));
    assert!(filter.matches(&channel_row(
        declared(2, DeclaredDetection::Unused { since: at(3) }),
        CrossTraffic::NONE
    )));
    assert!(!filter.matches(&channel_row(discovered(1), CrossTraffic::NONE)));
    assert!(!filter.matches(&channel_row(superseded(4), CrossTraffic::NONE)));
}

#[test]
fn confirmed_only_leaves_out_unconfirmed_channels_and_keeps_declarations() {
    let confirmed_only = ChannelFilter {
        listings: vec![ListingKind::Confirmed, ListingKind::Declaration],
        ..ChannelFilter::default()
    };
    assert!(confirmed_only.matches(&channel_row(discovered(1), CONFIRMED)));
    assert!(!confirmed_only.matches(&channel_row(discovered(1), UNCONFIRMED)));
    assert!(confirmed_only.matches(&channel_row(promoted(3), CrossTraffic::NONE)));
    let unconfirmed_only = ChannelFilter {
        listings: vec![ListingKind::Unconfirmed],
        ..ChannelFilter::default()
    };
    assert!(unconfirmed_only.matches(&channel_row(discovered(1), UNCONFIRMED)));
    assert!(!unconfirmed_only.matches(&channel_row(promoted(3), CrossTraffic::NONE)));
    // A superseded channel has no listing: its origin filter alone selects it.
    let superseded_too = ChannelFilter {
        origin: OriginFilter::WithSuperseded(Vec::new()),
        ..unconfirmed_only
    };
    assert!(superseded_too.matches(&channel_row(superseded(4), CrossTraffic::NONE)));
}

// The view filter.

#[test]
fn no_filter_admits_a_transmission_within_one_agent() {
    let route = Route::Channel(channel(1));
    let subject = FilterSubject {
        from: agent(2),
        to: agent(2),
        route: &route,
        topic: None,
        false_detection: false,
    };
    for filter in [
        TopologyFilter::default(),
        TopologyFilter {
            agents: vec![agent(2)],
            ..TopologyFilter::default()
        },
        TopologyFilter {
            route_kinds: vec![RouteKind::Channel],
            ..TopologyFilter::default()
        },
    ] {
        assert!(!filter.admits(&subject, NoAliases), "{filter:?}");
    }
    let between = FilterSubject {
        from: agent(1),
        ..subject
    };
    assert!(TopologyFilter::default().admits(&between, NoAliases));
}

#[test]
fn unconfirmed_channels_decide_access_admission() {
    let subject = |confirmation| AccessSubject {
        agent: agent(1),
        channel: channel(1),
        confirmation,
        channel_topics: &[],
    };
    let exclude = TopologyFilter {
        unconfirmed_channels: UnconfirmedChannels::Exclude,
        ..TopologyFilter::default()
    };
    let include = TopologyFilter::default();
    assert!(include.admits_access(&subject(Confirmation::Unconfirmed), NoAliases));
    assert!(include.admits_access(&subject(Confirmation::Confirmed), NoAliases));
    assert!(!exclude.admits_access(&subject(Confirmation::Unconfirmed), NoAliases));
    assert!(exclude.admits_access(&subject(Confirmation::Confirmed), NoAliases));
    assert!(UnconfirmedChannels::Include.keeps(Confirmation::Unconfirmed));
    assert!(!UnconfirmedChannels::Exclude.keeps(Confirmation::Unconfirmed));
}

#[test]
fn unconfirmed_channels_change_no_transmission_view() {
    let route = Route::Channel(channel(1));
    let subject = FilterSubject {
        from: agent(1),
        to: agent(2),
        route: &route,
        topic: None,
        false_detection: false,
    };
    let exclude = TopologyFilter {
        unconfirmed_channels: UnconfirmedChannels::Exclude,
        ..TopologyFilter::default()
    };
    assert_eq!(
        exclude.admits(&subject, NoAliases),
        TopologyFilter::default().admits(&subject, NoAliases)
    );
}

// Queues.

fn open_alert(n: u128, subject: AlertSubject) -> Alert {
    Alert {
        id: AlertId::from_ulid(n),
        rule: AlertRuleId::from_ulid(1),
        subject,
        raised_at: at(1),
        occurrences: 1,
        state: AlertState::Open,
    }
}

#[test]
fn queues_count_listed_channels_and_honour_unconfirmed_channels() {
    let rows = [
        channel_row(discovered(1), CONFIRMED),
        channel_row(discovered(2), UNCONFIRMED),
        channel_row(discovered(3), CrossTraffic::NONE),
        channel_row(
            declared(4, DeclaredDetection::AwaitingTraffic),
            CrossTraffic::NONE,
        ),
    ];
    let mut unreviewed_declaration = rows[3].channel().clone();
    unreviewed_declaration.policy = Policy::Unreviewed(None);
    let rows = [
        rows[0].clone(),
        rows[1].clone(),
        rows[2].clone(),
        channel_row(unreviewed_declaration, CrossTraffic::NONE),
    ];
    let include = QueueCounts::tally(&[], |_| true, &rows, UnconfirmedChannels::Include);
    assert_eq!(
        include.unreviewed_channels, 3,
        "confirmed, unconfirmed, declared"
    );
    assert_eq!(include.unconfirmed_channels, Some(1));
    let exclude = QueueCounts::tally(&[], |_| true, &rows, UnconfirmedChannels::Exclude);
    assert_eq!(exclude.unreviewed_channels, 2, "confirmed, declared");
    assert_eq!(exclude.unconfirmed_channels, None);
}

#[test]
fn alerts_about_hidden_subjects_are_not_shown_or_counted() {
    let hidden = |id: ChannelId| id == channel(3);
    let within = |id| id == transmission(5);
    let shown = |subject: AlertSubject| subject.shown(NoAliases, hidden, within);
    assert!(shown(AlertSubject::Channel(channel(1))));
    assert!(!shown(AlertSubject::Channel(channel(3))));
    assert!(shown(AlertSubject::Transmission(transmission(4))));
    assert!(!shown(AlertSubject::Transmission(transmission(5))));
    assert!(shown(AlertSubject::Agent(agent(1))));
    let alerts = [
        open_alert(1, AlertSubject::Channel(channel(1))),
        open_alert(2, AlertSubject::Channel(channel(3))),
        open_alert(3, AlertSubject::Transmission(transmission(5))),
    ];
    let counts = QueueCounts::tally(
        &alerts,
        |alert| shown(alert.subject),
        &[],
        UnconfirmedChannels::Include,
    );
    assert_eq!(counts.open_alerts, 1);
}

// Exports and the channel's transmissions.

#[test]
fn an_export_row_is_never_a_transmission_within_one_agent() {
    let summary = |from: u128| TransmissionSummary {
        id: transmission(1),
        to: agent(2),
        route: Route::Channel(channel(1)),
        opened_at: at(1),
        state: SummaryState::Confirmed {
            delivery: Delivery {
                from: agent(from),
                confirmed_at: at(2),
                matched_bytes: std::num::NonZeroU64::MIN,
            },
            verdict: None,
        },
    };
    assert_eq!(
        TransmissionRow::new(summary(2), MatchClass::Exact, None),
        Err(InvalidTransmissionRow::WithinOneAgent(agent(2)))
    );
    assert!(TransmissionRow::new(summary(1), MatchClass::Exact, None).is_ok());
}

#[test]
fn a_channels_transmissions_name_their_senders_and_skip_merged_ones() {
    let topic = |_| TopicUnder::Unassigned;
    let row = ChannelTransmission::of(&suspected(1, &[9, 3], 2), merges(), writer, |_| None, topic)
        .expect("agent 3 is another agent");
    assert_eq!(
        row.senders().iter().copied().collect::<Vec<_>>(),
        [agent(3)]
    );
    assert_eq!(row.confirmation(), Confirmation::Unconfirmed);
    assert_eq!(
        ChannelTransmission::of(&suspected(1, &[9], 2), merges(), writer, |_| None, topic),
        None
    );
    let sent = ChannelTransmission::of(&confirmed(2, 9, 3), merges(), writer, |_| None, topic)
        .expect("agents 2 and 3");
    assert_eq!(
        sent.senders().iter().copied().collect::<Vec<_>>(),
        [agent(2)]
    );
    assert_eq!(sent.confirmation(), Confirmation::Confirmed);
    let review = ChannelTransmissionFilter {
        confirmation: Some(Confirmation::Unconfirmed),
    };
    assert!(review.matches(&row));
    assert!(!review.matches(&sent));
    assert!(ChannelTransmissionFilter::default().matches(&sent));
}
