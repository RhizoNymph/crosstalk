//! Transmission evidence: what is listed follows from the transmission.

use std::cell::RefCell;
use std::time::Duration;

use crate::aggregates::quality::MatchClass;
use crate::derived::flow::access::Access;
use crate::derived::flow::evidence::CoAccess;
use crate::derived::flow::resource::{Locator, Resource};
use crate::derived::flow::transmission::{Confirmed, Transmission, TransmissionState};
use crate::derived::provenance::matching::ContentMatch;
use crate::ids::{AccessId, AgentId};
use crate::interfaces::l8_surface::evidence::{
    AccessDetail, EvidenceError, InvalidEvidence, MatchQuotes, TransmissionEvidence,
};
use crate::interfaces::l8_surface::excerpt::Excerpted;
use crate::interfaces::l8_surface::export::rows::TransmissionContent;
use crate::observed::message::ToolName;
use crate::support::NonEmpty;
use crate::tests::fixtures::{
    access, agent, at, content_match, message, read_access, resource, write_access,
};
use crate::tests::verdicts::{every_state, transmission_in};

fn dropped() -> MatchQuotes {
    MatchQuotes {
        origin: Excerpted::BodyDropped {
            message: message(5),
        },
        read: Excerpted::BodyDropped {
            message: message(6),
        },
    }
}

/// Agents 1 and 3 merged into 1.
fn merged(id: AgentId) -> AgentId {
    if id == agent(3) { agent(1) } else { id }
}

fn stored(id: u128) -> Resource {
    Resource {
        id: resource(id),
        locator: Locator::Opaque {
            tool: ToolName("wiki".into()),
            key: format!("page-{id}"),
        },
        first_seen: at(0),
    }
}

/// Accesses 1 (write by agent 3) and 2, 4 (reads by agent 2), all on
/// resource 1.
fn stored_access(id: AccessId) -> Access {
    match id.as_ulid() {
        1 => write_access(1, agent(3), resource(1), 1),
        n => read_access(n, agent(2), resource(1), n as u64),
    }
}

fn detail(id: AccessId) -> Result<AccessDetail, EvidenceError> {
    Ok(AccessDetail::new(stored_access(id), stored(1), merged)?)
}

fn co_access(write: u128, read: u128) -> CoAccess {
    CoAccess::new(
        &stored_access(access(write)),
        &stored_access(access(read)),
        Duration::from_secs(60),
    )
    .expect("valid co-access")
}

#[test]
fn matches_follow_the_transmissions_content_in_order() {
    let first = content_match(agent(1), agent(2), 8);
    let second = content_match(agent(1), agent(2), 3);
    let mut confirmed = Confirmed::new(NonEmpty::new(first.clone()), Vec::new(), at(4))
        .expect("one sender, one reader");
    confirmed.extend(second.clone()).expect("same sender");
    let transmission = transmission_in(1, TransmissionState::Confirmed(confirmed));
    let quoted = RefCell::new(Vec::new());
    let evidence = TransmissionEvidence::assemble(
        transmission.clone(),
        |m: &ContentMatch| {
            quoted.borrow_mut().push(m.clone());
            Ok::<_, EvidenceError>(dropped())
        },
        detail,
    )
    .expect("assembles");
    assert_eq!(quoted.into_inner(), vec![first.clone(), second.clone()]);
    let listed: Vec<&ContentMatch> = evidence
        .matches()
        .iter()
        .map(|m| m.content_match())
        .collect();
    assert_eq!(listed, vec![&first, &second]);
    assert_eq!(evidence.matches()[0].origin(), &dropped().origin);
    assert_eq!(evidence.matches()[0].read(), &dropped().read);
    assert_eq!(evidence.transmission(), &transmission);
}

#[test]
fn no_matches_before_confirmation_and_one_list_per_state() {
    for (state, _) in every_state() {
        let confirmed = state.confirmed().map(|c| c.content().count().get());
        let transmission = transmission_in(1, state);
        let evidence = TransmissionEvidence::assemble(transmission, |_| Ok(dropped()), detail)
            .expect("assembles");
        let expected = usize::try_from(confirmed.unwrap_or(0)).expect("small");
        assert_eq!(evidence.matches().len(), expected);
    }
}

#[test]
fn accesses_are_the_distinct_co_access_accesses_in_order_of_mention() {
    let state = TransmissionState::Suspected {
        co_access: NonEmpty::from_vec(vec![co_access(1, 2), co_access(1, 4), co_access(1, 2)])
            .expect("three records"),
        since: at(5),
    };
    let asked = RefCell::new(Vec::new());
    let evidence = TransmissionEvidence::assemble(
        transmission_in(1, state),
        |_| Ok(dropped()),
        |id| {
            asked.borrow_mut().push(id);
            detail(id)
        },
    )
    .expect("assembles");
    let expected = vec![access(1), access(2), access(4)];
    assert_eq!(asked.into_inner(), expected);
    let listed: Vec<AccessId> = evidence.accesses().iter().map(|d| d.access().id).collect();
    assert_eq!(listed, expected);
}

#[test]
fn a_detected_transmission_lists_no_accesses() {
    let evidence = TransmissionEvidence::assemble(
        transmission_in(1, TransmissionState::Detected),
        |_| Ok(dropped()),
        |_| -> Result<AccessDetail, EvidenceError> { panic!("no access to read") },
    )
    .expect("assembles");
    assert!(evidence.accesses().is_empty());
    assert!(evidence.matches().is_empty());
}

#[test]
fn access_detail_names_the_canonical_agent_and_keeps_the_record() {
    let write = stored_access(access(1));
    let detail = AccessDetail::new(write.clone(), stored(1), merged).expect("same resource");
    assert_eq!(detail.agent(), agent(1));
    assert_eq!(detail.access(), &write);
    assert_eq!(detail.access().agent, agent(3), "the record keeps its id");
    assert_eq!(detail.resource(), &stored(1));
}

#[test]
fn access_detail_rejects_another_resource() {
    assert_eq!(
        AccessDetail::new(stored_access(access(1)), stored(2), merged),
        Err(InvalidEvidence::ResourceMismatch {
            access: access(1),
            expected: resource(1),
            got: resource(2),
        })
    );
}

#[test]
fn a_detail_for_another_access_is_refused() {
    let state = TransmissionState::AwaitingContent {
        co_access: co_access(1, 2),
        window_closes_at: at(9),
    };
    let result = TransmissionEvidence::assemble(
        transmission_in(1, state),
        |_| Ok(dropped()),
        |_| detail(access(4)),
    );
    assert_eq!(
        result,
        Err(EvidenceError::Invalid(InvalidEvidence::WrongAccess {
            asked: access(1),
            got: access(4),
        }))
    );
}

#[test]
fn the_first_error_stops_assembly() {
    let confirmed = Confirmed::new(
        NonEmpty::new(content_match(agent(1), agent(2), 8)),
        vec![co_access(1, 2)],
        at(4),
    )
    .expect("one sender, one reader");
    let failure = EvidenceError::Store {
        reason: "span store down".into(),
    };
    let result = TransmissionEvidence::assemble(
        transmission_in(1, TransmissionState::Confirmed(confirmed)),
        |_| Err(failure.clone()),
        |_| -> Result<AccessDetail, EvidenceError> { panic!("not reached") },
    );
    assert_eq!(result, Err(failure));
}

#[test]
fn evidence_keeps_the_stored_transmission() {
    let transmission: Transmission = transmission_in(3, TransmissionState::Detected);
    let evidence = TransmissionEvidence::assemble(transmission.clone(), |_| Ok(dropped()), detail)
        .expect("assembles");
    assert_eq!(evidence.transmission(), &transmission);
}

#[test]
fn export_content_quotes_the_evidence_in_order() {
    let first = content_match(agent(1), agent(2), 8);
    let second = content_match(agent(1), agent(2), 3);
    let mut confirmed = Confirmed::new(NonEmpty::new(first.clone()), Vec::new(), at(4))
        .expect("one sender, one reader");
    confirmed.extend(second.clone()).expect("same sender");
    let transmission = transmission_in(1, TransmissionState::Confirmed(confirmed));
    let evidence =
        TransmissionEvidence::assemble(transmission, |_| Ok(dropped()), detail).expect("assembles");
    let content = TransmissionContent::of(&evidence, Some("billing".into()))
        .expect("a confirmed transmission has content");
    assert_eq!(content.topic_label.as_deref(), Some("billing"));
    let texts: Vec<_> = content.matches.iter().collect();
    assert_eq!(texts.len(), evidence.matches().len());
    for (text, evidence) in texts.into_iter().zip(evidence.matches()) {
        assert_eq!(
            text.class,
            MatchClass::from(evidence.content_match().kind())
        );
        assert_eq!(&text.quotes.origin, evidence.origin());
        assert_eq!(&text.quotes.read, evidence.read());
    }
}

#[test]
fn export_content_needs_a_match() {
    for (state, _) in every_state() {
        let confirmed = state.confirmed().is_some();
        let transmission = transmission_in(1, state);
        let evidence = TransmissionEvidence::assemble(transmission, |_| Ok(dropped()), detail)
            .expect("assembles");
        assert_eq!(
            TransmissionContent::of(&evidence, None).is_some(),
            confirmed,
            "content exactly for a confirmed transmission"
        );
    }
}

/// `surface.evidence.every-state`, at assembly: every state lists both
/// accesses of each co-access record it holds, write first; `Detected`,
/// with no record yet, lists none; the waiting, suspected and discarded
/// states list theirs although they have no match.
#[test]
fn every_state_lists_both_accesses_of_each_co_access() {
    for (state, _) in every_state() {
        let records = state.co_accesses();
        let detected = matches!(state, TransmissionState::Detected);
        let unconfirmed = matches!(
            state,
            TransmissionState::AwaitingContent { .. }
                | TransmissionState::Suspected { .. }
                | TransmissionState::Discarded { .. }
        );
        let evidence =
            TransmissionEvidence::assemble(transmission_in(1, state), |_| Ok(dropped()), detail)
                .expect("assembles");
        let listed: Vec<AccessId> = evidence.accesses().iter().map(|d| d.access().id).collect();
        for record in &records {
            let write = listed.iter().position(|id| *id == record.write());
            let read = listed.iter().position(|id| *id == record.read());
            assert!(write.is_some() && read.is_some() && write < read);
        }
        assert_eq!(detected, listed.is_empty());
        if unconfirmed {
            assert_eq!(listed, vec![access(1), access(2)]);
            assert!(evidence.matches().is_empty());
        }
    }
}
