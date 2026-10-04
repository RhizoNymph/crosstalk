//! Policy and promotion actions with the registry's semantics: decisions
//! recorded in the policy history, the preview agreeing with the action,
//! and promotion keeping the channel's id while superseding what its
//! pattern covers.

use std::collections::{BTreeSet, HashSet};

use crosstalk_spec::aggregates::alert::AlertSubject;
use crosstalk_spec::aggregates::edge::Weighting;
use crosstalk_spec::derived::flow::channel::policy::{Policy, PolicyAuthor, PolicyKind};
use crosstalk_spec::derived::flow::channel::{ChannelOrigin, DeclaredHistory};
use crosstalk_spec::derived::flow::resource::{Host, ResourcePattern};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l8_surface::channels::ChannelRow;
use crosstalk_spec::interfaces::l8_surface::lists::ChannelFilter;
use crosstalk_spec::interfaces::l8_surface::{ConflictKind, InputError, Permission, QueryError};

use super::super::clock::{NOW, all_time};
use super::super::world::ChannelKey;
use super::channels::{row, rows};
use super::{caller, collect, fresh, graph_of, researcher, scope_with, shared, week};
use crate::backend::Backend;
use crate::pages::channels::promote::patterns::candidates;
use crate::url::scope::ViewFilter;
use crosstalk_spec::aggregates::alert::{AlertState, SuppressReason};
use crosstalk_spec::interfaces::l8_surface::actions::SupersededChannels;
use crosstalk_spec::interfaces::l8_surface::{ActionError, ActionOutcome, OperatorAction};

use super::actions_support::{alert_state, channel, find_alert};

fn wiki_prefix() -> ResourcePattern {
    ResourcePattern::UrlPrefix {
        host: Host("wiki.example.org".to_owned()),
        path_prefix: "/wiki".to_owned(),
    }
}

fn promote(channel: ChannelId, pattern: ResourcePattern, policy: PolicyKind) -> OperatorAction {
    OperatorAction::PromoteChannel {
        channel,
        pattern,
        policy,
        note: None,
    }
}

#[tokio::test]
async fn sanctioning_records_the_decision_and_suppresses_the_channels_own_alerts() {
    let b = fresh();
    let c = researcher();
    let wiki = channel(&b, ChannelKey::HijackedWiki);
    let channel_alert = find_alert(&b, |a| {
        a.subject == AlertSubject::Channel(wiki) && a.state == AlertState::Open
    })
    .await;
    let tx_alert = {
        let state = b.state.read().await;
        state
            .alerts
            .iter()
            .find(|a| {
                a.state == AlertState::Open
                    && matches!(a.subject, AlertSubject::Transmission(t)
                        if b.world.tx(t).is_some_and(|r| r.transmission.route == Route::Channel(wiki)))
            })
            .expect("transmission alert on the wiki")
            .id
    };
    let outcome = b
        .act(
            &c,
            OperatorAction::SetPolicy {
                channel: wiki,
                policy: PolicyKind::Sanctioned,
                note: Some("ours".into()),
            },
        )
        .await;
    assert_eq!(outcome, Ok(ActionOutcome::Applied));
    assert!(matches!(
        alert_state(&b, channel_alert).await,
        AlertState::Suppressed { reason: SuppressReason::ChannelSanctioned, at } if at == NOW
    ));
    assert_eq!(
        alert_state(&b, tx_alert).await,
        AlertState::Open,
        "transmission alerts stay"
    );
    let sanctioned = row(&b, wiki, None).await;
    assert!(matches!(
        &sanctioned.channel().policy,
        Policy::Sanctioned(d) if d.by == PolicyAuthor::Operator(c.operator()) && d.at == NOW
    ));
    // Back to unreviewed is a reset: Unreviewed(Some), and both are in the
    // history, oldest first.
    b.act(
        &c,
        OperatorAction::SetPolicy {
            channel: wiki,
            policy: PolicyKind::Unreviewed,
            note: None,
        },
    )
    .await
    .expect("reset");
    assert!(matches!(
        row(&b, wiki, None).await.channel().policy,
        Policy::Unreviewed(Some(_))
    ));
    let history = b
        .policy_history(&c, wiki)
        .await
        .expect("read")
        .expect("history");
    assert_eq!(
        history.entries().iter().map(|e| e.kind).collect::<Vec<_>>(),
        [PolicyKind::Sanctioned, PolicyKind::Unreviewed]
    );
    // A superseded channel takes no policy and records nothing.
    let (old, notes) = (
        channel(&b, ChannelKey::OldTeamNotes),
        channel(&b, ChannelKey::TeamNotes),
    );
    let before = b.policy_history(&c, old).await.expect("read");
    let result = b
        .act(
            &c,
            OperatorAction::SetPolicy {
                channel: old,
                policy: PolicyKind::Sanctioned,
                note: None,
            },
        )
        .await;
    assert_eq!(
        result.err(),
        Some(ActionError::Conflict(ConflictKind::ChannelSuperseded {
            channel: old,
            by: notes
        }))
    );
    assert_eq!(b.policy_history(&c, old).await.expect("read"), before);
}

#[tokio::test]
async fn promotion_previews_what_promote_then_does() {
    let b = fresh();
    let c = researcher();
    let (wiki, talk) = (
        channel(&b, ChannelKey::HijackedWiki),
        channel(&b, ChannelKey::WikiTalk),
    );
    let pattern = wiki_prefix();
    let preview = b
        .promotion_preview(&c, wiki, &pattern)
        .await
        .expect("preview");
    assert_eq!(preview.conflict(), None);
    assert_eq!(preview.superseded_channels().as_slice(), [talk]);
    let covered = preview.covered_resources().expect("covered");
    let uncovered = preview.uncovered_resources().expect("uncovered");
    assert!(covered.total() > 0);
    assert!(covered.shown().iter().all(|r| pattern.matches(&r.locator)));
    assert!(
        uncovered
            .shown()
            .iter()
            .all(|r| !pattern.matches(&r.locator))
    );
    let held: BTreeSet<_> = b
        .world
        .resource_channel
        .iter()
        .filter(|(_, c)| **c == wiki || **c == talk)
        .map(|(r, _)| *r)
        .collect();
    assert_eq!(covered.total() + uncovered.total(), held.len() as u64);
    // The preview is View, not Govern.
    let viewer = caller(&[Permission::View]);
    assert!(b.promotion_preview(&viewer, wiki, &pattern).await.is_ok());

    let outcome = b
        .act(&c, promote(wiki, pattern.clone(), PolicyKind::Unreviewed))
        .await;
    assert_eq!(
        outcome,
        Ok(ActionOutcome::ChannelPromoted {
            channel: wiki,
            superseded: SupersededChannels::new([talk]),
        }),
        "the promoted channel keeps its id and names what it superseded"
    );
    let all = all_time().expect("window");
    let resources = collect(50, async |p| {
        b.channel_resources(&c, wiki, all, &p)
            .await
            .map(|page| page.value.page)
    })
    .await;
    let listed: BTreeSet<_> = resources.iter().map(|u| u.resource().id).collect();
    assert!(listed.is_subset(&held) && listed.len() > 1);
    assert!(
        resources
            .iter()
            .any(|u| b.world.resource_channel.get(&u.resource().id) == Some(&talk)),
        "the talk page's resources are the wiki's now"
    );
    // Afterwards the same previews report why they would be refused.
    let again = b
        .promotion_preview(&c, wiki, &pattern)
        .await
        .expect("preview");
    assert_eq!(
        again.conflict(),
        Some(&ConflictKind::ChannelNotDiscovered { channel: wiki })
    );
    assert!(again.covered_resources().is_none());
    let absorbed = b
        .promotion_preview(&c, talk, &pattern)
        .await
        .expect("preview");
    assert_eq!(
        absorbed.conflict(),
        Some(&ConflictKind::ChannelSuperseded {
            channel: talk,
            by: wiki
        })
    );
    let pastebin = channel(&b, ChannelKey::Pastebin);
    assert_eq!(
        b.promotion_preview(&c, pastebin, &pattern).await.err(),
        Some(QueryError::InvalidInput(InputError::PatternMissesSeed))
    );
    assert_eq!(
        b.promotion_preview(&c, ChannelId::from_ulid(1), &pattern)
            .await
            .err(),
        Some(QueryError::NotFound)
    );
}

#[tokio::test]
async fn an_overlapping_pattern_is_a_conflict_in_the_preview_and_the_action() {
    let b = fresh();
    let c = researcher();
    let (wiki, talk) = (
        channel(&b, ChannelKey::HijackedWiki),
        channel(&b, ChannelKey::WikiTalk),
    );
    let seed = row(&b, wiki, None)
        .await
        .seed()
        .expect("seed")
        .locator
        .clone();
    b.act(
        &c,
        promote(wiki, ResourcePattern::Exact(seed), PolicyKind::Unreviewed),
    )
    .await
    .expect("promote the page alone");
    let host = ResourcePattern::Host(Host("wiki.example.org".to_owned()));
    let overlap = ConflictKind::PatternOverlaps { existing: wiki };
    let preview = b.promotion_preview(&c, talk, &host).await.expect("preview");
    assert_eq!(preview.conflict(), Some(&overlap));
    assert!(preview.superseded_channels().as_slice().is_empty());
    assert_eq!(
        b.act(&c, promote(talk, host, PolicyKind::Sanctioned))
            .await
            .err(),
        Some(ActionError::Conflict(overlap))
    );
}

/// For each candidate pattern of a few discovered channels, in a fresh
/// world: a preview without a conflict is followed by a promotion
/// superseding exactly its channels, a conflict by the same conflict, an
/// error by the same error.
#[tokio::test]
async fn previews_agree_with_promotions() {
    let c = researcher();
    let template = shared();
    for key in [
        ChannelKey::HijackedWiki,
        ChannelKey::Gist,
        ChannelKey::McpMemory,
    ] {
        let id = channel(template, key);
        let seed = row(template, id, None)
            .await
            .seed()
            .expect("seed")
            .locator
            .clone();
        for pattern in candidates(&seed) {
            let b = fresh();
            let preview = b.promotion_preview(&c, id, &pattern).await;
            let acted = b
                .act(&c, promote(id, pattern.clone(), PolicyKind::Sanctioned))
                .await;
            match preview {
                Err(error) => assert_eq!(acted.err().map(QueryError::from), Some(error)),
                Ok(preview) => match preview.conflict() {
                    Some(kind) => {
                        assert_eq!(acted.err(), Some(ActionError::Conflict(kind.clone())));
                    }
                    None => {
                        let Ok(ActionOutcome::ChannelPromoted {
                            channel,
                            superseded,
                        }) = acted
                        else {
                            panic!("expected a promotion, got {acted:?}");
                        };
                        assert_eq!(channel, id);
                        let state = b.state.read().await;
                        let now_superseded: HashSet<ChannelId> = state
                            .channels
                            .values()
                            .filter(|r| r.channel().origin.supersession().map(|s| s.by) == Some(id))
                            .map(|r| r.channel().id)
                            .collect();
                        let previewed: HashSet<ChannelId> = preview
                            .superseded_channels()
                            .as_slice()
                            .iter()
                            .copied()
                            .collect();
                        assert_eq!(now_superseded, previewed, "{key:?} {pattern:?}");
                        let returned: HashSet<ChannelId> = superseded.iter().collect();
                        assert_eq!(returned, previewed, "the outcome names what it superseded");
                    }
                },
            }
        }
    }
}

#[tokio::test]
async fn promote_supersedes_covered_channels_and_graphs_follow() {
    let b = fresh();
    let c = researcher();
    let (wiki, talk) = (
        channel(&b, ChannelKey::HijackedWiki),
        channel(&b, ChannelKey::WikiTalk),
    );
    let pattern = wiki_prefix();
    let traffic = |r: ChannelRow| r.counts().map_or(0, |counts| counts.transmissions);
    let before = traffic(row(&b, wiki, None).await) + traffic(row(&b, talk, None).await);
    let raw = b
        .world
        .transmissions
        .iter()
        .filter(|t| {
            t.transmission.route == Route::Channel(wiki)
                || t.transmission.route == Route::Channel(talk)
        })
        .count() as u64;
    b.act(
        &c,
        OperatorAction::PromoteChannel {
            channel: wiki,
            pattern: pattern.clone(),
            policy: PolicyKind::Sanctioned,
            note: Some("our coordination page".into()),
        },
    )
    .await
    .expect("promote");
    let promoted = row(&b, wiki, None).await;
    assert!(matches!(
        &promoted.channel().origin,
        ChannelOrigin::Declared {
            history: DeclaredHistory::Promoted { .. },
            declaration,
        } if declaration.by == PolicyAuthor::Operator(c.operator()) && declaration.at == NOW
    ));
    assert!(matches!(promoted.channel().policy, Policy::Sanctioned(_)));
    let absorbed = row(&b, talk, None).await;
    assert_eq!(absorbed.supersession().map(|s| s.into()), Some(wiki));
    assert_eq!(
        traffic(promoted),
        before,
        "the promoted channel counts the absorbed channel's traffic"
    );
    // Graphs and lists follow.
    let on_talk = scope_with(
        week().window,
        ViewFilter {
            channels: vec![talk],
            ..Default::default()
        },
    );
    let routed = super::reads_support::rows_in(&b, &on_talk).await;
    assert_eq!(routed.len() as u64, raw);
    assert!(routed.iter().all(|t| t.route == Route::Channel(wiki)));
    let view = graph_of(&b, &c, &week(), Weighting::Transmissions)
        .await
        .expect("topology");
    assert!(
        view.value
            .edges
            .iter()
            .all(|e| e.route != Route::Channel(talk))
    );
    assert!(
        view.value
            .edges
            .iter()
            .any(|e| e.route == Route::Channel(wiki))
    );
    let listed = rows(&b, &ChannelFilter::default()).await;
    assert!(listed.iter().any(|r| r.channel().id == wiki));
    assert!(listed.iter().all(|r| r.channel().id != talk));
    // Sanctioning through promotion suppresses the absorbed channel's own
    // alerts too.
    let state = b.state.read().await;
    assert!(
        state
            .alerts
            .iter()
            .filter(|a| a.subject == AlertSubject::Channel(talk)
                || a.subject == AlertSubject::Channel(wiki))
            .all(|a| !crate::backend::alert_state::is_active(&a.state))
    );
    drop(state);
    // Refusals.
    let notes = channel(&b, ChannelKey::TeamNotes);
    let refused = async |id, pattern| {
        b.act(&c, promote(id, pattern, PolicyKind::Sanctioned))
            .await
            .err()
    };
    assert_eq!(
        refused(talk, pattern.clone()).await,
        Some(ActionError::Conflict(ConflictKind::ChannelSuperseded {
            channel: talk,
            by: wiki
        }))
    );
    assert_eq!(
        refused(wiki, pattern.clone()).await,
        Some(ActionError::Conflict(ConflictKind::ChannelNotDiscovered {
            channel: wiki
        }))
    );
    assert_eq!(
        refused(notes, pattern.clone()).await,
        Some(ActionError::Conflict(ConflictKind::ChannelNotDiscovered {
            channel: notes
        }))
    );
    let pastebin = channel(&b, ChannelKey::Pastebin);
    assert_eq!(
        refused(pastebin, pattern).await,
        Some(ActionError::InvalidInput(InputError::PatternMissesSeed))
    );
}
