//! Rule and alert reads: list order, cursors, the channel filter and the
//! alerts readers do not show.

use std::collections::BTreeMap;

use crosstalk_memory::model::build::{channel, operator, transmission, ts};
use crosstalk_spec::aggregates::alert::{
    AlertDraft, AlertRuleDef, AlertSubject, BuiltinRule, RuleName, RuleQueryText, RuleStatus,
    TriageOutcome, UserRule,
};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AlertRuleId, TransmissionId};
use crosstalk_spec::interfaces::l6_analysis::alerts::{AlertReadError, AlertReads};
use crosstalk_spec::interfaces::l6_analysis::{AlertRuleStore, AlertTriage};
use crosstalk_spec::interfaces::l8_surface::AlertFilter;
use crosstalk_spec::interfaces::l8_surface::lists::AlertRuleFilter;
use crosstalk_spec::paging::{Cursor, PageRequest, PageSize};
use crosstalk_spec::support::Change;

use super::super::{FactsError, SubjectFacts};
use super::{config, store, store_with};
use crate::pg::testing::database;

pub(crate) fn page<L>(size: u16, after: Option<Cursor<L>>) -> PageRequest<L> {
    PageRequest {
        size: PageSize::new(size).unwrap_or_else(|_| panic!("size")),
        after,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rules_list_builtins_first_then_user_rules_newest_first() {
    let Some(db) = database("rules_list_builtins_first_then_user_rules_newest_first").await else {
        return;
    };
    let (mut store, _, _) = store(db.pool().clone()).await;
    let mut users = Vec::new();
    for (n, at) in [3_000u64, 2_000, 5_000].into_iter().enumerate() {
        let rule = UserRule::SemanticQuery {
            text: RuleQueryText::new(&format!("query {n}")).unwrap_or_else(|_| panic!("text")),
            threshold: crosstalk_memory::model::build::similarity(0.5)
                .unwrap_or_else(|| panic!("t")),
        };
        let name = RuleName::new("r").unwrap_or_else(|_| panic!("name"));
        users.push(
            store
                .create(name, rule, Vec::new(), operator(1), ts(at))
                .await
                .unwrap_or_else(|error| panic!("{error:?}")),
        );
    }
    assert_eq!(
        store
            .set_enabled(BuiltinRule::NewChannel.id(), false, operator(1), ts(1))
            .await,
        Ok(Change::Applied)
    );
    let mut newest_first = users.clone();
    newest_first.sort_by(|a, b| b.cmp(a));
    let expected: Vec<AlertRuleId> = BuiltinRule::ALL
        .iter()
        .map(|rule| rule.id())
        .chain(newest_first.iter().copied())
        .collect();
    for size in [1, 2, 3, 5, 8, 20] {
        let mut request = page(size, None);
        let mut listed: Vec<AlertRuleId> = Vec::new();
        loop {
            let (items, next) = store
                .rules(&AlertRuleFilter::default(), &request)
                .await
                .unwrap_or_else(|error| panic!("{error:?}"))
                .into_parts();
            listed.extend(items.iter().map(AlertRuleDef::id));
            match next {
                Some(cursor) => request.after = Some(cursor),
                None => break,
            }
        }
        assert_eq!(listed, expected, "page size {size}");
    }
    // The filter keeps its own rules, in the same order; a cursor is bound
    // to its filter.
    let enabled = AlertRuleFilter {
        statuses: vec![RuleStatus::Enabled],
        stale: None,
    };
    let first = store
        .rules(&enabled, &page(2, None))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        first
            .items()
            .iter()
            .map(AlertRuleDef::id)
            .collect::<Vec<_>>(),
        vec![
            BuiltinRule::UnreviewedTraffic.id(),
            BuiltinRule::UnsanctionedTraffic.id()
        ]
    );
    let cursor = first.next().cloned();
    assert_eq!(
        store
            .rules(&AlertRuleFilter::default(), &page(2, cursor))
            .await
            .err(),
        Some(AlertReadError::InvalidCursor)
    );
}

/// Facts set by the test: routes, and the subjects readers do not show.
#[derive(Debug, Clone, Default)]
struct Facts {
    routes: BTreeMap<TransmissionId, Route>,
    hidden: Vec<AlertSubject>,
}

impl SubjectFacts for Facts {
    async fn route(&self, transmission: TransmissionId) -> Result<Option<Route>, FactsError> {
        Ok(self.routes.get(&transmission).cloned())
    }

    async fn shown(&self, subject: AlertSubject) -> Result<bool, FactsError> {
        Ok(!self.hidden.contains(&subject))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn alerts_list_filters_channels_and_hides_unshown_subjects() {
    let Some(db) = database("alerts_list_filters_channels_and_hides_unshown_subjects").await else {
        return;
    };
    let directory = crosstalk_memory::analysis::aliases::StaticDirectory::new();
    let facts = Facts {
        routes: BTreeMap::from([
            (transmission(1), Route::Channel(channel(1))),
            (transmission(2), Route::Unobserved),
        ]),
        hidden: vec![
            AlertSubject::Channel(channel(3)),
            AlertSubject::Transmission(transmission(3)),
        ],
    };
    let (mut store, _) = store_with(db.pool().clone(), directory.clone(), facts, config()).await;
    let mut opened = BTreeMap::new();
    for subject in [
        AlertSubject::Channel(channel(1)),
        AlertSubject::Channel(channel(2)),
        AlertSubject::Channel(channel(3)),
        AlertSubject::Transmission(transmission(1)),
        AlertSubject::Transmission(transmission(2)),
        AlertSubject::Transmission(transmission(3)),
    ] {
        let rule = match subject {
            AlertSubject::Channel(_) => BuiltinRule::NewChannel.id(),
            _ => BuiltinRule::SuspectedTransmission.id(),
        };
        let outcome = store
            .triage(AlertDraft {
                rule,
                subject,
                raised_at: ts(1),
            })
            .await;
        let Ok(TriageOutcome::Opened(alert)) = outcome else {
            panic!("{outcome:?}");
        };
        opened.insert(alert.id, subject);
    }
    let list = |filter: AlertFilter| {
        let store = store.clone();
        async move {
            let mut request = page(1, None);
            let mut subjects = Vec::new();
            loop {
                let (items, next) = store
                    .alerts(&filter, &request)
                    .await
                    .unwrap_or_else(|error| panic!("{error:?}"))
                    .into_parts();
                subjects.extend(items.iter().map(|alert| alert.subject));
                match next {
                    Some(cursor) => request.after = Some(cursor),
                    None => return subjects,
                }
            }
        }
    };
    // Newest first, hidden subjects left out.
    let mut shown: Vec<AlertSubject> = opened
        .iter()
        .rev()
        .map(|(_, subject)| *subject)
        .filter(|subject| !matches!(subject, AlertSubject::Channel(c) if *c == channel(3)))
        .filter(
            |subject| !matches!(subject, AlertSubject::Transmission(t) if *t == transmission(3)),
        )
        .collect();
    assert_eq!(list(AlertFilter::default()).await, shown);
    // Channel 1: its own alert and transmission 1's (routed through it);
    // after channel 2 is superseded by 1, channel 2's too.
    let on_one = AlertFilter {
        states: Vec::new(),
        channel: Some(channel(1)),
    };
    assert_eq!(
        list(on_one.clone()).await,
        vec![
            AlertSubject::Transmission(transmission(1)),
            AlertSubject::Channel(channel(1))
        ]
    );
    assert!(directory.supersede(channel(2), channel(1)).is_ok());
    assert_eq!(
        list(on_one).await,
        vec![
            AlertSubject::Transmission(transmission(1)),
            AlertSubject::Channel(channel(2)),
            AlertSubject::Channel(channel(1)),
        ]
    );
    shown.retain(|subject| matches!(subject, AlertSubject::Transmission(_)));
    let acknowledged = AlertFilter {
        states: vec![crosstalk_spec::aggregates::alert::AlertStateKind::Acknowledged],
        channel: None,
    };
    assert_eq!(list(acknowledged).await, Vec::new());
}
