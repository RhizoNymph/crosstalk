//! Alerts and alert rules as `QueryApi` lists them. Alerts are keyed by
//! `AlertId`, newest first; the channel filter compares resolved subjects
//! (`AlertSubject::resolved`) and resolved routes with the listed channel's
//! canonical channel. An alert about a hidden channel, or about a
//! transmission whose agents have merged into one, is not listed
//! ([`shown`]); an unmerge lists it again. Rules list the built-ins first, in
//! `BuiltinRule::ALL` order, then user rules newest first.

use crosstalk_spec::aggregates::alert::{Alert, AlertRuleDef, AlertSubject, BuiltinRule};
use crosstalk_spec::derived::flow::transmission::{Crossing, Route};
use crosstalk_spec::ids::{AlertId, ChannelId};
use crosstalk_spec::interfaces::l8_surface::AlertFilter;
use crosstalk_spec::interfaces::l8_surface::lists::AlertRuleFilter;
use crosstalk_spec::paging::{AlertList, AlertRuleList, Page, PageRequest};

use crate::backend::Result;
use crate::backend::alert_state;

use super::Ctx;
use super::page::{self, Key};

/// Whether an alert is about the canonical channel `channel`: its subject
/// resolves to it, or it is a transmission whose route resolves to it.
fn about_channel(ctx: &Ctx, alert: &Alert, channel: ChannelId) -> bool {
    match alert.subject.resolved(ctx.aliases()) {
        AlertSubject::Channel(c) => c == channel,
        AlertSubject::Transmission(t) => ctx
            .world
            .tx(t)
            .is_some_and(|r| ctx.route(&r.transmission.route) == Route::Channel(channel)),
        AlertSubject::Agent(_) => false,
    }
}

/// Whether the alert list shows `alert` at this read
/// (`AlertSubject::shown`): not when it is about a hidden channel or a
/// transmission whose agents have merged into one.
pub fn shown(ctx: &Ctx, alert: &Alert) -> bool {
    alert.subject.shown(
        ctx.aliases(),
        |channel| ctx.hidden(channel),
        |transmission| {
            ctx.world.tx(transmission).is_some_and(|record| {
                ctx.crossing(&record.transmission) == Crossing::WithinOneAgent
            })
        },
    )
}

/// Newest id first.
fn newest_id(id: AlertId) -> Key {
    (0, u128::MAX - id.as_ulid())
}

pub fn alerts(
    ctx: &Ctx,
    filter: &AlertFilter,
    page: &PageRequest<AlertList>,
) -> Result<Page<Alert, AlertList>> {
    let channel = filter.channel.map(|c| ctx.channel(c));
    let items = ctx
        .state
        .alerts
        .iter()
        .filter(|a| {
            filter.states.is_empty() || filter.states.contains(&alert_state::kind(&a.state))
        })
        .filter(|a| channel.is_none_or(|c| about_channel(ctx, a, c)))
        .filter(|a| shown(ctx, a))
        .map(|a| (newest_id(a.id), a.clone()))
        .collect();
    page::paginate("alerts", page::digest(filter), items, page)
}

/// The alert stored under `id`, as `alerts` lists it.
pub fn alert(ctx: &Ctx, id: AlertId) -> Option<Alert> {
    ctx.state.alerts.iter().find(|a| a.id == id).cloned()
}

/// A rule's place in the list: a built-in by its position in
/// `BuiltinRule::ALL`, after them a user rule by descending id.
fn rule_key(rule: &AlertRuleDef) -> Key {
    match BuiltinRule::ALL
        .iter()
        .position(|builtin| builtin.id() == rule.id())
    {
        Some(index) => (0, index as u128),
        None => (1, u128::MAX - rule.id().as_ulid()),
    }
}

pub fn alert_rules(
    ctx: &Ctx,
    filter: &AlertRuleFilter,
    page: &PageRequest<AlertRuleList>,
) -> Result<Page<AlertRuleDef, AlertRuleList>> {
    let items = ctx
        .state
        .rules
        .iter()
        .filter(|rule| filter.matches(rule))
        .map(|rule| (rule_key(rule), rule.clone()))
        .collect();
    page::paginate("alert-rules", page::digest(filter), items, page)
}
