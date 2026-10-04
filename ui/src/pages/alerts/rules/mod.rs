//! `/alerts/rules`: built-in rules (enable or disable), operator rules
//! (create, edit, enable, disable, update when stale) and, for `Govern`,
//! sinks with their last delivery.

pub mod edit;
pub mod form;
pub mod model;

use std::collections::HashMap;

use crosstalk_spec::aggregates::alert::{AlertRuleDef, RuleStatus};
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::AlertRuleId;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, SinkInfo};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::content::Form;
use topcoat::router::page;
use topcoat::view::{View, component, view};

use self::model::{
    Detail, RuleRow, TopicLabels, delivery, sink_kind, status_label, status_tone, watched_topics,
};
use crate::app::{backend, caller, can};
use crate::backend::Backend;
use crate::components::form::{BUTTON, LINK, SECTION, SECTION_TITLE, SMALL_BUTTON};
use crate::components::table::{ROW, TD, TD_MUTED};
use crate::components::{
    Tone, content_hidden, data_table, empty_state, error_panel, flash_banner, href, page_header,
    state_badge,
};
use crate::error::UiError;
use crate::pages::common::action::{Failure, done, perform, require, settled, status_of};
use crate::pages::common::flash::{Flash, flash};
use crate::pages::common::form::{FormFields, id, invalid, required};
use crate::pages::common::lookup::{OperatorNames, operator_names};
use crate::pages::common::rules::all_rules;
use crate::pages::common::topics::all_topics;
use crate::pages::view::view_state;
use crate::url::view_state::ViewState;
use crosstalk_spec::ids::SinkId;
use crosstalk_spec::interfaces::l8_surface::OperatorAction;

pub const PATH: &str = "/alerts/rules";

/// Rules, sinks and the topic labels rules refer to.
pub struct RulesData {
    pub rules: Vec<AlertRuleDef>,
    /// `None` without `Govern`: a delivery error can name a sink's
    /// endpoint.
    pub sinks: Option<Vec<SinkInfo>>,
    pub sink_names: HashMap<SinkId, String>,
    pub topics: TopicLabels,
    pub operators: OperatorNames,
}

/// Labels of the topics in `versions`, when the caller may read content.
async fn topic_labels(cx: &Cx, caller: &Caller, versions: Vec<TopicModelVersion>) -> TopicLabels {
    if !can(caller, Permission::Content) {
        return None;
    }
    let mut labels = HashMap::new();
    for version in versions {
        match all_topics(backend(cx), caller, TopicVersionSelector::Pinned(version)).await {
            Ok((_, topics)) => labels.extend(topics.into_iter().map(|t| (t.id, t.label))),
            Err(error) => {
                tracing::warn!(error = ?error, version = version.0, "topic labels unavailable")
            }
        }
    }
    Some(labels)
}

pub async fn load(cx: &Cx, caller: &Caller) -> std::result::Result<RulesData, UiError> {
    require(caller, Permission::View)?;
    let rules = all_rules(backend(cx), caller).await?;
    let sinks = if can(caller, Permission::Govern) {
        Some(backend(cx).sinks(caller).await?)
    } else {
        None
    };
    let mut versions: Vec<TopicModelVersion> = rules
        .iter()
        .filter_map(|r| watched_topics(r).map(|w| w.version))
        .collect();
    versions.sort_unstable_by_key(|v| v.0);
    versions.dedup();
    Ok(RulesData {
        sink_names: sinks
            .iter()
            .flatten()
            .map(|s| (s.id, s.name.clone()))
            .collect(),
        topics: topic_labels(cx, caller, versions).await,
        operators: operator_names(cx, caller).await,
        rules,
        sinks,
    })
}

/// The `status` a toggle posts: what the rule should become.
pub fn status_code(enabled: bool) -> &'static str {
    if enabled { "enabled" } else { "disabled" }
}

/// A validated enable or disable post.
pub fn parse_toggle(fields: &FormFields) -> std::result::Result<(OperatorAction, Flash), UiError> {
    if fields.text("action") != Some("set-enabled") {
        return Err(invalid("action", "unknown action"));
    }
    let rule = id::<AlertRuleId>(fields, "rule")?;
    let (enabled, flash) = match required(fields, "status")? {
        "enabled" => (true, Flash::RuleEnabled),
        "disabled" => (false, Flash::RuleDisabled),
        other => return Err(invalid("status", format!("unknown status {other:?}"))),
    };
    Ok((OperatorAction::SetRuleEnabled { id: rule, enabled }, flash))
}

#[page("/alerts/rules")]
async fn rules_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx).await?;
    let flash = flash(cx);
    Ok(view! { rules_page(state: state, flash: flash, failure: None) })
}

#[page(POST "/alerts/rules")]
async fn rules_post(cx: &Cx, Form(fields): Form<FormFields>) -> Result<impl View> {
    let state = view_state(cx).await?;
    let error = match parse_toggle(&fields) {
        Ok((action, flash)) => match perform(cx, action).await {
            Ok(outcome) => return Err(done(PATH, &state, &[], settled(&outcome, flash))),
            Err(error) => error,
        },
        Err(error) => error,
    };
    let failure: Failure<()> = Failure::new(None, error, fields);
    Ok(view! { rules_page(state: state, flash: None, failure: Some(failure)) })
}

/// The enable or disable button of a rule.
#[component]
async fn toggle(action: String, rule: String, enabled: bool) -> Result<impl View> {
    let (status, label) = if enabled {
        (status_code(false), "Disable")
    } else {
        (status_code(true), "Enable")
    };
    Ok(view! {
        <form method="post" action=(action) class="inline">
            <input type="hidden" name="action" value="set-enabled">
            <input type="hidden" name="rule" value=(rule)>
            <input type="hidden" name="status" value=(status)>
            <button type="submit" class=(SMALL_BUTTON)>(label)</button>
        </form>
    })
}

#[component]
async fn rules_page(
    cx: &Cx,
    state: ViewState,
    flash: Option<Flash>,
    failure: Option<Failure<()>>,
) -> Result<impl View> {
    let caller = caller(cx);
    let govern = can(&caller, Permission::Govern);
    let data = load(cx, &caller).await;
    let failed = failure.map(|f| (f.status(), f.error));
    let action_url = href(PATH, &state, &[]);
    let new_watched = href(&format!("{PATH}/new"), &state, &[("kind", "watched")]);
    let new_semantic = href(&format!("{PATH}/new"), &state, &[("kind", "semantic")]);
    let inbox_url = href("/alerts", &state, &[]);

    Ok(view! {
        <div class="mb-1 text-xs text-zinc-500"><a class=(LINK) href=(inbox_url)>"Alerts"</a> " / rules"</div>
        page_header(title: "Alert rules", subtitle: "What raises an alert, and where alerts are delivered. Rules are never deleted, so every alert keeps its rule.")
        if let Some((status, error)) = failed {
            (status)
            <div class="mb-4">error_panel(error: &error)</div>
        }
        if let Some(flash) = flash {
            flash_banner(message: flash.message())
        }
        match data {
            Err(error) => {
                (status_of(&error))
                error_panel(error: &error)
            },
            Ok(data) => {
                let rows: Vec<RuleRow> = data
                    .rules
                    .iter()
                    .map(|r| RuleRow::new(r, &data.topics, &data.sink_names, &data.operators, &state))
                    .collect();
                let (builtin, user): (Vec<RuleRow>, Vec<RuleRow>) = rows.into_iter().partition(|r| r.builtin);
                let no_builtin = builtin.is_empty();
                let no_user = user.is_empty();
                let sinks: Option<Vec<_>> = data.sinks.as_ref().map(|sinks| {
                    sinks
                        .iter()
                        .map(|s| {
                            let (text, tone) = delivery(s);
                            (s.name.clone(), sink_kind(s.kind), text, tone)
                        })
                        .collect()
                });
                let no_sinks = sinks.as_ref().is_some_and(Vec::is_empty);
                <section class=(SECTION)>
                    <h2 class=(SECTION_TITLE)>"Built-in rules"</h2>
                    if no_builtin {
                        empty_state(message: "The gateway reported no built-in rules.")
                    } else {
                        data_table(
                            headers: &["Rule", "Raises an alert when", "Status", ""],
                            for row in builtin {
                                let enabled = row.status == RuleStatus::Enabled;
                                <tr class=(ROW)>
                                    <td class=(TD)>(row.name.clone())</td>
                                    <td class=(TD)>
                                        match row.detail.clone() {
                                            Detail::Builtin(text) => (text),
                                            _ => "",
                                        }
                                    </td>
                                    <td class=(TD)>state_badge(label: status_label(row.status), tone: status_tone(row.status))</td>
                                    <td class=(TD)>
                                        if govern {
                                            toggle(action: action_url.clone(), rule: row.id.clone(), enabled: enabled)
                                        }
                                    </td>
                                </tr>
                            }
                        )
                    }
                </section>
                <section class=(SECTION)>
                    <div class="mb-2 flex items-center gap-2">
                        <h2 class=(format!("{SECTION_TITLE} mb-0"))>"Operator rules"</h2>
                        if govern {
                            <a class=(format!("{BUTTON} ml-auto")) href=(new_watched)>"Watch topics…"</a>
                            <a class=(BUTTON) href=(new_semantic)>"Semantic query…"</a>
                        }
                    </div>
                    if no_user {
                        empty_state(message: "No operator rules. Watch topics or describe what to look for with a semantic query.")
                    } else {
                        data_table(
                            headers: &["Rule", "Matches", "Status", "Created", "Delivers to", ""],
                            for row in user {
                                let enabled = row.status == RuleStatus::Enabled;
                                let stale = row.stale.is_some();
                                <tr class=(ROW)>
                                    <td class=(TD)>(row.name.clone())</td>
                                    <td class=(TD)>
                                        match row.detail.clone() {
                                            Detail::Topics { version, count, labels, remap_threshold } => {
                                                <div>(count) " topics of version " (version) ", remapped at similarity ≥ " (format!("{remap_threshold:.2}"))</div>
                                                match labels {
                                                    Some(labels) => <div class="text-xs text-zinc-600 dark:text-zinc-400">(labels.join(", "))</div>,
                                                    None => content_hidden(),
                                                }
                                            },
                                            Detail::Semantic { text, threshold, model } => {
                                                <div>"\u{201c}" (text) "\u{201d}"</div>
                                                <div class="text-xs text-zinc-500">"similarity ≥ " (format!("{threshold:.2}")) " under " (model)</div>
                                            },
                                            Detail::Builtin(text) => (text),
                                        }
                                    </td>
                                    <td class=(TD)>
                                        state_badge(label: status_label(row.status), tone: status_tone(row.status))
                                        if let Some(staleness) = row.stale.clone() {
                                            <span class="ml-1">state_badge(label: "stale", tone: Tone::Warn)</span>
                                            <div class="mt-1 max-w-xs text-xs text-amber-800 dark:text-amber-300">(staleness.reason)</div>
                                            if let Some(topics) = staleness.topics {
                                                <div class="text-xs text-zinc-500">(topics.join(", "))</div>
                                            }
                                        }
                                    </td>
                                    <td class=(TD_MUTED)>(row.created.clone())</td>
                                    <td class=(TD_MUTED)>(row.sinks.clone())</td>
                                    <td class=(TD)>
                                        if govern {
                                            <div class="flex items-center justify-end gap-1.5">
                                                if stale {
                                                    <a class=(SMALL_BUTTON) href=(row.edit_url.clone())>"Update…"</a>
                                                    // Disabling is always allowed; enabling waits for an update.
                                                    if enabled {
                                                        toggle(action: action_url.clone(), rule: row.id.clone(), enabled: true)
                                                    }
                                                } else {
                                                    <a class=(SMALL_BUTTON) href=(row.edit_url.clone())>"Edit"</a>
                                                    toggle(action: action_url.clone(), rule: row.id.clone(), enabled: enabled)
                                                }
                                            </div>
                                        }
                                    </td>
                                </tr>
                            }
                        )
                    }
                </section>
                <section class=(SECTION)>
                    <h2 class=(SECTION_TITLE)>"Sinks"</h2>
                    match sinks {
                        None => empty_state(message: "Sinks and their deliveries need the Govern permission: a delivery error can name a sink's endpoint."),
                        Some(_) if no_sinks => empty_state(message: "No sinks are configured; alerts stay in the inbox."),
                        Some(sinks) => data_table(
                            headers: &["Sink", "Kind", "Last delivery"],
                            for (name, kind, text, tone) in sinks {
                                <tr class=(ROW)>
                                    <td class=(TD)>(name)</td>
                                    <td class=(TD_MUTED)>(kind)</td>
                                    <td class=(TD)>state_badge(label: &text, tone: tone)</td>
                                </tr>
                            }
                        ),
                    }
                </section>
            },
        }
    })
}

#[cfg(test)]
mod tests {
    use topcoat::router::StatusCode;

    use super::*;
    use crate::components::href::tests::state;
    use crate::testing::{get, post};

    const RULE: &str = "01J9ZQ3W8D0000000000000004";

    #[test]
    fn toggles_parse() {
        let fields = FormFields::from_pairs(&[
            ("action", "set-enabled"),
            ("rule", RULE),
            ("status", "disabled"),
        ]);
        let (action, flash) = parse_toggle(&fields).expect("valid");
        assert_eq!(flash, Flash::RuleDisabled);
        assert!(matches!(
            action,
            OperatorAction::SetRuleEnabled { enabled: false, .. }
        ));
        let bad = FormFields::from_pairs(&[
            ("action", "set-enabled"),
            ("rule", RULE),
            ("status", "stale"),
        ]);
        assert!(
            parse_toggle(&bad).is_err(),
            "stale is reached only by the system"
        );
    }

    #[tokio::test]
    async fn rules_page_renders_rules_and_sinks() {
        let reply = get(&format!("/alerts/rules?{}", state().to_query())).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert!(reply.body.contains("Exfiltration to paste sites"));
        assert!(reply.body.contains("Engineering chatter (v1)"));
        assert!(reply.body.contains("soc-webhook"));
        assert!(reply.body.contains("Watch topics…"));
    }

    #[tokio::test]
    async fn stale_and_disabled_rules_read_as_such() {
        let reply = get(&format!("/alerts/rules?{}", state().to_query())).await;
        assert_eq!(reply.status, StatusCode::OK);
        let body = &reply.body;
        let stale = body.find("Engineering chatter (v1)").expect("stale rule");
        let row = &body[stale..stale + body[stale..].find("</tr>").expect("row end")];
        assert!(row.contains(">enabled</span>"), "it went stale enabled");
        assert!(row.contains(">stale</span>"));
        assert!(row.contains("had no match at or above the remap threshold in topic version 2"));
        assert!(
            row.contains("Engineering chatter</div>"),
            "the unmapped topic, named"
        );
        assert!(row.contains("Update…"));
        assert!(
            row.contains(">Disable</button>"),
            "disabling is always allowed"
        );
        assert!(!row.contains(">Enable</button>"));
        assert!(row.contains("inbox only"));
        let refunds = body.find("Refund escalations").expect("refund rule");
        let row = &body[refunds..refunds + body[refunds..].find("</tr>").expect("row end")];
        assert!(row.contains(">disabled</span>") && row.contains(">Enable</button>"));
        assert!(
            body.contains("rejected with HTTP 503"),
            "soc-webhook's last delivery"
        );
        assert!(body.contains("New channel"), "built-ins by their spec name");
    }

    #[tokio::test]
    async fn enabling_the_stale_rule_is_refused_in_words() {
        let session = crate::testing::Session::new();
        let url = format!("/alerts/rules?{}", state().to_query());
        let body = session.get(&url).await.body;
        let stale = body.find("Engineering chatter (v1)").expect("stale rule");
        let marker = "name=\"rule\" value=\"";
        let at = stale + body[stale..].find(marker).expect("rule id") + marker.len();
        let id = &body[at..at + 26];
        let reply = session
            .post(
                &url,
                &format!("action=set-enabled&rule={id}&status=enabled"),
            )
            .await;
        assert_eq!(reply.status, StatusCode::CONFLICT, "{}", reply.body);
        assert!(
            reply
                .body
                .contains("is stale; update it to retarget and enable it")
        );
    }

    #[tokio::test]
    async fn toggle_posts_validate() {
        let url = format!("/alerts/rules?{}", state().to_query());
        let reply = post(&url, "action=set-enabled&rule=x&status=enabled").await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(reply.body.contains("rule: expected 26 characters"));
        let reply = post(
            &url,
            &format!("action=set-enabled&rule={RULE}&status=enabled"),
        )
        .await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND);
    }
}
