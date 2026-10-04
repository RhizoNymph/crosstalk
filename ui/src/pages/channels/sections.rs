//! The sections of the channel page: resources with their writers and
//! readers, policy history, and the channel's alerts.

use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l8_surface::AlertStateKind;
use topcoat::Result;
use topcoat::view::{View, component, view};

use crate::components::form::{LINK, SECTION, SECTION_TITLE};
use crate::components::table::{ROW, TD, TD_MUTED, TD_NUM};
use crate::components::{
    data_table, empty_state, error_panel, format_time, kind_badge, locator_text,
};
use crate::contract::channels::ResourceUse;
use crate::contract::research::{AuditEntry, AuditOutcome};
use crate::error::UiError;
use crate::pages::alerts::model::AlertRow;
use crate::pages::audit::describe::{describe, note};
use crate::pages::common::links::agent_url;
use crate::pages::common::lookup::{AgentNames, OperatorNames};
use crate::url::view_state::ViewState;

/// An agent that wrote or read a resource, with how often.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentUse {
    pub url: String,
    pub name: String,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceRow {
    pub locator: Locator,
    pub first_seen: String,
    pub writers: Vec<AgentUse>,
    pub readers: Vec<AgentUse>,
}

fn agent_uses(uses: &[(AgentId, u64)], names: &AgentNames, state: &ViewState) -> Vec<AgentUse> {
    let mut out: Vec<AgentUse> = uses
        .iter()
        .map(|(id, count)| AgentUse {
            url: agent_url(*id, state),
            name: names.name(*id),
            count: *count,
        })
        .collect();
    out.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.name.cmp(&b.name)));
    out
}

pub fn resource_rows(
    uses: &[ResourceUse],
    names: &AgentNames,
    state: &ViewState,
) -> Vec<ResourceRow> {
    uses.iter()
        .map(|u| ResourceRow {
            locator: u.resource.locator.clone(),
            first_seen: format_time(u.resource.first_seen),
            writers: agent_uses(&u.writers, names, state),
            readers: agent_uses(&u.readers, names, state),
        })
        .collect()
}

#[component]
async fn agent_list(uses: Vec<AgentUse>) -> Result<impl View> {
    let empty = uses.is_empty();
    Ok(view! {
        if empty {
            <span class="text-xs text-zinc-400">"none"</span>
        } else {
            <ul class="space-y-0.5">
                for agent in uses {
                    <li class="whitespace-nowrap">
                        <a class=(LINK) href=(agent.url)>(agent.name)</a>
                        <span class="ml-1 text-xs tabular-nums text-zinc-500">"×" (agent.count)</span>
                    </li>
                }
            </ul>
        }
    })
}

#[component]
pub async fn resources_section(
    rows: std::result::Result<Vec<ResourceRow>, UiError>,
) -> Result<impl View> {
    let empty = rows.as_ref().is_ok_and(Vec::is_empty);
    Ok(view! {
        <section class=(SECTION)>
            <h2 class=(SECTION_TITLE)>"Resources in this window"</h2>
            match rows {
                Err(error) => error_panel(error: &error),
                Ok(_) if empty => empty_state(message: "No resource of this channel was accessed in the selected window."),
                Ok(rows) => data_table(
                    headers: &["Resource", "First seen", "Writers", "Readers"],
                    for row in rows {
                        <tr class=(ROW)>
                            <td class=(TD)><div class="max-w-md">locator_text(locator: &row.locator)</div></td>
                            <td class=(TD_MUTED)>(row.first_seen)</td>
                            <td class=(TD)>agent_list(uses: row.writers)</td>
                            <td class=(TD)>agent_list(uses: row.readers)</td>
                        </tr>
                    }
                ),
            }
        </section>
    })
}

/// One entry of the policy history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRow {
    pub at: String,
    pub by: String,
    pub what: String,
    pub note: Option<String>,
    /// `Err` holds why the gateway rejected it.
    pub outcome: std::result::Result<(), String>,
}

pub fn history_rows(entries: &[AuditEntry], operators: &OperatorNames) -> Vec<HistoryRow> {
    entries
        .iter()
        .map(|e| HistoryRow {
            at: format_time(e.at),
            by: operators.actor(e.by),
            what: describe(&e.action),
            note: note(&e.action).map(str::to_owned),
            outcome: match &e.outcome {
                AuditOutcome::Applied(_) => Ok(()),
                AuditOutcome::Rejected(error) => Err(crate::error::describe(error)),
            },
        })
        .collect()
}

#[component]
pub async fn history_section(
    rows: std::result::Result<Vec<HistoryRow>, UiError>,
    audit_url: String,
) -> Result<impl View> {
    let empty = rows.as_ref().is_ok_and(Vec::is_empty);
    Ok(view! {
        <section class=(SECTION)>
            <h2 class=(SECTION_TITLE)>
                "Policy history"
                <a class=(format!("{LINK} ml-2 normal-case tracking-normal font-normal")) href=(audit_url)>"full audit log"</a>
            </h2>
            match rows {
                Err(error) => error_panel(error: &error),
                Ok(_) if empty => empty_state(message: "No policy decision has been recorded for this channel."),
                Ok(rows) => data_table(
                    headers: &["When", "By", "Change", "Note", "Outcome"],
                    for row in rows {
                        <tr class=(ROW)>
                            <td class=(TD_MUTED)>(row.at)</td>
                            <td class=(TD)>(row.by)</td>
                            <td class=(TD)>(row.what)</td>
                            <td class=(TD)>(row.note.unwrap_or_default())</td>
                            <td class=(TD)>
                                match row.outcome {
                                    Ok(()) => <span class="text-xs text-emerald-700 dark:text-emerald-400">"applied"</span>,
                                    Err(reason) => <span class="text-xs text-red-700 dark:text-red-400">"rejected: " (reason)</span>,
                                }
                            </td>
                        </tr>
                    }
                ),
            }
        </section>
    })
}

#[component]
pub async fn alerts_section(
    rows: std::result::Result<Vec<AlertRow>, UiError>,
    inbox_url: String,
) -> Result<impl View> {
    let empty = rows.as_ref().is_ok_and(Vec::is_empty);
    Ok(view! {
        <section class=(SECTION)>
            <h2 class=(SECTION_TITLE)>
                "Alerts"
                <a class=(format!("{LINK} ml-2 normal-case tracking-normal font-normal")) href=(inbox_url)>"open in inbox"</a>
            </h2>
            match rows {
                Err(error) => error_panel(error: &error),
                Ok(_) if empty => empty_state(message: "No alerts about this channel or its transmissions."),
                Ok(rows) => data_table(
                    headers: &["State", "Rule", "Subject", "Occurrences", "Raised"],
                    for row in rows {
                        <tr class=(ROW)>
                            <td class=(TD)>
                                kind_badge(value: row.state)
                                if row.state != AlertStateKind::Open {
                                    <div class="mt-0.5 text-xs text-zinc-500">(row.state_detail)</div>
                                }
                            </td>
                            <td class=(TD)>(row.rule)</td>
                            <td class=(TD)><a class=(LINK) href=(row.subject_url)>(row.subject_label)</a></td>
                            <td class=(TD_NUM)>(row.occurrences)</td>
                            <td class=(TD_MUTED)>(row.raised)</td>
                        </tr>
                    }
                ),
            }
        </section>
    })
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::derived::flow::resource::Resource;
    use crosstalk_spec::ids::{ChannelId, OperatorId, ResourceId};
    use crosstalk_spec::interfaces::l8_surface::{PolicyKind, QueryError};
    use crosstalk_spec::support::Timestamp;

    use super::*;
    use crate::components::href::tests::state;
    use crate::pages::channels::model::tests::wiki;

    #[test]
    fn writers_sort_by_volume_and_use_names() {
        let uses = vec![ResourceUse {
            resource: Resource {
                id: ResourceId::from_ulid(1),
                locator: wiki(),
                first_seen: Timestamp::from_micros(0),
            },
            writers: vec![(AgentId::from_ulid(1), 2), (AgentId::from_ulid(2), 9)],
            readers: vec![],
        }];
        let names = AgentNames::from_pairs([(AgentId::from_ulid(2), "planner".to_owned())]);
        let rows = resource_rows(&uses, &names, &state());
        assert_eq!(rows[0].writers[0].name, "planner");
        assert_eq!(rows[0].writers[0].count, 9);
        assert_eq!(rows[0].writers[1].name, "…000001");
        assert!(rows[0].readers.is_empty());
    }

    #[test]
    fn history_names_the_operator_and_keeps_rejections() {
        use crate::contract::actions::OperatorAction;
        use crate::contract::research::{Actor, AuditedAction};
        use crosstalk_spec::ids::AuditId;
        use crosstalk_spec::interfaces::l8_surface::ConflictKind;

        let entry = AuditEntry {
            id: AuditId::from_ulid(1),
            at: Timestamp::from_micros(1_790_985_600_000_000),
            by: Actor::Operator(OperatorId::from_ulid(3)),
            action: AuditedAction::Operator(OperatorAction::SetPolicy {
                channel: ChannelId::from_ulid(1),
                policy: PolicyKind::Sanctioned,
                note: Some("ok".into()),
            }),
            subject: Some(crate::contract::research::AuditSubject::Channel(
                ChannelId::from_ulid(1),
            )),
            outcome: AuditOutcome::Rejected(QueryError::Conflict(
                ConflictKind::ChannelSuperseded {
                    channel: ChannelId::from_ulid(1),
                    by: ChannelId::from_ulid(2),
                },
            )),
        };
        let operators = OperatorNames::new([(OperatorId::from_ulid(3), "ada".to_owned())]);
        let rows = history_rows(&[entry], &operators);
        assert_eq!(rows[0].by, "ada");
        assert_eq!(rows[0].what, "set policy to sanctioned");
        assert_eq!(rows[0].note.as_deref(), Some("ok"));
        assert_eq!(
            rows[0].outcome,
            Err("the channel is superseded: 00000000000000000000000001 resolves to 00000000000000000000000002; act on that channel instead".to_owned())
        );
    }

    #[tokio::test]
    async fn sections_render_their_rows() {
        use topcoat::view::view;

        use crate::testing::{cx, render};

        let cx = &cx();
        let rows = vec![ResourceRow {
            locator: wiki(),
            first_seen: "2026-10-03 00:00:00 UTC".into(),
            writers: vec![AgentUse {
                url: "/agents/A".into(),
                name: "planner".into(),
                count: 3,
            }],
            readers: Vec::new(),
        }];
        let html = render(view! { cx => resources_section(rows: Ok(rows)) }, cx).await;
        assert!(html.contains("https://wiki.example.org/team/agents/notes"));
        assert!(html.contains(">planner</a>"));
        assert!(html.contains("none"), "no readers");

        let history = vec![HistoryRow {
            at: "t".into(),
            by: "ada".into(),
            what: "set policy to sanctioned".into(),
            note: None,
            outcome: Err("the channel is superseded".into()),
        }];
        let html = render(
            view! { cx => history_section(rows: Ok(history), audit_url: "/audit".to_owned()) },
            cx,
        )
        .await;
        assert!(html.contains("rejected: the channel is superseded"));

        let html = render(
            view! { cx => alerts_section(rows: Err(UiError::Query(QueryError::NotFound)), inbox_url: "/alerts".to_owned()) },
            cx,
        )
        .await;
        assert!(html.contains("not found"));
    }
}
