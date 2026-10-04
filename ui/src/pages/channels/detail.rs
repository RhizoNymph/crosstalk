//! `/channels/{id}`: one channel's origin, detection, policy, resources,
//! alerts and policy history. Posting `set-policy` changes its policy.

use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, Caller, Permission, PolicyKind};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::content::Form;
use topcoat::router::error::not_found;
use topcoat::router::{StatusCode, page, path_param};
use topcoat::view::{View, component, view};

use super::model::{DetectionDetail, decision, detection_detail, title};
use super::policy::{self, policy_form};
use super::sections::{
    HistoryRow, ResourceRow, alerts_section, history_rows, history_section, resource_rows,
    resources_section,
};
use crate::app::{backend, caller, can};
use crate::backend::Backend;
use crate::components::form::{BUTTON, LINK, PANEL, SECTION, SECTION_TITLE};
use crate::components::{
    empty_state, error_panel, flash_banner, format_time, href, kind_badge, page_header, short_id,
};
use crate::contract::alerts::Alert;
use crate::contract::channels::{ChannelSummary, DetectionKind, OriginKind, policy_kind};
use crate::contract::research::{AuditFilter, AuditSubject};
use crate::error::UiError;
use crate::pages::alerts::model::AlertRow;
use crate::pages::audit::describe::is_policy_change;
use crate::pages::audit::subject::subject_code;
use crate::pages::common::action::{
    Failure, done, error_for, fields_for, general_error, perform, require, status_of,
};
use crate::pages::common::flash::{Flash, flash};
use crate::pages::common::form::{FormFields, invalid};
use crate::pages::common::links::{channel_url, transmission_url};
use crate::pages::common::lookup::{OperatorNames, agent_names, operator_names, rule_names};
use crate::pages::common::paging::PAGE_SIZE;
use crate::pages::view::view_state;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

path_param!(channel_ulid);

/// The channel named by the path. An id that is not a ULID names nothing.
pub fn channel_id(cx: &Cx) -> Result<ChannelId> {
    ChannelId::parse_ulid(path_param::<ChannelUlid>(cx)).map_err(|_| not_found().into())
}

pub fn channel_path(id: ChannelId) -> String {
    format!("/channels/{}", id.to_ulid())
}

/// The forms on this page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelForm {
    Policy,
}

/// Everything above the sections, owned and display-ready.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub title: String,
    pub id: String,
    pub origin: OriginKind,
    pub detection: DetectionKind,
    pub detection_detail: DetectionDetail,
    pub policy: PolicyKind,
    /// Who decided the policy, when, and their note.
    pub decision: Option<(String, String, Option<String>)>,
    /// The declared channel that took this one over: link, label, by, at.
    pub superseded: Option<(String, String, String, String)>,
    pub writers: u32,
    pub readers: u32,
    pub transmissions: u64,
    pub last_activity: String,
}

pub fn header(summary: &ChannelSummary, operators: &OperatorNames, state: &ViewState) -> Header {
    let channel = &summary.channel;
    Header {
        title: title(summary),
        id: channel.id.to_ulid(),
        origin: OriginKind::of(&channel.origin),
        detection: DetectionKind::of(&channel.origin),
        detection_detail: detection_detail(&channel.origin),
        policy: policy_kind(&channel.policy),
        decision: decision(&channel.policy).map(|d| {
            (
                operators.policy_author(d.by),
                format_time(d.at),
                d.note.clone(),
            )
        }),
        superseded: summary.superseded.map(|s| {
            (
                channel_url(s.into, state),
                format!("channel {}", short_id(s.into.to_ulid())),
                operators.name(s.by),
                format_time(s.at),
            )
        }),
        writers: summary.writers,
        readers: summary.readers,
        transmissions: summary.transmissions,
        last_activity: summary
            .last_activity
            .map_or_else(|| "never".to_owned(), format_time),
    }
}

/// What the caller may do here. A superseded channel takes no policy and
/// cannot be promoted; only discovered channels are promoted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Abilities {
    pub set_policy: bool,
    pub promote: bool,
}

pub fn abilities(caller: &Caller, header: &Header) -> Abilities {
    let govern = can(caller, Permission::Govern);
    let live = header.superseded.is_none();
    Abilities {
        set_policy: govern && live,
        promote: govern && live && header.origin == OriginKind::Discovered,
    }
}

struct Loaded {
    header: Header,
    abilities: Abilities,
    resources: std::result::Result<Vec<ResourceRow>, UiError>,
    alerts: std::result::Result<Vec<AlertRow>, UiError>,
    history: std::result::Result<Vec<HistoryRow>, UiError>,
}

async fn load(
    cx: &Cx,
    caller: &Caller,
    id: ChannelId,
    state: &ViewState,
) -> std::result::Result<Option<Loaded>, UiError> {
    require(caller, Permission::View)?;
    let backend = backend(cx);
    let Some(summary) = backend.channel(caller, id).await? else {
        return Ok(None);
    };
    let operators = operator_names(cx, caller).await;
    let header = header(&summary, &operators, state);

    let resources = match backend
        .channel_resources(caller, id, state.scope.window)
        .await
    {
        Ok(uses) => {
            let ids = uses
                .iter()
                .flat_map(|u| u.writers.iter().chain(u.readers.iter()).map(|(a, _)| *a));
            let names = agent_names(cx, caller, ids.collect::<Vec<_>>()).await;
            Ok(resource_rows(&uses, &names, state))
        }
        Err(error) => Err(error.into()),
    };

    let rules = rule_names(cx, caller).await;
    let filter = AlertFilter {
        states: Vec::new(),
        channel: Some(id),
    };
    let alerts = backend
        .alerts(
            caller,
            &filter,
            &crate::pages::common::paging::first(PAGE_SIZE),
        )
        .await
        .map_err(UiError::from)
        .map(|page| {
            page.items()
                .iter()
                .map(|a: &Alert| AlertRow::new(a, &rules, &operators, state))
                .collect()
        });

    let audit_filter = AuditFilter {
        operators: Vec::new(),
        subject: Some(AuditSubject::Channel(id)),
        window: None,
    };
    let history = backend
        .audit(
            caller,
            &audit_filter,
            &crate::pages::common::paging::first(PAGE_SIZE),
        )
        .await
        .map_err(UiError::from)
        .map(|page| {
            let entries: Vec<_> = page
                .into_parts()
                .0
                .into_iter()
                .filter(|e| is_policy_change(&e.action))
                .collect();
            history_rows(&entries, &operators)
        });

    let abilities = abilities(caller, &header);
    Ok(Some(Loaded {
        header,
        abilities,
        resources,
        alerts,
        history,
    }))
}

#[page("/channels/{channel_ulid}")]
async fn channel_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx).await?;
    let id = channel_id(cx)?;
    let flash = flash(cx);
    Ok(view! { channel_page(id: id, state: state, flash: flash, failure: None) })
}

#[page(POST "/channels/{channel_ulid}")]
async fn channel_post(cx: &Cx, Form(fields): Form<FormFields>) -> Result<impl View> {
    let state = view_state(cx).await?;
    let id = channel_id(cx)?;
    let action = fields.text("action").map(str::to_owned);
    let failure = match action.as_deref() {
        Some("set-policy") => {
            let result = match policy::parse(id, &fields) {
                Ok(action) => perform(cx, action).await,
                Err(error) => Err(error),
            };
            match result {
                Ok(_) => return Err(done(&channel_path(id), &state, &[], Flash::PolicySet)),
                Err(error) => Failure::new(Some(ChannelForm::Policy), error, fields),
            }
        }
        _ => Failure::new(None, invalid("action", "unknown action"), fields),
    };
    Ok(view! { channel_page(id: id, state: state, flash: None, failure: Some(failure)) })
}

#[component]
async fn channel_page(
    cx: &Cx,
    id: ChannelId,
    state: ViewState,
    flash: Option<Flash>,
    failure: Option<Failure<ChannelForm>>,
) -> Result<impl View> {
    let caller = caller(cx);
    let loaded = load(cx, &caller, id, &state).await;
    let failed_status = failure.as_ref().map(Failure::status);
    let general = general_error(failure.as_ref());
    // A failed post whose form is not shown (the channel is gone, or the
    // caller cannot use the form) still shows its error.
    let any_error = failure.as_ref().map(|f| f.error.clone());
    let policy_error = error_for(failure.as_ref(), ChannelForm::Policy);
    let policy_fields = fields_for(failure.as_ref(), ChannelForm::Policy);
    let list_url = href("/channels", &state, &[]);
    let action_url = href(&channel_path(id), &state, &[]);
    let promote_url = href(&format!("{}/promote", channel_path(id)), &state, &[]);
    let mut inbox_state = state.clone();
    inbox_state.scope.filter.channels = vec![id];
    let inbox_url = href("/alerts", &inbox_state, &[]);
    let subject = subject_code(AuditSubject::Channel(id));
    let audit_url = href("/audit", &state, &[("subject", &subject), ("span", "all")]);

    Ok(view! {
        if let Some(status) = failed_status {
            (status)
        }
        <div class="mb-1 text-xs text-zinc-500">
            <a class=(LINK) href=(list_url)>"Channels"</a>
            " / "
            <span class="font-mono">(short_id(id.to_ulid()))</span>
        </div>
        match loaded {
            Err(error) => {
                (status_of(&error))
                page_header(title: "Channel", subtitle: "")
                if let Some(failed) = any_error {
                    <div class="mb-4">error_panel(error: &failed)</div>
                }
                error_panel(error: &error)
            },
            Ok(None) => {
                (StatusCode::NOT_FOUND)
                page_header(title: "Channel not found", subtitle: "")
                if let Some(failed) = any_error {
                    <div class="mb-4">error_panel(error: &failed)</div>
                }
                empty_state(message: "No channel has this id. It may have been mistyped.")
            },
            Ok(Some(loaded)) => {
                let header = loaded.header;
                let abilities = loaded.abilities;
                let top_error = general.or(if abilities.set_policy { None } else { policy_error.clone() });
                let last_transmission = header
                    .detection_detail
                    .last_transmission
                    .map(|t| transmission_url(t, &state));
                <header class="mb-4">
                    <h1 class="break-all font-mono text-base font-semibold">(header.title)</h1>
                    <p class="font-mono text-xs text-zinc-500">(header.id)</p>
                    <div class="mt-2 flex flex-wrap items-center gap-1.5">
                        kind_badge(value: header.origin)
                        kind_badge(value: header.detection)
                        kind_badge(value: header.policy)
                    </div>
                    <p class="mt-2 text-sm text-zinc-600 dark:text-zinc-400">
                        (header.detection_detail.text)
                        if let Some(url) = last_transmission {
                            " "
                            <a class=(LINK) href=(url)>"Latest transmission"</a>
                        }
                    </p>
                    <dl class="mt-2 flex flex-wrap gap-x-6 gap-y-1 text-xs text-zinc-500">
                        <div><dt class="inline">"writers "</dt><dd class="inline tabular-nums text-zinc-800 dark:text-zinc-200">(header.writers)</dd></div>
                        <div><dt class="inline">"readers "</dt><dd class="inline tabular-nums text-zinc-800 dark:text-zinc-200">(header.readers)</dd></div>
                        <div><dt class="inline">"transmissions "</dt><dd class="inline tabular-nums text-zinc-800 dark:text-zinc-200">(header.transmissions)</dd></div>
                        <div><dt class="inline">"last activity "</dt><dd class="inline text-zinc-800 dark:text-zinc-200">(header.last_activity)</dd></div>
                    </dl>
                </header>
                if let Some((url, label, by, at)) = header.superseded {
                    <div class="mb-4 rounded border border-amber-300 bg-amber-50 px-3 py-2 text-sm text-amber-900 dark:border-amber-800 dark:bg-amber-950 dark:text-amber-100">
                        "Superseded by "
                        <a class="font-medium underline" href=(url)>(label)</a>
                        ", promoted by " (by) " at " (at) ". "
                        "This channel takes no new resources; its traffic is counted on the declared channel."
                    </div>
                }
                if let Some(flash) = flash {
                    flash_banner(message: flash.message())
                }
                if let Some(error) = top_error {
                    <div class="mb-4">error_panel(error: &error)</div>
                }
                <section class=(SECTION)>
                    <h2 class=(SECTION_TITLE)>"Policy"</h2>
                    <div class=(PANEL)>
                        <div class="flex flex-wrap items-center gap-2 text-sm">
                            kind_badge(value: header.policy)
                            match header.decision {
                                Some((by, at, note)) => {
                                    <span class="text-zinc-600 dark:text-zinc-400">"decided by " (by) " at " (at)</span>
                                    if let Some(note) = note {
                                        <p class="w-full text-sm italic text-zinc-700 dark:text-zinc-300">"\u{201c}" (note) "\u{201d}"</p>
                                    }
                                },
                                None => <span class="text-zinc-500">"Never reviewed."</span>,
                            }
                        </div>
                        if abilities.set_policy {
                            <div class="mt-3 border-t border-zinc-200 pt-3 dark:border-zinc-800">
                                policy_form(
                                    action: action_url,
                                    current: header.policy,
                                    retained: policy_fields,
                                    error: policy_error,
                                )
                            </div>
                        }
                        if abilities.promote {
                            <div class="mt-3 flex items-center gap-3 border-t border-zinc-200 pt-3 text-sm dark:border-zinc-800">
                                <a class=(BUTTON) href=(promote_url)>"Promote to declared channel…"</a>
                                <span class="text-xs text-zinc-500">"Declare a pattern for this resource and take over the discovered channels it covers."</span>
                            </div>
                        }
                    </div>
                </section>
                resources_section(rows: loaded.resources)
                alerts_section(rows: loaded.alerts, inbox_url: inbox_url)
                history_section(rows: loaded.history, audit_url: audit_url)
            },
        }
    })
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::derived::flow::channel::policy::{Decision, Policy, PolicyAuthor};
    use crosstalk_spec::ids::OperatorId;
    use crosstalk_spec::support::Timestamp;
    use topcoat::router::StatusCode;

    use super::*;
    use crate::components::href::tests::state;
    use crate::contract::channels::Supersession;
    use crate::pages::channels::model::tests::discovered;
    use crate::testing::{get, post};

    fn caller(permissions: Vec<Permission>) -> Caller {
        crate::testing::caller_of(OperatorId::from_ulid(1), &permissions)
    }

    #[test]
    fn header_shows_decision_and_supersession() {
        let mut summary = discovered(1);
        summary.channel.policy = Policy::Sanctioned(Decision {
            by: PolicyAuthor::Operator(OperatorId::from_ulid(3)),
            at: Timestamp::from_micros(1_790_985_600_000_000),
            note: Some("team wiki".into()),
        });
        summary.superseded = Some(Supersession {
            into: ChannelId::from_ulid(2),
            by: OperatorId::from_ulid(3),
            at: Timestamp::from_micros(1_790_985_600_000_000),
        });
        let operators = OperatorNames::new([(OperatorId::from_ulid(3), "ada".to_owned())]);
        let header = header(&summary, &operators, &state());
        assert_eq!(
            header.decision,
            Some((
                "ada".to_owned(),
                "2026-10-03 00:00:00 UTC".to_owned(),
                Some("team wiki".to_owned())
            ))
        );
        let (url, label, by, _) = header.superseded.clone().expect("superseded");
        assert!(url.starts_with("/channels/00000000000000000000000002?"));
        assert_eq!((label.as_str(), by.as_str()), ("channel …000002", "ada"));
        let abilities = abilities(&caller(vec![Permission::View, Permission::Govern]), &header);
        assert_eq!(
            abilities,
            Abilities {
                set_policy: false,
                promote: false
            },
            "a superseded channel takes no actions"
        );
    }

    #[test]
    fn actions_need_govern() {
        let header = header(&discovered(1), &OperatorNames::default(), &state());
        assert_eq!(
            abilities(&caller(vec![Permission::View]), &header),
            Abilities {
                set_policy: false,
                promote: false
            }
        );
        assert_eq!(
            abilities(&caller(vec![Permission::View, Permission::Govern]), &header),
            Abilities {
                set_policy: true,
                promote: true
            }
        );
    }

    const ID: &str = "01J9ZQ3W8D0000000000000001";

    #[tokio::test]
    async fn unknown_channel_is_not_found() {
        let reply = get(&format!("/channels/{ID}?{}", state().to_query())).await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND);
        assert!(reply.body.contains("Channel not found"));
        let reply = get(&format!("/channels/not-a-ulid?{}", state().to_query())).await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn invalid_policy_posts_are_rejected_before_the_backend() {
        let url = format!("/channels/{ID}?{}", state().to_query());
        let reply = post(&url, "action=set-policy&policy=allow").await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            reply.body.contains("policy: unknown policy"),
            "{}",
            reply.body
        );
        let reply = post(&url, "action=launch").await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(reply.body.contains("action: unknown action"));
    }

    #[tokio::test]
    async fn valid_posts_reach_the_backend() {
        let url = format!("/channels/{ID}?{}", state().to_query());
        let reply = post(&url, "action=set-policy&policy=sanctioned&note=ok").await;
        // The stub backend knows no channel, so the action is not found.
        assert_eq!(reply.status, StatusCode::NOT_FOUND);
        assert!(reply.body.contains("not found"));
    }

    #[tokio::test]
    async fn resources_name_their_agents_from_one_lookup() {
        use crate::backend::fixture::ChannelKey;
        use crate::testing::{channel_id, operator, world};

        let wiki = channel_id(ChannelKey::HijackedWiki);
        let c = operator().caller();
        let uses = world()
            .channel_resources(&c, wiki, state().scope.window)
            .await
            .expect("resources");
        let ids: Vec<_> = uses
            .iter()
            .flat_map(|u| u.writers.iter().chain(u.readers.iter()).map(|(a, _)| *a))
            .collect();
        let names = world().agent_names(&c, &ids).await.expect("names");
        let label = names
            .values()
            .find_map(|n| n.label.clone())
            .expect("a labelled agent uses the wiki");
        let reply = get(&format!(
            "/channels/{}?{}",
            wiki.to_ulid(),
            state().to_query()
        ))
        .await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert!(reply.body.contains(label.as_str()), "{label:?}");
    }
}
