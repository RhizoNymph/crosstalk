//! `/channels/{id}`: one channel's origin, detection, confirmation, policy,
//! suspected transmissions, resources, alerts and policy history, counted
//! in the view's window. An unconfirmed, declared-only or hidden channel
//! says so in a banner. Posting `set-policy` changes its policy; posting
//! `set-verdict` records a verdict on one of its suspected transmissions.

use crosstalk_spec::aggregates::node::CanonicalOriginKind;
use crosstalk_spec::derived::flow::channel::ChannelOrigin;
use crosstalk_spec::derived::flow::channel::confirmation::{CrossTraffic, Listing};
use crosstalk_spec::derived::flow::channel::detection::DetectionKind;
use crosstalk_spec::ids::ChannelId;
use crosstalk_spec::interfaces::l8_surface::channels::ChannelRow;
use crosstalk_spec::interfaces::l8_surface::{AlertFilter, Caller, Permission, PolicyKind};
use crosstalk_spec::paging::ResourceUseList;
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::content::Form;
use topcoat::router::error::not_found;
use topcoat::router::{StatusCode, page, path_param};
use topcoat::view::{View, component, view};

use super::list::Activity;
use super::model::{
    DetectionDetail, detection_detail, listing_text, origin_kind, origin_text, title,
};
use super::policy::{self, policy_form};
use super::sections::{
    PolicyRow, Resources, alerts_section, policy_history_section, policy_rows, resource_rows,
    resources_section,
};
use super::suspected::{self, Suspected, suspected_section};
use crate::app::{backend, caller, can};
use crate::components::form::{BUTTON, LINK, PANEL, SECTION, SECTION_TITLE};
use crate::components::live::{live_watch, watch_one};
use crate::components::{
    PageLinks, empty_state, error_panel, flash_banner, format_time, href, kind_badge, page_header,
    short_id,
};
use crate::error::UiError;
use crate::pages::alerts::model::AlertRow;
use crate::pages::audit::subject::subject_code;
use crate::pages::common::action::{
    Failure, done, error_for, fields_for, general_error, perform, require, settled, status_of,
};
use crate::pages::common::flash::{Flash, flash};
use crate::pages::common::form::{FormFields, id as form_id, invalid};
use crate::pages::common::links::{channel_url, transmission_url};
use crate::pages::common::lookup::{OperatorNames, agent_names, operator_names};
use crate::pages::common::paging::{PAGE_SIZE, page_request};
use crate::pages::common::rules::rule_names;
use crate::pages::common::transmissions::ChannelNames;
use crate::pages::transmission::verdict;
use crate::pages::view::view_state;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;
use crosstalk_spec::aggregates::alert::Alert;
use crosstalk_spec::interfaces::l8_surface::QueryApi;
use crosstalk_spec::interfaces::l8_surface::audit::AuditSubject;

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
    /// A verdict button of the suspected transmissions.
    Verdict,
}

/// The channel in force that superseded this one, as its banner shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupersededBanner {
    pub url: String,
    pub name: String,
    pub by: String,
    pub at: String,
}

/// Everything above the sections, owned and display-ready.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub title: String,
    pub id: String,
    pub origin: CanonicalOriginKind,
    pub origin_text: &'static str,
    /// Only a discovered channel in force can be promoted.
    pub discovered: bool,
    pub detection: DetectionKind,
    pub detection_detail: DetectionDetail,
    pub policy: PolicyKind,
    /// Who decided the policy, when, and their note.
    pub decision: Option<(String, String, Option<String>)>,
    pub superseded: Option<SupersededBanner>,
    /// Where the channel is listed; `None` when superseded.
    pub listing: Option<Listing>,
    /// Cross-agent transmissions over all time; `None` when superseded.
    pub traffic: Option<CrossTraffic>,
    /// Counted in the view's window.
    pub activity: Activity,
}

impl Header {
    /// What the banner under the header says about the listing, if
    /// anything.
    pub fn listing_banner(&self) -> Option<&'static str> {
        self.listing.and_then(listing_text)
    }

    /// Whether the page lists suspected transmissions: in force with
    /// unconfirmed cross-agent traffic.
    pub fn has_suspected(&self) -> bool {
        self.traffic.is_some_and(|traffic| traffic.unconfirmed > 0)
    }
}

/// The header of `row`. `channels` names the channel in force a
/// superseded one resolves to.
pub fn header(
    row: &ChannelRow,
    operators: &OperatorNames,
    channels: &ChannelNames,
    state: &ViewState,
) -> Header {
    let channel = row.channel();
    Header {
        title: title(row),
        id: channel.id.to_ulid(),
        origin: origin_kind(&channel.origin),
        origin_text: origin_text(&channel.origin),
        discovered: matches!(channel.origin, ChannelOrigin::Discovered { .. }),
        detection: channel.origin.detection_kind(),
        detection_detail: detection_detail(&channel.origin),
        policy: channel.policy.kind(),
        decision: channel.policy.decision().map(|d| {
            (
                operators.policy_author(d.by),
                format_time(d.at),
                d.note.clone(),
            )
        }),
        superseded: row.supersession().map(|s| SupersededBanner {
            url: channel_url(s.into(), state),
            name: channels.name(s.into()),
            by: operators.name(s.by()),
            at: format_time(s.at()),
        }),
        listing: row.listing(),
        traffic: row.traffic(),
        activity: Activity::of(row),
    }
}

/// What the caller may do here. A superseded channel takes no policy and
/// cannot be promoted; only discovered channels are promoted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Abilities {
    pub set_policy: bool,
    pub promote: bool,
    /// Record verdicts on its suspected transmissions.
    pub judge: bool,
}

pub fn abilities(caller: &Caller, header: &Header) -> Abilities {
    let govern = can(caller, Permission::Govern);
    let live = header.superseded.is_none();
    Abilities {
        set_policy: govern && live,
        promote: govern && live && header.discovered,
        judge: can(caller, Permission::Triage),
    }
}

struct Loaded {
    header: Header,
    abilities: Abilities,
    resources: std::result::Result<Resources, UiError>,
    /// `None` when it has no unconfirmed traffic.
    suspected: Option<std::result::Result<Suspected, UiError>>,
    alerts: std::result::Result<Vec<AlertRow>, UiError>,
    history: std::result::Result<Vec<PolicyRow>, UiError>,
}

/// One page of the channel's resources in the view's window (the page's
/// `cursor` key), its agents named in one lookup.
async fn resources(
    cx: &Cx,
    caller: &Caller,
    id: ChannelId,
    state: &ViewState,
) -> std::result::Result<Resources, UiError> {
    let request = page_request::<ResourceUseList>(cx)?;
    let page = backend(cx)
        .channel_resources(caller, id, state.scope.window, &request)
        .await?
        .value;
    let uses = page.page.items();
    let agents = uses.iter().flat_map(|u| {
        u.writers()
            .iter()
            .chain(u.readers())
            .map(|entry| entry.agent)
    });
    let names = agent_names(cx, caller, agents.collect::<Vec<_>>()).await;
    Ok(Resources {
        rows: resource_rows(uses, &names, state),
        links: PageLinks::new(
            &channel_path(id),
            state,
            &[],
            request.after.as_ref(),
            page.page.next(),
        ),
        resolved_to: (page.channel != id).then(|| channel_url(page.channel, state)),
    })
}

async fn load(
    cx: &Cx,
    caller: &Caller,
    id: ChannelId,
    state: &ViewState,
) -> std::result::Result<Option<Loaded>, UiError> {
    require(caller, Permission::View)?;
    let backend = backend(cx);
    let Some(row) = backend
        .channel(caller, id, Some(state.scope.window))
        .await?
    else {
        return Ok(None);
    };
    let row = row.value;
    let operators = operator_names(cx, caller).await;
    let in_force = row.supersession().map(|s| s.into());
    let channels = crate::pages::common::transmissions::channel_names(cx, caller, in_force).await;
    let header = header(&row, &operators, &channels, state);
    let resources = resources(cx, caller, id, state).await;
    let suspected = if header.has_suspected() {
        Some(suspected::load(cx, caller, id, &channel_path(id), state).await)
    } else {
        None
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

    let history = backend
        .policy_history(caller, id)
        .await
        .map_err(UiError::from)
        .map(|history| history.map_or_else(Vec::new, |history| policy_rows(&history, &operators)));

    let abilities = abilities(caller, &header);
    Ok(Some(Loaded {
        header,
        abilities,
        resources,
        suspected,
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
                Ok(outcome) => {
                    let flash = settled(&outcome, Flash::PolicySet);
                    return Err(done(&channel_path(id), &state, &[], flash));
                }
                Err(error) => Failure::new(Some(ChannelForm::Policy), error, fields),
            }
        }
        Some("set-verdict") => {
            let result = match form_id(&fields, "transmission")
                .and_then(|transmission| verdict::parse(transmission, &fields))
            {
                Ok((action, flash)) => perform(cx, action).await.map(|outcome| (outcome, flash)),
                Err(error) => Err(error),
            };
            match result {
                Ok((outcome, flash)) => {
                    let flash = settled(&outcome, flash);
                    return Err(done(&channel_path(id), &state, &[], flash));
                }
                Err(error) => Failure::new(Some(ChannelForm::Verdict), error, fields),
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
    let verdict_error = error_for(failure.as_ref(), ChannelForm::Verdict);
    let policy_fields = fields_for(failure.as_ref(), ChannelForm::Policy);
    let list_url = href("/channels", &state, &[]);
    let action_url = href(&channel_path(id), &state, &[]);
    let promote_url = href(&format!("{}/promote", channel_path(id)), &state, &[]);
    let mut inbox_state = state.clone();
    inbox_state.scope.filter.channels = vec![id];
    let inbox_url = href("/alerts", &inbox_state, &[]);
    let subject = subject_code(AuditSubject::Channel(id));
    let audit_url = href("/audit", &state, &[("subject", &subject), ("span", "all")]);

    // A promotion names every channel it superseded, so this id is enough;
    // the page also lists alerts about the channel.
    // A merge can hide the channel; a verdict changes a suspected row.
    let watch = format!("{} alert agent verdict", watch_one("channel", id));
    Ok(view! {
        if let Some(status) = failed_status {
            (status)
        }
        live_watch(tokens: watch)
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
                let top_error = general
                    .or(if abilities.set_policy { None } else { policy_error.clone() })
                    .or(verdict_error.clone());
                let listing_banner = header.listing_banner();
                let verdict_action = abilities.judge.then(|| action_url.clone());
                let last_transmission = header
                    .detection_detail
                    .last_transmission
                    .map(|t| transmission_url(t, &state));
                let [writers, readers, transmissions] = header.activity.cells();
                let last_activity = header.activity.last();
                <header class="mb-4">
                    <h1 class="break-all font-mono text-base font-semibold">(header.title)</h1>
                    <p class="font-mono text-xs text-zinc-500">(header.id)</p>
                    <div class="mt-2 flex flex-wrap items-center gap-1.5">
                        kind_badge(value: header.origin)
                        kind_badge(value: header.detection)
                        if let Some(listing) = header.listing {
                            kind_badge(value: listing)
                        }
                        kind_badge(value: header.policy)
                    </div>
                    <p class="mt-2 text-sm text-zinc-600 dark:text-zinc-400">
                        (header.origin_text)
                        " "
                        (header.detection_detail.text)
                        if let Some(url) = last_transmission {
                            " "
                            <a class=(LINK) href=(url)>"Latest transmission"</a>
                        }
                    </p>
                    <dl class="mt-2 flex flex-wrap gap-x-6 gap-y-1 text-xs text-zinc-500">
                        <div><dt class="inline">"writers "</dt><dd class="inline tabular-nums text-zinc-800 dark:text-zinc-200">(writers)</dd></div>
                        <div><dt class="inline">"readers "</dt><dd class="inline tabular-nums text-zinc-800 dark:text-zinc-200">(readers)</dd></div>
                        <div><dt class="inline">"transmissions "</dt><dd class="inline tabular-nums text-zinc-800 dark:text-zinc-200">(transmissions)</dd></div>
                        <div><dt class="inline">"last activity "</dt><dd class="inline text-zinc-800 dark:text-zinc-200">(last_activity)</dd></div>
                    </dl>
                </header>
                if let Some(banner) = header.superseded {
                    <div class="mb-4 rounded border border-amber-300 bg-amber-50 px-3 py-2 text-sm text-amber-900 dark:border-amber-800 dark:bg-amber-950 dark:text-amber-100">
                        "Superseded by "
                        <a class="font-medium underline" href=(banner.url)>(banner.name)</a>
                        ", promoted by " (banner.by) " at " (banner.at) ". "
                        "This channel takes no new resources; its traffic and resources are counted on the channel in force."
                    </div>
                }
                if let Some(text) = listing_banner {
                    <div class="mb-4 rounded border border-dashed border-amber-300 bg-amber-50 px-3 py-2 text-sm text-amber-900 dark:border-amber-800 dark:bg-amber-950 dark:text-amber-100">
                        (text)
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
                if let Some(rows) = loaded.suspected {
                    suspected_section(rows: rows, verdict_action: verdict_action)
                }
                resources_section(resources: loaded.resources)
                alerts_section(rows: loaded.alerts, inbox_url: inbox_url)
                policy_history_section(rows: loaded.history, audit_url: audit_url)
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
    use crate::pages::channels::model::tests::{discovered, superseded, with_policy};
    use crate::testing::{get, post};

    fn caller(permissions: Vec<Permission>) -> Caller {
        crate::testing::caller_of(OperatorId::from_ulid(1), &permissions)
    }

    #[test]
    fn header_shows_decision_and_supersession() {
        let decided = with_policy(
            1,
            Policy::Sanctioned(Decision {
                by: PolicyAuthor::Operator(OperatorId::from_ulid(3)),
                at: Timestamp::from_micros(1_790_985_600_000_000),
                note: Some("team wiki".into()),
            }),
        );
        let operators = OperatorNames::new([(OperatorId::from_ulid(3), "ada".to_owned())]);
        let names = ChannelNames::default();
        let shown = header(&decided, &operators, &names, &state());
        assert_eq!(
            shown.decision,
            Some((
                "ada".to_owned(),
                "2026-10-03 00:00:00 UTC".to_owned(),
                Some("team wiki".to_owned())
            ))
        );
        let names =
            ChannelNames::from_pairs([(ChannelId::from_ulid(2), "wiki.example.org/*".to_owned())]);
        let header = header(&superseded(1, 2), &operators, &names, &state());
        let banner = header.superseded.clone().expect("superseded");
        assert!(
            banner
                .url
                .starts_with("/channels/00000000000000000000000002?")
        );
        assert_eq!(
            (banner.name.as_str(), banner.by.as_str()),
            ("wiki.example.org/*", "ada")
        );
        assert_eq!(header.activity, Activity::Superseded);
        let abilities = abilities(&caller(vec![Permission::View, Permission::Govern]), &header);
        assert_eq!(
            abilities,
            Abilities {
                set_policy: false,
                promote: false,
                judge: false,
            },
            "a superseded channel takes no actions"
        );
    }

    #[test]
    fn actions_need_govern() {
        let header = header(
            &discovered(1),
            &OperatorNames::default(),
            &ChannelNames::default(),
            &state(),
        );
        assert_eq!(
            abilities(&caller(vec![Permission::View]), &header),
            Abilities {
                set_policy: false,
                promote: false,
                judge: false,
            }
        );
        assert_eq!(
            abilities(&caller(vec![Permission::View, Permission::Govern]), &header),
            Abilities {
                set_policy: true,
                promote: true,
                judge: false,
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
            .channel_resources(
                &c,
                wiki,
                state().scope.window,
                &crate::pages::common::paging::first(PAGE_SIZE),
            )
            .await
            .expect("resources")
            .value
            .page;
        let ids: Vec<_> = uses
            .items()
            .iter()
            .flat_map(|u| u.writers().iter().chain(u.readers()).map(|e| e.agent))
            .collect();
        let batch = crosstalk_spec::batch::IdBatch::new(ids).expect("one batch");
        let names = world().agent_names(&c, &batch).await.expect("names");
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

    #[tokio::test]
    async fn an_unconfirmed_channel_lists_its_suspected_transmissions_for_review() {
        use crate::backend::fixture::ChannelKey;
        use crate::pages::topology::tests::fixture_state;
        use crate::testing::channel_id;

        let s3 = channel_id(ChannelKey::S3Handoff);
        let url = format!("/channels/{}?{}", s3.to_ulid(), fixture_state().to_query());
        let reply = get(&url).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert!(
            reply
                .body
                .contains("Unconfirmed: every transmission between agents")
        );
        assert!(reply.body.contains("Suspected transmissions"));
        assert!(
            reply.body.contains("value=\"set-verdict\""),
            "verdict buttons"
        );
        assert!(reply.body.contains("False detection"));
        assert!(
            reply.body.contains("/transmissions/"),
            "each links its evidence"
        );
    }

    #[tokio::test]
    async fn a_verdict_posted_from_the_channel_page_is_recorded() {
        use crate::backend::fixture::ChannelKey;
        use crate::pages::topology::tests::fixture_state;
        use crate::testing::{Session, channel_id, operator, world};
        use crosstalk_spec::aggregates::filter::TopicVersionSelector;
        use crosstalk_spec::derived::flow::channel::confirmation::Confirmation;
        use crosstalk_spec::interfaces::l8_surface::channel_traffic::ChannelTransmissionFilter;
        use crosstalk_spec::interfaces::l8_surface::summary::TransmissionStateKind;

        let s3 = channel_id(ChannelKey::S3Handoff);
        let page = world()
            .channel_transmissions(
                &operator().caller(),
                s3,
                &ChannelTransmissionFilter {
                    confirmation: Some(Confirmation::Unconfirmed),
                },
                TopicVersionSelector::Current,
                &crate::pages::common::paging::first(PAGE_SIZE),
            )
            .await
            .expect("suspected");
        let judgeable = page
            .page
            .items()
            .iter()
            .find(|row| row.summary().state.kind() == TransmissionStateKind::Suspected)
            .expect("a suspected transmission")
            .summary()
            .id;
        let session = Session::new();
        let url = format!("/channels/{}?{}", s3.to_ulid(), fixture_state().to_query());
        let reply = session
            .post(
                &url,
                &format!(
                    "action=set-verdict&transmission={}&verdict=false-detection",
                    judgeable.to_ulid()
                ),
            )
            .await;
        assert_eq!(reply.status, StatusCode::SEE_OTHER, "{}", reply.body);
        let location = reply.location.expect("redirect");
        assert!(location.contains("flash=verdict-recorded"), "{location}");
        let shown = session.get(&location).await;
        assert!(shown.body.contains("Verdict recorded."));
        assert!(shown.body.contains("false detection"));
        let bad = session
            .post(&url, "action=set-verdict&transmission=nope&verdict=genuine")
            .await;
        assert_eq!(bad.status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn hidden_and_declared_channels_say_why() {
        use crate::backend::fixture::ChannelKey;
        use crate::pages::topology::tests::fixture_state;
        use crate::testing::channel_id;

        let page =
            |id: ChannelId| format!("/channels/{}?{}", id.to_ulid(), fixture_state().to_query());
        let hidden = get(&page(channel_id(ChannelKey::SelfNotes))).await;
        assert_eq!(hidden.status, StatusCode::OK, "{}", hidden.body);
        assert!(
            hidden
                .body
                .contains("Hidden: every transmission through this channel")
        );
        let declared = get(&page(channel_id(ChannelKey::DesignDocs))).await;
        assert_eq!(declared.status, StatusCode::OK);
        assert!(declared.body.contains("Declared, no traffic yet:"));
        assert!(!declared.body.contains("Suspected transmissions"));
        let wiki = get(&page(channel_id(ChannelKey::HijackedWiki))).await;
        assert!(!wiki.body.contains("Unconfirmed:"));
    }

    #[tokio::test]
    async fn a_superseded_channel_names_its_channel_in_force_and_histories_show() {
        use crate::backend::fixture::ChannelKey;
        use crate::testing::channel_id;

        let old = channel_id(ChannelKey::OldTeamNotes);
        let notes = channel_id(ChannelKey::TeamNotes);
        let page = |id: ChannelId| format!("/channels/{}?{}", id.to_ulid(), state().to_query());
        let reply = get(&page(old)).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert!(reply.body.contains("Superseded by"));
        assert!(
            reply.body.contains("notes.corp.internal/team-a*"),
            "the banner names the channel in force"
        );
        assert!(reply.body.contains(&notes.to_ulid()));
        assert!(
            !reply.body.contains("Set policy"),
            "a superseded channel takes no policy"
        );
        let reply = get(&page(notes)).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert!(reply.body.contains("promoted"));
        assert!(
            reply
                .body
                .contains("team notes are an approved handoff space")
        );
        let reply = get(&page(channel_id(ChannelKey::McpMemory))).await;
        assert!(reply.body.contains("reset to unreviewed"));
        assert!(
            reply.body.contains("internal memory server"),
            "older decisions stay"
        );
    }
}
