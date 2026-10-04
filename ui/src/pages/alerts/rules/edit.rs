//! `/alerts/rules/new?kind=…` and `/alerts/rules/{id}`: create an operator
//! rule, or edit one. Updating a stale rule re-targets it to the current
//! topic version and enables it.

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::AlertRuleId;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission};
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::content::Form;
use topcoat::router::error::not_found;
use topcoat::router::{page, path_param, query_params};
use topcoat::view::{View, component, view};

use super::PATH;
use super::form::{
    Choices, RuleKindChoice, TopicOption, Values, parse_semantic, parse_watched, rule_form,
};
use super::model::{StatusKind, staleness};
use crate::app::{backend, caller, can};
use crate::backend::Backend;
use crate::components::form::LINK;
use crate::components::{error_panel, href, page_header, state_badge};
use crate::contract::actions::OperatorAction;
use crate::contract::errors::{ConflictKind, QueryError};
use crate::contract::rules::{RuleDef, RuleKind, RuleStatus, UserRule};
use crate::pages::common::action::{Failure, done, perform, require, status_of};
use crate::pages::common::flash::Flash;
use crate::pages::common::form::FormFields;
use crate::pages::common::links::rule_url;
use crate::pages::view::view_state;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;

path_param!(rule_ulid);

#[query_params]
struct NewQuery {
    kind: Option<String>,
}

/// What the page edits.
#[derive(Debug, Clone, PartialEq)]
pub enum Target {
    New(RuleKindChoice),
    Existing(AlertRuleId),
}

/// The topic version to pick topics from: the newest fitted one.
async fn current_version(
    cx: &Cx,
    caller: &Caller,
) -> std::result::Result<TopicModelVersion, QueryError> {
    let versions = backend(cx).topic_versions(caller).await?;
    Ok(versions
        .iter()
        .map(|v| v.version)
        .max_by_key(|v| v.0)
        .unwrap_or_else(|| backend(cx).current_topic_version()))
}

/// The topics and sinks a form may pick. Topics need `Content`: their
/// labels come from message text.
struct Options {
    choices: Choices,
    topics: Vec<TopicOption>,
    sinks: Vec<(String, String)>,
}

async fn options(cx: &Cx, caller: &Caller) -> std::result::Result<Options, QueryError> {
    let version = current_version(cx, caller).await?;
    let sinks = backend(cx).sinks(caller).await?;
    let topics = if can(caller, Permission::Content) {
        backend(cx).topics(caller, version).await?
    } else {
        Vec::new()
    };
    Ok(Options {
        choices: Choices {
            version,
            topics: topics.iter().map(|t| t.id).collect(),
            sinks: sinks.iter().map(|s| s.id).collect(),
        },
        topics: topics
            .into_iter()
            .map(|t| TopicOption {
                id: t.id.to_ulid(),
                label: Some(t.label),
            })
            .collect(),
        sinks: sinks
            .into_iter()
            .map(|s| (s.id.to_ulid(), s.name))
            .collect(),
    })
}

/// The rule being edited; built-in rules are not edited here.
async fn existing(
    cx: &Cx,
    caller: &Caller,
    id: AlertRuleId,
) -> std::result::Result<RuleDef, QueryError> {
    let rule = backend(cx)
        .rules(caller)
        .await?
        .into_iter()
        .find(|r| r.id == id)
        .ok_or(QueryError::NotFound)?;
    if matches!(rule.rule, RuleKind::Builtin(_)) {
        return Err(QueryError::Conflict(ConflictKind::BuiltinRule));
    }
    Ok(rule)
}

fn kind_of(rule: &RuleDef) -> RuleKindChoice {
    match rule.rule {
        RuleKind::User(UserRule::SemanticQuery { .. }) => RuleKindChoice::Semantic,
        _ => RuleKindChoice::Watched,
    }
}

/// The form's values for an existing rule. Topics are kept only when the
/// rule is on the version the form offers.
pub fn values_of(rule: &RuleDef, version: TopicModelVersion) -> Values {
    let sinks = rule.sinks.iter().map(|s| s.to_ulid()).collect();
    match &rule.rule {
        RuleKind::User(UserRule::WatchedTopic {
            version: rule_version,
            topics,
            remap_threshold,
        }) => Values {
            name: rule.name.as_str().to_owned(),
            topics: if *rule_version == version {
                topics.iter().map(|t| t.to_ulid()).collect()
            } else {
                Vec::new()
            },
            threshold: format!("{:.2}", remap_threshold.get()),
            text: String::new(),
            sinks,
        },
        RuleKind::User(UserRule::SemanticQuery {
            text, threshold, ..
        }) => Values {
            name: rule.name.as_str().to_owned(),
            topics: Vec::new(),
            threshold: format!("{:.2}", threshold.get()),
            text: text.clone(),
            sinks,
        },
        RuleKind::Builtin(_) => Values::default(),
    }
}

/// Runs a post: validates against the current options and creates or
/// updates the rule.
async fn submit(
    cx: &Cx,
    target: &Target,
    fields: &FormFields,
) -> std::result::Result<Flash, QueryError> {
    let caller = caller(cx);
    require(&caller, Permission::Govern)?;
    let (kind, id) = match target {
        Target::New(kind) => (*kind, None),
        Target::Existing(id) => (kind_of(&existing(cx, &caller, *id).await?), Some(*id)),
    };
    let options = options(cx, &caller).await?;
    let (name, rule, sinks) = match kind {
        RuleKindChoice::Semantic => return Err(parse_semantic(fields, &options.choices.sinks)),
        RuleKindChoice::Watched => {
            require(&caller, Permission::Content)?;
            parse_watched(fields, &options.choices)?
        }
    };
    let (action, flash) = match id {
        None => (
            OperatorAction::CreateRule { name, rule, sinks },
            Flash::RuleCreated,
        ),
        Some(id) => (
            OperatorAction::UpdateRule {
                id,
                name,
                rule,
                sinks,
            },
            Flash::RuleUpdated,
        ),
    };
    perform(cx, action).await?;
    Ok(flash)
}

#[page("/alerts/rules/new")]
async fn new_rule_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx)?;
    let kind = query_params::<NewQuery>(cx)
        .ok()
        .and_then(|q| q.kind.clone());
    let target = RuleKindChoice::parse(kind.as_deref()).map(Target::New);
    Ok(view! { rule_page(state: state, target: target, failure: None) })
}

#[page(POST "/alerts/rules/new")]
async fn new_rule_post(cx: &Cx, Form(fields): Form<FormFields>) -> Result<impl View> {
    let state = view_state(cx)?;
    let target = RuleKindChoice::parse(fields.text("kind")).map(Target::New);
    let error = match &target {
        Ok(target) => match submit(cx, target, &fields).await {
            Ok(flash) => return Err(done(PATH, &state, &[], flash)),
            Err(error) => error,
        },
        Err(error) => error.clone(),
    };
    let failure = Failure::new(None, error, fields);
    Ok(view! { rule_page(state: state, target: target, failure: Some(failure)) })
}

fn rule_id(cx: &Cx) -> Result<AlertRuleId> {
    AlertRuleId::parse_ulid(path_param::<RuleUlid>(cx)).map_err(|_| not_found().into())
}

#[page("/alerts/rules/{rule_ulid}")]
async fn rule_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx)?;
    let id = rule_id(cx)?;
    Ok(view! { rule_page(state: state, target: Ok(Target::Existing(id)), failure: None) })
}

#[page(POST "/alerts/rules/{rule_ulid}")]
async fn rule_post(cx: &Cx, Form(fields): Form<FormFields>) -> Result<impl View> {
    let state = view_state(cx)?;
    let id = rule_id(cx)?;
    let target = Target::Existing(id);
    let error = match submit(cx, &target, &fields).await {
        Ok(flash) => return Err(done(PATH, &state, &[], flash)),
        Err(error) => error,
    };
    let failure = Failure::new(None, error, fields);
    Ok(view! { rule_page(state: state, target: Ok(target), failure: Some(failure)) })
}

/// Everything the form page shows.
struct Editor {
    title: String,
    kind: RuleKindChoice,
    action: String,
    values: Values,
    options: Options,
    /// The edited rule's status, and why it is stale.
    status: Option<(StatusKind, Option<String>)>,
}

async fn editor(
    cx: &Cx,
    caller: &Caller,
    target: &Target,
    state: &ViewState,
    failed: Option<&FormFields>,
) -> std::result::Result<Editor, QueryError> {
    require(caller, Permission::View)?;
    require(caller, Permission::Govern)?;
    let options = options(cx, caller).await?;
    let (title, kind, action, values, status) = match target {
        Target::New(kind) => (
            match kind {
                RuleKindChoice::Watched => "New watched-topic rule".to_owned(),
                RuleKindChoice::Semantic => "New semantic query rule".to_owned(),
            },
            *kind,
            href(&format!("{PATH}/new"), state, &[]),
            Values::defaults(*kind),
            None,
        ),
        Target::Existing(id) => {
            let rule = existing(cx, caller, *id).await?;
            let status = match &rule.status {
                RuleStatus::Enabled => (StatusKind::Enabled, None),
                RuleStatus::Disabled => (StatusKind::Disabled, None),
                RuleStatus::Stale(reason) => {
                    (StatusKind::Stale, Some(staleness(reason, &None).reason))
                }
            };
            (
                format!("Edit \u{201c}{}\u{201d}", rule.name.as_str()),
                kind_of(&rule),
                rule_url(*id, state),
                values_of(&rule, options.choices.version),
                Some(status),
            )
        }
    };
    let values = match failed {
        Some(fields) => Values::from_fields(kind, fields),
        None => values,
    };
    Ok(Editor {
        title,
        kind,
        action,
        values,
        options,
        status,
    })
}

#[component]
async fn rule_page(
    cx: &Cx,
    state: ViewState,
    target: std::result::Result<Target, QueryError>,
    failure: Option<Failure<()>>,
) -> Result<impl View> {
    let caller = caller(cx);
    let failed_fields = failure.as_ref().map(|f| f.fields.clone());
    let loaded = match &target {
        Ok(target) => editor(cx, &caller, target, &state, failed_fields.as_ref()).await,
        Err(error) => Err(error.clone()),
    };
    let failed = failure.map(|f| (f.status(), f.error));
    let rules_url = href(PATH, &state, &[]);
    let content = can(&caller, Permission::Content);

    Ok(view! {
        <div class="mb-1 text-xs text-zinc-500"><a class=(LINK) href=(rules_url)>"Alert rules"</a> " / edit"</div>
        match loaded {
            Err(error) => {
                (status_of(&error))
                page_header(title: "Alert rule", subtitle: "")
                error_panel(error: &error)
            },
            Ok(editor) => {
                let submit = match (&editor.status, editor.kind) {
                    (None, _) => "Create rule",
                    (Some((StatusKind::Stale, _)), _) => "Update and re-enable",
                    (Some(_), _) => "Save changes",
                };
                let version = editor.options.choices.version.0;
                let watched = editor.kind == RuleKindChoice::Watched;
                page_header(
                    title: &editor.title,
                    subtitle: "Watched-topic rules follow their topics across re-fits while they remap above the threshold.",
                )
                if let Some((status, reason)) = editor.status {
                    <div class="mb-3 flex flex-wrap items-center gap-2 text-sm">
                        state_badge(label: status.label(), tone: status.tone())
                        if let Some(reason) = reason {
                            <span class="text-amber-800 dark:text-amber-300">(reason) " Saving re-targets the rule to version " (version) " and enables it."</span>
                        }
                    </div>
                }
                if watched && !content {
                    <p class="mb-3 text-sm text-zinc-500">"Picking topics needs the Content permission: topic labels come from message text."</p>
                }
                if let Some((status, error)) = failed {
                    (status)
                    <div class="mb-3">error_panel(error: &error)</div>
                }
                rule_form(
                    kind: editor.kind,
                    action: editor.action,
                    values: editor.values,
                    version: version,
                    topics: editor.options.topics,
                    sinks: editor.options.sinks,
                    submit: submit,
                    error: None,
                )
            },
        }
    })
}

#[cfg(test)]
mod tests {
    use topcoat::router::StatusCode;

    use super::*;
    use crate::components::href::tests::state;
    use crate::pages::alerts::rules::model::tests::watched;
    use crate::testing::{get, post};

    #[test]
    fn editing_keeps_topics_only_on_the_same_version() {
        let rule = watched(1, RuleStatus::Enabled);
        let same = values_of(&rule, TopicModelVersion(2));
        assert_eq!(same.topics.len(), 1);
        assert_eq!(same.threshold, "0.80");
        let refit = values_of(&rule, TopicModelVersion(3));
        assert!(refit.topics.is_empty());
        assert_eq!(refit.name, "credentials talk");
    }

    #[tokio::test]
    async fn new_rule_forms_render() {
        let q = state().to_query();
        let reply = get(&format!("/alerts/rules/new?{q}&kind=watched")).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert!(reply.body.contains("New watched-topic rule"));
        assert!(!reply.body.contains("No topics to pick from"));
        let reply = get(&format!("/alerts/rules/new?{q}&kind=semantic")).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert!(reply.body.contains("not supported yet"));
        let reply = get(&format!("/alerts/rules/new?{q}&kind=regex")).await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn rule_posts_validate() {
        let q = state().to_query();
        let url = format!("/alerts/rules/new?{q}");
        let reply = post(&url, "kind=watched&name=keys&version=2&remap_threshold=0.8").await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            reply.body.contains("topic: pick at least one topic"),
            "{}",
            reply.body
        );
        assert!(reply.body.contains("value=\"keys\""), "typed input is kept");
        let reply = post(&url, "kind=semantic&name=keys&text=api+keys&threshold=0.7").await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(reply.body.contains("cannot be saved from the UI yet"));
        let reply = post(&url, "kind=semantic&name=keys&text=api+keys&threshold=2").await;
        assert!(reply.body.contains("threshold: must lie between 0 and 1"));
    }

    #[tokio::test]
    async fn unknown_rules_are_not_found() {
        let q = state().to_query();
        let reply = get(&format!("/alerts/rules/01J9ZQ3W8D0000000000000004?{q}")).await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND);
        let reply = get(&format!("/alerts/rules/nope?{q}")).await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND);
    }
}
