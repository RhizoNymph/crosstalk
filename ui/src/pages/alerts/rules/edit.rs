//! `/alerts/rules/new?kind=…` and `/alerts/rules/{id}`: create an operator
//! rule, or edit one. Updating a stale rule re-targets it to the current
//! topic version and enables it.

use crosstalk_spec::aggregates::alert::{
    AlertRule, AlertRuleDef, AlertRuleKind, ContentRule, RuleStatus,
};
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::{AlertRuleId, TopicId};
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
use super::model::{semantic_query, staleness, status_label, status_tone, watched_topics};
use crate::app::{backend, caller, can};
use crate::components::form::LINK;
use crate::components::{Tone, error_panel, href, page_header, state_badge};
use crate::error::UiError;
use crate::pages::common::action::{Failure, done, perform, require, settled, status_of};
use crate::pages::common::flash::Flash;
use crate::pages::common::form::FormFields;
use crate::pages::common::links::rule_url;
use crate::pages::common::rules::rule;
use crate::pages::common::topics::{all_topics, default_version};
use crate::pages::view::view_state;
use crate::url::ulid::UlidId;
use crate::url::view_state::ViewState;
use crosstalk_spec::interfaces::l8_surface::ConflictKind;
use crosstalk_spec::interfaces::l8_surface::OperatorAction;
use crosstalk_spec::interfaces::l8_surface::QueryApi;
use crosstalk_spec::interfaces::l8_surface::QueryError;

path_param!(rule_ulid);

#[query_params]
struct NewQuery {
    kind: Option<String>,
    /// Preselects one topic of a new watched-topic rule.
    topic: Option<String>,
}

/// A new rule's starting values. `?topic=<id>` (the explore page's "Watch"
/// links) picks that topic when the form offers it.
fn new_values(cx: &Cx, kind: RuleKindChoice, offered: &[TopicId]) -> Values {
    let mut values = Values::defaults(kind);
    let topic = query_params::<NewQuery>(cx)
        .ok()
        .and_then(|q| q.topic.as_deref())
        .and_then(|t| TopicId::parse_ulid(t).ok());
    if kind == RuleKindChoice::Watched
        && let Some(topic) = topic.filter(|t| offered.contains(t))
    {
        values.topics = vec![topic.to_ulid()];
    }
    values
}

/// What the page edits.
#[derive(Debug, Clone, PartialEq)]
pub enum Target {
    New(RuleKindChoice),
    Existing(AlertRuleId),
}

/// The topic version to pick topics from: the history's active one.
async fn current_version(
    cx: &Cx,
    caller: &Caller,
) -> std::result::Result<TopicModelVersion, UiError> {
    Ok(default_version(backend(cx), caller).await?)
}

/// The topics and sinks a form may pick. Topics need `Content`: their
/// labels come from message text.
struct Options {
    choices: Choices,
    topics: Vec<TopicOption>,
    sinks: Vec<(String, String)>,
}

async fn options(cx: &Cx, caller: &Caller) -> std::result::Result<Options, UiError> {
    let version = current_version(cx, caller).await?;
    let sinks = backend(cx).sinks(caller).await?;
    let topics = if can(caller, Permission::Content) {
        all_topics(backend(cx), caller, TopicVersionSelector::Pinned(version))
            .await?
            .1
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
) -> std::result::Result<AlertRuleDef, UiError> {
    let found = rule(backend(cx), caller, id)
        .await?
        .ok_or(UiError::Query(QueryError::NotFound))?;
    if matches!(found.rule(), AlertRule::Builtin(_)) {
        return Err(UiError::Query(QueryError::Conflict(
            ConflictKind::RuleNotEditable { rule: id },
        )));
    }
    Ok(found)
}

fn kind_of(rule: &AlertRuleDef) -> RuleKindChoice {
    match rule.kind() {
        AlertRuleKind::SemanticQuery => RuleKindChoice::Semantic,
        _ => RuleKindChoice::Watched,
    }
}

/// The form's values for an existing user rule. Topics are kept only when
/// the rule watches the version the form offers, so a stale rule's are
/// picked again.
pub fn values_of(rule: &AlertRuleDef, version: TopicModelVersion) -> Values {
    let threshold = match rule.rule() {
        AlertRule::User { content, .. } => match content {
            ContentRule::WatchedTopic {
                remap_threshold, ..
            } => format!("{:.2}", remap_threshold.get()),
            ContentRule::SemanticQuery { threshold, .. } => format!("{:.2}", threshold.get()),
        },
        AlertRule::Builtin(_) => return Values::default(),
    };
    Values {
        name: rule.name().to_owned(),
        topics: watched_topics(rule)
            .filter(|watched| watched.version == version)
            .map(|watched| watched.topics.iter().map(|t| t.to_ulid()).collect())
            .unwrap_or_default(),
        threshold,
        text: semantic_query(rule)
            .map(|query| query.text.as_str().to_owned())
            .unwrap_or_default(),
        sinks: rule.sinks.iter().map(|s| s.to_ulid()).collect(),
    }
}

/// Runs a post: validates against the current options and creates or
/// updates the rule.
async fn submit(
    cx: &Cx,
    target: &Target,
    fields: &FormFields,
) -> std::result::Result<Flash, UiError> {
    let caller = caller(cx);
    require(&caller, Permission::Govern)?;
    let (kind, id) = match target {
        Target::New(kind) => (*kind, None),
        Target::Existing(id) => (kind_of(&existing(cx, &caller, *id).await?), Some(*id)),
    };
    let options = options(cx, &caller).await?;
    let (name, rule, sinks) = match kind {
        RuleKindChoice::Semantic => parse_semantic(fields, &options.choices.sinks)?,
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
    let outcome = perform(cx, action).await?;
    Ok(settled(&outcome, flash))
}

/// What a rule of `kind` does, under the form's title.
fn subtitle(kind: RuleKindChoice) -> &'static str {
    match kind {
        RuleKindChoice::Watched => {
            "Watched-topic rules follow their topics across re-fits while they remap above the threshold."
        }
        RuleKindChoice::Semantic => {
            "Semantic query rules raise an alert for transmissions whose text is at least as similar to the query as the threshold."
        }
    }
}

#[page("/alerts/rules/new")]
async fn new_rule_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx).await?;
    let kind = query_params::<NewQuery>(cx)
        .ok()
        .and_then(|q| q.kind.clone());
    let target = RuleKindChoice::parse(kind.as_deref()).map(Target::New);
    Ok(view! { rule_page(state: state, target: target, failure: None) })
}

#[page(POST "/alerts/rules/new")]
async fn new_rule_post(cx: &Cx, Form(fields): Form<FormFields>) -> Result<impl View> {
    let state = view_state(cx).await?;
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
    let state = view_state(cx).await?;
    let id = rule_id(cx)?;
    Ok(view! { rule_page(state: state, target: Ok(Target::Existing(id)), failure: None) })
}

#[page(POST "/alerts/rules/{rule_ulid}")]
async fn rule_post(cx: &Cx, Form(fields): Form<FormFields>) -> Result<impl View> {
    let state = view_state(cx).await?;
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
    status: Option<(RuleStatus, Option<String>)>,
}

async fn editor(
    cx: &Cx,
    caller: &Caller,
    target: &Target,
    state: &ViewState,
    failed: Option<&FormFields>,
) -> std::result::Result<Editor, UiError> {
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
            new_values(cx, *kind, &options.choices.topics),
            None,
        ),
        Target::Existing(id) => {
            let rule = existing(cx, caller, *id).await?;
            let stale = rule.stale_reason().map(|r| staleness(&r, &None).reason);
            let status = (rule.status, stale);
            (
                format!("Edit \u{201c}{}\u{201d}", rule.name()),
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
    target: std::result::Result<Target, UiError>,
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
                let submit = match &editor.status {
                    None => "Create rule",
                    Some((_, Some(_))) => "Update and re-enable",
                    Some((_, None)) => "Save changes",
                };
                let version = editor.options.choices.version.0;
                let watched = editor.kind == RuleKindChoice::Watched;
                page_header(title: &editor.title, subtitle: subtitle(editor.kind))
                if let Some((status, reason)) = editor.status {
                    <div class="mb-3 flex flex-wrap items-center gap-2 text-sm">
                        state_badge(label: status_label(status), tone: status_tone(status))
                        if let Some(reason) = reason {
                            state_badge(label: "stale", tone: Tone::Warn)
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
    use crate::testing::{Session, get, post};

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
        assert!(reply.body.contains("New semantic query rule"));
        assert!(reply.body.contains("Describe what to look for"));
        assert!(reply.body.contains(subtitle(RuleKindChoice::Semantic)));
        assert!(
            !reply.body.contains(subtitle(RuleKindChoice::Watched)),
            "a semantic rule is not described as a watched-topic rule"
        );
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
        let reply = post(&url, "kind=semantic&name=keys&text=+&threshold=0.7").await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(reply.body.contains("text: required"));
        let reply = post(&url, "kind=semantic&name=keys&text=api+keys&threshold=2").await;
        assert!(reply.body.contains("threshold: must lie between 0 and 1"));
    }

    /// The edit URL of the rule named `name` on the rules page.
    fn edit_url_of(body: &str, name: &str) -> String {
        let at = body.find(name).unwrap_or_else(|| panic!("{name} listed"));
        let start = at + body[at..].find("/alerts/rules/").expect("edit link");
        let end = start + body[start..].find('"').expect("link end");
        body[start..end].replace("&amp;", "&")
    }

    #[tokio::test]
    async fn semantic_rules_save_and_edit_end_to_end() {
        let session = Session::new();
        let q = state().to_query();
        let reply = session
            .post(
                &format!("/alerts/rules/new?{q}"),
                "kind=semantic&name=Keys+in+chat&text=api+keys+pasted+in+chat&threshold=0.72",
            )
            .await;
        assert_eq!(reply.status, StatusCode::SEE_OTHER, "{}", reply.body);
        let back = reply.location.expect("location");
        assert!(back.starts_with("/alerts/rules?"), "{back}");
        let rules = session.get(&back).await;
        assert_eq!(rules.status, StatusCode::OK);
        assert!(rules.body.contains("Keys in chat"));
        assert!(
            rules
                .body
                .contains("\u{201c}api keys pasted in chat\u{201d}")
        );
        assert!(
            rules
                .body
                .contains("similarity ≥ 0.72 under fixture-minilm-16")
        );
        // The edit form starts from the stored text and saves a new one.
        let edit = edit_url_of(&rules.body, "Keys in chat");
        let form = session.get(&edit).await;
        assert_eq!(form.status, StatusCode::OK, "{}", form.body);
        assert!(form.body.contains(">api keys pasted in chat</textarea>"));
        let reply = session
            .post(
                &edit,
                "kind=semantic&name=Keys+in+chat&text=tokens+in+a+paste&threshold=0.8",
            )
            .await;
        assert_eq!(reply.status, StatusCode::SEE_OTHER, "{}", reply.body);
        let rules = session.get(&reply.location.expect("location")).await;
        assert!(rules.body.contains("\u{201c}tokens in a paste\u{201d}"));
        assert!(!rules.body.contains("api keys pasted in chat"));
    }

    #[tokio::test]
    async fn names_and_texts_are_checked_where_the_spec_checks_them() {
        let q = state().to_query();
        let url = format!("/alerts/rules/new?{q}");
        let long_name = "n".repeat(81);
        let reply = post(
            &url,
            &format!("kind=semantic&name={long_name}&text=api+keys&threshold=0.7"),
        )
        .await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            reply
                .body
                .contains("name: rule name is longer than 80 characters")
        );
        // The embedder, not the form, says how long a query may be.
        let long_text = "key+".repeat(1200);
        let reply = post(
            &url,
            &format!("kind=semantic&name=keys&text={long_text}&threshold=0.7"),
        )
        .await;
        assert_eq!(
            reply.status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{}",
            reply.body
        );
        assert!(reply.body.contains("the text is too long to embed"));
    }

    #[tokio::test]
    async fn watch_links_preselect_a_topic_of_the_current_version() {
        use crosstalk_spec::aggregates::filter::TopicVersionSelector;

        let (version, topics) = all_topics(
            crate::testing::world(),
            &crate::testing::operator().caller(),
            TopicVersionSelector::Current,
        )
        .await
        .expect("topics");
        assert_eq!(version, TopicModelVersion(2));
        let topic = topics[0].id.to_ulid();
        let q = state().to_query();
        let reply = get(&format!("/alerts/rules/new?{q}&kind=watched&topic={topic}")).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert!(
            reply.body.contains(&format!("value=\"{topic}\" checked")),
            "{}",
            reply.body
        );
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
