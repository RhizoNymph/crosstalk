//! `/channels/{id}/promote`: turn a discovered channel into a declared one.
//!
//! The operator picks a pattern derived from the seed locator
//! (`?pattern=<n>`), sees which known resources it covers and which other
//! discovered channels it would supersede (`promotion_preview`), then
//! confirms with a policy and a note. The promoted channel keeps its id.

pub mod patterns;
mod screen;

use crate::pending::channel_semantics::ChannelRow;
use crosstalk_spec::derived::flow::channel::ChannelOrigin;
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::ids::ChannelId;
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::content::Form;
use topcoat::router::{page, query_params};
use topcoat::view::{View, view};

use self::patterns::{candidates, pick};
use self::screen::promote_page;
use super::detail::{channel_id, channel_path};
use crate::app::{backend, caller};
use crate::error::UiError;
use crate::pages::common::action::{Failure, done, perform, settled};
use crate::pages::common::flash::Flash;
use crate::pages::common::form::{FormFields, invalid, note, policy};
use crate::pages::view::view_state;
use crate::url::view_state::ViewState;
use crosstalk_spec::interfaces::l8_surface::ConflictKind;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::interfaces::l8_surface::{ActionOutcome, OperatorAction};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromoteForm {
    Promote,
}

#[query_params]
struct PromoteQuery {
    pattern: Option<String>,
}

/// The seed of a channel that can be promoted: discovered, not superseded,
/// with its seed resource known. The refusals are the ones `PromoteChannel`
/// would answer with.
pub fn promotable_seed(row: &ChannelRow) -> std::result::Result<&Locator, UiError> {
    let channel = row.channel().id;
    if let Some(superseded) = row.supersession() {
        return Err(UiError::Query(QueryError::Conflict(
            ConflictKind::ChannelSuperseded {
                channel,
                by: superseded.into(),
            },
        )));
    }
    if !matches!(row.channel().origin, ChannelOrigin::Discovered { .. }) {
        return Err(UiError::Query(QueryError::Conflict(
            ConflictKind::ChannelNotDiscovered { channel },
        )));
    }
    row.seed()
        .map(|seed| &seed.locator)
        .ok_or_else(|| invalid("channel", "the seed resource of this channel is unknown"))
}

/// The promotion a form asks for. The pattern is an index into the
/// candidates derived from the seed, so it always covers the seed.
pub fn parse(
    channel: ChannelId,
    seed: &Locator,
    fields: &FormFields,
) -> std::result::Result<OperatorAction, UiError> {
    let options = candidates(seed);
    let index = pick(&options, fields.text("pattern"))
        .map_err(|reason| invalid("pattern", reason))?
        .ok_or_else(|| invalid("pattern", "choose a pattern"))?;
    let pattern = options
        .get(index)
        .cloned()
        .ok_or_else(|| invalid("pattern", "choose a pattern"))?;
    Ok(OperatorAction::PromoteChannel {
        channel,
        pattern,
        policy: policy(fields, "policy")?,
        note: note(fields, "note")?,
    })
}

/// Promotes the channel: the promoted channel (the same id) and the flash
/// saying how many discovered channels it superseded.
async fn submit(
    cx: &Cx,
    channel: ChannelId,
    state: &ViewState,
    fields: &FormFields,
) -> std::result::Result<(ChannelId, Flash), UiError> {
    let caller = caller(cx);
    let row = backend(cx)
        .channel(&caller, channel, Some(state.scope.window))
        .await?
        .ok_or(UiError::Query(QueryError::NotFound))?
        .value;
    let action = parse(channel, promotable_seed(&row)?, fields)?;
    match perform(cx, action).await? {
        ActionOutcome::ChannelPromoted {
            channel: promoted,
            superseded,
        } => Ok((promoted, Flash::promoted(superseded.as_slice().len()))),
        outcome => Ok((channel, settled(&outcome, Flash::promoted(0)))),
    }
}

#[page("/channels/{channel_ulid}/promote")]
async fn promote_get(cx: &Cx) -> Result<impl View> {
    let state = view_state(cx).await?;
    let id = channel_id(cx)?;
    let pattern = query_params::<PromoteQuery>(cx)
        .ok()
        .and_then(|q| q.pattern.clone());
    Ok(view! { promote_page(id: id, state: state, pattern: pattern, failure: None) })
}

#[page(POST "/channels/{channel_ulid}/promote")]
async fn promote_post(cx: &Cx, Form(fields): Form<FormFields>) -> Result<impl View> {
    let state = view_state(cx).await?;
    let id = channel_id(cx)?;
    let error = match submit(cx, id, &state, &fields).await {
        Ok((promoted, flash)) => {
            return Err(done(&channel_path(promoted), &state, &[], flash));
        }
        Err(error) => error,
    };
    let pattern = fields.text("pattern").map(str::to_owned);
    let failure = Failure::new(Some(PromoteForm::Promote), error, fields);
    Ok(view! { promote_page(id: id, state: state, pattern: pattern, failure: Some(failure)) })
}

#[cfg(test)]
mod tests {
    use crosstalk_spec::derived::flow::resource::ResourcePattern;
    use crosstalk_spec::interfaces::l8_surface::PolicyKind;
    use topcoat::router::StatusCode;

    use super::*;
    use crate::components::href::tests::state;
    use crate::pages::channels::model::tests::{declared, discovered, superseded, wiki};
    use crate::testing::{get, post};

    #[test]
    fn parses_a_picked_pattern() {
        let fields = FormFields::from_pairs(&[("pattern", "0"), ("policy", "sanctioned")]);
        assert_eq!(
            parse(ChannelId::from_ulid(1), &wiki(), &fields),
            Ok(OperatorAction::PromoteChannel {
                channel: ChannelId::from_ulid(1),
                pattern: ResourcePattern::Exact(wiki()),
                policy: PolicyKind::Sanctioned,
                note: None,
            })
        );
    }

    #[test]
    fn rejects_missing_or_out_of_range_patterns() {
        let none = FormFields::from_pairs(&[("policy", "sanctioned")]);
        assert_eq!(
            parse(ChannelId::from_ulid(1), &wiki(), &none),
            Err(invalid("pattern", "choose a pattern"))
        );
        let far = FormFields::from_pairs(&[("pattern", "40"), ("policy", "sanctioned")]);
        assert!(parse(ChannelId::from_ulid(1), &wiki(), &far).is_err());
        let no_policy = FormFields::from_pairs(&[("pattern", "1")]);
        assert_eq!(
            parse(ChannelId::from_ulid(1), &wiki(), &no_policy),
            Err(invalid("policy", "required"))
        );
    }

    #[test]
    fn only_live_discovered_channels_with_a_seed_promote() {
        let row = discovered(1);
        assert_eq!(promotable_seed(&row), Ok(&wiki()));
        assert_eq!(
            promotable_seed(&superseded(1, 2)),
            Err(UiError::Query(QueryError::Conflict(
                ConflictKind::ChannelSuperseded {
                    channel: ChannelId::from_ulid(1),
                    by: ChannelId::from_ulid(2),
                }
            )))
        );
        assert_eq!(
            promotable_seed(&declared(3)),
            Err(UiError::Query(QueryError::Conflict(
                ConflictKind::ChannelNotDiscovered {
                    channel: ChannelId::from_ulid(3),
                }
            )))
        );
    }

    const ID: &str = "01J9ZQ3W8D0000000000000001";

    #[tokio::test]
    async fn promote_page_and_post_for_an_unknown_channel() {
        let url = format!("/channels/{ID}/promote?{}", state().to_query());
        let reply = get(&url).await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND, "{}", reply.body);
        let reply = post(&url, "pattern=0&policy=sanctioned").await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND, "{}", reply.body);
        assert!(reply.body.contains("not found"));
    }

    #[tokio::test]
    async fn the_preview_comes_from_the_backend_and_promotion_follows_it() {
        use crate::backend::fixture::ChannelKey;
        use crate::testing::{Session, channel_id};
        use crate::url::ulid::UlidId;

        let (wiki, talk) = (
            channel_id(ChannelKey::HijackedWiki),
            channel_id(ChannelKey::WikiTalk),
        );
        let session = Session::new();
        let url = format!(
            "/channels/{}/promote?{}",
            wiki.to_ulid(),
            state().to_query()
        );
        // Pattern 2 is the `/wiki` prefix, which covers the talk page too.
        let reply = session.get(&format!("{url}&pattern=2")).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert!(reply.body.contains("Also supersedes <strong>1</strong>"));
        assert!(
            reply
                .body
                .contains(&crate::components::short_id(talk.to_ulid()))
        );
        assert!(reply.body.contains("covered"));
        let reply = session.get(&format!("{url}&pattern=0")).await;
        assert!(
            reply
                .body
                .contains("No other discovered channel is covered.")
        );
        let reply = session.post(&url, "pattern=2&policy=unsanctioned").await;
        assert_eq!(reply.status, StatusCode::SEE_OTHER, "{}", reply.body);
        let promoted = reply.location.expect("location");
        assert!(
            promoted.starts_with(&format!("/channels/{}?", wiki.to_ulid())),
            "promotion keeps the channel's id: {promoted}"
        );
        assert!(promoted.ends_with("&flash=promoted-1"), "{promoted}");
        let reply = session.get(&promoted).await;
        assert!(
            reply
                .body
                .contains("It superseded 1 discovered channel its pattern matches")
        );
        let reply = session.get(&format!("{url}&pattern=2")).await;
        assert_eq!(reply.status, StatusCode::CONFLICT);
        assert!(reply.body.contains("already declared"), "{}", reply.body);
        let talk_url = format!(
            "/channels/{}/promote?{}&pattern=0",
            talk.to_ulid(),
            state().to_query()
        );
        let reply = session.get(&talk_url).await;
        assert_eq!(reply.status, StatusCode::CONFLICT);
        assert!(reply.body.contains("the channel is superseded"));
    }
}
