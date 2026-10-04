//! `/channels/{id}/promote`: turn a discovered channel into a declared one.
//!
//! The operator picks a pattern derived from the seed locator
//! (`?pattern=<n>`), sees which known resources it covers and which other
//! discovered channels it would supersede, then confirms with a policy and
//! a note.

pub mod patterns;
mod screen;

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
use crate::backend::Backend;
use crate::contract::actions::{ActionOutcome, OperatorAction};
use crate::contract::channels::ChannelSummary;
use crate::contract::errors::{ConflictKind, QueryError};
use crate::pages::common::action::{Failure, done, perform};
use crate::pages::common::flash::Flash;
use crate::pages::common::form::{FormFields, invalid, note, policy};
use crate::pages::view::view_state;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromoteForm {
    Promote,
}

#[query_params]
struct PromoteQuery {
    pattern: Option<String>,
}

/// The seed of a channel that can be promoted: discovered, not superseded,
/// with its seed resource known.
pub fn promotable_seed(summary: &ChannelSummary) -> std::result::Result<&Locator, QueryError> {
    if !matches!(summary.channel.origin, ChannelOrigin::Discovered { .. }) {
        return Err(QueryError::Conflict(ConflictKind::ChannelNotDiscovered));
    }
    if summary.superseded.is_some() {
        return Err(QueryError::Conflict(ConflictKind::ChannelSuperseded));
    }
    summary
        .seed
        .as_ref()
        .map(|seed| &seed.locator)
        .ok_or_else(|| invalid("channel", "the seed resource of this channel is unknown"))
}

/// The promotion a form asks for. The pattern is an index into the
/// candidates derived from the seed, so it always covers the seed.
pub fn parse(
    channel: ChannelId,
    seed: &Locator,
    fields: &FormFields,
) -> std::result::Result<OperatorAction, QueryError> {
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

async fn submit(
    cx: &Cx,
    channel: ChannelId,
    fields: &FormFields,
) -> std::result::Result<ChannelId, QueryError> {
    let caller = caller(cx);
    let summary = backend(cx)
        .channel(&caller, channel)
        .await?
        .ok_or(QueryError::NotFound)?;
    let action = parse(channel, promotable_seed(&summary)?, fields)?;
    match perform(cx, action).await? {
        ActionOutcome::ChannelPromoted(declared) => Ok(declared),
        _ => Ok(channel),
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
    let error = match submit(cx, id, &fields).await {
        Ok(declared) => {
            return Err(done(
                &channel_path(declared),
                &state,
                &[],
                Flash::ChannelPromoted,
            ));
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
    use crate::contract::channels::Supersession;
    use crate::pages::channels::model::tests::{discovered, wiki};
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
        let summary = discovered(1);
        assert_eq!(promotable_seed(&summary), Ok(&wiki()));
        let mut superseded = discovered(1);
        superseded.superseded = Some(Supersession {
            into: ChannelId::from_ulid(2),
            by: crosstalk_spec::ids::OperatorId::from_ulid(1),
            at: crosstalk_spec::support::Timestamp::from_micros(0),
        });
        assert_eq!(
            promotable_seed(&superseded),
            Err(QueryError::Conflict(ConflictKind::ChannelSuperseded))
        );
        let mut seedless = discovered(1);
        seedless.seed = None;
        assert!(promotable_seed(&seedless).is_err());
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
}
