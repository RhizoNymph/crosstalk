//! The version picker's pin control: `PinTopicVersion` keeps a version's
//! data whatever retention decides, `UnpinTopicVersion` lets retention drop
//! it again. Both need `Govern`; the control is offered where the action
//! can apply (a pin on a version neither dropped nor fitting, an unpin on a
//! pinned one), and the surface still refuses the rest with typed errors
//! shown next to the control.

use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::interfaces::l8_surface::OperatorAction;
use topcoat::Result;
use topcoat::context::Cx;
use topcoat::router::content::Form;
use topcoat::router::page;
use topcoat::view::{View, component, view};

use super::model::VersionTab;
use super::{PATH, parse_version, topics_page};
use crate::components::form::SMALL_BUTTON;
use crate::error::UiError;
use crate::pages::common::action::{Failure, done, perform, settled};
use crate::pages::common::flash::Flash;
use crate::pages::common::form::{FormFields, invalid, required};
use crate::pages::view::view_state;

/// What the control offers for a version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinChoice {
    Pin,
    Unpin,
}

impl PinChoice {
    pub fn code(self) -> &'static str {
        match self {
            Self::Pin => "pin",
            Self::Unpin => "unpin",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Pin => "Pin",
            Self::Unpin => "Unpin",
        }
    }

    fn action(self, version: TopicModelVersion) -> (OperatorAction, Flash) {
        match self {
            Self::Pin => (
                OperatorAction::PinTopicVersion { version },
                Flash::VersionPinned,
            ),
            Self::Unpin => (
                OperatorAction::UnpinTopicVersion { version },
                Flash::VersionUnpinned,
            ),
        }
    }
}

/// The control for `tab`: unpin a pinned version, pin one whose data is
/// kept and whose fit has returned, nothing otherwise.
pub fn choice(tab: &VersionTab) -> Option<PinChoice> {
    if tab.pinned {
        Some(PinChoice::Unpin)
    } else if tab.dropped || tab.fitting {
        None
    } else {
        Some(PinChoice::Pin)
    }
}

/// A validated pin post: the action, the flash it ends with, and the
/// version to show afterwards.
pub fn parse(fields: &FormFields) -> std::result::Result<(OperatorAction, Flash, u32), UiError> {
    let choice = match fields.text("action") {
        Some("pin") => PinChoice::Pin,
        Some("unpin") => PinChoice::Unpin,
        _ => return Err(invalid("action", "unknown action")),
    };
    let version =
        parse_version(Some(required(fields, "ver")?))?.ok_or_else(|| invalid("ver", "required"))?;
    let (action, flash) = choice.action(version);
    Ok((action, flash, version.0))
}

#[page(POST "/topics")]
async fn topics_post(cx: &Cx, Form(fields): Form<FormFields>) -> Result<impl View> {
    let state = view_state(cx).await?;
    let error = match parse(&fields) {
        Ok((action, flash, version)) => match perform(cx, action).await {
            Ok(outcome) => {
                let ver = version.to_string();
                let flash = settled(&outcome, flash);
                return Err(done(PATH, &state, &[("ver", &ver)], flash));
            }
            Err(error) => error,
        },
        Err(error) => error,
    };
    let selected = parse_version(fields.text("ver"))
        .ok()
        .flatten()
        .unwrap_or(state.scope.topic_version);
    let failure: Failure<()> = Failure::new(None, error, fields);
    Ok(
        view! { topics_page(state: state, selected: Ok(selected), flash: None, failure: Some(failure)) },
    )
}

/// The pin or unpin button of the shown version.
#[component]
pub async fn pin_control(action: String, version: u32, choice: PinChoice) -> Result<impl View> {
    Ok(view! {
        <form method="post" action=(action) class="inline-flex items-center gap-2">
            <input type="hidden" name="action" value=(choice.code())>
            <input type="hidden" name="ver" value=(version.to_string())>
            <button type="submit" class=(SMALL_BUTTON)>(choice.label()) " v" (version)</button>
        </form>
    })
}

#[cfg(test)]
mod tests {
    use topcoat::router::StatusCode;

    use super::*;
    use crate::pages::topology::tests::fixture_state;
    use crate::testing::Session;

    fn tab(pinned: bool, dropped: bool, fitting: bool) -> VersionTab {
        VersionTab {
            version: 1,
            label: "v1".to_owned(),
            detail: String::new(),
            pinned,
            newest: false,
            dropped,
            fitting,
            readable: !dropped && !fitting,
            in_view: false,
        }
    }

    #[test]
    fn the_control_follows_retention() {
        assert_eq!(choice(&tab(true, false, false)), Some(PinChoice::Unpin));
        assert_eq!(choice(&tab(false, false, false)), Some(PinChoice::Pin));
        assert_eq!(choice(&tab(false, true, false)), None);
        assert_eq!(choice(&tab(false, false, true)), None);
    }

    #[test]
    fn posts_parse_into_pin_actions() {
        let pin = FormFields::from_pairs(&[("action", "pin"), ("ver", "2")]);
        assert_eq!(
            parse(&pin),
            Ok((
                OperatorAction::PinTopicVersion {
                    version: TopicModelVersion(2)
                },
                Flash::VersionPinned,
                2
            ))
        );
        let unpin = FormFields::from_pairs(&[("action", "unpin"), ("ver", "1")]);
        assert!(matches!(
            parse(&unpin),
            Ok((
                OperatorAction::UnpinTopicVersion { .. },
                Flash::VersionUnpinned,
                1
            ))
        ));
        let bad = FormFields::from_pairs(&[("action", "pin"), ("ver", "v1")]);
        assert_eq!(
            parse(&bad),
            Err(invalid("ver", "not a topic model version"))
        );
        let missing = FormFields::from_pairs(&[("action", "pin")]);
        assert_eq!(parse(&missing), Err(invalid("ver", "required")));
        let unknown = FormFields::from_pairs(&[("action", "drop"), ("ver", "1")]);
        assert_eq!(parse(&unknown), Err(invalid("action", "unknown action")));
    }

    #[tokio::test]
    async fn pinning_and_unpinning_from_the_picker() {
        let session = Session::new();
        let url = format!("{PATH}?{}", fixture_state().to_query());
        let reply = session.get(&format!("{url}&ver=2")).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert!(reply.body.contains(">Pin v2</button>"), "{}", reply.body);

        let reply = session.post(&url, "action=pin&ver=2").await;
        assert_eq!(reply.status, StatusCode::SEE_OTHER, "{}", reply.body);
        let back = reply.location.expect("location");
        assert!(back.ends_with("&ver=2&flash=version-pinned"), "{back}");
        let reply = session.get(&back).await;
        assert!(reply.body.contains("Version pinned."));
        assert!(reply.body.contains("v2 · pinned"));
        assert!(reply.body.contains(">Unpin v2</button>"));

        // Pinning it again changes nothing, and says so.
        let reply = session.post(&url, "action=pin&ver=2").await;
        let back = reply.location.expect("location");
        assert!(back.ends_with("&flash=unchanged"), "{back}");

        let reply = session.post(&url, "action=unpin&ver=2").await;
        let back = reply.location.expect("location");
        assert!(back.ends_with("&flash=version-unpinned"), "{back}");

        // A dropped version cannot be pinned: the typed conflict is shown.
        let reply = session.post(&url, "action=pin&ver=0").await;
        assert_eq!(reply.status, StatusCode::CONFLICT, "{}", reply.body);
        assert!(
            reply
                .body
                .contains("topic model version 0 has been dropped")
        );
        let reply = session.post(&url, "action=pin&ver=9").await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND, "{}", reply.body);
        let reply = session.post(&url, "action=pin&ver=x").await;
        assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
    }
}
