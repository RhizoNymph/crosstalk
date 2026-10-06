//! The wiki relay on Claude Pro/Max logins: fake OAuth access tokens with
//! the OAuth capability, A's refreshed between its two exchanges. L0 marks
//! every credential `OauthAccessToken`; L3 still resolves one agent per
//! session; detection is unchanged; and nothing the surface answers holds
//! any piece of a token.

use std::time::Duration;

use crosstalk_e2e::read::{self, Agents};
use crosstalk_e2e::scenario::{SUBSCRIPTION_TOKENS, Scenario};
use crosstalk_e2e::{Capture, Composition, compose, feed, options};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::interfaces::l8_surface::QueryApi;
use crosstalk_spec::observed::client::CredentialScheme;
use crosstalk_spec::support::TimeWindow;

use crate::support::{Failure, unexpected};

const PATIENCE: Duration = Duration::from_secs(10);

/// How long a window of a token must be to count as a leak.
const WINDOW: usize = 12;

fn scenario() -> Scenario {
    Scenario::wiki_relay_subscription(crosstalk_e2e::scenario::DEFAULT_START)
}

/// The first window of a token's secret part found in `text`.
fn leaked(text: &str) -> Option<String> {
    SUBSCRIPTION_TOKENS.iter().find_map(|token| {
        let secret = &token.as_bytes()["sk-ant-oat01-".len()..];
        secret
            .windows(WINDOW)
            .find(|window| {
                text.as_bytes()
                    .windows(WINDOW)
                    .any(|candidate| candidate == *window)
            })
            .map(|window| String::from_utf8_lossy(window).into_owned())
    })
}

#[test]
fn every_credential_is_an_oauth_token_and_a_refresh_changes_its_digest() -> Result<(), Failure> {
    let scenario = scenario();
    let capture = Capture::new()?;
    let mut by_label = Vec::new();
    for exchange in &scenario.exchanges {
        assert!(
            exchange.request.headers.iter().any(|(name, value)| {
                name == "anthropic-beta" && value.contains("oauth-2025-04-20")
            }),
            "{}: no OAuth capability",
            exchange.label
        );
        assert!(
            !exchange
                .request
                .headers
                .iter()
                .any(|(name, _)| name == "x-api-key"),
            "{}: an API key on a subscription",
            exchange.label
        );
        let raw = capture.raw(exchange)?;
        let credential = raw
            .meta
            .client
            .credential
            .ok_or_else(|| unexpected(format!("{}: no credential", exchange.label)))?;
        assert_eq!(credential.scheme, CredentialScheme::OauthAccessToken);
        assert_eq!(leaked(&format!("{raw:?}")), None, "{}", exchange.label);
        by_label.push((exchange.label, credential.hash, raw.meta.client.ids.session));
    }
    let [
        (_, a1, a1_session),
        (_, a2, a2_session),
        (_, b1, _),
        (_, b2, _),
    ] = by_label.as_slice()
    else {
        return Err(unexpected("four exchanges"));
    };
    assert_ne!(a1, a2, "A's refreshed token has its own digest");
    assert_eq!(
        a1_session, a2_session,
        "A keeps its session across the refresh"
    );
    assert_eq!(b1, b2);
    Ok(())
}

async fn ingested() -> Result<(Scenario, Composition, TimeWindow), Failure> {
    let scenario = scenario();
    let composition = compose(scenario.start).await?;
    let clock = composition.clock.clone();
    feed(&scenario, &composition.pipeline, |at| clock.set(at)).await?;
    let window = read::window(&scenario, options::BUCKET)?;
    Ok((scenario, composition, window))
}

/// `reconstruct.identity.refresh-keeps-session-agent` and
/// `ingress.credential.absent-end-to-end` through the composition: A is one
/// agent across its refresh, the relay is still detected, and no surface
/// answer holds a piece of any token.
#[tokio::test]
async fn the_relay_is_detected_and_no_answer_holds_a_token() -> Result<(), Failure> {
    let (scenario, composition, window) = ingested().await?;
    let surface = composition.surface.as_ref();
    let caller = &composition.caller;
    let deadline = tokio::time::Instant::now() + PATIENCE;
    let (agents, edge) = loop {
        let found = read::agents(surface, caller, &scenario, window).await?;
        let edges = read::edges(surface, caller, window).await?;
        if let Some(agents) = found.agents
            && let Some(edge) = read::channel_edge(&edges, agents)
        {
            assert_eq!(found.listed.len(), 2, "exactly the two sessions' agents");
            break (agents, edge.clone());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(unexpected("agents a and b and the channel edge not seen"));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    let Agents { a, b } = agents;
    assert_ne!(a, b);
    assert!(matches!(edge.route, Route::Channel(_)));
    assert_eq!(edge.stats.transmissions.get(), 1);

    let mut answers = vec![
        format!(
            "{:?}",
            read::agents(surface, caller, &scenario, window).await?
        ),
        format!("{:?}", read::edges(surface, caller, window).await?),
        format!("{:?}", read::channels(surface, caller).await?),
    ];
    for id in [a, b] {
        let detail = surface
            .agent(caller, id, window)
            .await
            .map_err(|error| unexpected(format!("agent {id:?}: {error:?}")))?;
        answers.push(format!("{detail:?}"));
    }
    let behind = read::edge_transmissions(surface, caller, &edge, window).await?;
    answers.push(format!("{behind:?}"));
    let ids: Vec<_> = behind.iter().map(|row| row.transmission).collect();
    answers.push(format!(
        "{:?}",
        read::summaries(surface, caller, ids.clone()).await?
    ));
    for id in ids {
        let evidence = read::evidence(surface, caller, id).await?;
        assert!(evidence.is_some(), "the transmission has evidence");
        answers.push(format!("{evidence:?}"));
    }
    for answer in &answers {
        assert_eq!(leaked(answer), None, "a token in a surface answer");
    }
    composition.shutdown().await;
    Ok(())
}
