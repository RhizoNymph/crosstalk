//! Harness claims: recorded exactly as the headers state them, and never
//! able to steer forwarding.

use crosstalk_spec::interfaces::l0_ingress::{ClientIdentifier, RequestHead, UpstreamRouter};
use crosstalk_spec::observed::client::{HarnessClaim, HarnessFamily, HarnessIds, RequestClass};
use proptest::prelude::*;

use super::support::{PREFIX, anthropic_api, cases, identifier, route};
use crate::routing::Routes;

fn routes() -> Routes {
    let mut other = route("https://api.openai.com/v1", anthropic_api());
    other.name.0 = "openai".to_owned();
    other.prefix = "/openai".to_owned();
    other.upstream.id.0 = "openai".to_owned();
    Routes::new(&[route("https://api.anthropic.com", anthropic_api()), other]).expect("routes")
}

fn harness_header() -> impl Strategy<Value = (String, String)> {
    let name = prop_oneof![
        Just("user-agent"),
        Just("x-app"),
        Just("x-claude-code-session-id"),
        Just("x-claude-code-agent-id"),
        Just("x-claude-code-parent-agent-id"),
        Just("x-claude-code-request-class"),
        Just("session-id"),
        Just("thread-id"),
        Just("session_id"),
        Just("x-openai-subagent"),
        Just("x-codex-parent-thread-id"),
    ];
    (name.prop_map(str::to_owned), "[ -~]{0,40}")
}

proptest! {
    /// Adding, changing or removing harness headers never changes the
    /// route or the upstream URI a request goes to.
    #[test]
    fn harness_headers_do_not_affect_forwarding(
        path in prop_oneof![
            Just(format!("{PREFIX}/v1/messages")),
            Just("/openai/chat/completions".to_owned()),
            Just("/elsewhere".to_owned()),
            "/[a-z]{1,10}(/[a-z]{1,6}){0,2}",
        ],
        query in proptest::option::of("[a-z]{1,4}=[a-z]{1,4}"),
        before in proptest::collection::vec(harness_header(), 0..6),
        after in proptest::collection::vec(harness_header(), 0..6),
    ) {
        let routes = routes();
        let head = |headers: &[(String, String)]| RequestHead {
            method: "POST".to_owned(),
            path: path.clone(),
            query: query.clone(),
            headers: headers.to_vec(),
        };
        let one = head(&before);
        let other = head(&after);
        prop_assert_eq!(routes.route(&one), routes.route(&other));
        prop_assert_eq!(
            routes.resolve(&one.path, one.query.as_deref()),
            routes.resolve(&other.path, other.query.as_deref())
        );
    }
}

/// For every corpus case, the claim, ids and class are exactly what its
/// headers state (its metadata records them independently); a Codex-style
/// request and a request with no harness headers read as theirs.
#[test]
fn harness_headers_recorded_per_harness_fixture() {
    let identifier = identifier();
    for case in cases() {
        let (claim, ids, class) = identifier.harness(&case.request.head());
        assert_eq!(
            claim.as_ref(),
            Some(&case.meta.harness.claim),
            "case {}",
            case.name
        );
        assert_eq!(ids, case.meta.harness.ids, "case {}", case.name);
        assert_eq!(class, case.meta.harness.class, "case {}", case.name);
    }
    let codex = RequestHead {
        method: "POST".to_owned(),
        path: "/backend-api/codex/responses".to_owned(),
        query: None,
        headers: [
            (
                "user-agent",
                "codex_cli_rs/0.48.0 (Linux 6.8; x86_64) xterm",
            ),
            ("session-id", "root-thread"),
            ("thread-id", "child-thread"),
            ("x-codex-parent-thread-id", "root-thread"),
            ("x-openai-subagent", "review"),
            ("authorization", "Bearer eyJhbGciOi.eyJzdWIiOi.c2ln"),
        ]
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect(),
    };
    let (claim, ids, class) = identifier.harness(&codex);
    assert_eq!(
        claim,
        Some(HarnessClaim {
            family: HarnessFamily::Codex,
            version: Some("0.48.0".to_owned()),
            user_agent: "codex_cli_rs/0.48.0 (Linux 6.8; x86_64) xterm".to_owned(),
        })
    );
    assert_eq!(
        ids,
        HarnessIds {
            session: Some("root-thread".to_owned()),
            agent: Some("child-thread".to_owned()),
            parent_agent: Some("root-thread".to_owned()),
        }
    );
    assert_eq!(class, RequestClass::Subagent);
    let bare = RequestHead {
        headers: Vec::new(),
        ..codex
    };
    assert_eq!(
        identifier.harness(&bare),
        (
            None,
            HarnessIds {
                session: None,
                agent: None,
                parent_agent: None
            },
            RequestClass::Unknown
        )
    );
}
