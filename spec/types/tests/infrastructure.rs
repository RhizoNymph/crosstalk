use std::num::NonZeroU32;
use std::time::Duration;

use crate::derived::flow::resource::Host;
use crate::derived::flow::transmission::{
    DelegationDirection, DirectCarrier, NonChannelRoute, Route,
};
use crate::interfaces::l0_ingress::{
    AuthHostRejected, InterceptAllowlist, ResponseFraming, ResponseHead,
};
use crate::interfaces::l2_transport::{InvalidRetryPolicy, RetryPolicy};
use crate::observed::exchange::Transport;

#[test]
fn retry_policy_requires_ordered_non_zero_backoff() {
    let attempts = NonZeroU32::new(5).expect("5 is not zero");
    assert_eq!(
        RetryPolicy::new(attempts, Duration::ZERO, Duration::from_secs(1)),
        Err(InvalidRetryPolicy::ZeroBackoff)
    );
    assert_eq!(
        RetryPolicy::new(attempts, Duration::from_secs(2), Duration::from_secs(1)),
        Err(InvalidRetryPolicy::InitialAboveMax)
    );
    let policy = RetryPolicy::new(attempts, Duration::from_secs(1), Duration::from_secs(1))
        .expect("equal bounds are allowed");
    assert_eq!(policy.max_attempts(), attempts);
}

#[test]
fn intercept_allowlist_rejects_auth_hosts() {
    for auth in InterceptAllowlist::AUTH_HOSTS {
        let host = Host((*auth).into());
        assert_eq!(
            InterceptAllowlist::new(vec![host.clone()]),
            Err(AuthHostRejected(host))
        );
    }
    let copilot = Host("api.individual.githubcopilot.com".into());
    let allowlist = InterceptAllowlist::new(vec![copilot.clone()]).expect("not an auth host");
    assert!(allowlist.contains(&copilot));
    assert!(!allowlist.contains(&Host("platform.claude.com".into())));
}

#[test]
fn non_channel_routes_convert_without_channel() {
    let cases = [
        (
            NonChannelRoute::Delegation(DelegationDirection::ChildToParent),
            Route::Delegation(DelegationDirection::ChildToParent),
        ),
        (
            NonChannelRoute::Direct(DirectCarrier::UserTurn),
            Route::Direct(DirectCarrier::UserTurn),
        ),
        (NonChannelRoute::Unobserved, Route::Unobserved),
    ];
    for (from, to) in cases {
        assert_eq!(Route::from(from), to);
    }
}

fn response_head(headers: &[(&str, &str)]) -> ResponseHead {
    ResponseHead {
        status: 200,
        headers: headers
            .iter()
            .map(|(name, value)| ((*name).into(), (*value).into()))
            .collect(),
    }
}

#[test]
fn event_stream_content_type_frames_as_sse() {
    let head = response_head(&[("content-type", "text/event-stream")]);
    assert_eq!(head.framing(), ResponseFraming::EventStream);
    assert_eq!(head.framing().transport(), Transport::Sse);
}

#[test]
fn event_stream_detection_ignores_case_and_parameters() {
    let head = response_head(&[("Content-Type", "Text/Event-Stream; charset=utf-8")]);
    assert_eq!(
        head.content_type(),
        Some("Text/Event-Stream; charset=utf-8")
    );
    assert_eq!(head.framing(), ResponseFraming::EventStream);
}

#[test]
fn other_content_types_frame_as_whole_body() {
    let json = response_head(&[("content-type", "application/json")]);
    assert_eq!(json.framing(), ResponseFraming::Whole);
    assert_eq!(json.framing().transport(), Transport::Http);
    let prefixed = response_head(&[("content-type", "text/event-stream-ish")]);
    assert_eq!(prefixed.framing(), ResponseFraming::Whole);
}

#[test]
fn missing_content_type_frames_as_whole_body() {
    let head = response_head(&[("x-request-id", "req_1")]);
    assert_eq!(head.content_type(), None);
    assert_eq!(head.framing(), ResponseFraming::Whole);
}

#[test]
fn framing_reads_the_first_content_type_header() {
    let head = response_head(&[
        ("content-type", "application/json"),
        ("content-type", "text/event-stream"),
    ]);
    assert_eq!(head.framing(), ResponseFraming::Whole);
}
