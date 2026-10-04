use std::num::NonZeroU32;
use std::time::Duration;

use crate::derived::flow::resource::Host;
use crate::derived::flow::transmission::{
    DelegationDirection, DirectCarrier, NonChannelRoute, Route,
};
use crate::interfaces::l0_ingress::{AuthHostRejected, InterceptAllowlist};
use crate::interfaces::l2_transport::{InvalidRetryPolicy, RetryPolicy};

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
