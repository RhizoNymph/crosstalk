use std::time::Duration;

use crate::aggregates::alert::{AlertRule, AlertRuleKind};
use crate::derived::flow::channel::policy::{Decision, Policy, PolicyAuthor, TrafficVerdict};
use crate::derived::flow::evidence::CoAccess;
use crate::derived::flow::resource::{Host, Locator, ResourcePattern};
use crate::derived::flow::transmission::{Confirmed, MixedMatches};
use crate::observed::message::ToolName;
use crate::support::NonEmpty;
use crate::tests::fixtures::{access, agent, at, content_match};

fn url(host: &str, path: &str) -> Locator {
    Locator::Url {
        scheme: "https".into(),
        host: Host(host.into()),
        path: path.into(),
        query: None,
    }
}

fn file(path: &str) -> Locator {
    Locator::File {
        host: None,
        path: path.into(),
    }
}

#[test]
fn confirmed_requires_one_origin() {
    let content = NonEmpty::from_vec(vec![
        content_match(agent(1), agent(3), 10),
        content_match(agent(2), agent(3), 10),
    ])
    .expect("two matches");
    assert_eq!(
        Confirmed::new(content, Vec::new(), at(5)),
        Err(MixedMatches::SeveralOrigins)
    );
}

#[test]
fn confirmed_requires_one_reader() {
    let content = NonEmpty::from_vec(vec![
        content_match(agent(1), agent(3), 10),
        content_match(agent(1), agent(4), 10),
    ])
    .expect("two matches");
    assert_eq!(
        Confirmed::new(content, Vec::new(), at(5)),
        Err(MixedMatches::SeveralReaders)
    );
}

#[test]
fn confirmed_takes_sender_from_matches_and_sums_bytes() {
    let content = NonEmpty::from_vec(vec![
        content_match(agent(1), agent(3), 10),
        content_match(agent(1), agent(3), 32),
    ])
    .expect("two matches");
    let co_access = vec![CoAccess {
        write: access(1),
        read: access(2),
        lag: Duration::from_secs(30),
    }];
    let confirmed = Confirmed::new(content, co_access, at(5)).expect("one origin, one reader");
    assert_eq!(confirmed.from(), agent(1));
    assert_eq!(confirmed.matched_bytes(), 42);
    assert_eq!(confirmed.co_access().len(), 1);
    assert_eq!(confirmed.at(), at(5));
}

#[test]
fn exact_pattern_matches_only_that_locator() {
    let pattern = ResourcePattern::Exact(url("wiki.example", "/a"));
    assert!(pattern.matches(&url("wiki.example", "/a")));
    assert!(!pattern.matches(&url("wiki.example", "/b")));
}

#[test]
fn host_pattern_matches_any_path_on_host() {
    let pattern = ResourcePattern::Host(Host("wiki.example".into()));
    assert!(pattern.matches(&url("wiki.example", "/")));
    assert!(pattern.matches(&url("wiki.example", "/deep/page")));
    assert!(!pattern.matches(&url("other.example", "/")));
    assert!(!pattern.matches(&file("/wiki.example")));
}

#[test]
fn url_prefix_pattern_needs_host_and_prefix() {
    let pattern = ResourcePattern::UrlPrefix {
        host: Host("forum.example".into()),
        path_prefix: "/t/".into(),
    };
    assert!(pattern.matches(&url("forum.example", "/t/123")));
    assert!(!pattern.matches(&url("forum.example", "/u/123")));
    assert!(!pattern.matches(&url("evil.example", "/t/123")));
}

#[test]
fn path_prefix_pattern_matches_files_only() {
    let pattern = ResourcePattern::PathPrefix {
        host: None,
        prefix: "/shared/".into(),
    };
    assert!(pattern.matches(&file("/shared/notes.md")));
    assert!(!pattern.matches(&file("/home/notes.md")));
    assert!(!pattern.matches(&url("shared", "/shared/notes.md")));
}

#[test]
fn mcp_server_pattern_matches_any_tool_on_server() {
    let pattern = ResourcePattern::McpServer("notion".into());
    let on_server = Locator::Mcp {
        server: "notion".into(),
        tool: ToolName("search".into()),
        target: None,
    };
    let elsewhere = Locator::Mcp {
        server: "linear".into(),
        tool: ToolName("search".into()),
        target: None,
    };
    assert!(pattern.matches(&on_server));
    assert!(!pattern.matches(&elsewhere));
}

#[test]
fn policy_routes_traffic() {
    let decision = Decision {
        by: PolicyAuthor::Config,
        at: at(1),
        note: None,
    };
    assert_eq!(
        Policy::Unreviewed.on_traffic(),
        TrafficVerdict::Raise(AlertRuleKind::UnreviewedTraffic)
    );
    assert_eq!(
        Policy::Unsanctioned(decision.clone()).on_traffic(),
        TrafficVerdict::Raise(AlertRuleKind::UnsanctionedTraffic)
    );
    assert_eq!(
        Policy::Sanctioned(decision).on_traffic(),
        TrafficVerdict::Drop
    );
}

#[test]
fn every_alert_rule_reports_its_kind() {
    let cases = [
        (AlertRule::NewChannel, AlertRuleKind::NewChannel),
        (
            AlertRule::UnreviewedTraffic,
            AlertRuleKind::UnreviewedTraffic,
        ),
        (
            AlertRule::UnsanctionedTraffic,
            AlertRuleKind::UnsanctionedTraffic,
        ),
        (AlertRule::SanctionedUnused, AlertRuleKind::SanctionedUnused),
        (
            AlertRule::SuspectedTransmission,
            AlertRuleKind::SuspectedTransmission,
        ),
    ];
    for (rule, kind) in cases {
        assert_eq!(rule.kind(), kind);
    }
}
