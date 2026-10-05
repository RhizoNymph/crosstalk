//! `Locator::Repository`: one canonical locator per forge repository.

use proptest::prelude::*;

use crate::derived::flow::resource::{Host, InvalidRepository, Locator, ResourcePattern};

fn atlas() -> Locator {
    Locator::Repository {
        host: Host("github.com".into()),
        owner: "agentvillage".into(),
        name: "atlas".into(),
    }
}

#[test]
fn spellings_of_one_repository_are_one_locator() {
    for (host, owner, name) in [
        ("github.com", "agentvillage", "atlas"),
        ("GitHub.com", "AgentVillage", "Atlas"),
        ("www.github.com", "agentvillage", "atlas.git"),
        ("github.com:443", "/agentvillage/", "/Atlas.git/"),
        ("github.com.", "agentvillage", "atlas"),
    ] {
        assert_eq!(
            Locator::repository(host, owner, name),
            Ok(atlas()),
            "{host} {owner} {name}"
        );
    }
}

#[test]
fn nested_groups_keep_their_path() {
    assert_eq!(
        Locator::repository("GitLab.com", "Village/Infra", "Atlas"),
        Ok(Locator::Repository {
            host: Host("gitlab.com".into()),
            owner: "village/infra".into(),
            name: "atlas".into(),
        })
    );
}

#[test]
fn invalid_parts_are_refused() {
    assert_eq!(
        Locator::repository("", "a", "b"),
        Err(InvalidRepository::Host(String::new()))
    );
    assert_eq!(
        Locator::repository("git hub.com", "a", "b"),
        Err(InvalidRepository::Host("git hub.com".into()))
    );
    assert_eq!(
        Locator::repository("github.com", "a//b", "c"),
        Err(InvalidRepository::Owner("a//b".into()))
    );
    assert_eq!(
        Locator::repository("github.com", "..", "c"),
        Err(InvalidRepository::Owner("..".into()))
    );
    assert_eq!(
        Locator::repository("github.com", "a", ".git"),
        Err(InvalidRepository::Name(".git".into()))
    );
    assert_eq!(
        Locator::repository("github.com", "a", "b/c"),
        Err(InvalidRepository::Name("b/c".into()))
    );
}

#[test]
fn a_repository_files_host_is_its_path() {
    assert_eq!(
        atlas().repository_file_host(),
        Some(Host("github.com/agentvillage/atlas".into()))
    );
    let file = Locator::File {
        host: None,
        path: "/x".into(),
    };
    assert_eq!(file.repository_file_host(), None);
}

#[test]
fn only_an_exact_pattern_matches_a_repository() {
    assert!(ResourcePattern::Exact(atlas()).matches(&atlas()));
    assert!(!ResourcePattern::Host(Host("github.com".into())).matches(&atlas()));
    assert!(
        !ResourcePattern::PathPrefix {
            host: Some(Host("github.com".into())),
            prefix: "/agentvillage".into(),
        }
        .matches(&atlas())
    );
}

fn segment() -> impl Strategy<Value = String> {
    "[A-Za-z0-9][A-Za-z0-9_.-]{0,8}".prop_filter("not a dot segment", |s| s != "." && s != "..")
}

proptest! {
    /// `flow.resource.repository-canonical` (spec side): canonicalizing is
    /// idempotent, and case, `www.`, a port and `.git` never change the
    /// locator.
    #[test]
    fn repository_locator_is_canonical(
        host in "[a-z][a-z0-9-]{0,8}\\.[a-z]{2,4}",
        owner in prop::collection::vec(segment(), 1..3),
        name in segment().prop_filter("not only .git", |s| !s.eq_ignore_ascii_case(".git")),
        upper in any::<bool>(),
        www in any::<bool>(),
        port in prop::option::of(1u16..),
        git in any::<bool>(),
    ) {
        let owner = owner.join("/");
        let Ok(plain) = Locator::repository(&host, &owner, &name) else {
            return Ok(());
        };
        let case = |text: &str| if upper { text.to_ascii_uppercase() } else { text.to_owned() };
        let mut spelled_host = case(&host);
        if www {
            spelled_host = format!("www.{spelled_host}");
        }
        if let Some(port) = port {
            spelled_host = format!("{spelled_host}:{port}");
        }
        let spelled_name = if git { format!("{}.git", case(&name)) } else { case(&name) };
        prop_assert_eq!(
            Locator::repository(&spelled_host, &case(&owner), &spelled_name),
            Ok(plain.clone())
        );
        let Locator::Repository { host, owner, name } = &plain else {
            return Err(TestCaseError::fail("not a repository"));
        };
        prop_assert_eq!(Locator::repository(&host.0, owner, name), Ok(plain.clone()));
    }
}
