use crosstalk_client::{BaseUrl, BearerToken as ClientToken, InvalidBaseUrl};
use crosstalk_spec::interfaces::l8_surface::PermissionSet;
use crosstalk_spec::interfaces::l8_surface::operators::InvalidOperatorName;

use super::*;

const TOKEN_ENV: &str = "CROSSTALK_API_TOKEN";
const TOKEN: &str = "ui-config-test-token-0123456789";

/// The environment of these tests: only the API token is set.
fn env(name: &str) -> Option<String> {
    (name == TOKEN_ENV).then(|| TOKEN.to_owned())
}

fn no_env(_: &str) -> Option<String> {
    None
}

fn parse(text: &str) -> Result<Config, ParseError> {
    Config::parse(text, env)
}

fn http(backend: &str) -> String {
    format!(r#"{{"listen":"127.0.0.1:1","backend":{{"http":{backend}}}}}"#)
}

const LOCAL_OPERATOR: &str = r#""operator":{"id":"00000000000000000000000001","name":"a"}"#;

fn local(backend: &str) -> String {
    format!(r#"{{"listen":"127.0.0.1:1",{LOCAL_OPERATOR},"backend":{backend}}}"#)
}

#[test]
fn parses_the_shipped_config() {
    let text = include_str!("../../config.json");
    let config = parse(text).ok().expect("shipped config parses");
    let BackendConfig::Fixture {
        access,
        seed,
        replay,
    } = &config.backend
    else {
        panic!("the shipped config is the fixture's: {:?}", config.backend);
    };
    assert_eq!(access.name(), "researcher");
    assert_eq!((*seed, *replay), (7, None));
    assert_eq!(access.caller().permissions(), PermissionSet::ALL);
}

#[test]
fn parses_the_demo_config_with_a_replay() {
    let text = include_str!("../../config.demo.json");
    let config = parse(text).ok().expect("demo config parses");
    let BackendConfig::Fixture { seed, replay, .. } = config.backend else {
        panic!("the demo config is the fixture's");
    };
    assert_eq!(seed, 7);
    assert_eq!(
        replay,
        Some(ReplayConfig {
            window_minutes: 240,
            speed: 10
        })
    );
}

#[test]
fn parses_the_world_backend() {
    let config = parse(&local(r#"{"world":{"seed":7}}"#))
        .ok()
        .expect("world config parses");
    let BackendConfig::World { access, seed } = config.backend else {
        panic!("a world backend");
    };
    assert_eq!(seed, 7);
    assert_eq!(access.name(), "a");
}

#[test]
fn the_live_backend_is_gone() {
    let live = local(r#"{"live":{}}"#);
    assert!(matches!(parse(&live), Err(ParseError::Json(_))));
}

#[test]
fn parses_the_shipped_http_config_with_the_token_from_the_environment() {
    let text = include_str!("../../config.http.json");
    let config = parse(text).ok().expect("http config parses");
    let BackendConfig::Http(http) = config.backend else {
        panic!("an http backend");
    };
    assert_eq!(
        http.url,
        BaseUrl::parse("http://crosstalk:8081").expect("url")
    );
    assert_eq!(http.token, ClientToken::new(TOKEN).expect("token"));
}

#[test]
fn an_http_url_may_carry_a_prefix() {
    let text =
        http(r#"{"url":"http://127.0.0.1:8081/api/","token":{"env":"CROSSTALK_API_TOKEN"}}"#);
    let BackendConfig::Http(http) = parse(&text).ok().expect("parses").backend else {
        panic!("an http backend");
    };
    assert_eq!(http.url.to_string(), "http://127.0.0.1:8081/api");
}

#[test]
fn the_token_is_never_inline() {
    let inline =
        http(r#"{"url":"http://crosstalk:8081","token":"ui-config-test-token-0123456789"}"#);
    assert!(matches!(parse(&inline), Err(ParseError::Json(_))));
    let literal = http(
        r#"{"url":"http://crosstalk:8081","token":{"value":"ui-config-test-token-0123456789"}}"#,
    );
    assert!(matches!(parse(&literal), Err(ParseError::Json(_))));
}

#[test]
fn an_unset_token_variable_fails() {
    let text = http(r#"{"url":"http://crosstalk:8081","token":{"env":"CROSSTALK_API_TOKEN"}}"#);
    assert!(matches!(
        Config::parse(&text, no_env),
        Err(ParseError::Config(ConfigError::TokenUnset { env })) if env == TOKEN_ENV
    ));
}

#[test]
fn a_short_or_malformed_token_fails() {
    let text = http(r#"{"url":"http://crosstalk:8081","token":{"env":"CROSSTALK_API_TOKEN"}}"#);
    let short = |_: &str| Some("too-short".to_owned());
    assert!(matches!(
        Config::parse(&text, short),
        Err(ParseError::Config(ConfigError::Token {
            reason: TokenError::TooShort { min: 16 },
            ..
        }))
    ));
    let spaced = |_: &str| Some("not a b64token but long enough".to_owned());
    assert!(matches!(
        Config::parse(&text, spaced),
        Err(ParseError::Config(ConfigError::Token {
            reason: TokenError::NotB64Token,
            ..
        }))
    ));
}

#[test]
fn an_invalid_url_fails() {
    for (url, why) in [
        ("https://crosstalk:8081", InvalidBaseUrl::NotHttp),
        ("http://crosstalk:8081/?a=1", InvalidBaseUrl::Query),
    ] {
        let text = http(&format!(
            r#"{{"url":"{url}","token":{{"env":"CROSSTALK_API_TOKEN"}}}}"#
        ));
        match parse(&text) {
            Err(ParseError::Config(ConfigError::Url(error))) => assert_eq!(error, why, "{url}"),
            Err(ParseError::Json(error)) => panic!("{url}: {error}"),
            Err(ParseError::Config(error)) => panic!("{url}: {error}"),
            Ok(_) => panic!("{url} parsed"),
        }
    }
    let nonsense = http(r#"{"url":"not a url","token":{"env":"CROSSTALK_API_TOKEN"}}"#);
    assert!(matches!(
        parse(&nonsense),
        Err(ParseError::Config(ConfigError::Url(
            InvalidBaseUrl::NotAUri { .. }
        )))
    ));
}

#[test]
fn the_http_backend_names_no_operator() {
    // The server says who the token is (`QueryApi::me`): an operator id in
    // the http section is an unknown key.
    let text = http(
        r#"{"url":"http://crosstalk:8081","token":{"env":"CROSSTALK_API_TOKEN"},"operator":"00000000000000000000000002"}"#,
    );
    assert!(matches!(parse(&text), Err(ParseError::Json(_))));
}

#[test]
fn unknown_http_keys_fail() {
    let text = http(
        r#"{"url":"http://crosstalk:8081","token":{"env":"CROSSTALK_API_TOKEN"},"timeout":1}"#,
    );
    assert!(matches!(parse(&text), Err(ParseError::Json(_))));
    let token_key = http(
        r#"{"url":"http://crosstalk:8081","token":{"env":"CROSSTALK_API_TOKEN","default":"x"}}"#,
    );
    assert!(matches!(parse(&token_key), Err(ParseError::Json(_))));
}

#[test]
fn the_http_backend_takes_its_operator_from_the_server_not_the_config() {
    let text = format!(
        r#"{{"listen":"127.0.0.1:1",{LOCAL_OPERATOR},"backend":{{"http":{{"url":"http://crosstalk:8081","token":{{"env":"CROSSTALK_API_TOKEN"}}}}}}}}"#
    );
    assert!(matches!(
        parse(&text),
        Err(ParseError::Config(ConfigError::OperatorBesideHttp))
    ));
}

#[test]
fn local_backends_need_a_trusted_operator() {
    for backend in [r#"{"fixture":{"seed":1}}"#, r#"{"world":{"seed":1}}"#] {
        let text = format!(r#"{{"listen":"127.0.0.1:1","backend":{backend}}}"#);
        assert!(
            matches!(
                parse(&text),
                Err(ParseError::Config(ConfigError::MissingOperator { .. }))
            ),
            "{backend}"
        );
    }
}

#[test]
fn rejects_unknown_keys_and_blank_names() {
    let unknown = format!(
        r#"{{"listen":"127.0.0.1:1",{LOCAL_OPERATOR},"backend":{{"fixture":{{"seed":1}}}},"extra":1}}"#
    );
    assert!(matches!(parse(&unknown), Err(ParseError::Json(_))));
    let blank = r#"{"listen":"127.0.0.1:1","operator":{"id":"00000000000000000000000001","name":"  "},"backend":{"fixture":{"seed":1}}}"#;
    assert!(matches!(
        parse(blank),
        Err(ParseError::Config(ConfigError::OperatorName(
            InvalidOperatorName::Blank
        )))
    ));
}

#[test]
fn the_token_never_shows_in_debug_output() {
    let text = include_str!("../../config.http.json");
    let config = parse(text).ok().expect("http config parses");
    assert!(!format!("{config:?}").contains(TOKEN));
}
