//! Configuration is checked when built.

use std::num::NonZeroU32;
use std::time::Duration;

use crate::{BaseUrl, ClientConfig, InvalidBaseUrl, InvalidConfig, ReconnectPolicy};

#[test]
fn a_base_url_is_an_http_origin_and_an_optional_prefix() {
    for (text, shown) in [
        ("http://gateway:8081", "http://gateway:8081"),
        ("http://gateway:8081/", "http://gateway:8081"),
        ("http://127.0.0.1:8081/api", "http://127.0.0.1:8081/api"),
        ("http://gateway/api/v/", "http://gateway/api/v"),
    ] {
        let base = BaseUrl::parse(text).unwrap_or_else(|error| panic!("{text}: {error}"));
        assert_eq!(base.to_string(), shown);
    }
    assert_eq!(
        BaseUrl::parse("http://gateway:8081/api").map(|base| base.join("/watermark", "")),
        Ok("http://gateway:8081/api/watermark".to_owned())
    );
    assert_eq!(
        BaseUrl::parse("http://gateway:8081").map(|base| base.join("/agents", "page=1")),
        Ok("http://gateway:8081/agents?page=1".to_owned())
    );
    assert_eq!(BaseUrl::parse("https://gateway"), Err(InvalidBaseUrl::NotHttp));
    assert_eq!(BaseUrl::parse("/api"), Err(InvalidBaseUrl::NotHttp));
    assert_eq!(BaseUrl::parse("http://gateway/?a=1"), Err(InvalidBaseUrl::Query));
    assert!(matches!(
        BaseUrl::parse("http://gate way"),
        Err(InvalidBaseUrl::NotAUri { .. })
    ));
}

#[test]
fn reconnect_delays_double_up_to_the_cap() {
    let policy = ReconnectPolicy::new(
        NonZeroU32::new(6).unwrap_or(NonZeroU32::MIN),
        Duration::from_millis(100),
        Duration::from_millis(500),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    let delays: Vec<u128> = (1..=6).map(|n| policy.delay(n).as_millis()).collect();
    assert_eq!(delays, vec![0, 100, 200, 400, 500, 500]);
    assert_eq!(policy.delay(u32::MAX), Duration::from_millis(500));
    assert_eq!(
        ReconnectPolicy::new(NonZeroU32::MIN, Duration::from_secs(2), Duration::from_secs(1)),
        Err(InvalidConfig::DelayCapBelowFirst)
    );
}

#[test]
fn no_timeout_or_limit_is_zero() {
    let config = ClientConfig::default();
    assert_eq!(
        config.with_request_timeout(Duration::ZERO),
        Err(InvalidConfig::ZeroTimeout)
    );
    assert_eq!(
        config.with_idle_timeout(Duration::ZERO),
        Err(InvalidConfig::ZeroTimeout)
    );
    assert_eq!(
        config.with_max_response_bytes(0),
        Err(InvalidConfig::ZeroLimit)
    );
    assert!(!config.request_timeout().is_zero());
    assert!(config.idle_timeout() > config.request_timeout());
}
