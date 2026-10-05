//! The HTTP tool contract: `http_request {method, url, body?}` and the
//! other configured names.

use proptest::prelude::*;
use serde_json::{Value, json};

use crosstalk_spec::derived::flow::access::AccessKind;
use crosstalk_spec::derived::flow::access::Extraction::Structured;
use crosstalk_spec::derived::flow::resource::{Host, Locator};
use crosstalk_spec::interfaces::l5_flow::{ExtractError, ResourceExtractor};
use crosstalk_spec::observed::message::{ToolOutcome, ToolResult};

use super::generate;
use super::support::*;
use crate::extract::{Classified, ExtractConfig, ExtractedOp, ToolExtractors, WriteOutcome};

use WriteOutcome::{Delivered, Rejected, Unknown};

fn http(
    name: &str,
    args: Value,
    result: Option<ToolResult>,
) -> Result<Vec<Classified>, ExtractError> {
    extract(
        &ExtractConfig::default(),
        &context(),
        &call(name, args),
        result.as_ref(),
    )
}

fn drop_box() -> Locator {
    https("dead-drops.example", "/box/7")
}

#[test]
fn http_tool_calls() {
    let url = "https://Dead-Drops.example:443/box/7#top";
    let cases: Vec<(&str, Value, Option<ToolResult>, Vec<Classified>)> = vec![
        (
            "http_request",
            json!({ "method": "GET", "url": url }),
            Some(ok("meet at 9")),
            vec![read(drop_box(), Structured)],
        ),
        (
            "http_request",
            json!({ "method": "head", "url": url }),
            Some(ok("")),
            vec![read(drop_box(), Structured)],
        ),
        (
            "http_request",
            json!({ "method": "GET", "url": url }),
            None,
            vec![],
        ),
        (
            "http_request",
            json!({ "method": "GET", "url": url }),
            Some(failed("404")),
            vec![],
        ),
        (
            "http_request",
            json!({ "method": "POST", "url": url, "body": "meet at 9" }),
            Some(ok("201 Created")),
            vec![write(drop_box(), Delivered, Structured)],
        ),
        (
            "http_request",
            json!({ "method": "put", "url": url, "content": "meet at 9" }),
            None,
            vec![write(drop_box(), Unknown, Structured)],
        ),
        (
            "http_request",
            json!({ "method": "PATCH", "url": url, "data": { "text": "x" } }),
            Some(failed("403 Forbidden")),
            vec![write(drop_box(), Rejected, Structured)],
        ),
        (
            "http_request",
            json!({ "method": "DELETE", "url": url }),
            Some(ok("")),
            vec![write(drop_box(), Delivered, Structured)],
        ),
        (
            "http_request",
            json!({ "method": "OPTIONS", "url": url }),
            Some(ok("")),
            vec![],
        ),
        (
            "fetch",
            json!({ "method": "GET", "url": url }),
            Some(ok("x")),
            vec![read(drop_box(), Structured)],
        ),
        (
            "curl",
            json!({ "method": "POST", "url": url, "text": "x" }),
            Some(ok("")),
            vec![write(drop_box(), Delivered, Structured)],
        ),
        // `web_fetch` with a method is an HTTP tool; without one, the fetch
        // tool of that name.
        (
            "web_fetch",
            json!({ "method": "POST", "url": url, "body": "x" }),
            Some(ok("")),
            vec![write(drop_box(), Delivered, Structured)],
        ),
        (
            "web_fetch",
            json!({ "url": url }),
            Some(ok("x")),
            vec![read(drop_box(), Structured)],
        ),
        // WebFetch stays read-only, method or not.
        (
            "WebFetch",
            json!({ "method": "POST", "url": url, "prompt": "x" }),
            Some(ok("x")),
            vec![read(drop_box(), Structured)],
        ),
        // A wiki edit through the API writes the page it names; a GET of the
        // article reads it.
        (
            "http_request",
            json!({
                "method": "POST",
                "url": "https://en.wikipedia.org/w/api.php",
                "body": "action=edit&title=dead_drop&appendtext=meet+at+9&token=t"
            }),
            Some(ok("{\"edit\":{\"result\":\"Success\"}}")),
            vec![write(
                https("en.wikipedia.org", "/wiki/Dead_drop"),
                Delivered,
                Structured,
            )],
        ),
        (
            "http_request",
            json!({
                "method": "POST",
                "url": "https://en.wikipedia.org/w/api.php",
                "body": { "action": "edit", "title": "Dead drop", "text": "x" }
            }),
            Some(ok("{}")),
            vec![write(
                https("en.wikipedia.org", "/wiki/Dead_drop"),
                Delivered,
                Structured,
            )],
        ),
        (
            "http_request",
            json!({ "method": "GET", "url": "https://en.m.wikipedia.org/wiki/dead_drop" }),
            Some(ok("meet at 9")),
            vec![read(
                https("en.wikipedia.org", "/wiki/Dead_drop"),
                Structured,
            )],
        ),
        // The method decides the op: an API query sent as POST is a write of
        // the API URL, not a read of a page.
        (
            "http_request",
            json!({
                "method": "POST",
                "url": "https://en.wikipedia.org/w/api.php",
                "body": "action=query&titles=Dead_drop"
            }),
            Some(ok("{}")),
            vec![write(
                https("en.wikipedia.org", "/w/api.php"),
                Delivered,
                Structured,
            )],
        ),
        (
            "http_request",
            json!({ "method": "PUT", "url": "https://api.github.com/repos/AgentVillage/Atlas/contents/notes.md", "body": "{}" }),
            Some(ok("{}")),
            vec![write(
                Locator::File {
                    host: Some(Host("github.com/agentvillage/atlas".to_owned())),
                    path: "/notes.md".to_owned(),
                },
                Delivered,
                Structured,
            )],
        ),
    ];
    for (name, args, result, expected) in cases {
        assert_eq!(
            http(name, args.clone(), result),
            Ok(expected),
            "{name} {args}"
        );
    }
    for (name, args) in [
        ("http_request", json!({ "url": url })),
        ("http_request", json!({ "method": "GET" })),
        (
            "http_request",
            json!({ "method": "GET", "url": "not a url" }),
        ),
        ("http_request", json!({ "method": 1, "url": url })),
        ("fetch", json!({ "url": url })),
    ] {
        assert!(
            matches!(
                http(name, args.clone(), Some(ok(""))),
                Err(ExtractError::Arguments { .. })
            ),
            "{name} {args}",
        );
    }
}

#[test]
fn http_tool_names_are_configured() {
    let config = ExtractConfig::from_json(r#"{"http_tools": ["my_http"]}"#).expect("valid");
    let context = context();
    let extractors = ToolExtractors::new(&config, &context);
    let args = json!({ "method": "POST", "url": "https://x.example/a", "body": "b" });
    assert!(extractors.handles(&call("my_http", args.clone())));
    assert!(!extractors.handles(&call("http_request", args.clone())));
    assert_eq!(
        extractors.extract_classified(&call("my_http", args), None),
        Ok(vec![write(https("x.example", "/a"), Unknown, Structured)]),
    );
    assert!(ExtractConfig::from_json(r#"{"http_tools": [""]}"#).is_err());
    assert_eq!(
        ExtractConfig::default().http_tools(),
        ["http_request", "fetch", "web_fetch", "curl"],
    );
}

const HTTP_TOOLS: [&str; 4] = ["http_request", "fetch", "web_fetch", "curl"];

fn method() -> impl Strategy<Value = String> {
    (
        prop::sample::select(
            &[
                "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "TRACE", "x",
            ][..],
        ),
        any::<bool>(),
    )
        .prop_map(|(method, lower)| {
            if lower {
                method.to_lowercase()
            } else {
                method.to_owned()
            }
        })
}

proptest! {
    /// `flow.extract.http-method-op`: the method alone decides the op.
    #[test]
    fn http_method_maps_to_op(
        name in prop::sample::select(&HTTP_TOOLS[..]),
        method in method(),
        url in generate::url(),
        body in prop::option::of(generate::json_value()),
        result in generate::tool_result(),
    ) {
        let mut args = json!({ "method": method, "url": url });
        if let (Some(body), Some(members)) = (body, args.as_object_mut()) {
            members.insert("body".to_owned(), body);
        }
        let got = http(name, args, result.clone());
        // Only a URL that is not one fails.
        let Ok(accesses) = got else {
            prop_assert!(crate::extract::resource::url_locator(&url).is_err());
            return Ok(());
        };
        let upper = method.to_uppercase();
        let delivered = result.as_ref().is_some_and(|r| r.outcome != ToolOutcome::Error);
        match upper.as_str() {
            "GET" | "HEAD" => {
                prop_assert!(accesses.iter().all(|a| a.op == ExtractedOp::Read));
                prop_assert_eq!(accesses.is_empty(), !delivered);
            }
            "POST" | "PUT" | "PATCH" | "DELETE" => {
                prop_assert!(!accesses.is_empty());
                prop_assert!(accesses.iter().all(|a| a.op.kind() == AccessKind::Write));
            }
            _ => prop_assert!(accesses.is_empty()),
        }
    }

    /// `flow.resource.http-url-tool-independent`: spellings of one URL, read
    /// and written through any HTTP tool with any method, are one resource,
    /// and that resource is the canonical URL.
    #[test]
    fn same_url_same_resource_across_tools(
        calls in prop::collection::vec(
            (
                prop::sample::select(&HTTP_TOOLS[..]),
                prop::sample::select(&["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"][..]),
                prop::sample::select(&[
                    "https://dead-drops.example/box/7?b=2&a=1",
                    "HTTPS://Dead-Drops.Example:443/box/./7?a=1&b=2#x",
                    "https://dead-drops.example/box/x/../7?a=1&b=2",
                    "https://dead-drops.example:443/box/%37?b=2&a=1&",
                ][..]),
                prop::option::of("\\PC{0,12}"),
            ),
            1..6,
        ),
    ) {
        let expected = url("https", "dead-drops.example", "/box/7", Some("a=1&b=2"));
        for (name, method, written, body) in calls {
            let mut args = json!({ "method": method, "url": written });
            if let (Some(body), Some(members)) = (body, args.as_object_mut()) {
                members.insert("body".to_owned(), Value::String(body));
            }
            let accesses = http(name, args, Some(ok("ok"))).expect("extracts");
            prop_assert_eq!(accesses.len(), 1);
            prop_assert_eq!(&accesses[0].locator, &expected);
        }
    }
}
