//! `serve --role all` end to end: the gateway's own start, on ephemeral
//! ports, with a `Live` process behind the HTTP API. The scenario is
//! ingested through the live pipeline, settled past the watermark's bound,
//! and read back over HTTP with crosstalk-client: the transmissions export
//! and the evidence page both show the confirmed A→B transmission.

use crosstalk_client::{BaseUrl, BearerToken, ClientConfig, HttpClient};
use crosstalk_e2e::read::{self, Agents};
use crosstalk_e2e::{feed, options};
use crosstalk_gateway::config::{ApiOperator, GatewayConfig};
use crosstalk_gateway::gateway::{self, Running};
use crosstalk_gateway::live::LiveClock;
use crosstalk_gateway::role::Role;
use crosstalk_memory::support::ManualClock;
use crosstalk_spec::aggregates::filter::TopologyFilter;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::derived::provenance::matching::Carrier;
use crosstalk_spec::interfaces::l8_surface::QueryApi;
use crosstalk_spec::interfaces::l8_surface::export::{
    ExportDataset, ExportFormat, ExportRequest, ExportRow, ExportScope, ExportStep, ExportStream,
};
use crosstalk_spec::interfaces::l8_surface::operators::RequestIdentity;
use crosstalk_spec::support::Timestamp;

use crate::support::{Failure, relay, unexpected};

const SECRET_ENV: &str = "CROSSTALK_SERVE_TEST_SECRET_V1";
const SECRET_HEX: &str = "5ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2";
const TOKEN_ENV: &str = "CROSSTALK_SERVE_TEST_API_TOKEN";
const TOKEN: &str = "serve-test-token-0123456789abcdef";

fn ms(duration: std::time::Duration) -> u128 {
    duration.as_millis()
}

fn config(data_dir: &std::path::Path) -> Result<GatewayConfig, Failure> {
    let json = serde_json::json!({
        "ingress": {
            "listen": "127.0.0.1:0",
            "routes": [{
                "name": "anthropic",
                "prefix": "/anthropic",
                "upstream": {
                    "id": "anthropic",
                    "kind": {"type": "vendor_api", "data": {"type": "anthropic"}},
                    "base_url": "http://127.0.0.1:9",
                },
            }],
            "secrets": {"current": {"version": 1, "env": SECRET_ENV}},
            "capture": {"channel_capacity": 64},
        },
        "api": {"listen": "127.0.0.1:0", "token": {"env": TOKEN_ENV}},
        "ops": {"listen": "127.0.0.1:0"},
        "blobs": {"root": data_dir.join("blobs")},
        "flow": {
            "correlation_window_ms": ms(options::CORRELATION_WINDOW),
            "evidence_window_ms": ms(options::EVIDENCE_WINDOW),
            "suspected_ttl_ms": ms(options::SUSPECTED_TTL),
            "shards": 1,
            "tick_ms": 50,
        },
    });
    Ok(GatewayConfig::from_json(&json.to_string())?)
}

fn lookup(name: &str) -> Option<String> {
    match name {
        SECRET_ENV => Some(SECRET_HEX.to_owned()),
        TOKEN_ENV => Some(TOKEN.to_owned()),
        _ => None,
    }
}

fn client(running: &Running) -> Result<HttpClient, Failure> {
    let addr = running
        .api_addr()
        .ok_or_else(|| unexpected("role all serves the api"))?;
    let base = BaseUrl::parse(&format!("http://{addr}"))?;
    Ok(HttpClient::new(base, ClientConfig::default()).with_token(BearerToken::new(TOKEN)?))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_all_exports_and_shows_the_confirmed_transmission_over_http() -> Result<(), Failure> {
    let dir = tempfile::tempdir()?;
    let scenario = relay();
    let clock = ManualClock::at(scenario.start);
    let running = gateway::start_on(
        &config(&dir.path().join("data"))?,
        Role::All,
        lookup,
        LiveClock::Manual(clock.clone()),
    )
    .await?;
    let live = running
        .live()
        .ok_or_else(|| unexpected("role all runs a live process"))?;
    feed(&scenario, live.pipeline(), |at| clock.set(at)).await?;
    let bound = options::EVIDENCE_WINDOW + options::SUSPECTED_TTL + options::BUCKET;
    let until =
        Timestamp::from_micros(scenario.ends_at().as_micros() + u64::try_from(bound.as_micros())?);
    live.settle(until).await?;
    assert!(live.watermark() > scenario.ends_at());

    // The caller argument is ignored over HTTP: the token names the
    // operator. One is still needed to call the traits.
    let caller = live
        .caller(RequestIdentity::Verified(ApiOperator::ID))
        .await
        .map_err(|error| unexpected(format!("caller: {error:?}")))?;
    let http = client(&running)?;
    let window = read::window(&scenario, options::BUCKET)?;
    let Agents { a, b } = read::agents(&http, &caller, &scenario, window)
        .await?
        .agents
        .ok_or_else(|| unexpected("agents a and b over http"))?;

    let request = ExportRequest::new(
        ExportDataset::Transmissions(ExportScope {
            window,
            filter: TopologyFilter::default(),
        }),
        ExportFormat::Jsonl,
        false,
    )
    .map_err(|error| unexpected(format!("export request: {error:?}")))?;
    let export = http
        .export(&caller, &request)
        .await
        .map_err(|error| unexpected(format!("export: {error:?}")))?;
    let mut rows = Vec::new();
    let mut stream = export.rows;
    let trailer = loop {
        match stream.next().await {
            ExportStep::Row(row, rest) => {
                rows.push(row);
                stream = rest;
            }
            ExportStep::End(trailer) => break trailer,
        }
    };
    assert!(trailer.is_complete(), "{trailer:?}");
    let [ExportRow::Transmission(row)] = rows.as_slice() else {
        return Err(unexpected(format!(
            "{} rows, expected one transmission",
            rows.len()
        )));
    };
    assert_eq!(row.summary().to, b);
    assert_eq!(row.delivery().from, a);
    assert!(
        matches!(row.summary().route, Route::Channel(_)),
        "{:?}",
        row.summary().route
    );
    let id = row.summary().id;

    let evidence = read::evidence(&http, &caller, id)
        .await?
        .ok_or_else(|| unexpected("no evidence page over http"))?;
    let (read_id, _, _) = crosstalk_e2e::scenario::read_call();
    assert!(
        evidence.matches().iter().any(|found| matches!(
            found.content_match().carrier(),
            Carrier::ToolResult(call) if call.0 == read_id
        )),
        "no match carried by B's read"
    );

    // The token's operator is the configured admin; without the token,
    // nothing is served.
    let operators = http
        .operators(&caller)
        .await
        .map_err(|error| unexpected(format!("operators: {error:?}")))?;
    assert_eq!(
        operators
            .iter()
            .map(|operator| operator.name.as_str())
            .collect::<Vec<_>>(),
        ["admin"]
    );
    assert!(
        http.without_token().operators(&caller).await.is_err(),
        "a request without the token was served"
    );

    running.shutdown().await;
    Ok(())
}
