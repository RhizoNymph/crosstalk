//! The router served on a real socket: HTTP/1.1, an export sent chunked
//! with no `Content-Length`, and a graceful stop.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;

use super::fake::{ExportParts, Fake};
use super::{FULL, golden_bytes, server, token};
use crate::http::{bind, serve};
use crosstalk_spec::interfaces::l8_surface::export::{ExportLine, ExportRow, read_jsonl};
use crosstalk_spec::support::Timestamp;

/// One HTTP/1.1 exchange on a fresh connection; the whole response.
async fn exchange(addr: std::net::SocketAddr, request: String) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("the server accepts");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("the request is sent");
    let mut response = Vec::new();
    let mut chunk = [0_u8; 4096];
    // A cut response may end in a reset rather than an end of stream:
    // either way, what arrived is the response.
    while let Ok(read) = stream.read(&mut chunk).await {
        if read == 0 {
            break;
        }
        response.extend_from_slice(&chunk[..read]);
    }
    String::from_utf8(response).expect("a text response")
}

#[tokio::test]
async fn the_api_serves_on_a_listener_and_stops() {
    let read = read_jsonl(&golden_bytes("surface_reads/export/export_complete.jsonl"))
        .expect("the golden export reads");
    let fake = Arc::new(Fake::default());
    fake.respond("watermark", "\"2026-10-04T12:00:00.000000Z\"".to_owned());
    fake.export_with(ExportParts {
        header: read.header.clone(),
        rows: read.rows,
        trailer: read.trailer.expect("a trailer"),
    });
    let listener = bind("127.0.0.1:0".parse().expect("an address"))
        .await
        .expect("a free port");
    let addr = listener.local_addr().expect("bound");
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(serve(listener, server(&fake), async {
        let _ = stopped.await;
    }));
    let auth = format!("Authorization: Bearer {}\r\n", token(FULL));

    let response = exchange(
        addr,
        format!("GET /watermark HTTP/1.1\r\nHost: api\r\n{auth}Connection: close\r\n\r\n"),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    assert!(
        response.contains("\r\ncache-control: no-store\r\n"),
        "{response}"
    );
    assert!(
        response.ends_with("\r\n\r\n\"2026-10-04T12:00:00.000000Z\""),
        "{response}"
    );

    let body = serde_json::to_string(read.header.request()).expect("JSON");
    let response = exchange(
        addr,
        format!(
            "POST /exports HTTP/1.1\r\nHost: api\r\n{auth}Content-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    let head = response
        .split("\r\n\r\n")
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    assert!(head.contains("\r\ntransfer-encoding: chunked"), "{head}");
    assert!(!head.contains("\r\ncontent-length:"), "{head}");
    assert!(
        response.ends_with("\r\n0\r\n\r\n"),
        "the body ends with its last chunk"
    );

    stop.send(()).expect("the server is running");
    server
        .await
        .expect("the server task ends")
        .expect("a clean stop");
}

/// A line the server cannot write (a row whose timestamp has no JSON)
/// means no trailer can follow: the response is cut without its last
/// chunk, so the client's HTTP stack sees the cut, and the body read so
/// far has no trailer.
#[tokio::test]
async fn an_export_that_cannot_end_with_its_trailer_is_cut() {
    let read = read_jsonl(&golden_bytes("surface_reads/export/export_complete.jsonl"))
        .expect("the golden export reads");
    let mut unwritable: ExportRow = super::golden("surface_reads/export/export_row_verdicts");
    let ExportRow::Verdict(verdict) = &mut unwritable else {
        panic!("a verdict row");
    };
    verdict.at = Timestamp::from_micros(u64::MAX);
    assert!(
        serde_json::to_string(&unwritable).is_err(),
        "no JSON after year 9999"
    );
    let mut rows = read.rows.clone();
    rows.insert(1, unwritable);
    let fake = Arc::new(Fake::default());
    fake.export_with(ExportParts {
        header: read.header.clone(),
        rows,
        trailer: read.trailer.clone().expect("a trailer"),
    });
    let listener = bind("127.0.0.1:0".parse().expect("an address"))
        .await
        .expect("a free port");
    let addr = listener.local_addr().expect("bound");
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(serve(listener, server(&fake), async {
        let _ = stopped.await;
    }));
    let body = serde_json::to_string(read.header.request()).expect("JSON");
    let response = exchange(
        addr,
        format!(
            "POST /exports HTTP/1.1\r\nHost: api\r\nAuthorization: Bearer {}\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            token(FULL),
            body.len()
        ),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    assert!(
        !response.ends_with("\r\n0\r\n\r\n"),
        "no terminating chunk: {response}"
    );
    let header_line =
        serde_json::to_string(&ExportLine::Header(Box::new(read.header.clone()))).expect("JSON");
    assert!(response.contains(&header_line), "the header was sent");
    assert!(!response.contains("\"type\":\"trailer\""), "no trailer");
    stop.send(()).expect("the server is running");
    server
        .await
        .expect("the server task ends")
        .expect("a clean stop");
}
