//! `crosstalk healthcheck --url <url>`: GET a URL and succeed on a 2xx.
//! The deployment's runtime image has no shell and no curl, so the
//! container healthcheck runs this. Plain HTTP only (the ops listener is
//! never behind TLS inside the container).

use std::time::Duration;

use bytes::Bytes;
use http_body_util::Empty;
use hyper::client::conn::http1;
use hyper::header::HOST;
use hyper::{Request, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;

/// How long the whole check may take.
pub const TIMEOUT: Duration = Duration::from_secs(5);

/// Why the check failed.
#[derive(Debug, thiserror::Error)]
pub enum CheckError {
    #[error("not an http:// URL with a host: {0}")]
    BadUrl(String),
    #[error("connecting to {authority}: {source}")]
    Connect {
        authority: String,
        source: std::io::Error,
    },
    #[error("HTTP exchange failed: {0}")]
    Http(#[from] hyper::Error),
    #[error("building the request: {0}")]
    Request(#[from] hyper::http::Error),
    #[error("no answer within {0:?}")]
    TimedOut(Duration),
    #[error("answered {0}")]
    Status(StatusCode),
}

/// GET `url`; `Ok` with the status when it is 2xx.
pub async fn check(url: &str) -> Result<StatusCode, CheckError> {
    tokio::time::timeout(TIMEOUT, get(url))
        .await
        .map_err(|_| CheckError::TimedOut(TIMEOUT))?
}

async fn get(url: &str) -> Result<StatusCode, CheckError> {
    let uri: Uri = url
        .parse()
        .map_err(|_| CheckError::BadUrl(url.to_owned()))?;
    if uri.scheme_str() != Some("http") {
        return Err(CheckError::BadUrl(url.to_owned()));
    }
    let Some(host) = uri.host() else {
        return Err(CheckError::BadUrl(url.to_owned()));
    };
    let port = uri.port_u16().unwrap_or(80);
    let authority = format!("{host}:{port}");
    let stream = TcpStream::connect((host.trim_matches(['[', ']']), port))
        .await
        .map_err(|source| CheckError::Connect {
            authority: authority.clone(),
            source,
        })?;
    let (mut sender, connection) = http1::handshake(TokioIo::new(stream)).await?;
    let driver = tokio::spawn(connection);
    let target = uri
        .path_and_query()
        .map_or("/", |path| path.as_str())
        .to_owned();
    let request = Request::get(target)
        .header(HOST, authority)
        .body(Empty::<Bytes>::new())?;
    let response = sender.send_request(request).await;
    driver.abort();
    let status = response?.status();
    if status.is_success() {
        Ok(status)
    } else {
        Err(CheckError::Status(status))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_plain_http_urls_with_a_host_are_checked() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        runtime.block_on(async {
            for url in [
                "https://127.0.0.1:9464/readyz",
                "127.0.0.1:9464",
                "not a url",
                "/readyz",
            ] {
                assert!(
                    matches!(check(url).await, Err(CheckError::BadUrl(_))),
                    "{url}"
                );
            }
        });
    }
}
