//! The shared wiki: a tiny HTTP page store, in memory. This is the channel
//! the swarm's agents talk through, and the one crosstalk should discover.
//!
//! - `GET /pages`: `{"pages": [{"page", "version", "bytes", "author"}]}`
//! - `GET /pages/<name>`: the text (`text/plain`), with `x-wiki-version`
//!   and `x-wiki-author`; 404 when there is no such page
//! - `PUT /pages/<name>`: the body (UTF-8) becomes the page; `x-wiki-author`
//!   names the writer; 201 when created, 200 when replaced, with
//!   `{"page", "version", "bytes"}`
//! - `GET /healthz`
//!
//! One task owns the pages; handlers reach it over a channel with one-shot
//! answers.

pub mod store;

use std::net::SocketAddr;

use bytes::Bytes;
use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use hyper::header::HeaderValue;
use hyper::{Method, Request, Response, StatusCode};
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use crate::http::{DemoBody, json_response, serve, text_response};
use crate::protocol::PageSlug;
use store::{Author, Page, Summary, Wiki, WriteError, Written};

/// The header naming a page's writer.
pub const AUTHOR_HEADER: &str = "x-wiki-author";
/// The header carrying a page's version.
pub const VERSION_HEADER: &str = "x-wiki-version";

/// What the wiki serves with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WikiConfig {
    pub listen: SocketAddr,
    pub max_page_bytes: usize,
    pub max_pages: usize,
}

/// Why the wiki could not start.
#[derive(Debug, thiserror::Error)]
pub enum WikiError {
    #[error("binding {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        source: std::io::Error,
    },
}

/// A request to the task that owns the pages.
#[derive(Debug)]
enum Command {
    Get(PageSlug, oneshot::Sender<Option<Page>>),
    Put {
        slug: PageSlug,
        text: String,
        author: Author,
        answer: oneshot::Sender<Result<Written, WriteError>>,
    },
    List(oneshot::Sender<Vec<Summary>>),
}

/// Binds `config.listen` and serves until `shutdown` resolves.
pub async fn run(
    config: WikiConfig,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), WikiError> {
    let listener = TcpListener::bind(config.listen)
        .await
        .map_err(|source| WikiError::Bind {
            addr: config.listen,
            source,
        })?;
    serve_on(listener, config, shutdown).await;
    Ok(())
}

/// Serves on an already bound listener (tests bind port 0).
pub async fn serve_on(
    listener: TcpListener,
    config: WikiConfig,
    shutdown: impl std::future::Future<Output = ()>,
) {
    tracing::info!(
        listen = %listener.local_addr().map_or_else(|e| e.to_string(), |a| a.to_string()),
        max_page_bytes = config.max_page_bytes,
        max_pages = config.max_pages,
        "wiki serving"
    );
    let (commands, inbox) = mpsc::channel(256);
    let owner = tokio::spawn(own(
        Wiki::new(config.max_page_bytes, config.max_pages),
        inbox,
    ));
    let max = config.max_page_bytes;
    serve(
        listener,
        move |request| handle(request, commands.clone(), max),
        shutdown,
    )
    .await;
    owner.abort();
}

/// The task that owns every page.
async fn own(mut wiki: Wiki, mut inbox: mpsc::Receiver<Command>) {
    while let Some(command) = inbox.recv().await {
        // An answer channel is closed when its client went away; nothing to
        // tell it then.
        match command {
            Command::Get(slug, answer) => {
                let _ = answer.send(wiki.get(&slug).cloned());
            }
            Command::Put {
                slug,
                text,
                author,
                answer,
            } => {
                let bytes = text.len();
                let written = wiki.put(slug.clone(), text, author.clone());
                match &written {
                    Ok(w) => {
                        tracing::debug!(page = %slug, version = w.version, bytes, author = author.as_str(), "page written")
                    }
                    Err(error) => tracing::warn!(page = %slug, %error, "page write refused"),
                }
                let _ = answer.send(written);
            }
            Command::List(answer) => {
                let _ = answer.send(wiki.list());
            }
        }
    }
}

fn error(status: StatusCode, message: &str) -> Response<DemoBody> {
    json_response(status, &json!({"error": message}))
}

fn stopped() -> Response<DemoBody> {
    error(StatusCode::SERVICE_UNAVAILABLE, "the wiki is stopping")
}

async fn ask<T>(
    commands: &mpsc::Sender<Command>,
    command: impl FnOnce(oneshot::Sender<T>) -> Command,
) -> Option<T> {
    let (answer, answered) = oneshot::channel();
    commands.send(command(answer)).await.ok()?;
    answered.await.ok()
}

async fn handle(
    request: Request<Incoming>,
    commands: mpsc::Sender<Command>,
    max_page_bytes: usize,
) -> Response<DemoBody> {
    let path = request.uri().path().to_owned();
    if path == "/healthz" && request.method() == Method::GET {
        return text_response(StatusCode::OK, "ok");
    }
    if path == "/pages" && request.method() == Method::GET {
        return match ask(&commands, Command::List).await {
            Some(pages) => json_response(StatusCode::OK, &json!({"pages": pages})),
            None => stopped(),
        };
    }
    let Some(name) = path.strip_prefix("/pages/") else {
        return error(StatusCode::NOT_FOUND, "no such route");
    };
    let slug: PageSlug = match name.parse() {
        Ok(slug) => slug,
        Err(bad) => return error(StatusCode::BAD_REQUEST, &bad.to_string()),
    };
    match request.method().clone() {
        Method::GET => match ask(&commands, |a| Command::Get(slug.clone(), a)).await {
            Some(Some(page)) => {
                let mut response = text_response(StatusCode::OK, Bytes::from(page.text));
                let headers = response.headers_mut();
                headers.insert(VERSION_HEADER, HeaderValue::from(page.version));
                if let Ok(author) = HeaderValue::from_str(page.author.as_str()) {
                    headers.insert(AUTHOR_HEADER, author);
                }
                response
            }
            Some(None) => error(
                StatusCode::NOT_FOUND,
                &format!("page {slug} does not exist"),
            ),
            None => stopped(),
        },
        Method::PUT => {
            let author = match request.headers().get(AUTHOR_HEADER) {
                None => Author::anonymous(),
                Some(value) => match value.to_str().ok().and_then(Author::new) {
                    Some(author) => author,
                    None => return error(StatusCode::BAD_REQUEST, "bad x-wiki-author"),
                },
            };
            // One byte over the limit is enough to know it is too large.
            let body = match Limited::new(request.into_body(), max_page_bytes + 1)
                .collect()
                .await
            {
                Ok(body) => body.to_bytes(),
                Err(_) => return error(StatusCode::PAYLOAD_TOO_LARGE, "page too large"),
            };
            let Ok(text) = String::from_utf8(body.to_vec()) else {
                return error(StatusCode::BAD_REQUEST, "page text is not UTF-8");
            };
            let bytes = text.len();
            let put = ask(&commands, |answer| Command::Put {
                slug: slug.clone(),
                text,
                author,
                answer,
            })
            .await;
            match put {
                Some(Ok(written)) => json_response(
                    if written.created {
                        StatusCode::CREATED
                    } else {
                        StatusCode::OK
                    },
                    &json!({"page": slug.as_str(), "version": written.version, "bytes": bytes}),
                ),
                Some(Err(refused @ WriteError::TooLarge { .. })) => {
                    error(StatusCode::PAYLOAD_TOO_LARGE, &refused.to_string())
                }
                Some(Err(refused @ WriteError::Full { .. })) => {
                    error(StatusCode::INSUFFICIENT_STORAGE, &refused.to_string())
                }
                None => stopped(),
            }
        }
        _ => error(StatusCode::METHOD_NOT_ALLOWED, "GET or PUT"),
    }
}
