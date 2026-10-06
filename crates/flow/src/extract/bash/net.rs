//! `curl` and `wget` as HTTP requests ([`HttpRequest`]), with the form
//! fields they send, so site rules see a wiki edit whose title is in the
//! body.
//!
//! The method is the explicit one (`-X`, `--method`), else `POST` when a
//! body is sent (`-d`, `--json`, `-F`, `-T`, `--post-data`), else `GET`
//! (`-G` sends the data as the query; `-I` is `HEAD`). The response body
//! reaches the result unless stdout goes to a file or the command saves it
//! (`curl -o FILE`/`-O`, `wget` without `-O -`); a saved-to file is
//! written.
//!
//! Each request's accesses are judged by its output
//! ([`CommandRule::Http`]): a 4xx/5xx status the output shows rejects
//! them.

use crosstalk_spec::derived::flow::access::Extraction;
use crosstalk_spec::derived::flow::resource::Locator;

use crate::extract::http::{self, HttpRequest, Method, form_fields};
use crate::extract::op::Candidate;
use crate::extract::outcome::CommandRule;
use crate::extract::resource::url_locator;

use super::commands::{Found, Shell};
use super::lex::Word;
use super::options::{OptSpec, Options};

const CURL: OptSpec = OptSpec {
    short_values: "XdHouAeFTbcmwxKrEzYyCQ",
    long_values: &[
        "--request",
        "--data",
        "--data-raw",
        "--data-binary",
        "--data-urlencode",
        "--data-ascii",
        "--json",
        "--header",
        "--output",
        "--user",
        "--user-agent",
        "--referer",
        "--form",
        "--form-string",
        "--upload-file",
        "--cookie",
        "--cookie-jar",
        "--max-time",
        "--connect-timeout",
        "--write-out",
        "--proxy",
        "--proxy-user",
        "--retry",
        "--retry-delay",
        "--retry-max-time",
        "--config",
        "--range",
        "--cacert",
        "--cert",
        "--key",
        "--resolve",
        "--connect-to",
        "--url",
        "--output-dir",
        "--variable",
        "--oauth2-bearer",
        "--aws-sigv4",
        "--unix-socket",
        "--limit-rate",
        "--max-filesize",
        "--interface",
        "--continue-at",
        "--speed-limit",
        "--speed-time",
        "--time-cond",
        "--proto",
        "--proto-redir",
        "--trace",
        "--trace-ascii",
        "--stderr",
        "--netrc-file",
        "--header-file",
    ],
};

const WGET: OptSpec = OptSpec {
    short_values: "OoaiPUtTeBlwQD",
    long_values: &[
        "--output-document",
        "--output-file",
        "--append-output",
        "--input-file",
        "--directory-prefix",
        "--user-agent",
        "--tries",
        "--timeout",
        "--execute",
        "--post-data",
        "--post-file",
        "--body-data",
        "--body-file",
        "--method",
        "--header",
        "--user",
        "--password",
        "--referer",
        "--level",
        "--wait",
        "--quota",
        "--domains",
        "--load-cookies",
        "--save-cookies",
        "--base",
    ],
};

const CURL_URLENCODED: [&str; 5] = [
    "-d",
    "--data",
    "--data-ascii",
    "--data-binary",
    "--data-raw",
];

const CURL_BODY: [&str; 12] = [
    "-d",
    "--data",
    "--data-raw",
    "--data-binary",
    "--data-urlencode",
    "--data-ascii",
    "--json",
    "-F",
    "--form",
    "--form-string",
    "-T",
    "--upload-file",
];

impl Shell<'_> {
    pub(super) fn curl(&self, args: &[Word], stdout_to_file: bool, found: &mut Vec<Found>) {
        let parsed = Options::parse(args, &CURL);
        let forced_get = parsed.has(&["-G", "--get"]);
        let method = match parsed.last(&["-X", "--request"]) {
            Some(method) => Method::parse(&method.text),
            None if parsed.has(&["-I", "--head"]) => Method::Head,
            None if parsed.has(&CURL_BODY) && !forced_get => Method::Post,
            None => Method::Get,
        };
        let mut form = Vec::new();
        for (name, value) in parsed.with_values() {
            let Some(text) = value.as_literal() else {
                continue;
            };
            if CURL_URLENCODED.contains(&name) {
                if name == "--data-raw" || !text.starts_with('@') {
                    form.extend(form_fields(text));
                }
            } else if name == "--data-urlencode" {
                // `name=content`, content not yet encoded; `@file` forms
                // name file content the gateway cannot see.
                if let Some((field, content)) = text.split_once('=')
                    && !field.contains('@')
                {
                    form.push((field.to_owned(), content.to_owned()));
                }
            } else if matches!(name, "-F" | "--form" | "--form-string")
                && let Some((field, content)) = text.split_once('=')
                && (name == "--form-string" || !content.starts_with(['@', '<']))
            {
                form.push((field.to_owned(), content.to_owned()));
            }
        }
        let mut body_to_result =
            !stdout_to_file && !parsed.has(&["-O", "--remote-name", "--remote-name-all"]);
        for output in parsed.values(&["-o", "--output"]) {
            if output.text != "-" {
                body_to_result = false;
                if let Some(locator) = self.file(output) {
                    self.found(found, Candidate::write(locator, Extraction::Parsed));
                }
            }
        }
        let mut urls: Vec<&Word> = parsed.operands.clone();
        urls.extend(parsed.values(&["--url"]));
        let status_written = parsed.values(&["-w", "--write-out"]).iter().any(|format| {
            format.text.contains("http_code") || format.text.contains("response_code")
        });
        let rule = CommandRule::Http { status_written };
        self.requests(&urls, method, &form, body_to_result, rule, found);
    }

    pub(super) fn wget(&self, args: &[Word], stdout_to_file: bool, found: &mut Vec<Found>) {
        let parsed = Options::parse(args, &WGET);
        let body = parsed.last(&["--post-data", "--body-data"]);
        let method = match parsed.last(&["--method"]) {
            Some(method) => Method::parse(&method.text),
            None if body.is_some() || parsed.has(&["--post-file", "--body-file"]) => Method::Post,
            None => Method::Get,
        };
        let form = body
            .and_then(Word::as_literal)
            .map(form_fields)
            .unwrap_or_default();
        let mut body_to_result = false;
        for output in parsed.values(&["-O", "--output-document"]) {
            if output.text == "-" {
                body_to_result = !stdout_to_file;
            } else if let Some(locator) = self.file(output) {
                self.found(found, Candidate::write(locator, Extraction::Parsed));
            }
        }
        let rule = CommandRule::Http {
            status_written: false,
        };
        self.requests(&parsed.operands, method, &form, body_to_result, rule, found);
    }

    fn requests(
        &self,
        urls: &[&Word],
        method: Method,
        form: &[(String, String)],
        body_to_result: bool,
        rule: CommandRule,
        found: &mut Vec<Found>,
    ) {
        for word in urls {
            let Some(url) = word.as_literal().and_then(command_url) else {
                continue;
            };
            let request = HttpRequest {
                url,
                method,
                form: form.to_vec(),
            };
            self.request(&request, body_to_result, rule, found);
        }
    }

    /// The accesses of one request, judged by `rule`.
    pub(super) fn request(
        &self,
        request: &HttpRequest,
        body_to_result: bool,
        rule: CommandRule,
        found: &mut Vec<Found>,
    ) {
        for candidate in http::candidates(request, body_to_result, Extraction::Parsed, self.sites) {
            self.found(found, candidate.judged_by(rule));
        }
    }
}

/// A URL operand: with a scheme, or a bare host (curl assumes http).
fn command_url(text: &str) -> Option<Locator> {
    if text.contains("://") {
        return url_locator(text).ok();
    }
    let host = text.split(['/', '?', '#']).next().unwrap_or(text);
    let host_name = host.split(':').next().unwrap_or(host);
    let looks_like_host = !host_name.is_empty()
        && !text.starts_with('-')
        && (host_name.contains('.') || host_name == "localhost")
        && host_name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-'));
    if looks_like_host {
        url_locator(&format!("http://{text}")).ok()
    } else {
        None
    }
}
