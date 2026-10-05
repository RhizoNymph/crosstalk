//! Reads and writes of shared resources, tagged from bash commands.
//!
//! Agents share code and pages through repositories and web APIs. This
//! module reads an executed bash command and its output and says which
//! canonical resource ([`super::resource`]) it read or wrote:
//!
//! | Command | Access | Resource from |
//! | --- | --- | --- |
//! | `git push` | write | the output's `To <remote>` line, else a URL argument, else the directory's known remote |
//! | `git clone <url>` | read | the URL (and the clone's directory learns it) |
//! | `git pull`, `git fetch` | read | the output's `From <remote>` line, else a URL argument, else the directory's known remote |
//! | `gh`/`glab` `issue`/`pr`/`mr` `create`, `comment`, `note`, `edit`, `close`, `reopen`, `merge`, `review`, `approve` | write | `-R`/`--repo`, else a URL argument, else the directory's remote, else a URL in the output |
//! | `gh`/`glab` `issue`/`pr`/`mr` `view`, `list`, `diff`, `checks`, `status` | read | the same |
//! | `gh api` / `glab api` (`repos/…`, `projects/…`) | the method (`-X`, else POST with a field flag, else GET) | the API path |
//! | `curl` | the method (`-X`/`--request`, `-I` HEAD, else POST with a data/form flag unless `-G`, PUT with `-T`, else GET) | each URL argument |
//! | `wget` | the method (`--method`, else POST with `--post-data`/`--post-file`, else GET) | each URL argument |
//!
//! HTTP commands follow the agreed L5 `HttpTool` contract ([`http`]):
//! `GET`/`HEAD` read, `POST`/`PUT`/`PATCH`/`DELETE` write, any other method
//! is no access, and each such access keeps the `http_request` call it is
//! equivalent to ([`Access::http`]). git and the forge CLIs' issue, PR and
//! MR commands speak their own protocols: only a Bash extractor sees them
//! (`http` is `None`).
//!
//! Each write carries the spec's `WriteOutcome`, judged from the output
//! ([`outcome`]); a read whose output shows a failure is no access.
//!
//! [`Shell`] keeps what one agent's shell has revealed: its working
//! directory (the bash tool is one persistent shell) and the remote of each
//! directory, learnt from `cd`, `git clone`, `git remote add/set-url`,
//! `git remote -v` output and push/pull output. A write keeps its
//! **payload**: the text it carried (`--body`, `--title`, `--description`,
//! `--message` of issue and review commands, `-d`/`--data`/`--json` of curl),
//! so a reader's output can be checked for it.

pub mod http;
pub mod outcome;
pub mod shell;

use std::collections::BTreeMap;

use crosstalk_spec::derived::flow::access::WriteOutcome;
use crosstalk_spec::derived::flow::resource::Locator;

pub use http::{HTTP_TOOL, HttpMethod, HttpRequest};

use super::resource::{Forge, from_remote, from_url, repo, urls};
use shell::{SimpleCommand, commands, heredoc_argument};

/// The bash tool's home directory in the village's computers.
pub const HOME: &str = "/home/computeruse";

/// A read, or a write with its outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Op {
    Read,
    Write(WriteOutcome),
}

impl Op {
    pub fn is_write(self) -> bool {
        matches!(self, Self::Write(_))
    }

    /// Whether this access can be one side of a pair: every read, and
    /// every write the spec pairs (`WriteOutcome::pairs`).
    pub fn pairs(self) -> bool {
        match self {
            Self::Read => true,
            Self::Write(outcome) => outcome.pairs(),
        }
    }
}

/// The command that made an access.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Tool {
    Git,
    GitHubCli,
    GitLabCli,
    Curl,
    Wget,
}

/// One read or write of a canonical resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Access {
    pub op: Op,
    pub resource: Locator,
    pub tool: Tool,
    /// What the command did, for reports (`git push`, `glab issue comment`).
    pub verb: String,
    /// Text a write carried; empty for reads.
    pub payload: Vec<String>,
    /// The `http_request` call an HTTP command is equivalent to; `None`
    /// for git and the forge CLIs' issue commands (Bash extractor only).
    pub http: Option<HttpRequest>,
}

impl Access {
    /// Whether L5's `HttpTool` extractor would see this access had the
    /// agent used the equivalent call.
    pub fn http_visible(&self) -> bool {
        self.http.is_some()
    }
}

/// An access before its output is judged.
#[derive(Debug, Clone)]
struct Draft {
    write: bool,
    resource: Locator,
    tool: Tool,
    verb: String,
    payload: Vec<String>,
    http: Option<HttpRequest>,
}

impl Draft {
    /// The access, judged from `output`: a write gets its outcome, a read
    /// with a failed output is dropped.
    fn judge(self, output: &str) -> Option<Access> {
        let op = if self.write {
            Op::Write(outcome::write_outcome(self.tool, output))
        } else if outcome::failed(output) {
            return None;
        } else {
            Op::Read
        };
        Some(Access {
            op,
            resource: self.resource,
            tool: self.tool,
            verb: self.verb,
            payload: self.payload,
            http: self.http,
        })
    }
}

/// One agent's shell: working directory and known remotes.
#[derive(Debug, Clone)]
pub struct Shell {
    cwd: String,
    remotes: BTreeMap<String, Locator>,
}

impl Default for Shell {
    fn default() -> Self {
        Self {
            cwd: HOME.to_owned(),
            remotes: BTreeMap::new(),
        }
    }
}

const WRITE_VERBS: &[&str] = &[
    "create", "comment", "note", "edit", "close", "reopen", "merge", "review", "approve",
];
const READ_VERBS: &[&str] = &["view", "list", "diff", "checks", "status"];
const PAYLOAD_FLAGS: &[&str] = &[
    "--body",
    "-b",
    "--title",
    "-t",
    "--description",
    "--message",
    "-m",
];
const CURL_DATA: &[&str] = &[
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
];

impl Shell {
    pub fn cwd(&self) -> &str {
        &self.cwd
    }

    /// The remote this shell knows for `dir`.
    pub fn remote_of(&self, dir: &str) -> Option<&Locator> {
        self.remotes.get(dir)
    }

    /// The accesses of one executed command, given its output (stdout and
    /// stderr together). Updates the shell's directory and remotes.
    pub fn accesses(&mut self, command: &str, output: &str) -> Vec<Access> {
        self.drafts(command, output)
            .into_iter()
            .filter_map(|draft| draft.judge(output))
            .collect()
    }

    fn drafts(&mut self, command: &str, output: &str) -> Vec<Draft> {
        let mut out = Vec::new();
        for simple in commands(command) {
            let words = strip_prefixes(&simple);
            let Some(program) = words.first() else {
                continue;
            };
            let program = program.rsplit('/').next().unwrap_or(program);
            match program {
                "cd" => {
                    let target = words.get(1).map_or(HOME, String::as_str);
                    self.cwd = self.resolve(target);
                }
                "git" => out.extend(self.git(&words[1..], output)),
                "gh" => out.extend(self.forge_cli(Forge::GitHub, &words[1..], output)),
                "glab" => out.extend(self.forge_cli(Forge::GitLab, &words[1..], output)),
                "curl" => out.extend(curl(&words[1..])),
                "wget" => out.extend(wget(&words[1..])),
                _ => {}
            }
        }
        out
    }

    /// `path` as an absolute directory (`~` is the home directory).
    fn resolve(&self, path: &str) -> String {
        let path = path.trim();
        let absolute = if let Some(rest) = path.strip_prefix('~') {
            format!("{HOME}{rest}")
        } else if path.starts_with('/') {
            path.to_owned()
        } else {
            format!("{}/{path}", self.cwd)
        };
        crate::reference::route::normalize_path(&absolute)
    }

    fn learn(&mut self, dir: String, remote: &Locator) {
        self.remotes.insert(dir, remote.clone());
    }

    fn git(&mut self, args: &[String], output: &str) -> Vec<Draft> {
        let mut dir = self.cwd.clone();
        let mut at = 0;
        while at < args.len() && args[at].starts_with('-') {
            if (args[at] == "-C" || args[at] == "-c") && at + 1 < args.len() {
                if args[at] == "-C" {
                    dir = self.resolve(&args[at + 1]);
                }
                at += 2;
            } else {
                at += 1;
            }
        }
        let Some(sub) = args.get(at) else {
            return Vec::new();
        };
        let rest: Vec<&String> = args[at + 1..]
            .iter()
            .filter(|a| !a.starts_with('-'))
            .collect();
        let url_arg = rest.iter().find_map(|a| from_remote(a));
        let access = |write: bool, resource: Locator, verb: &str| Draft {
            write,
            resource,
            tool: Tool::Git,
            verb: format!("git {verb}"),
            payload: Vec::new(),
            http: None,
        };
        match sub.as_str() {
            "clone" => {
                let Some(resource) = url_arg else {
                    return Vec::new();
                };
                let target = rest.get(1).map(|d| d.as_str()).unwrap_or_else(|| {
                    rest.first()
                        .and_then(|url| url.trim_end_matches('/').rsplit('/').next())
                        .map_or("", |name| name.trim_end_matches(".git"))
                });
                if !target.is_empty() {
                    let target = resolve_in(&dir, target);
                    self.learn(target, &resource);
                }
                if outcome::failed(output) {
                    return Vec::new();
                }
                vec![access(false, resource, "clone")]
            }
            "push" | "pull" | "fetch" => {
                let marker = if sub == "push" { "To " } else { "From " };
                let from_output = output.lines().find_map(|line| {
                    line.trim()
                        .strip_prefix(marker)
                        .and_then(|rest| from_remote(rest.split_whitespace().next()?))
                });
                let resource = from_output
                    .or(url_arg)
                    .or_else(|| self.remotes.get(&dir).cloned());
                let Some(resource) = resource else {
                    return Vec::new();
                };
                self.learn(dir, &resource);
                vec![access(sub == "push", resource, sub)]
            }
            "remote" => {
                match rest.first().map(|s| s.as_str()) {
                    Some("add") | Some("set-url") => {
                        if let Some(resource) = rest.get(2).and_then(|url| from_remote(url)) {
                            self.learn(dir, &resource);
                        }
                    }
                    _ => {
                        // `git remote -v`: learn the first fetch remote.
                        if let Some(resource) = output.lines().find_map(|line| {
                            let mut fields = line.split_whitespace();
                            let _name = fields.next()?;
                            from_remote(fields.next()?)
                        }) {
                            self.learn(dir, &resource);
                        }
                    }
                }
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    fn forge_cli(&mut self, forge: Forge, args: &[String], output: &str) -> Vec<Draft> {
        let tool = match forge {
            Forge::GitHub => Tool::GitHubCli,
            Forge::GitLab => Tool::GitLabCli,
        };
        let program = match forge {
            Forge::GitHub => "gh",
            Forge::GitLab => "glab",
        };
        let Some(noun) = args.first() else {
            return Vec::new();
        };
        if noun == "api" {
            return api(forge, tool, &args[1..]).into_iter().collect();
        }
        if !matches!(noun.as_str(), "issue" | "pr" | "mr") {
            return Vec::new();
        }
        let Some(verb) = args.get(1) else {
            return Vec::new();
        };
        let write = if WRITE_VERBS.contains(&verb.as_str()) {
            true
        } else if READ_VERBS.contains(&verb.as_str()) {
            false
        } else {
            return Vec::new();
        };
        let flag = |names: &[&str]| -> Vec<String> {
            let mut values = Vec::new();
            let mut at = 2;
            while at < args.len() {
                let word = &args[at];
                if let Some((name, value)) = word.split_once('=')
                    && names.contains(&name)
                {
                    values.push(value.to_owned());
                } else if names.contains(&word.as_str()) && at + 1 < args.len() {
                    values.push(args[at + 1].clone());
                    at += 1;
                }
                at += 1;
            }
            values
        };
        let named = flag(&["-R", "--repo"])
            .into_iter()
            .find_map(|value| from_url(&value).or_else(|| repo(forge, &value)));
        let url_arg = args[2..]
            .iter()
            .filter(|a| a.starts_with("http"))
            .find_map(|a| from_url(a));
        let output_url = || urls(output).into_iter().find_map(from_url);
        let Some(resource) = named
            .or(url_arg)
            .or_else(|| self.remotes.get(&self.cwd).cloned())
            .or_else(output_url)
        else {
            return Vec::new();
        };
        let mut payload_flags: Vec<&str> = PAYLOAD_FLAGS.to_vec();
        if forge == Forge::GitLab {
            payload_flags.push("-d");
        }
        let payload = if write {
            flag(&payload_flags)
                .into_iter()
                .map(|value| heredoc_argument(&value).unwrap_or(value))
                .filter(|value| !value.trim().is_empty())
                .collect()
        } else {
            Vec::new()
        };
        vec![Draft {
            write,
            resource,
            tool,
            verb: format!("{program} {noun} {verb}"),
            payload,
            http: None,
        }]
    }
}

fn resolve_in(dir: &str, target: &str) -> String {
    if target.starts_with('/') {
        crate::reference::route::normalize_path(target)
    } else if let Some(rest) = target.strip_prefix('~') {
        crate::reference::route::normalize_path(&format!("{HOME}{rest}"))
    } else {
        crate::reference::route::normalize_path(&format!("{dir}/{target}"))
    }
}

/// The words after environment assignments and `sudo`, `env`, `timeout N`,
/// `time`, `nohup`, `command`, `exec`.
fn strip_prefixes(simple: &SimpleCommand) -> Vec<String> {
    let mut words = simple.words.as_slice();
    loop {
        match words.first().map(String::as_str) {
            Some(word)
                if word.contains('=')
                    && !word.starts_with('-')
                    && word.split('=').next().is_some_and(|name| {
                        !name.is_empty()
                            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                    }) =>
            {
                words = &words[1..];
            }
            Some("sudo" | "env" | "time" | "nohup" | "command" | "exec") => words = &words[1..],
            Some("timeout") => {
                words = &words[1..];
                while words.first().is_some_and(|w| w.starts_with('-')) {
                    words = &words[1..];
                }
                if !words.is_empty() {
                    words = &words[1..];
                }
            }
            _ => break,
        }
    }
    words.to_vec()
}

/// `gh api` / `glab api` on a repository or project path: the method is
/// `-X`, else POST with a field flag, else GET; field values are the
/// payload and the body (a JSON object, as the CLIs send it).
fn api(forge: Forge, tool: Tool, args: &[String]) -> Option<Draft> {
    let mut method: Option<String> = None;
    let mut fields: Vec<(String, String)> = Vec::new();
    let mut input = false;
    let mut path: Option<&str> = None;
    let mut at = 0;
    while at < args.len() {
        let word = args[at].as_str();
        match word {
            "-X" | "--method" => {
                method = args.get(at + 1).cloned();
                at += 1;
            }
            "-f" | "-F" | "--field" | "--raw-field" => {
                if let Some((name, value)) = args.get(at + 1).and_then(|f| f.split_once('=')) {
                    fields.push((name.to_owned(), value.to_owned()));
                }
                at += 1;
            }
            "--input" => {
                input = true;
                at += 1;
            }
            _ if word.starts_with('-') => {}
            _ if path.is_none() => path = Some(word),
            _ => {}
        }
        at += 1;
    }
    let path = path?.trim_start_matches('/');
    let (url, resource) = match forge {
        Forge::GitHub => {
            let url = format!("https://api.github.com/{path}");
            let segments: Vec<&str> = path.split(['/', '?']).collect();
            let resource = match segments.as_slice() {
                ["repos", owner, name, ..] => repo(Forge::GitHub, &format!("{owner}/{name}")),
                _ => None,
            }?;
            (url, resource)
        }
        Forge::GitLab => {
            let path = path.strip_prefix("api/v4/").unwrap_or(path);
            if !path.starts_with("projects/") {
                return None;
            }
            let url = format!("https://gitlab.com/api/v4/{path}");
            let resource = from_url(&url)?;
            (url, resource)
        }
    };
    let method = match method {
        Some(name) => HttpMethod::parse(&name)?,
        None if !fields.is_empty() || input => HttpMethod::Post,
        None => HttpMethod::Get,
    };
    let write = method.writes();
    let body = (write && !fields.is_empty()).then(|| {
        serde_json::Value::Object(
            fields
                .iter()
                .map(|(name, value)| (name.clone(), value.clone().into()))
                .collect(),
        )
        .to_string()
    });
    let payload = if write {
        fields
            .into_iter()
            .map(|(_, value)| value)
            .filter(|value| !value.trim().is_empty())
            .collect()
    } else {
        Vec::new()
    };
    let program = match forge {
        Forge::GitHub => "gh",
        Forge::GitLab => "glab",
    };
    Some(Draft {
        write,
        resource,
        tool,
        verb: format!("{program} api"),
        payload,
        http: Some(HttpRequest { method, url, body }),
    })
}

/// Combined short options (`-sSL`, `-sX`) as separate flags; the last one
/// keeps any value that follows.
fn split_short(args: &[String]) -> Vec<String> {
    let mut out = Vec::with_capacity(args.len());
    for arg in args {
        match arg.strip_prefix('-') {
            Some(flags)
                if flags.len() > 1
                    && !flags.starts_with('-')
                    && flags.chars().all(|c| c.is_ascii_alphabetic()) =>
            {
                out.extend(flags.chars().map(|c| format!("-{c}")));
            }
            _ => out.push(arg.clone()),
        }
    }
    out
}

fn curl(args: &[String]) -> Vec<Draft> {
    let args = split_short(args);
    let args = args.as_slice();
    let mut method: Option<String> = None;
    let mut data: Vec<String> = Vec::new();
    let mut get = false;
    let mut upload = false;
    let mut targets: Vec<&str> = Vec::new();
    let mut at = 0;
    while at < args.len() {
        let word = args[at].as_str();
        let (name, inline) = match word.split_once('=') {
            Some((name, value)) if name.starts_with("--") => (name, Some(value)),
            _ => (word, None),
        };
        let value = || {
            inline
                .map(str::to_owned)
                .or_else(|| args.get(at + 1).cloned())
        };
        let takes_value = inline.is_none();
        match name {
            "-X" | "--request" => {
                method = value();
                if takes_value {
                    at += 1;
                }
            }
            "-G" | "--get" => get = true,
            "-I" | "--head" => method = Some("HEAD".to_owned()),
            "-T" | "--upload-file" => {
                upload = true;
                if takes_value {
                    at += 1;
                }
            }
            "--url" => match inline {
                Some(url) => targets.push(url),
                None => {
                    if let Some(url) = args.get(at + 1) {
                        targets.push(url.as_str());
                    }
                    at += 1;
                }
            },
            _ if CURL_DATA.contains(&name) => {
                if let Some(value) = value() {
                    data.push(value);
                }
                if takes_value {
                    at += 1;
                }
            }
            // Options with a value the converter does not need.
            "-H" | "--header" | "-o" | "--output" | "-u" | "--user" | "-A" | "--user-agent"
            | "-e" | "--referer" | "-m" | "--max-time" | "-w" | "--write-out" | "-b"
            | "--cookie" | "-c" | "--cookie-jar" | "--connect-timeout" | "--retry" => {
                if takes_value {
                    at += 1;
                }
            }
            _ if word.starts_with("http://") || word.starts_with("https://") => {
                targets.push(word);
            }
            _ => {}
        }
        at += 1;
    }
    let method = match method {
        Some(name) => match HttpMethod::parse(&name) {
            Some(method) => method,
            None => return Vec::new(),
        },
        None if upload => HttpMethod::Put,
        None if !data.is_empty() && !get => HttpMethod::Post,
        None => HttpMethod::Get,
    };
    let write = method.writes();
    let sent: Vec<String> = data
        .into_iter()
        .filter(|value| !value.starts_with('@'))
        .collect();
    let body = (write && !sent.is_empty()).then(|| sent.join("&"));
    let payload = if write { sent } else { Vec::new() };
    drafts(targets, method, body, payload, Tool::Curl, "curl")
}

/// One draft per URL target of an HTTP command.
fn drafts(
    targets: Vec<&str>,
    method: HttpMethod,
    body: Option<String>,
    payload: Vec<String>,
    tool: Tool,
    program: &str,
) -> Vec<Draft> {
    let write = method.writes();
    targets
        .into_iter()
        .filter_map(|url| Some((url, from_url(url)?)))
        .map(|(url, resource)| Draft {
            write,
            resource,
            tool,
            verb: format!("{program} {}", if write { "write" } else { "read" }),
            payload: payload.clone(),
            http: Some(HttpRequest {
                method,
                url: url.to_owned(),
                body: body.clone(),
            }),
        })
        .collect()
}

fn wget(args: &[String]) -> Vec<Draft> {
    let mut method: Option<String> = None;
    let mut post: Option<String> = None;
    let mut post_file = false;
    let mut targets: Vec<&str> = Vec::new();
    let mut at = 0;
    while at < args.len() {
        let word = args[at].as_str();
        let (name, inline) = match word.split_once('=') {
            Some((name, value)) if name.starts_with("--") => (name, Some(value.to_owned())),
            _ => (word, None),
        };
        let mut value = || {
            inline.clone().or_else(|| {
                at += 1;
                args.get(at).cloned()
            })
        };
        match name {
            "--method" => method = value(),
            "--post-data" | "--body-data" => post = value(),
            "--post-file" | "--body-file" => {
                post_file = true;
                let _ = value();
            }
            _ if word.starts_with("http://") || word.starts_with("https://") => {
                targets.push(word);
            }
            _ => {}
        }
        at += 1;
    }
    let method = match method {
        Some(name) => match HttpMethod::parse(&name) {
            Some(method) => method,
            None => return Vec::new(),
        },
        None if post.is_some() || post_file => HttpMethod::Post,
        None => HttpMethod::Get,
    };
    let write = method.writes();
    let body = post.filter(|_| write);
    let payload = body.iter().cloned().collect();
    drafts(targets, method, body, payload, Tool::Wget, "wget")
}
