//! The forge CLIs, `gh` (GitHub) and `glab` (GitLab): clones, issues and
//! pull/merge requests, and their API commands.
//!
//! - `gh repo clone <repo> [<dir>]`, `glab repo clone <repo> [<dir>]`:
//!   clone (bind the directory, read the repository) as `git clone` does.
//! - `gh issue|pr`, `glab issue|mr` `<verb>`:
//!
//!   | Verb | Access | Locator |
//!   | --- | --- | --- |
//!   | `create` | write | the collection ([`ForgeRepo::collection`]): the number is not known from the call |
//!   | `comment`, `note`, `edit`, `update`, `review` | write | the thread of the number or URL operand ([`ForgeRepo::thread`]), else the collection |
//!   | `view` | read | the thread, else the collection |
//!   | `list` | read | the collection |
//!
//!   A write's content is its arguments (`--title`, `--body`,
//!   `--description`, `--message`), so it carries the call's spans. The
//!   repository is `-R`/`--repo` (`owner/name`, `host/owner/name`,
//!   `group/subgroup/name` or a URL), else a URL operand's, else the one
//!   the clone the command runs in is bound to; without one there is no
//!   access. A write's locator never depends on the result: the gateway
//!   holds writes from the call alone (`flow.extract.write-locators-from-call`).
//! - `gh api <endpoint>`, `glab api <endpoint>`: the HTTP request the
//!   `HttpTool` contract describes, at `https://api.github.com/<endpoint>`
//!   (`https://<hostname>/api/v3/<endpoint>` with `--hostname`) or
//!   `https://gitlab.com/api/v4/<endpoint>`, `{owner}`/`{repo}` (gh) and
//!   `:fullpath`/`:id`/`:namespace`/`:group`/`:repo` (glab) filled from the
//!   bound repository. The method is `-X`/`--method`, else `POST` with a
//!   field (`-f`, `-F`, `--raw-field`, `--field`) or `--input`, else
//!   `GET`; the fields are the form the site rules read. `graphql` is no
//!   access.
//!
//! Every access is judged by the CLI's output (`CommandRule::ForgeCli`).
//!
//! [`ForgeRepo::collection`]: crate::extract::resource::ForgeRepo::collection
//! [`ForgeRepo::thread`]: crate::extract::resource::ForgeRepo::thread

use crosstalk_spec::derived::flow::access::Extraction;
use crosstalk_spec::derived::flow::resource::Locator;

use crate::extract::http::{HttpRequest, Method, form_fields};
use crate::extract::op::Candidate;
use crate::extract::outcome::CommandRule;
use crate::extract::resource::{ForgeStyle, RepoId, ThreadKind, url_locator};
use crate::extract::sites::{github, gitlab};

use super::commands::{Found, Shell};
use super::lex::Word;
use super::options::{OptSpec, Options};
use crate::extract::resource::repo::ORIGIN;

/// Which forge CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Cli {
    Gh,
    Glab,
}

impl Cli {
    fn style(self) -> ForgeStyle {
        match self {
            Self::Gh => ForgeStyle::GitHub,
            Self::Glab => ForgeStyle::GitLab,
        }
    }

    fn default_host(self) -> &'static str {
        match self {
            Self::Gh => "github.com",
            Self::Glab => "gitlab.com",
        }
    }

    fn threads(self) -> &'static OptSpec {
        match self {
            Self::Gh => &GH_THREADS,
            Self::Glab => &GLAB_THREADS,
        }
    }
}

const CLONE: OptSpec = OptSpec {
    short_values: "u",
    long_values: &["--upstream-remote-name"],
};

const THREAD_LONG_VALUES: &[&str] = &[
    "--repo",
    "--title",
    "--body",
    "--body-file",
    "--description",
    "--message",
    "--assignee",
    "--label",
    "--milestone",
    "--project",
    "--base",
    "--head",
    "--reviewer",
    "--limit",
    "--state",
    "--search",
    "--jq",
    "--template",
    "--json",
    "--author",
    "--app",
    "--mention",
    "--add-label",
    "--remove-label",
    "--add-assignee",
    "--remove-assignee",
    "--add-reviewer",
    "--remove-reviewer",
    "--add-project",
    "--remove-project",
    "--target-branch",
    "--source-branch",
    "--page",
    "--per-page",
    "--output",
];

const GH_THREADS: OptSpec = OptSpec {
    short_values: "RtbFalmpBHrLsSqA",
    long_values: THREAD_LONG_VALUES,
};

const GLAB_THREADS: OptSpec = OptSpec {
    short_values: "RtdmlasbpPF",
    long_values: THREAD_LONG_VALUES,
};

const API: OptSpec = OptSpec {
    short_values: "XfFHqtp",
    long_values: &[
        "--method",
        "--raw-field",
        "--field",
        "--header",
        "--input",
        "--jq",
        "--template",
        "--hostname",
        "--cache",
        "--preview",
    ],
};

impl Shell<'_> {
    pub(super) fn forge_cli(
        &mut self,
        cli: Cli,
        args: &[Word],
        stdout_to_file: bool,
        found: &mut Vec<Found>,
    ) {
        let Some((group, rest)) = args.split_first() else {
            return;
        };
        match (cli, group.text.as_str()) {
            (_, "repo") => self.forge_clone(cli, rest, found),
            (_, "issue") => self.thread_command(cli, ThreadKind::Issue, rest, found),
            (Cli::Gh, "pr") | (Cli::Glab, "mr") => {
                self.thread_command(cli, ThreadKind::Change, rest, found);
            }
            (_, "api") => self.api(cli, rest, stdout_to_file, found),
            _ => {}
        }
    }

    fn forge_clone(&mut self, cli: Cli, args: &[Word], found: &mut Vec<Found>) {
        let Some((verb, rest)) = args.split_first() else {
            return;
        };
        if verb.text != "clone" {
            return;
        }
        let parsed = Options::parse(rest, &CLONE);
        let dir = self.state.cwd().cloned();
        if let Some(repo) =
            self.clone_into(dir.as_ref(), &parsed.operands, ORIGIN, |remote, cwd| {
                repo_argument(cli, remote).or_else(|| RepoId::parse(remote, cwd))
            })
        {
            self.found(
                found,
                Candidate::read(repo.locator().clone(), Extraction::Parsed)
                    .judged_by(CommandRule::GitTransfer),
            );
        }
    }

    fn thread_command(&self, cli: Cli, kind: ThreadKind, args: &[Word], found: &mut Vec<Found>) {
        let Some((verb, rest)) = args.split_first() else {
            return;
        };
        let parsed = Options::parse(rest, cli.threads());
        let target = parsed.operands.first().and_then(|word| word.as_literal());
        let url_thread = target
            .filter(|text| text.contains("://"))
            .and_then(|text| thread_url(cli, text));
        let number = target.and_then(thread_number);
        let repo = match parsed.last(&["-R", "--repo"]) {
            Some(word) => word.as_literal().and_then(|text| repo_argument(cli, text)),
            None => self.bound_repo(self.state.cwd()),
        };
        let thread = || -> Option<Locator> {
            if let Some(url) = &url_thread {
                return Some(url.clone());
            }
            let repo = repo.as_ref()?;
            let forge = repo.forge_parts()?;
            Some(match number {
                Some(number) => forge.thread(cli.style(), kind, number),
                None => forge.collection(cli.style(), kind),
            })
        };
        let collection = || -> Option<Locator> {
            let forge = repo.as_ref()?.forge_parts()?;
            Some(forge.collection(cli.style(), kind))
        };
        let candidate = match verb.text.as_str() {
            "create" => collection().map(|l| Candidate::write(l, Extraction::Parsed)),
            "comment" | "note" | "edit" | "update" | "review" => {
                thread().map(|l| Candidate::write(l, Extraction::Parsed))
            }
            "view" => thread().map(|l| Candidate::read(l, Extraction::Parsed)),
            "list" => collection().map(|l| Candidate::read(l, Extraction::Parsed)),
            _ => None,
        };
        if let Some(candidate) = candidate {
            self.found(found, candidate.judged_by(CommandRule::ForgeCli));
        }
    }

    fn api(&self, cli: Cli, args: &[Word], stdout_to_file: bool, found: &mut Vec<Found>) {
        let parsed = Options::parse(args, &API);
        let Some(endpoint) = parsed.operands.first().and_then(|word| word.as_literal()) else {
            return;
        };
        if endpoint == "graphql" {
            return;
        }
        let Some(endpoint) = self.fill_placeholders(cli, endpoint) else {
            return;
        };
        let url = if endpoint.contains("://") {
            endpoint
        } else {
            let endpoint = endpoint.trim_start_matches('/');
            match (cli, parsed.last(&["--hostname"]).and_then(Word::as_literal)) {
                (Cli::Gh, Some(host)) if host != "github.com" => {
                    format!("https://{host}/api/v3/{endpoint}")
                }
                (Cli::Gh, _) => format!("https://api.github.com/{endpoint}"),
                (Cli::Glab, _) => format!("https://gitlab.com/api/v4/{endpoint}"),
            }
        };
        let Ok(url) = url_locator(&url) else {
            return;
        };
        let fields: Vec<(String, String)> = parsed
            .with_values()
            .filter(|(name, _)| matches!(*name, "-f" | "-F" | "--raw-field" | "--field"))
            .filter_map(|(_, value)| value.as_literal())
            .flat_map(form_fields)
            .collect();
        let method = match parsed.last(&["-X", "--method"]) {
            Some(method) => Method::parse(&method.text),
            None if !fields.is_empty() || parsed.has(&["--input"]) => Method::Post,
            None => Method::Get,
        };
        let request = HttpRequest {
            url,
            method,
            form: fields,
        };
        self.request(&request, !stdout_to_file, CommandRule::ForgeCli, found);
    }

    /// `endpoint` with its repository placeholders filled from the bound
    /// repository; `None` when one is left that cannot be filled.
    fn fill_placeholders(&self, cli: Cli, endpoint: &str) -> Option<String> {
        let placeholders: &[&str] = match cli {
            Cli::Gh => &["{owner}", "{repo}"],
            Cli::Glab => &[":fullpath", ":id", ":namespace", ":group", ":repo"],
        };
        if !placeholders.iter().any(|p| endpoint.contains(p)) {
            return Some(endpoint.to_owned());
        }
        let repo = self.bound_repo(self.state.cwd())?;
        let forge = repo.forge_parts()?;
        let encoded = format!("{}%2F{}", forge.owner.replace('/', "%2F"), forge.name);
        let filled = match cli {
            Cli::Gh => endpoint
                .replace("{owner}", forge.owner)
                .replace("{repo}", forge.name),
            Cli::Glab => endpoint
                .replace(":fullpath", &encoded)
                .replace(":id", &encoded)
                .replace(":namespace", forge.owner)
                .replace(":group", forge.owner)
                .replace(":repo", forge.name),
        };
        Some(filled)
    }
}

/// A repository argument (`-R`, `repo clone`): `owner/name`,
/// `host/owner/name` (a first segment with a dot is a host),
/// `group/subgroup/name`, or a URL or git remote.
fn repo_argument(cli: Cli, text: &str) -> Option<RepoId> {
    if text.contains("://") || text.contains('@') {
        return RepoId::parse(text, None);
    }
    match text.split_once('/') {
        Some((host, path)) if host.contains('.') && path.contains('/') => RepoId::forge(host, path),
        _ => RepoId::forge(cli.default_host(), text),
    }
}

/// `123` or `#123`.
fn thread_number(text: &str) -> Option<u64> {
    let digits = text.strip_prefix('#').unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// The thread a forge URL operand names (`https://github.com/o/n/pull/5`),
/// read through the forge's site rule whatever the sites configuration.
fn thread_url(cli: Cli, text: &str) -> Option<Locator> {
    let request = HttpRequest::get(url_locator(text).ok()?);
    let access = match cli {
        Cli::Gh => github::apply(&request),
        Cli::Glab => gitlab::apply(&request),
    }?;
    let locator = access.locators.into_iter().next()?;
    matches!(locator, Locator::Url { .. }).then_some(locator)
}
