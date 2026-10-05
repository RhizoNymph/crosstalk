//! The MCP tool mapping: which tool of which server reads or writes which
//! resource, named by which argument.
//!
//! Structured JSON in the spec's conventions (snake_case keys, enums tagged
//! `type`/`data`, unknown fields refused). [`ExtractConfig::from_json`]
//! parses and checks; a document that names a server or tool twice, or a
//! tool with no access, is refused.
//!
//! A tool maps to a resource, not to itself: `read_page` and `write_page`
//! of a wiki both name the resource collection `page`, so a write by one
//! agent and a read by another meet on one locator,
//! `Locator::Mcp { server: "wiki", tool: "page", target: Some(<key>) }`.
//! The server is its configured canonical name, whatever alias an agent's
//! harness registered it under.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::extract::args::ArgPath;
use crate::extract::resource::KeyCanon;
use crate::extract::sites::SitesConfig;

/// The extractors' configuration: the MCP tool mapping, the HTTP tool
/// names, the fetch tool names and the site rules. The default maps no MCP
/// tool, has the default HTTP tools ([`DEFAULT_HTTP_TOOLS`]), no configured
/// fetch tool (the built-in ones stay known) and the built-in site rules
/// ([`SitesConfig::default`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawExtractConfig", into = "RawExtractConfig")]
pub struct ExtractConfig {
    servers: Vec<McpServerConfig>,
    /// (name or alias, tool) to (server index, tool index).
    index: HashMap<(String, String), (usize, usize)>,
    http_tools: Vec<String>,
    fetch_tools: Vec<String>,
    sites: SitesConfig,
}

impl Default for ExtractConfig {
    fn default() -> Self {
        Self {
            servers: Vec::new(),
            index: HashMap::new(),
            http_tools: default_http_tools(),
            fetch_tools: Vec::new(),
            sites: SitesConfig::default(),
        }
    }
}

/// The tools whose calls carry `url` and `method` (and maybe a body): the
/// eval converter's `http_request` and the like.
pub const DEFAULT_HTTP_TOOLS: [&str; 4] = ["http_request", "fetch", "web_fetch", "curl"];

fn default_http_tools() -> Vec<String> {
    DEFAULT_HTTP_TOOLS
        .iter()
        .map(|name| (*name).to_owned())
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawExtractConfig {
    #[serde(default)]
    mcp_servers: Vec<McpServerConfig>,
    #[serde(default = "default_http_tools")]
    http_tools: Vec<String>,
    #[serde(default)]
    fetch_tools: Vec<String>,
    #[serde(default)]
    sites: SitesConfig,
}

/// One MCP server: its canonical name, the other names harnesses register
/// it under, and its mapped tools.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct McpServerConfig {
    /// The server name every locator carries.
    pub server: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    pub tools: Vec<McpToolRule>,
}

/// One tool: the accesses a call of it implies, and how its result text
/// reports a refusal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct McpToolRule {
    pub tool: String,
    /// At least one.
    pub accesses: Vec<McpAccessRule>,
    /// The tool's content rule: a successful (or unflagged) result whose
    /// text matches any of these is a refusal (`WriteOutcome::Rejected`).
    /// Empty: the tool has no content rule and the wire's flag decides.
    #[serde(default)]
    pub refusal: Vec<RefusalMarker>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct McpAccessRule {
    pub op: RuleOp,
    pub resource: McpResource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleOp {
    Read,
    Write,
}

/// The resource an access touches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum McpResource {
    /// `Locator::Mcp { server, tool: collection, target }`: `target` is the
    /// argument at `target` folded by `canon`, or none for a tool that
    /// touches the whole collection (a page listing).
    Keyed {
        collection: String,
        #[serde(default)]
        target: Option<ArgPath>,
        #[serde(default)]
        canon: KeyCanon,
    },
    /// The URL in the argument at `arg` (a fetch server).
    Url { arg: ArgPath },
    /// The file path in the argument at `arg` (a filesystem server),
    /// resolved like a file tool's.
    File { arg: ArgPath },
}

/// How a result's text says the tool refused the call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum RefusalMarker {
    /// The text, with leading whitespace trimmed, starts with this.
    Prefix(String),
    /// The text contains this.
    Contains(String),
}

impl RefusalMarker {
    pub fn matches(&self, text: &str) -> bool {
        match self {
            Self::Prefix(prefix) => text.trim_start().starts_with(prefix.as_str()),
            Self::Contains(needle) => text.contains(needle.as_str()),
        }
    }
}

/// Why a configuration document was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    #[error("not a valid extractor configuration: {0}")]
    Json(String),
    #[error("an MCP server, alias, tool or collection name is empty")]
    EmptyName,
    #[error("MCP server name `{0}` is used twice")]
    DuplicateServer(String),
    #[error("MCP tool `{tool}` of server `{server}` is mapped twice")]
    DuplicateTool { server: String, tool: String },
    #[error("MCP tool `{tool}` of server `{server}` has no access")]
    NoAccess { server: String, tool: String },
    #[error("a refusal marker of MCP tool `{tool}` of server `{server}` is empty")]
    EmptyMarker { server: String, tool: String },
    #[error("tool `{0}` is configured both as an HTTP tool and as a fetch tool")]
    HttpAndFetch(String),
}

impl ExtractConfig {
    /// Parse and check a JSON document.
    pub fn from_json(text: &str) -> Result<Self, ConfigError> {
        let raw: RawExtractConfig =
            serde_json::from_str(text).map_err(|error| ConfigError::Json(error.to_string()))?;
        Self::try_from(raw)
    }

    /// Check `servers` and index them by every name and tool; the HTTP
    /// tools and the site rules are the defaults.
    pub fn new(servers: Vec<McpServerConfig>) -> Result<Self, ConfigError> {
        let mut index = HashMap::new();
        let mut names = HashSet::new();
        for (s, server) in servers.iter().enumerate() {
            for name in std::iter::once(&server.server).chain(&server.aliases) {
                if name.is_empty() {
                    return Err(ConfigError::EmptyName);
                }
                if !names.insert(name.clone()) {
                    return Err(ConfigError::DuplicateServer(name.clone()));
                }
            }
            for (t, rule) in server.tools.iter().enumerate() {
                check_rule(&server.server, rule)?;
                for name in std::iter::once(&server.server).chain(&server.aliases) {
                    if index
                        .insert((name.clone(), rule.tool.clone()), (s, t))
                        .is_some()
                    {
                        return Err(ConfigError::DuplicateTool {
                            server: server.server.clone(),
                            tool: rule.tool.clone(),
                        });
                    }
                }
            }
        }
        Ok(Self {
            servers,
            index,
            http_tools: default_http_tools(),
            fetch_tools: Vec::new(),
            sites: SitesConfig::default(),
        })
    }

    /// The same configuration with `names` as its HTTP tools. Refuses an
    /// empty name, or one that is also a configured fetch tool.
    pub fn with_http_tools(self, names: Vec<String>) -> Result<Self, ConfigError> {
        if names.iter().any(String::is_empty) {
            return Err(ConfigError::EmptyName);
        }
        if let Some(both) = names.iter().find(|name| self.fetch_tools.contains(name)) {
            return Err(ConfigError::HttpAndFetch(both.clone()));
        }
        Ok(Self {
            http_tools: names,
            ..self
        })
    }

    /// The same configuration with `names` as its configured fetch tools:
    /// tools whose `url` argument names the page they read and whose result
    /// is the page (AgentDojo's `get_webpage`, say). Refuses an empty name,
    /// or one that is also an HTTP tool.
    pub fn with_fetch_tools(self, names: Vec<String>) -> Result<Self, ConfigError> {
        if names.iter().any(String::is_empty) {
            return Err(ConfigError::EmptyName);
        }
        if let Some(both) = names.iter().find(|name| self.http_tools.contains(name)) {
            return Err(ConfigError::HttpAndFetch(both.clone()));
        }
        Ok(Self {
            fetch_tools: names,
            ..self
        })
    }

    /// The names of the tools read as HTTP requests when their arguments
    /// carry `url` and `method`.
    pub fn http_tools(&self) -> &[String] {
        &self.http_tools
    }

    /// The configured fetch tools' names, besides the built-in ones
    /// (`catalog::FETCH_TOOLS`): each reads the URL in its `url` argument.
    pub fn fetch_tools(&self) -> &[String] {
        &self.fetch_tools
    }

    /// The same configuration with `sites` as its site rules.
    pub fn with_sites(self, sites: SitesConfig) -> Self {
        Self { sites, ..self }
    }

    pub fn servers(&self) -> &[McpServerConfig] {
        &self.servers
    }

    pub fn sites(&self) -> &SitesConfig {
        &self.sites
    }

    /// The server and tool rule for `tool` of the server a harness
    /// registered as `server` (its name or an alias).
    pub fn rule(&self, server: &str, tool: &str) -> Option<(&McpServerConfig, &McpToolRule)> {
        let (s, t) = *self.index.get(&(server.to_owned(), tool.to_owned()))?;
        let server = self.servers.get(s)?;
        Some((server, server.tools.get(t)?))
    }
}

fn check_rule(server: &str, rule: &McpToolRule) -> Result<(), ConfigError> {
    let named = |server: &str| (server.to_owned(), rule.tool.clone());
    if rule.tool.is_empty() {
        return Err(ConfigError::EmptyName);
    }
    if rule.accesses.is_empty() {
        let (server, tool) = named(server);
        return Err(ConfigError::NoAccess { server, tool });
    }
    let empty_collection = rule.accesses.iter().any(|access| {
        matches!(&access.resource, McpResource::Keyed { collection, .. } if collection.is_empty())
    });
    if empty_collection {
        return Err(ConfigError::EmptyName);
    }
    let empty_marker = rule.refusal.iter().any(|marker| match marker {
        RefusalMarker::Prefix(text) | RefusalMarker::Contains(text) => text.is_empty(),
    });
    if empty_marker {
        let (server, tool) = named(server);
        return Err(ConfigError::EmptyMarker { server, tool });
    }
    Ok(())
}

impl TryFrom<RawExtractConfig> for ExtractConfig {
    type Error = ConfigError;

    fn try_from(raw: RawExtractConfig) -> Result<Self, Self::Error> {
        Self::new(raw.mcp_servers)?
            .with_http_tools(raw.http_tools)?
            .with_fetch_tools(raw.fetch_tools)
            .map(|config| config.with_sites(raw.sites))
    }
}

impl From<ExtractConfig> for RawExtractConfig {
    fn from(config: ExtractConfig) -> Self {
        Self {
            mcp_servers: config.servers,
            http_tools: config.http_tools,
            fetch_tools: config.fetch_tools,
            sites: config.sites,
        }
    }
}
