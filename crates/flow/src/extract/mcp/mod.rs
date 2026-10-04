//! MCP tools, mapped by configuration ([`config`]): each configured access
//! of a call becomes one `Structured` candidate.

pub mod config;

use crosstalk_spec::derived::flow::access::Extraction;
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::observed::message::ToolName;

use crate::extract::args::{ArgError, ArgPath, Args};
use crate::extract::catalog::McpTool;
use crate::extract::context::ConversationContext;
use crate::extract::http::{HttpRequest, Method};
use crate::extract::op::Candidate;
use crate::extract::resource::{file_locator, url_locator};
use crate::extract::sites::SitesConfig;

use config::{McpResource, RuleOp};

pub(crate) fn candidates(
    tool: McpTool<'_>,
    name: &ToolName,
    args: &Args,
    context: &ConversationContext,
    sites: &SitesConfig,
) -> Result<Vec<Candidate>, ArgError> {
    let mut found = Vec::new();
    for access in &tool.rule.accesses {
        let locator = locator(&tool.server.server, &access.resource, name, args, context)?;
        // A URL goes through the site rules, so a wiki page or a forge
        // file fetched through an MCP server is the page or file.
        let locators = match (&access.resource, access.op) {
            (McpResource::Url { .. }, op) => {
                let method = match op {
                    RuleOp::Read => Method::Get,
                    RuleOp::Write => Method::Post,
                };
                let request = HttpRequest {
                    url: locator.clone(),
                    method,
                    form: Vec::new(),
                };
                sites
                    .apply(&request)
                    .map_or_else(|| vec![locator], |site| site.locators)
            }
            _ => vec![locator],
        };
        found.extend(locators.into_iter().map(|locator| match access.op {
            RuleOp::Read => Candidate::read(locator, Extraction::Structured),
            RuleOp::Write => Candidate::write(locator, Extraction::Structured),
        }));
    }
    Ok(found)
}

fn locator(
    server: &str,
    resource: &McpResource,
    name: &ToolName,
    args: &Args,
    context: &ConversationContext,
) -> Result<Locator, ArgError> {
    match resource {
        McpResource::Keyed {
            collection,
            target,
            canon,
        } => {
            let target = match target {
                Some(path) => {
                    let raw = required(args, path)?;
                    let key = canon
                        .canonical(&raw)
                        .map_err(|error| ArgError::invalid(path.as_str(), error))?;
                    Some(key)
                }
                None => None,
            };
            Ok(Locator::Mcp {
                server: server.to_owned(),
                tool: ToolName(collection.clone()),
                target,
            })
        }
        McpResource::Url { arg } => url_locator(&required(args, arg)?)
            .map_err(|error| ArgError::invalid(arg.as_str(), error)),
        McpResource::File { arg } => file_locator(&required(args, arg)?, name, context.scope())
            .map_err(|error| ArgError::invalid(arg.as_str(), error)),
    }
}

fn required(args: &Args, path: &ArgPath) -> Result<String, ArgError> {
    args.text_at(path)?
        .ok_or_else(|| ArgError::Missing(path.as_str().to_owned()))
}
