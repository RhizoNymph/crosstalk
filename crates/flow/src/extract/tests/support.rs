//! Builders for tool calls, results, contexts and expected accesses.

use serde_json::Value;

use crosstalk_spec::derived::flow::access::Extraction;
use crosstalk_spec::derived::flow::resource::{Host, Locator};
use crosstalk_spec::interfaces::l5_flow::ExtractError;
use crosstalk_spec::observed::message::{
    CanonicalJson, Text, ToolArguments, ToolCall, ToolCallId, ToolExecution, ToolName, ToolOutcome,
    ToolResult, ToolResultContent,
};

use crate::extract::{
    AbsolutePath, Classified, ConversationContext, ExtractConfig, ExtractedOp, ToolExtractors,
    WriteOutcome,
};

pub const CWD: &str = "/home/alice/project";

pub fn call(name: &str, arguments: Value) -> ToolCall {
    ToolCall {
        id: ToolCallId("toolu_01".to_owned()),
        name: ToolName(name.to_owned()),
        // serde_json's map is ordered by key, so this is the canonical text
        // for the arguments these tests use.
        arguments: ToolArguments::Json(CanonicalJson(arguments.to_string())),
        execution: ToolExecution::Client,
    }
}

pub fn result(outcome: ToolOutcome, text: &str) -> ToolResult {
    ToolResult {
        call_id: ToolCallId("toolu_01".to_owned()),
        content: vec![ToolResultContent::Text(Text(text.to_owned()))],
        outcome,
    }
}

pub fn ok(text: &str) -> ToolResult {
    result(ToolOutcome::Success, text)
}

pub fn failed(text: &str) -> ToolResult {
    result(ToolOutcome::Error, text)
}

pub fn context() -> ConversationContext {
    context_in(CWD)
}

pub fn context_in(cwd: &str) -> ConversationContext {
    ConversationContext::new(AbsolutePath::parse(cwd).ok(), None)
}

pub fn no_cwd() -> ConversationContext {
    ConversationContext::default()
}

pub fn wiki_config() -> ExtractConfig {
    ExtractConfig::from_json(include_str!("wiki.json")).expect("the wiki fixture is valid")
}

pub fn extract(
    config: &ExtractConfig,
    context: &ConversationContext,
    call: &ToolCall,
    result: Option<&ToolResult>,
) -> Result<Vec<Classified>, ExtractError> {
    ToolExtractors::new(config, context).extract_classified(call, result)
}

pub fn file(path: &str) -> Locator {
    Locator::File {
        host: None,
        path: path.to_owned(),
    }
}

pub fn url(scheme: &str, host: &str, path: &str, query: Option<&str>) -> Locator {
    Locator::Url {
        scheme: scheme.to_owned(),
        host: Host(host.to_owned()),
        path: path.to_owned(),
        query: query.map(str::to_owned),
    }
}

pub fn https(host: &str, path: &str) -> Locator {
    url("https", host, path, None)
}

pub fn page(target: &str) -> Locator {
    Locator::Mcp {
        server: "wiki".to_owned(),
        tool: ToolName("page".to_owned()),
        target: Some(target.to_owned()),
    }
}

pub fn opaque(tool: &str, key: &str) -> Locator {
    Locator::Opaque {
        tool: ToolName(tool.to_owned()),
        key: key.to_owned(),
    }
}

pub fn write(locator: Locator, outcome: WriteOutcome, via: Extraction) -> Classified {
    Classified {
        op: ExtractedOp::Write(outcome),
        locator,
        via,
    }
}

pub fn read(locator: Locator, via: Extraction) -> Classified {
    Classified {
        op: ExtractedOp::Read,
        locator,
        via,
    }
}
