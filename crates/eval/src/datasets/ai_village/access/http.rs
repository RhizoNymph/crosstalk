//! The agreed L5 `HttpTool` contract, as the village's raw HTTP commands
//! would meet it.
//!
//! L5's HTTP tool extractor reads a call named `http_request` (or `fetch`,
//! `web_fetch`, `curl`) with `url` and `method` arguments: `GET` and `HEAD`
//! read the URL, `POST`, `PUT`, `PATCH` and `DELETE` write it (the written
//! spans from the first of `body`, `content`, `text`, `data`), any other
//! method is no access, and the locator is the canonical URL. AI Village
//! agents never make such a call: their HTTP goes through `curl`, `wget`
//! and `gh`/`glab api` inside bash. [`HttpRequest`] is the call each of
//! those commands is equivalent to; the exchanges keep the original bash
//! call, and [`HttpRequest::tool_call`] builds the equivalent for checks and
//! topology demos.

use crosstalk_spec::observed::message::{
    CanonicalJson, ToolArguments, ToolCall, ToolCallId, ToolExecution, ToolName,
};
use serde::{Deserialize, Serialize};

/// The HTTP tool name the equivalent calls use.
pub const HTTP_TOOL: &str = "http_request";

/// The methods the contract gives an access.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    Get,
    Head,
    Post,
    Put,
    Patch,
    Delete,
}

impl HttpMethod {
    /// A method name, any case. `None` for methods outside the contract.
    pub fn parse(text: &str) -> Option<Self> {
        match text.trim().to_ascii_uppercase().as_str() {
            "GET" => Some(Self::Get),
            "HEAD" => Some(Self::Head),
            "POST" => Some(Self::Post),
            "PUT" => Some(Self::Put),
            "PATCH" => Some(Self::Patch),
            "DELETE" => Some(Self::Delete),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Head => "HEAD",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
        }
    }

    /// Whether the contract makes this method a write.
    pub fn writes(self) -> bool {
        match self {
            Self::Get | Self::Head => false,
            Self::Post | Self::Put | Self::Patch | Self::Delete => true,
        }
    }
}

/// The `http_request` call a bash HTTP command is equivalent to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpRequest {
    pub method: HttpMethod,
    /// The URL as the command named it (the extractor canonicalizes it).
    pub url: String,
    /// What a write sent; `None` for reads and bodiless writes.
    pub body: Option<String>,
}

impl HttpRequest {
    /// `http_request {method, url, body?}` with canonical JSON arguments.
    pub fn tool_call(&self, id: ToolCallId) -> ToolCall {
        let mut arguments = serde_json::Map::new();
        if let Some(body) = &self.body {
            arguments.insert("body".to_owned(), body.clone().into());
        }
        arguments.insert("method".to_owned(), self.method.as_str().into());
        arguments.insert("url".to_owned(), self.url.clone().into());
        ToolCall {
            id,
            name: ToolName(HTTP_TOOL.to_owned()),
            arguments: ToolArguments::Json(CanonicalJson(
                serde_json::Value::Object(arguments).to_string(),
            )),
            execution: ToolExecution::Client,
            signature: None,
        }
    }
}
