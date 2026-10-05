//! File tools: a known argument names the file, so every access is
//! `Structured`.

use crosstalk_spec::derived::flow::access::Extraction;
use crosstalk_spec::observed::message::ToolName;

use crate::extract::args::{ArgError, Args};
use crate::extract::catalog::{FileOp, FileTool};
use crate::extract::context::ConversationContext;
use crate::extract::op::Candidate;
use crate::extract::resource::file_locator;

/// The access a call of `tool` names.
pub(crate) fn candidates(
    tool: &FileTool,
    name: &ToolName,
    args: &Args,
    context: &ConversationContext,
) -> Result<Vec<Candidate>, ArgError> {
    let read = match tool.op {
        FileOp::Read => true,
        FileOp::Write => false,
        FileOp::ByCommand { key, reads, writes } => {
            let command = args.first_str(&[key])?;
            if reads.contains(&command) {
                true
            } else if writes.contains(&command) {
                false
            } else {
                return Err(ArgError::invalid(
                    key,
                    format!("unknown command `{command}`"),
                ));
            }
        }
    };
    let path = args.first_str(tool.path_keys)?;
    let locator = file_locator(path, name, context.scope())
        .map_err(|error| ArgError::invalid(tool.path_keys[0], error))?;
    let candidate = if read {
        Candidate::read(locator, Extraction::Structured)
    } else {
        Candidate::write(locator, Extraction::Structured)
    };
    Ok(vec![candidate])
}
