//! File writes in a recorded SWE trajectory: what a sender originates.
//!
//! Two forms are recognised:
//!
//! - **Editor create**: a `str_replace_editor` call with
//!   `{"command": "create", "path": P, "file_text": T}` writes `T` to `P`.
//! - **Shell heredoc**: a shell call whose `command` holds
//!   `cat > P << 'EOF'` (or `cat <<EOF > P`, any delimiter, quoted or not,
//!   after an optional `cd DIR &&`) writes the lines up to the delimiter,
//!   each ending in a newline. `>>` appends and is not a write of the whole
//!   file, so it is skipped.
//!
//! A relative path is resolved against the `cd` directory of its command,
//! or the trajectory's working directory.

use serde_json::Value;

use crate::datasets::chat::ChatMessage;
use crate::reference::route::normalize_path;

/// How the file was written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteForm {
    EditorCreate,
    Heredoc,
}

/// One whole-file write by a tool call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileWrite {
    /// Index of the assistant message making the call.
    pub message: usize,
    /// Index of the call in that message.
    pub call: usize,
    /// Absolute, normalized.
    pub path: String,
    pub content: String,
    pub form: WriteForm,
}

/// Every whole-file write in `messages`, in order.
pub fn writes(messages: &[ChatMessage], cwd: &str) -> Vec<FileWrite> {
    let mut out = Vec::new();
    for (message, recorded) in messages.iter().enumerate() {
        if !recorded.is_assistant() {
            continue;
        }
        for (call, tool_call) in recorded.calls().iter().enumerate() {
            let Ok(Value::Object(args)) = serde_json::from_str::<Value>(tool_call.arguments())
            else {
                continue;
            };
            let text = |name: &str| args.get(name).and_then(Value::as_str);
            if tool_call.function.name == "str_replace_editor"
                && text("command") == Some("create")
                && let (Some(path), Some(content)) = (text("path"), text("file_text"))
            {
                out.push(FileWrite {
                    message,
                    call,
                    path: resolve(path, cwd),
                    content: content.to_owned(),
                    form: WriteForm::EditorCreate,
                });
                continue;
            }
            if let Some(command) = text("command") {
                for (path, content) in heredocs(command, cwd) {
                    out.push(FileWrite {
                        message,
                        call,
                        path,
                        content,
                        form: WriteForm::Heredoc,
                    });
                }
            }
        }
    }
    out
}

/// `path` made absolute against `cwd` and normalized.
pub fn resolve(path: &str, cwd: &str) -> String {
    if path.starts_with('/') {
        normalize_path(path)
    } else {
        normalize_path(&format!("{cwd}/{path}"))
    }
}

/// `(path, content)` of each `cat` heredoc writing a whole file in a shell
/// command.
pub fn heredocs(command: &str, cwd: &str) -> Vec<(String, String)> {
    let lines: Vec<&str> = command.split('\n').collect();
    let mut out = Vec::new();
    let mut at = 0;
    while at < lines.len() {
        let Some(open) = opening(lines[at], cwd) else {
            at += 1;
            continue;
        };
        let mut body = String::new();
        let mut end = None;
        for (offset, line) in lines[at + 1..].iter().enumerate() {
            let candidate = if open.strip_tabs {
                line.trim_start_matches('\t')
            } else {
                line
            };
            if candidate.trim_end_matches('\r') == open.delimiter {
                end = Some(at + 1 + offset);
                break;
            }
            body.push_str(candidate);
            body.push('\n');
        }
        match end {
            Some(close) => {
                out.push((open.path, body));
                at = close + 1;
            }
            None => break,
        }
    }
    out
}

struct Opening {
    path: String,
    delimiter: String,
    strip_tabs: bool,
}

/// A line opening a `cat` heredoc into a file, as its target path and
/// delimiter.
fn opening(line: &str, cwd: &str) -> Option<Opening> {
    let mut dir = cwd.to_owned();
    for segment in line.split("&&").flat_map(|part| part.split(';')) {
        let segment = segment.trim();
        if let Some(target) = segment.strip_prefix("cd ") {
            let target = unquote(target.trim());
            dir = resolve(target, &dir);
            continue;
        }
        if !(segment.starts_with("cat ") || segment.starts_with("cat<")) {
            continue;
        }
        let Some(heredoc) = segment.find("<<") else {
            continue;
        };
        let mut rest = &segment[heredoc + 2..];
        let strip_tabs = rest.starts_with('-');
        if strip_tabs {
            rest = &rest[1..];
        }
        let delimiter = unquote(token(rest.trim_start()));
        let Some(path) = redirect_target(segment).filter(|_| !delimiter.is_empty()) else {
            continue;
        };
        return Some(Opening {
            path: resolve(&path, &dir),
            delimiter: delimiter.to_owned(),
            strip_tabs,
        });
    }
    None
}

/// The file a lone `>` redirects to (not `>>`, not `2>`, not inside `<<`).
fn redirect_target(segment: &str) -> Option<String> {
    let bytes = segment.as_bytes();
    for (at, &byte) in bytes.iter().enumerate() {
        if byte != b'>' {
            continue;
        }
        let before = at.checked_sub(1).map(|i| bytes[i]);
        let after = bytes.get(at + 1).copied();
        if after == Some(b'>') || before == Some(b'>') || before.is_some_and(|b| b.is_ascii_digit())
        {
            return None;
        }
        let target = unquote(token(segment[at + 1..].trim_start()));
        if target.is_empty() || target.starts_with('&') {
            return None;
        }
        return Some(target.to_owned());
    }
    None
}

/// The leading shell word of `text`: up to whitespace or a redirect.
fn token(text: &str) -> &str {
    let end = text
        .find(|ch: char| ch.is_whitespace() || matches!(ch, '<' | '>' | ';' | '|'))
        .unwrap_or(text.len());
    &text[..end]
}

fn unquote(text: &str) -> &str {
    let text = text.trim();
    for quote in ['\'', '"'] {
        if let Some(inner) = text
            .strip_prefix(quote)
            .and_then(|rest| rest.strip_suffix(quote))
        {
            return inner;
        }
    }
    text
}
