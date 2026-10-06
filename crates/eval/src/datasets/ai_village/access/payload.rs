//! What a write's author wrote: the text inside a command that a reader
//! could get back.
//!
//! L5 keeps a write's content as the call's own spans
//! (`WritePayload::CallArguments`): whatever the writer typed into the
//! command. For a label the converter needs the part of that a reader can
//! receive verbatim, not the command around it:
//!
//! - here-document bodies (`cat > notes.md <<'EOF' … EOF`, `--body
//!   "$(cat <<'EOF' … EOF)"`);
//! - the values of body flags (`--body`, `-b`, `--title`, `-t`,
//!   `--description`, `--message`, `-m`, glab's `-d`), API fields (`-f`,
//!   `-F`, `--field`, `--raw-field`: the value after `=`) and curl and
//!   wget data (`-d`, `--data*`, `--json`, `--form*`, `--post-data`);
//! - `echo` and `printf` arguments (redirected into a file).

use super::shell::commands;

const VALUE_FLAGS: &[&str] = &[
    "--body",
    "-b",
    "--title",
    "-t",
    "--description",
    "-d",
    "--message",
    "-m",
    "--data",
    "--data-raw",
    "--data-binary",
    "--data-urlencode",
    "--data-ascii",
    "--json",
    "--form",
    "--form-string",
    "--post-data",
    "--body-data",
];

const FIELD_FLAGS: &[&str] = &["-f", "-F", "--field", "--raw-field"];

/// The authored texts of `command`, in order, without empty ones.
pub fn authored(command: &str) -> Vec<String> {
    let mut out = heredoc_bodies(command);
    for simple in commands(command) {
        let words = &simple.words;
        let program = words
            .first()
            .map(|word| word.rsplit('/').next().unwrap_or(word));
        if matches!(program, Some("echo" | "printf")) {
            let text: Vec<&str> = words[1..]
                .iter()
                .map(String::as_str)
                .filter(|word| !word.starts_with('-'))
                .collect();
            out.push(text.join(" "));
            continue;
        }
        let mut at = 1;
        while at < words.len() {
            let word = words[at].as_str();
            if let Some((name, value)) = word.split_once('=')
                && name.starts_with("--")
                && VALUE_FLAGS.contains(&name)
            {
                out.push(value.to_owned());
            } else if VALUE_FLAGS.contains(&word) && at + 1 < words.len() {
                out.push(words[at + 1].clone());
                at += 1;
            } else if FIELD_FLAGS.contains(&word) && at + 1 < words.len() {
                let field = &words[at + 1];
                out.push(
                    field
                        .split_once('=')
                        .map_or(field.as_str(), |(_, value)| value)
                        .to_owned(),
                );
                at += 1;
            }
            at += 1;
        }
    }
    out.retain(|text| !text.trim().is_empty() && !text.starts_with("$(cat"));
    out
}

/// Every here-document body in `script`: the lines after a `<<WORD`,
/// `<<-WORD`, `<<'WORD'` or `<<"WORD"` up to the line that is `WORD`.
pub fn heredoc_bodies(script: &str) -> Vec<String> {
    let lines: Vec<&str> = script.lines().collect();
    let mut bodies = Vec::new();
    let mut at = 0;
    while at < lines.len() {
        let delimiters = delimiters(lines[at]);
        at += 1;
        for delimiter in delimiters {
            let mut body = Vec::new();
            while at < lines.len() {
                let line = lines[at];
                at += 1;
                if line.trim_start_matches('\t').trim_end() == delimiter {
                    break;
                }
                body.push(line);
            }
            bodies.push(body.join("\n"));
        }
    }
    bodies
}

/// The here-document delimiters a line opens, in order. `<<<` is a
/// here-string, not one.
fn delimiters(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = line;
    while let Some(at) = rest.find("<<") {
        let after = &rest[at + 2..];
        if let Some(string) = after.strip_prefix('<') {
            rest = string;
            continue;
        }
        let after = after.strip_prefix('-').unwrap_or(after).trim_start();
        let end = after
            .find(|c: char| c.is_whitespace() || matches!(c, ';' | '|' | '&' | ')' | '>'))
            .unwrap_or(after.len());
        let word: String = after[..end]
            .chars()
            .filter(|c| !matches!(c, '\'' | '"' | '\\'))
            .collect();
        if !word.is_empty() {
            out.push(word);
        }
        rest = &after[end..];
    }
    out
}
