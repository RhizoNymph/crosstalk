//! File paths: lexical normalization and the file locator.
//!
//! Resolution is lexical only (`flow.resource.path-normalization`): the
//! gateway cannot see the agent's filesystem, so symlinks are not followed
//! and `..` above the root stays at the root, as the kernel resolves it.

use std::fmt;

use crosstalk_spec::derived::flow::resource::{Host, Locator};
use crosstalk_spec::observed::message::ToolName;

use super::repo::RepoBindings;

/// An absolute POSIX path with `.`, `..`, repeated and trailing `/`
/// resolved. Only [`AbsolutePath::parse`] and [`AbsolutePath::join`] make
/// one, so every value is canonical: two equal paths name one file.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AbsolutePath(String);

/// Why text is not a path a locator can be made from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PathError {
    #[error("the path is empty")]
    Empty,
    #[error("the path contains a NUL byte")]
    Nul,
    #[error("the path is not absolute")]
    NotAbsolute,
}

impl AbsolutePath {
    /// The root, `/`.
    pub fn root() -> Self {
        Self("/".to_owned())
    }

    /// `text`, which must start with `/`, normalized.
    pub fn parse(text: &str) -> Result<Self, PathError> {
        check(text)?;
        if !text.starts_with('/') {
            return Err(PathError::NotAbsolute);
        }
        Ok(Self::root().join_checked(text))
    }

    /// `relative` resolved against this directory, lexically. An absolute
    /// `relative` replaces it.
    pub fn join(&self, relative: &str) -> Result<Self, PathError> {
        check(relative)?;
        Ok(self.join_checked(relative))
    }

    fn join_checked(&self, relative: &str) -> Self {
        let mut segments: Vec<&str> = if relative.starts_with('/') {
            Vec::new()
        } else {
            self.segments().collect()
        };
        for segment in relative.split('/') {
            match segment {
                "" | "." => {}
                ".." => {
                    segments.pop();
                }
                name => segments.push(name),
            }
        }
        let mut path = String::with_capacity(relative.len() + self.0.len());
        for segment in &segments {
            path.push('/');
            path.push_str(segment);
        }
        if path.is_empty() {
            path.push('/');
        }
        Self(path)
    }

    fn segments(&self) -> impl Iterator<Item = &str> {
        self.0.split('/').filter(|segment| !segment.is_empty())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for AbsolutePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn check(text: &str) -> Result<(), PathError> {
    if text.is_empty() {
        return Err(PathError::Empty);
    }
    if text.contains('\0') {
        return Err(PathError::Nul);
    }
    Ok(())
}

/// How a path was written, which decides how it is keyed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WrittenPath<'a> {
    Absolute(AbsolutePath),
    /// Relative to the working directory.
    Relative(&'a str),
    /// A form only the agent's machine can resolve: `~` (a home directory)
    /// or a Windows drive or UNC path. Always keyed as written.
    Unresolvable(&'a str),
}

impl<'a> WrittenPath<'a> {
    pub fn classify(text: &'a str) -> Result<Self, PathError> {
        check(text)?;
        if text.starts_with('/') {
            return AbsolutePath::parse(text).map(Self::Absolute);
        }
        if text.starts_with('~') || is_windows(text) {
            return Ok(Self::Unresolvable(text));
        }
        Ok(Self::Relative(text))
    }
}

/// `C:\x`, `C:/x`, `\\server\share`.
fn is_windows(text: &str) -> bool {
    let bytes = text.as_bytes();
    let drive = bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'\\' | b'/');
    drive || text.starts_with("\\\\")
}

/// What a file path resolves against: the working directory, the host
/// files live on, and the known clones of shared repositories.
#[derive(Debug, Clone, Copy)]
pub struct FileScope<'a> {
    pub cwd: Option<&'a AbsolutePath>,
    pub host: Option<&'a Host>,
    pub repos: &'a RepoBindings,
}

/// The locator of the file `written` names, as `tool` wrote it:
/// - absolute: inside a known clone, the repository's file
///   ([`RepoId::file`]); otherwise `Locator::File` on the scope's host,
///   normalized;
/// - relative, with a working directory: resolved against it, then as
///   absolute (`flow.resource.relative-path-cwd`);
/// - relative without one, or unresolvable: `Locator::Opaque` on the tool
///   and the path as written (`flow.resource.relative-path-opaque`).
///
/// [`RepoId::file`]: super::repo::RepoId::file
pub fn file_locator(
    written: &str,
    tool: &ToolName,
    scope: FileScope<'_>,
) -> Result<Locator, PathError> {
    let absolute = match WrittenPath::classify(written)? {
        WrittenPath::Absolute(path) => path,
        WrittenPath::Relative(relative) => match scope.cwd {
            Some(cwd) => cwd.join(relative)?,
            None => return Ok(opaque(tool, written)),
        },
        WrittenPath::Unresolvable(text) => return Ok(opaque(tool, text)),
    };
    Ok(absolute_locator(absolute, scope))
}

/// The locator of an absolute path in `scope`.
pub fn absolute_locator(path: AbsolutePath, scope: FileScope<'_>) -> Locator {
    if let Some((repo, inside)) = scope.repos.locate(&path) {
        return repo.file(&inside);
    }
    Locator::File {
        host: scope.host.cloned(),
        path: path.into_string(),
    }
}

fn opaque(tool: &ToolName, key: &str) -> Locator {
    Locator::Opaque {
        tool: tool.clone(),
        key: key.to_owned(),
    }
}
