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
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct AbsolutePath(String);

/// Why stored text is not an [`AbsolutePath`]: only a canonical path (one
/// [`AbsolutePath::parse`] leaves unchanged) is read back.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoredPathError {
    #[error(transparent)]
    Invalid(#[from] PathError),
    #[error("{0:?} is not a canonical path")]
    NotCanonical(String),
}

impl TryFrom<String> for AbsolutePath {
    type Error = StoredPathError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        let path = Self::parse(&text)?;
        if path.0 == text {
            Ok(path)
        } else {
            Err(StoredPathError::NotCanonical(text))
        }
    }
}

impl From<AbsolutePath> for String {
    fn from(path: AbsolutePath) -> Self {
        path.0
    }
}

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

    /// `relative` resolved against this directory, lexically, unless a
    /// `..` would climb above it: `None` then, and for an absolute
    /// `relative`. Used under a home directory whose own path is unknown,
    /// where climbing out of it names an unknown place.
    pub fn join_within(&self, relative: &str) -> Option<Self> {
        check(relative).ok()?;
        if relative.starts_with('/') {
            return None;
        }
        let mut segments: Vec<&str> = self.segments().collect();
        for segment in relative.split('/') {
            match segment {
                "" | "." => {}
                ".." => {
                    segments.pop()?;
                }
                name => segments.push(name),
            }
        }
        Some(Self::root().join_checked(&segments.join("/")))
    }

    /// The path with `prefix`'s segments in front: `/a` under `/home/u` is
    /// `/home/u/a`.
    pub fn under(&self, prefix: &AbsolutePath) -> Self {
        prefix.join_checked(self.0.trim_start_matches('/'))
    }

    /// This path less `suffix`'s trailing segments, when it ends with
    /// them: `/home/u/a` less `/a` is `/home/u`. A root `suffix` gives the
    /// path itself.
    pub fn strip_suffix(&self, suffix: &AbsolutePath) -> Option<Self> {
        if suffix.as_str() == "/" {
            return Some(self.clone());
        }
        let rest = self.0.strip_suffix(suffix.as_str())?;
        if rest.is_empty() {
            return Some(Self::root());
        }
        (!rest.ends_with('/')).then(|| Self(rest.to_owned()))
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

/// A directory or file a shell names: an absolute path, or a path under
/// the home directory while the home directory's own path is not known
/// (`~/repo` is `Home("/repo")`, `~` is `Home("/")`). A shell state that
/// knows its home holds no `Home` place
/// ([`crate::extract::bash::state::ShellState`]).
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Place {
    Absolute(AbsolutePath),
    Home(AbsolutePath),
}

impl Place {
    /// `relative` from this place; `None` when it climbs out of an
    /// unknown home.
    pub fn join(&self, relative: &str) -> Option<Self> {
        match self {
            Self::Absolute(path) => path.join(relative).ok().map(Self::Absolute),
            Self::Home(path) => path.join_within(relative).map(Self::Home),
        }
    }

    pub fn absolute(&self) -> Option<&AbsolutePath> {
        match self {
            Self::Absolute(path) => Some(path),
            Self::Home(_) => None,
        }
    }

    /// The place with the home directory known: a `Home` place becomes
    /// absolute under `home`.
    pub fn resolve_home(self, home: &AbsolutePath) -> Self {
        match self {
            Self::Home(path) => Self::Absolute(path.under(home)),
            absolute @ Self::Absolute(_) => absolute,
        }
    }

    /// `path` relative to this place, when this place is an ancestor of
    /// it (or it), as an absolute path inside this place.
    pub fn contains(&self, path: &Place) -> Option<AbsolutePath> {
        let (root, path) = match (self, path) {
            (Self::Absolute(root), Self::Absolute(path)) | (Self::Home(root), Self::Home(path)) => {
                (root, path)
            }
            (Self::Absolute(_), Self::Home(_)) | (Self::Home(_), Self::Absolute(_)) => return None,
        };
        if root.as_str() == "/" {
            return Some(path.clone());
        }
        let rest = path.as_str().strip_prefix(root.as_str())?;
        if rest.is_empty() {
            return Some(AbsolutePath::root());
        }
        rest.starts_with('/')
            .then(|| AbsolutePath::parse(rest).ok())
            .flatten()
    }

    /// The written form a `Home` place is keyed by when it is no file of
    /// a known clone: `~/a/b` (`~` for the home itself).
    pub fn home_key(path: &AbsolutePath) -> String {
        if path.as_str() == "/" {
            "~".to_owned()
        } else {
            format!("~{path}")
        }
    }
}

impl fmt::Display for Place {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Absolute(path) => path.fmt(f),
            Self::Home(path) => f.write_str(&Self::home_key(path)),
        }
    }
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
