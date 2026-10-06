//! A shell's state as the conversation's calls showed it: where it is, the
//! directory it was in before (`cd -`), its home directory once the output
//! showed it, and the clones it knows by remote.
//!
//! Every transition is a method here, so the invariant holds by
//! construction: once the home directory is known, no place is
//! home-relative (`Place::Home`): learning it rewrites the working
//! directory, the previous one and every clone root under it
//! ([`ShellState::learn_home`]), and new places are made absolute from
//! then on ([`ShellState::place`]).
//!
//! The state is a function of the calls and results observed, in order:
//! nothing is read from a clock or the environment, and the clone bindings
//! are bounded ([`crate::extract::resource::repo::MAX_BINDINGS`]).

use crate::extract::resource::{AbsolutePath, Place, RepoBindings, RepoId};

use super::lex::Word;

/// Where a shell is and what it knows.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "StoredShell")]
pub struct ShellState {
    /// The working directory; `None` when it is unknown.
    cwd: Option<Place>,
    /// Where the last `cd` left from (`$OLDPWD`).
    previous: Option<Place>,
    /// The home directory, once the output showed it.
    home: Option<AbsolutePath>,
    repos: RepoBindings,
}

/// A [`ShellState`] as stored, checked on the way back in.
#[derive(serde::Deserialize)]
struct StoredShell {
    cwd: Option<Place>,
    previous: Option<Place>,
    home: Option<AbsolutePath>,
    repos: RepoBindings,
}

/// Why a stored shell state was refused: it knows its home and still
/// holds a home-relative place.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a shell state that knows its home holds a home-relative place")]
pub struct StoredShellError;

impl TryFrom<StoredShell> for ShellState {
    type Error = StoredShellError;

    fn try_from(stored: StoredShell) -> Result<Self, Self::Error> {
        let home_relative = matches!(stored.cwd, Some(Place::Home(_)))
            || matches!(stored.previous, Some(Place::Home(_)))
            || stored.repos.has_home_relative();
        if stored.home.is_some() && home_relative {
            return Err(StoredShellError);
        }
        Ok(Self {
            cwd: stored.cwd,
            previous: stored.previous,
            home: stored.home,
            repos: stored.repos,
        })
    }
}

/// What a `cd` was asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CdTarget {
    /// `cd`, `cd ~`: the home directory.
    Home,
    /// `cd -`: the previous directory.
    Previous,
    /// A place the word names.
    Place(Place),
    /// A target the extractor cannot follow (an expansion, a relative
    /// one from an unknown directory, `~user`).
    Unknown,
}

impl ShellState {
    pub fn new(cwd: Option<AbsolutePath>) -> Self {
        Self {
            cwd: cwd.map(Place::Absolute),
            ..Self::default()
        }
    }

    pub fn cwd(&self) -> Option<&Place> {
        self.cwd.as_ref()
    }

    /// The working directory, when it is known as an absolute path.
    pub fn absolute_cwd(&self) -> Option<&AbsolutePath> {
        self.cwd.as_ref().and_then(Place::absolute)
    }

    /// Where the last `cd` left from.
    pub fn previous(&self) -> Option<&Place> {
        self.previous.as_ref()
    }

    pub fn home(&self) -> Option<&AbsolutePath> {
        self.home.as_ref()
    }

    pub fn repos(&self) -> &RepoBindings {
        &self.repos
    }

    /// Set the working directory (a harness's note that it moved the
    /// shell, or a `pwd` the output answered).
    pub fn set_cwd(&mut self, cwd: Option<Place>) {
        self.cwd = cwd.map(|place| self.normalized(place));
    }

    /// The home directory as a place: absolute once known.
    pub fn home_place(&self) -> Place {
        match &self.home {
            Some(home) => Place::Absolute(home.clone()),
            None => Place::Home(AbsolutePath::root()),
        }
    }

    /// The place `word` names from `from`: an absolute path, a path under
    /// the home directory, or a relative path from `from`. `None` for an
    /// expansion, or a relative path from an unknown place.
    pub fn place(&self, from: Option<&Place>, word: &Word) -> Option<Place> {
        if let Some(rest) = word.home() {
            return self.home_place().join(rest.trim_start_matches('/'));
        }
        let path = word.as_literal()?;
        if path.starts_with('/') {
            return AbsolutePath::parse(path).ok().map(Place::Absolute);
        }
        if path.starts_with('~') {
            return None;
        }
        from?.join(path)
    }

    /// Run `cd`: the new directory, and the old one as the previous.
    pub fn cd(&mut self, target: CdTarget) {
        let next = match target {
            CdTarget::Home => Some(self.home_place()),
            CdTarget::Previous => self.previous.clone(),
            CdTarget::Place(place) => Some(self.normalized(place)),
            CdTarget::Unknown => None,
        };
        self.previous = std::mem::replace(&mut self.cwd, next);
    }

    /// Bind the remote `remote` of the clone at `root`.
    pub fn bind(&mut self, root: Place, remote: &str, repo: RepoId) {
        let root = self.normalized(root);
        self.repos.bind_remote(root, remote, repo);
    }

    /// Learn the home directory: every home-relative place becomes
    /// absolute. A home already known is kept (the first answer wins).
    pub fn learn_home(&mut self, home: AbsolutePath) {
        if self.home.is_some() {
            return;
        }
        self.cwd = self.cwd.take().map(|place| place.resolve_home(&home));
        self.previous = self.previous.take().map(|place| place.resolve_home(&home));
        self.repos.resolve_home(&home);
        self.home = Some(home);
    }

    /// `place` with the home directory resolved, once it is known.
    pub fn normalized(&self, place: Place) -> Place {
        match &self.home {
            Some(home) => place.resolve_home(home),
            None => place,
        }
    }
}
