//! Reverse-proxy routing: from a request's path to a configured upstream.
//!
//! A route matches on the request head's path alone, never on headers or
//! the body, so a harness cannot steer its own routing
//! (`ingress.harness.never-changes-forwarding`). The longest matching prefix
//! wins; a request no route matches is answered 421 by the proxy and never
//! forwarded (`ingress.routing.unrouted-answered-421`).

use std::collections::HashSet;

use crosstalk_spec::derived::flow::resource::Host;
use crosstalk_spec::interfaces::l0_ingress::{RequestHead, UpstreamRouter};
use crosstalk_spec::observed::client::{IngressMode, RouteName, Upstream};
use hyper::Uri;
use hyper::http::uri::{Authority, Scheme};

use crate::config::RouteConfig;

/// Why a route configuration was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    #[error("route {route}: prefix {prefix:?} {reason}")]
    InvalidPrefix {
        route: String,
        prefix: String,
        reason: &'static str,
    },
    #[error("route {route}: base_url {reason}")]
    InvalidBaseUrl { route: String, reason: &'static str },
    #[error("two routes are named {0}")]
    DuplicateName(String),
    #[error("two routes have the prefix {0:?}")]
    DuplicatePrefix(String),
}

/// A checked route prefix: `/`, or `/`-separated non-empty segments with no
/// trailing slash, query or fragment.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoutePrefix(String);

impl RoutePrefix {
    pub fn new(text: &str) -> Result<Self, &'static str> {
        if !text.starts_with('/') {
            return Err("must start with /");
        }
        if text.contains(['?', '#']) {
            return Err("must not contain a query or fragment");
        }
        if text == "/" {
            return Ok(Self(String::new()));
        }
        if text.ends_with('/') {
            return Err("must not end with /");
        }
        if text.split('/').skip(1).any(str::is_empty) {
            return Err("must not contain an empty segment");
        }
        Ok(Self(text.to_owned()))
    }

    /// What is left of `path` under this prefix (`""` or `/…`), or `None`
    /// when the path is not under it. Matches whole segments only:
    /// `/anthropic` matches `/anthropic/v1` but not `/anthropics`.
    pub fn strip<'p>(&self, path: &'p str) -> Option<&'p str> {
        let rest = path.strip_prefix(self.0.as_str())?;
        (rest.is_empty() || rest.starts_with('/')).then_some(rest)
    }

    pub fn as_str(&self) -> &str {
        if self.0.is_empty() { "/" } else { &self.0 }
    }
}

/// A checked upstream base URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamBase {
    scheme: Scheme,
    authority: Authority,
    /// Without a trailing slash; empty for the root.
    path: String,
}

impl UpstreamBase {
    pub fn parse(text: &str) -> Result<Self, &'static str> {
        let uri: Uri = text.parse().map_err(|_| "is not a URI")?;
        let scheme = uri.scheme().cloned().ok_or("has no scheme")?;
        if scheme != Scheme::HTTP && scheme != Scheme::HTTPS {
            return Err("must be http or https");
        }
        let authority = uri.authority().cloned().ok_or("has no host")?;
        if authority.as_str().contains('@') {
            return Err("must not carry user info");
        }
        if uri.query().is_some() {
            return Err("must not have a query");
        }
        let path = uri.path().trim_end_matches('/').to_owned();
        Ok(Self {
            scheme,
            authority,
            path,
        })
    }

    /// The upstream URI for a request whose path under the route prefix is
    /// `rest`, with the client's query unchanged.
    fn uri(&self, rest: &str, query: Option<&str>) -> Option<Uri> {
        let path = self.path(rest);
        let path_and_query = match query {
            Some(query) => format!("{path}?{query}"),
            None => path,
        };
        Uri::builder()
            .scheme(self.scheme.clone())
            .authority(self.authority.clone())
            .path_and_query(path_and_query)
            .build()
            .ok()
    }

    fn path(&self, rest: &str) -> String {
        let joined = format!("{}{}", self.path, rest);
        if joined.is_empty() {
            "/".to_owned()
        } else {
            joined
        }
    }

    pub fn host(&self) -> &str {
        self.authority.host()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Route {
    name: RouteName,
    prefix: RoutePrefix,
    upstream: Upstream,
    base: UpstreamBase,
}

/// Where one request goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub mode: IngressMode,
    pub upstream: Upstream,
    /// The absolute upstream URI: base URL, path under the prefix, query.
    pub uri: Uri,
    /// The request path as the upstream sees it: what adapters classify.
    pub upstream_path: String,
}

/// The configured reverse-proxy routes, longest prefix first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Routes {
    routes: Vec<Route>,
}

impl Routes {
    /// Check and index `configs`: valid prefixes and base URLs, unique names
    /// and prefixes. An empty list is allowed (every request is answered
    /// 421).
    pub fn new(configs: &[RouteConfig]) -> Result<Self, ConfigError> {
        let mut names = HashSet::new();
        let mut prefixes = HashSet::new();
        let mut routes = Vec::with_capacity(configs.len());
        for config in configs {
            let route = config.name.0.clone();
            let prefix =
                RoutePrefix::new(&config.prefix).map_err(|reason| ConfigError::InvalidPrefix {
                    route: route.clone(),
                    prefix: config.prefix.clone(),
                    reason,
                })?;
            let base = UpstreamBase::parse(&config.upstream.base_url).map_err(|reason| {
                ConfigError::InvalidBaseUrl {
                    route: route.clone(),
                    reason,
                }
            })?;
            if !names.insert(route.clone()) {
                return Err(ConfigError::DuplicateName(route));
            }
            if !prefixes.insert(prefix.clone()) {
                return Err(ConfigError::DuplicatePrefix(prefix.as_str().to_owned()));
            }
            routes.push(Route {
                name: config.name.clone(),
                prefix,
                upstream: Upstream {
                    id: config.upstream.id.clone(),
                    kind: config.upstream.kind.clone(),
                },
                base,
            });
        }
        routes.sort_by_key(|route| std::cmp::Reverse(route.prefix.0.len()));
        Ok(Self { routes })
    }

    /// The route for `path` and the upstream URI for `path` and `query`.
    /// Looks at nothing else.
    pub fn resolve(&self, path: &str, query: Option<&str>) -> Option<Resolved> {
        self.routes.iter().find_map(|route| {
            let rest = route.prefix.strip(path)?;
            let uri = route.base.uri(rest, query)?;
            Some(Resolved {
                mode: IngressMode::ReverseProxy {
                    route: route.name.clone(),
                },
                upstream: route.upstream.clone(),
                upstream_path: route.base.path(rest),
                uri,
            })
        })
    }
}

impl UpstreamRouter for Routes {
    fn route(&self, head: &RequestHead) -> Option<(IngressMode, Upstream)> {
        self.resolve(&head.path, head.query.as_deref())
            .map(|resolved| (resolved.mode, resolved.upstream))
    }

    /// The reverse proxy intercepts nothing: forward-proxy mode (TLS
    /// interception for allowlisted hosts) is roadmap item P8.
    fn intercept(&self, _host: &Host) -> Option<Upstream> {
        None
    }
}
