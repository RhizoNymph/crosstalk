//! Per-request access to the configured backend, the caller, and the
//! backend's present.

use crosstalk_spec::interfaces::l8_surface::present::Present;
use crosstalk_spec::interfaces::l8_surface::{Caller, Permission, QueryApi, QueryError};
use topcoat::context::{Cx, app_context, memoize};

pub use crate::backend::AppBackend;
use crate::config::Access;
use crate::identity::Identity;

pub fn backend(cx: &Cx) -> &AppBackend {
    app_context::<AppBackend>(cx)
}

/// The caller of this request: the configured trusted operator with every
/// permission (the local backends), or the token's operator with the
/// permissions the server gives it (the http backend). Shards and
/// procedures call this themselves, since page guards do not run for their
/// endpoints.
pub fn caller(cx: &Cx) -> Caller {
    access(cx).caller()
}

/// Who this request acts as ([`Identity::current`]).
pub fn access(cx: &Cx) -> Access {
    app_context::<Identity>(cx).current()
}

pub fn can(caller: &Caller, permission: Permission) -> bool {
    caller.has(permission)
}

/// The backend's present (`QueryApi::present`), read at most once per
/// request: the view defaults, the layout and every component of the page
/// share the first read, so the clock, bucket width and formats one
/// response shows agree.
#[memoize(as_ref)]
pub async fn present(cx: &Cx) -> Result<Present, QueryError> {
    backend(cx).present(&caller(cx)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_request_shares_one_present() {
        let cx = crate::testing::cx();
        let first = present(&cx).await.expect("present");
        let again = present(&cx).await.expect("present");
        assert!(std::ptr::eq(first, again), "the second read is the first");
        let other = crate::testing::cx();
        let elsewhere = present(&other).await.expect("present");
        assert!(!std::ptr::eq(first, elsewhere), "requests do not share it");
        assert_eq!(first, elsewhere);
    }
}
