//! Where the data is in time: two things the UI needs from the gateway that
//! `crosstalk-spec`'s L8 traits do not expose.
//!
//! - **The bucket width.** Graph, channel, agent and series queries refuse a
//!   window that is not on bucket boundaries
//!   (`InvalidInput(UnalignedWindow)`) and a series grid built for another
//!   width (`InvalidInput(BucketWidthMismatch)`), but only L7 knows the
//!   width (`EdgeStore::bucket_width`); `QueryApi` has no method for it. The
//!   UI needs it to snap windows and the time brush, and to build a
//!   `SeriesGrid`.
//! - **The present.** A default view is "the last 24 hours". `QueryApi`
//!   offers the watermark, which trails the newest data by the settling
//!   delay, but no clock. A gateway answers with its wall clock; the fixture
//!   answers with the end of its generated week plus the time since startup.
//!
//! Both are proposed for `QueryApi`; when they land there this module is
//! deleted.

use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::interfaces::l8_surface::{Caller, QueryError};
use crosstalk_spec::support::Timestamp;

pub trait Present {
    /// The edge store's bucket width: every window a view sends starts and
    /// ends on a multiple of it.
    fn bucket_width(&self) -> BucketWidth;

    /// View. The end of the data a default view shows.
    async fn now(&self, caller: &Caller) -> Result<Timestamp, QueryError>;

    /// View. Where a default view's window ends: `now`, unless the backend
    /// replays data up to a fixed end.
    async fn view_end(&self, caller: &Caller) -> Result<Timestamp, QueryError> {
        self.now(caller).await
    }
}
