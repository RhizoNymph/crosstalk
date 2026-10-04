//! The fixture backend (`crosstalk-fixture`) and the UI's gap traits over
//! it.
//!
//! The fixture implements the spec's `QueryApi`, `OperatorActions` and
//! `LiveFeed` itself. The gap traits are the UI's
//! (`crate::contract`), so they are implemented here, over the fixture's
//! `BUCKET_WIDTH`, `present` and `EXPORT_FORMATS`.

use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::interfaces::l8_surface::export::ExportFormat;
use crosstalk_spec::support::Timestamp;

pub use crosstalk_fixture::*;

use crate::contract::formats::ExportFormats;
use crate::contract::present::Present;

impl Present for FixtureBackend {
    fn bucket_width(&self) -> BucketWidth {
        Self::BUCKET_WIDTH
    }

    async fn now(&self, caller: &Caller) -> super::Result<Timestamp> {
        self.present(caller).await
    }
}

impl ExportFormats for FixtureBackend {
    fn export_formats(&self) -> &'static [ExportFormat] {
        Self::EXPORT_FORMATS
    }
}
