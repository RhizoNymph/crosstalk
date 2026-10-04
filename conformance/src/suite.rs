//! Running the suite: [`run`] drives one test on a current-thread runtime,
//! and [`suite!`](crate::suite) instantiates every test for a harness.

use crate::harness::Harness;

/// The runtime a test needed could not be built.
#[derive(Debug, thiserror::Error)]
#[error("building the test runtime: {0}")]
pub struct RunError(#[from] std::io::Error);

/// Runs `test` against `harness` on a fresh current-thread runtime with
/// time enabled. Nothing is spawned, so no future needs to be `Send`.
pub fn run<H: Harness>(harness: H, test: impl AsyncFnOnce(&H)) -> Result<(), RunError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(test(&harness));
    Ok(())
}

/// Instantiates every conformance test for a harness: one `#[test]` per
/// suite test, in a module per area, each building its own harness with
/// `$harness` (an expression evaluated in a child module of the caller,
/// which sees the caller's names).
///
/// ```ignore
/// mod conformance {
///     crosstalk_conformance::suite!(crate::conformance::MyHarness::new());
/// }
/// ```
#[macro_export]
macro_rules! suite {
    ($harness:expr) => {
        $crate::__suite_tests! {
            ($harness)
            scenarios {
                every_named_scenario_is_valid,
                hijacked_wiki,
                late_confirmation,
                impersonation,
                merges,
                hidden_channel,
                suspected,
                declared,
                lone_resource,
                promotion,
                policies,
                topics,
                verdicts,
                dropped_bodies,
                pipeline,
                routes,
                registered,
                everything,
            }
        }
    };
}

/// The expansion of [`suite!`](crate::suite).
#[doc(hidden)]
#[macro_export]
macro_rules! __suite_tests {
    (($harness:expr) $($area:ident { $($test:ident),* $(,)? })*) => {
        $(
            mod $area {
                #[allow(unused_imports)]
                use super::*;
                $(
                    #[test]
                    fn $test() -> ::core::result::Result<(), $crate::RunError> {
                        $crate::run($harness, async |harness| {
                            $crate::tests::$area::$test(harness).await
                        })
                    }
                )*
            }
        )*
    };
}
