//! Every async trait method in `interfaces/` returns a `Send` future, and
//! every associated stream or per-connection handle is `Send + 'static`
//! (`canonical.interface.send-futures`).
//!
//! The tests are checked by the compiler, not at run time. Each trait has:
//!
//! - an implementation for [`Dummy`], written with `async fn` as a real
//!   implementation would be, which shows that an `async fn` meets the
//!   trait's `Send` bound;
//! - a check function generic over the trait, which calls every async
//!   method and hands the future to [`assert_send`], and names every
//!   associated stream in [`assert_send_static`]. Inside a generic
//!   function the compiler knows only what the trait declares, so if a
//!   method lost its `+ Send` (or a stream its bound), the check would stop
//!   compiling. A concrete type would not do: the compiler sees through a
//!   concrete implementation's future and finds it `Send` regardless.
//!
//! The `#[test]` functions name each check at `Dummy`, which proves the
//! dummy implements the trait. Nothing is ever called: `Dummy` has no
//! values, so the futures and arguments exist only for the type checker.

mod detection;
mod pipeline;
mod surface;

/// Implements every async trait. It has no values, so no method body runs.
pub(super) enum Dummy {}

/// Compiles only when `T` is `Send`.
pub(super) fn assert_send<T: Send>(_: T) {}

/// Compiles only when `T` is `Send + 'static`.
pub(super) fn assert_send_static<T: Send + 'static>() {}

/// An argument of any type, for calls the type checker sees and nothing
/// runs: there is no `Dummy` to pass.
pub(super) fn arg<V>(never: &Dummy) -> V {
    match *never {}
}
