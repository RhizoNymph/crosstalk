//! The tokio runtime `serve` (and every `crosstalk` command) runs on: one
//! builder, so tests that must run the gateway exactly as `serve` does
//! build theirs from it and cannot drift from it.
//!
//! A multi-thread runtime with tokio's defaults (one worker per core,
//! tokio's default worker stack size) and every driver enabled. Callers
//! may name the threads; nothing else is changed.

use tokio::runtime::{Builder, Runtime};

/// The builder `serve` builds its runtime from.
pub fn builder() -> Builder {
    let mut builder = Builder::new_multi_thread();
    builder.enable_all();
    builder
}

/// `serve`'s runtime.
pub fn build() -> std::io::Result<Runtime> {
    builder().build()
}

#[cfg(test)]
mod tests {
    #[test]
    fn serve_runtime_runs_tasks_on_worker_threads() {
        let runtime = super::builder()
            .thread_name("serve-runtime-test")
            .build()
            .expect("a runtime");
        let name = runtime
            .block_on(runtime.spawn(async { std::thread::current().name().map(str::to_owned) }))
            .expect("joins");
        assert_eq!(name.as_deref(), Some("serve-runtime-test"));
    }
}
