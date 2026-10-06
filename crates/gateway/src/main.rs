//! The `crosstalk` binary. See [`crosstalk_gateway::cli`] for the commands.

use std::process::ExitCode;
use std::sync::Arc;

use crosstalk_gateway::cli::{Command, USAGE};
use crosstalk_gateway::config::GatewayConfig;
use crosstalk_gateway::logging::{self, Sink};
use crosstalk_gateway::role::Role;
use crosstalk_gateway::{gateway, healthcheck, inspect, spool, store};
use crosstalk_spec::support::SystemClock;

fn main() -> ExitCode {
    let command = match Command::parse(std::env::args().skip(1)) {
        Ok(command) => command,
        Err(error) => {
            eprintln!("crosstalk: {error}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    if command == Command::Help {
        print!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("crosstalk: starting the tokio runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(run(command))
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

async fn run(command: Command) -> ExitCode {
    match command {
        Command::Help => ExitCode::SUCCESS,
        Command::Serve { role, config } => {
            logging::init(Sink::Stdout, "info");
            match GatewayConfig::load(&config) {
                Ok(config) => serve(&config, role).await,
                Err(error) => fail("loading the config", &error),
            }
        }
        Command::Migrate {
            config,
            reset_correlator,
        } => {
            logging::init(Sink::Stdout, "info");
            let config = match GatewayConfig::load(&config) {
                Ok(config) => config,
                Err(error) => return fail("loading the config", &error),
            };
            let options = store::MigrateOptions { reset_correlator };
            match store::migrate(&config, env, options, &SystemClock).await {
                Ok(()) => {
                    tracing::info!("migrations complete");
                    ExitCode::SUCCESS
                }
                Err(error) => fail("migrating", &error),
            }
        }
        Command::SpoolDiscardCorrupt { config } => {
            logging::init(Sink::Stdout, "info");
            let config = match GatewayConfig::load(&config) {
                Ok(config) => config,
                Err(error) => return fail("loading the config", &error),
            };
            match spool::discard_corrupt(&config).await {
                Ok(Some(discarded)) => {
                    println!(
                        "discarded {} bytes of {} from offset {}",
                        discarded.bytes, discarded.segment, discarded.offset
                    );
                    ExitCode::SUCCESS
                }
                Ok(None) => {
                    println!("the spool has no corruption");
                    ExitCode::SUCCESS
                }
                Err(error) => fail("discarding the spool's corruption", &error),
            }
        }
        Command::Healthcheck { url } => match healthcheck::check(&url).await {
            Ok(_) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("crosstalk healthcheck {url}: {error}");
                ExitCode::FAILURE
            }
        },
        Command::Inspect { config, exchange } => {
            logging::init(Sink::Stderr, "warn");
            let config = match GatewayConfig::load(&config) {
                Ok(config) => config,
                Err(error) => return fail("loading the config", &error),
            };
            let shown = match exchange {
                None => inspect::list(&config).await,
                Some(exchange) => inspect::show(&config, &exchange).await,
            };
            match shown {
                Ok(text) => {
                    println!("{}", text.trim_end());
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    eprintln!("crosstalk inspect: {error}");
                    ExitCode::FAILURE
                }
            }
        }
    }
}

async fn serve(config: &GatewayConfig, role: Role) -> ExitCode {
    let running = match gateway::start(config, role, env, Arc::new(SystemClock)).await {
        Ok(running) => running,
        Err(error) => return fail("starting the gateway", &error),
    };
    match shutdown_signal().await {
        Ok(signal) => tracing::info!(signal, "shutdown requested"),
        Err(error) => {
            tracing::error!(error = %error, "cannot listen for shutdown signals; stopping")
        }
    }
    running.shutdown().await;
    ExitCode::SUCCESS
}

/// Wait for SIGINT or SIGTERM.
async fn shutdown_signal() -> std::io::Result<&'static str> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    tokio::select! {
        _ = terminate.recv() => Ok("SIGTERM"),
        _ = interrupt.recv() => Ok("SIGINT"),
    }
}

fn fail(what: &str, error: &dyn std::error::Error) -> ExitCode {
    tracing::error!(error = %error, "{what} failed");
    eprintln!("crosstalk: {what}: {error}");
    ExitCode::FAILURE
}
