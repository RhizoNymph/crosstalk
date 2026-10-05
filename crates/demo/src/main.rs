//! `crosstalk-demo`: the fake upstream, the wiki, the swarm and the
//! container healthcheck, as subcommands of one binary.

use std::process::ExitCode;

use crosstalk_demo::cli::{Command, USAGE};
use crosstalk_demo::{http, logging, swarm, upstream, wiki};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = match Command::parse(&args) {
        Ok(command) => command,
        Err(error) => {
            eprintln!("crosstalk-demo: {error}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    if command == Command::Help {
        print!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    logging::init();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            tracing::error!(%error, "cannot start the runtime");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(run(command))
}

async fn run(command: Command) -> ExitCode {
    match command {
        Command::Help => ExitCode::SUCCESS,
        Command::Upstream(config) => match upstream::run(config, http::shutdown_signal()).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                tracing::error!(%error, "fake upstream failed");
                ExitCode::FAILURE
            }
        },
        Command::Wiki(config) => match wiki::run(config, http::shutdown_signal()).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                tracing::error!(%error, "wiki failed");
                ExitCode::FAILURE
            }
        },
        Command::Swarm { config, json } => match swarm::run(*config, http::shutdown_signal()).await
        {
            Ok(report) => {
                if json {
                    match serde_json::to_string_pretty(&report) {
                        Ok(text) => println!("{text}"),
                        Err(error) => {
                            tracing::error!(%error, "cannot encode the report");
                            return ExitCode::FAILURE;
                        }
                    }
                } else {
                    print!("{report}");
                }
                ExitCode::SUCCESS
            }
            Err(error) => {
                tracing::error!(%error, "swarm failed");
                ExitCode::FAILURE
            }
        },
        Command::Healthcheck { url } => match http::healthcheck(&url).await {
            Ok(_) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("crosstalk-demo healthcheck: {url}: {error}");
                ExitCode::FAILURE
            }
        },
    }
}
