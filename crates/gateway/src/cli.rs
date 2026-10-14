//! The `crosstalk` command line.
//!
//! ```text
//! crosstalk serve --role <all|proxy|pipeline|api|analysis> --config <path>
//! crosstalk migrate --config <path> [--reset-correlator]
//! crosstalk spool --config <path> --discard-corrupt
//! crosstalk healthcheck --url <url>
//! crosstalk inspect --config <path> [<exchange-id>]
//! crosstalk help
//! ```
//!
//! `serve`, `migrate` and `healthcheck` are the deployment contract
//! (`docs/features/deploy.md`); `inspect` reads what the gateway captured.
//! `migrate --reset-correlator` replaces L5's checkpoint after an
//! incompatible one (decision Q2); `spool --discard-corrupt` drops the
//! corrupt record that stopped the publish spool's drain, and everything
//! after it in its segment (the gateway must be stopped).

use std::path::PathBuf;

use crate::role::{Role, UnknownRole};

/// What the binary was asked to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Serve {
        role: Role,
        config: PathBuf,
    },
    Migrate {
        config: PathBuf,
        /// `--reset-correlator`: also reset L5's checkpoint.
        reset_correlator: bool,
    },
    /// `spool --discard-corrupt`.
    SpoolDiscardCorrupt {
        config: PathBuf,
    },
    Healthcheck {
        url: String,
    },
    Inspect {
        config: PathBuf,
        exchange: Option<String>,
    },
    Help,
}

/// Why the arguments name no command.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UsageError {
    #[error("no command given")]
    Missing,
    #[error("unknown command {0:?}")]
    UnknownCommand(String),
    #[error("unexpected argument {0:?}")]
    Unexpected(String),
    #[error("{command} needs {flag} <value>")]
    MissingFlag {
        command: &'static str,
        flag: &'static str,
    },
    #[error("{0} is given twice")]
    Repeated(&'static str),
    #[error("{command} needs {switch}")]
    MissingSwitch {
        command: &'static str,
        switch: &'static str,
    },
    #[error(transparent)]
    Role(#[from] UnknownRole),
}

pub const USAGE: &str = "\
usage:
  crosstalk serve --role <all|proxy|pipeline|api|analysis> --config <path>
  crosstalk migrate --config <path> [--reset-correlator]
  crosstalk spool --config <path> --discard-corrupt
  crosstalk healthcheck --url <url>
  crosstalk inspect --config <path> [<exchange-id>]
  crosstalk help

environment:
  DATABASE_URL                   postgres, when the config has a store section
  the variables the config names ingress.secrets.*.env (CROSSTALK_SECRET_V1)
  RUST_LOG                       tracing filter (default info); logs are JSON on stdout
";

/// The flags one command accepts, read in any order.
#[derive(Default)]
struct Flags {
    role: Option<String>,
    config: Option<String>,
    url: Option<String>,
    reset_correlator: bool,
    discard_corrupt: bool,
    positional: Vec<String>,
}

impl Flags {
    fn read(
        mut args: impl Iterator<Item = String>,
        allowed: &[&'static str],
    ) -> Result<Self, UsageError> {
        let mut flags = Self::default();
        while let Some(arg) = args.next() {
            let switch = match arg.as_str() {
                "--reset-correlator" if allowed.contains(&"--reset-correlator") => {
                    Some((&mut flags.reset_correlator, "--reset-correlator"))
                }
                "--discard-corrupt" if allowed.contains(&"--discard-corrupt") => {
                    Some((&mut flags.discard_corrupt, "--discard-corrupt"))
                }
                _ => None,
            };
            if let Some((set, name)) = switch {
                if *set {
                    return Err(UsageError::Repeated(name));
                }
                *set = true;
                continue;
            }
            let (slot, name) = match arg.as_str() {
                "--role" if allowed.contains(&"--role") => (&mut flags.role, "--role"),
                "--config" if allowed.contains(&"--config") => (&mut flags.config, "--config"),
                "--url" if allowed.contains(&"--url") => (&mut flags.url, "--url"),
                other if other.starts_with("--") => return Err(UsageError::Unexpected(arg)),
                _ => {
                    flags.positional.push(arg);
                    continue;
                }
            };
            if slot.is_some() {
                return Err(UsageError::Repeated(name));
            }
            *slot = Some(args.next().ok_or(UsageError::Unexpected(arg))?);
        }
        Ok(flags)
    }

    fn no_positional(&mut self) -> Result<(), UsageError> {
        match self.positional.drain(..).next() {
            None => Ok(()),
            Some(extra) => Err(UsageError::Unexpected(extra)),
        }
    }
}

fn required(
    value: Option<String>,
    command: &'static str,
    flag: &'static str,
) -> Result<String, UsageError> {
    value.ok_or(UsageError::MissingFlag { command, flag })
}

impl Command {
    /// Parse the arguments after the program name.
    pub fn parse<I: IntoIterator<Item = String>>(args: I) -> Result<Self, UsageError> {
        let mut args = args.into_iter();
        let command = args.next().ok_or(UsageError::Missing)?;
        match command.as_str() {
            "serve" => {
                let mut flags = Flags::read(args, &["--role", "--config"])?;
                flags.no_positional()?;
                let role = required(flags.role, "serve", "--role")?.parse()?;
                let config = required(flags.config, "serve", "--config")?.into();
                Ok(Self::Serve { role, config })
            }
            "migrate" => {
                let mut flags = Flags::read(args, &["--config", "--reset-correlator"])?;
                flags.no_positional()?;
                let config = required(flags.config, "migrate", "--config")?.into();
                Ok(Self::Migrate {
                    config,
                    reset_correlator: flags.reset_correlator,
                })
            }
            "spool" => {
                let mut flags = Flags::read(args, &["--config", "--discard-corrupt"])?;
                flags.no_positional()?;
                let config = required(flags.config, "spool", "--config")?.into();
                if !flags.discard_corrupt {
                    return Err(UsageError::MissingSwitch {
                        command: "spool",
                        switch: "--discard-corrupt",
                    });
                }
                Ok(Self::SpoolDiscardCorrupt { config })
            }
            "healthcheck" => {
                let mut flags = Flags::read(args, &["--url"])?;
                flags.no_positional()?;
                let url = required(flags.url, "healthcheck", "--url")?;
                Ok(Self::Healthcheck { url })
            }
            "inspect" => {
                let flags = Flags::read(args, &["--config"])?;
                let config = required(flags.config, "inspect", "--config")?.into();
                let mut positional = flags.positional.into_iter();
                let exchange = positional.next();
                if let Some(extra) = positional.next() {
                    return Err(UsageError::Unexpected(extra));
                }
                Ok(Self::Inspect { config, exchange })
            }
            "help" | "--help" | "-h" => {
                let mut flags = Flags::read(args, &[])?;
                flags.no_positional()?;
                Ok(Self::Help)
            }
            other => Err(UsageError::UnknownCommand(other.to_owned())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Command, UsageError> {
        Command::parse(args.iter().map(|arg| (*arg).to_owned()))
    }

    #[test]
    fn the_deployment_commands_parse() {
        assert_eq!(
            parse(&[
                "serve",
                "--role",
                "all",
                "--config",
                "/etc/crosstalk/crosstalk.json"
            ]),
            Ok(Command::Serve {
                role: Role::All,
                config: PathBuf::from("/etc/crosstalk/crosstalk.json")
            })
        );
        assert_eq!(
            parse(&["serve", "--config", "c.json", "--role", "proxy"]),
            Ok(Command::Serve {
                role: Role::Proxy,
                config: PathBuf::from("c.json")
            })
        );
        assert_eq!(
            parse(&["migrate", "--config", "/etc/crosstalk/crosstalk.json"]),
            Ok(Command::Migrate {
                config: PathBuf::from("/etc/crosstalk/crosstalk.json"),
                reset_correlator: false
            })
        );
        assert_eq!(
            parse(&["healthcheck", "--url", "http://127.0.0.1:9464/readyz"]),
            Ok(Command::Healthcheck {
                url: "http://127.0.0.1:9464/readyz".to_owned()
            })
        );
    }

    #[test]
    fn inspect_and_help_parse() {
        assert_eq!(
            parse(&["inspect", "--config", "c.json"]),
            Ok(Command::Inspect {
                config: PathBuf::from("c.json"),
                exchange: None
            })
        );
        assert_eq!(
            parse(&[
                "inspect",
                "--config",
                "c.json",
                "01M3TC5H00000001R000000003"
            ]),
            Ok(Command::Inspect {
                config: PathBuf::from("c.json"),
                exchange: Some("01M3TC5H00000001R000000003".to_owned())
            })
        );
        assert_eq!(parse(&["help"]), Ok(Command::Help));
        assert_eq!(parse(&["--help"]), Ok(Command::Help));
    }

    #[test]
    fn bad_arguments_are_usage_errors() {
        assert_eq!(parse(&[]), Err(UsageError::Missing));
        assert_eq!(
            parse(&["serve", "--config", "c.json"]),
            Err(UsageError::MissingFlag {
                command: "serve",
                flag: "--role"
            })
        );
        assert_eq!(
            parse(&["serve", "--role", "all"]),
            Err(UsageError::MissingFlag {
                command: "serve",
                flag: "--config"
            })
        );
        assert!(matches!(
            parse(&["serve", "--role", "everything", "--config", "c.json"]),
            Err(UsageError::Role(_))
        ));
        assert_eq!(
            parse(&["serve", "--role", "all", "--role", "proxy", "--config", "c"]),
            Err(UsageError::Repeated("--role"))
        );
        assert_eq!(
            parse(&["serve", "--role", "all", "--config", "c", "extra"]),
            Err(UsageError::Unexpected("extra".to_owned()))
        );
        assert_eq!(
            parse(&["migrate", "--url", "x"]),
            Err(UsageError::Unexpected("--url".to_owned()))
        );
        assert_eq!(
            parse(&["healthcheck", "--url"]),
            Err(UsageError::Unexpected("--url".to_owned()))
        );
        assert_eq!(
            parse(&["inspect", "--config", "c", "id", "x"]),
            Err(UsageError::Unexpected("x".to_owned()))
        );
        assert_eq!(
            parse(&["proxy"]),
            Err(UsageError::UnknownCommand("proxy".to_owned()))
        );
    }

    #[test]
    fn the_postgres_maintenance_commands_parse() {
        assert_eq!(
            parse(&["migrate", "--reset-correlator", "--config", "c.json"]),
            Ok(Command::Migrate {
                config: PathBuf::from("c.json"),
                reset_correlator: true
            })
        );
        assert_eq!(
            parse(&["spool", "--config", "c.json", "--discard-corrupt"]),
            Ok(Command::SpoolDiscardCorrupt {
                config: PathBuf::from("c.json")
            })
        );
        assert_eq!(
            parse(&["spool", "--config", "c.json"]),
            Err(UsageError::MissingSwitch {
                command: "spool",
                switch: "--discard-corrupt"
            })
        );
        assert_eq!(
            parse(&[
                "migrate",
                "--config",
                "c",
                "--reset-correlator",
                "--reset-correlator"
            ]),
            Err(UsageError::Repeated("--reset-correlator"))
        );
        assert_eq!(
            parse(&[
                "serve",
                "--role",
                "all",
                "--config",
                "c",
                "--discard-corrupt"
            ]),
            Err(UsageError::Unexpected("--discard-corrupt".to_owned()))
        );
    }
}
