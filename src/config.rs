use std::env;
use clap::{arg, Command, command};
use crate::Error;

pub(crate) enum Config {
    Inspect(InspectConfig),
    Join(JoinConfig),
}

pub(crate) struct InspectConfig {
    pub(crate) sum_stats: Option<String>,
    pub(crate) covariances: Option<String>,
}

pub(crate) struct JoinConfig {
    pub(crate) covariances1: String,
    pub(crate) covariances2: String,
    pub(crate) out: String,
}

pub(crate) fn get_config() -> Result<Config, Error> {
    const INSPECT: &str = "inspect";
    const JOIN: &str = "join";
    let matches = command!()
        .propagate_version(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new(INSPECT)
                .about("Inspect files.")
                .arg(arg!(-s --sum_stats <FILE> "Sum stats file").required(false))
                .arg(arg!(-c --covariances <FILE> "Covariances file").required(false))
        ).subcommand(
        Command::new(JOIN)
            .about("Join two covariances files.")
            .arg(arg!(-i --covariances1 <FILE> "Covariances file 1"))
            .arg(arg!(-j --covariances2 <FILE> "Covariances file 2"))
            .arg(arg!(-o --out <FILE> "Output file"))
    ).get_matches();
    let matches = matches.subcommand();
    match matches {
        Some((INSPECT, matches)) => {
            let sum_stats = matches.value_of("sum_stats").map(String::from);
            let covariances = matches.value_of("covariances").map(String::from);
            Ok(Config::Inspect(InspectConfig { sum_stats, covariances }))
        }
        Some((JOIN, matches)) => {
            let covariances1 =
                String::from(
                    matches.value_of("covariances1")
                        .ok_or_else(|| Error::from("Missing argument 'covariances1'."))?
                );
            let covariances2 =
                String::from(
                    matches.value_of("covariances2")
                        .ok_or_else(|| Error::from("Missing argument 'covariances2'."))?
                );
            let out =
                String::from(
                    matches.value_of("out")
                        .ok_or_else(|| Error::from("Missing argument 'out'."))?
                );
            Ok(Config::Join(JoinConfig { covariances1, covariances2, out }))
        }
        Some((subcommand, _)) => {
            Err(Error::from(
                format!("{} is not a valid subcommand. Valid subcommands are {} and {}.",
                        subcommand, INSPECT, JOIN)
            ))
        }
        None => {
            Err(Error::from(
                format!("Missing subcommand. Valid subcommands are {} and {}.", INSPECT,
                        JOIN)
            ))
        }
    }
}
