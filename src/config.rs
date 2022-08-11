use std::env;
use clap::{arg, Command, command};
use crate::Error;

pub(crate) enum Config {
    Inspect(InspectConfig),
    JoinCov(JoinCovConfig),
    JoinSum(JoinSumConfig),
}

pub(crate) struct InspectConfig {
    pub(crate) sum_stats: Option<String>,
    pub(crate) covariances: Option<String>,
}

pub(crate) struct JoinCovConfig {
    pub(crate) covariances1: String,
    pub(crate) covariances2: String,
    pub(crate) out: String,
}

pub(crate) struct JoinSumConfig {
    pub(crate) sum_stats1: String,
    pub(crate) sum_stats2: String,
    pub(crate) out: String,
}

pub(crate) fn get_config() -> Result<Config, Error> {
    const INSPECT: &str = "inspect";
    const JOIN_COV: &str = "join_cov";
    const JOIN_SUM: &str = "join_sum";
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
        Command::new(JOIN_COV)
            .about("Join two covariances files.")
            .arg(arg!(-i --covariances1 <FILE> "Covariances file 1"))
            .arg(arg!(-j --covariances2 <FILE> "Covariances file 2"))
            .arg(arg!(-o --out <FILE> "Output file"))
    ).subcommand(
        Command::new(JOIN_SUM)
            .about("Join two sum stat files.")
            .arg(arg!(-i --sum_stats1 <FILE> "Sum stats file 1"))
            .arg(arg!(-j --sum_stats2 <FILE> "Sum stats file 2"))
            .arg(arg!(-o --out <FILE> "Output file"))
    ).get_matches();
    let matches = matches.subcommand();
    match matches {
        Some((INSPECT, matches)) => {
            let sum_stats = matches.value_of("sum_stats").map(String::from);
            let covariances = matches.value_of("covariances").map(String::from);
            Ok(Config::Inspect(InspectConfig { sum_stats, covariances }))
        }
        Some((JOIN_COV, matches)) => {
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
            Ok(Config::JoinCov(JoinCovConfig { covariances1, covariances2, out }))
        }
        Some((JOIN_SUM, matches)) => {
            let sum_stats1 =
                String::from(
                    matches.value_of("sum_stats1")
                        .ok_or_else(|| Error::from("Missing argument 'sum_stats1'."))?
                );
            let sum_stats2 =
                String::from(
                    matches.value_of("sum_stats2")
                        .ok_or_else(|| Error::from("Missing argument 'sum_stats2'."))?
                );
            let out =
                String::from(
                    matches.value_of("out")
                        .ok_or_else(|| Error::from("Missing argument 'out'."))?
                );
            Ok(Config::JoinSum(JoinSumConfig { sum_stats1, sum_stats2, out }))
        }
        Some((subcommand, _)) => {
            Err(Error::from(
                format!("{} is not a valid subcommand. Valid subcommands are {}, {} and {}.",
                        subcommand, INSPECT, JOIN_COV, JOIN_SUM)
            ))
        }
        None => {
            Err(Error::from(
                format!("Missing subcommand. Valid subcommands are {}, {} and {}.", INSPECT,
                        JOIN_COV, JOIN_SUM)
            ))
        }
    }
}
