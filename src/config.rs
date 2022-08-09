use std::env;
use crate::Error;

pub(crate) struct Files {
    pub(crate) sum_stats: String,
    pub(crate) covariances: String
}

pub(crate) fn get_files() -> Result<Files, Error> {
    const SUM_OPT: &str = "-s";
    const COV_OPT: &str = "-c";
    let mut args = env::args();
    let mut sum_stats_file: Option<String> = None;
    let mut covariances_file: Option<String> = None;
    while let Some(arg) = args.next() {
        if arg == SUM_OPT {
            if let Some(sum_stats_arg) = args.next() {
                sum_stats_file = Some(sum_stats_arg)
            }
        } else if arg == COV_OPT {
            if let Some(covariances_arg) = args.next() {
                covariances_file = Some(covariances_arg)
            }
        }
    }
    match (sum_stats_file, covariances_file) {
        (Some(sum_stats), Some(covariances)) => {
            Ok(Files { sum_stats, covariances})
        }
        (Some(_), None) => {
            Err(Error::from(format!("Missing argument {}.", SUM_OPT)))
        }
        (None, Some(_)) => {
            Err(Error::from(format!("Missing argument {}.", COV_OPT)))
        }
        (None, None) => {
            Err(Error::from(format!("Missing arguments {} and {}.", SUM_OPT, COV_OPT)))
        }
    }
}

