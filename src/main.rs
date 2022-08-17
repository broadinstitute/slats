mod error;
mod config;
mod meta;
mod join_cov;
mod data;
mod inspect;
mod join_sum;

use error::Error;
use crate::config::Config;

fn run() -> Result<(), Error> {
    match config::get_config()? {
        Config::Meta(meta_config) => { meta::meta(&meta_config) }
        Config::Inspect(inspect_config) => { inspect::inspect(&inspect_config) }
        Config::JoinCov(join_cov_config) => { join_cov::join(&join_cov_config) }
        Config::JoinSum(join_sum_config) => { join_sum::join(&join_sum_config) }
    }
}

fn main() {
    match run() {
        Ok(_) => { println!("Done!") }
        Err(error) => { println!("Error: {}", error) }
    }
}
