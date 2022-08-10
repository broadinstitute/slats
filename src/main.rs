mod error;
mod config;
mod meta;
mod join;
mod data;
mod inspect;

use error::Error;
use crate::config::Config;

fn run() -> Result<(), Error> {
    match config::get_config()? {
        Config::Inspect(inspect_config) => { inspect::inspect(&inspect_config) }
        Config::Join(join_config) => { join::join(&join_config) }
    }
}

fn main() {
    match run() {
        Ok(_) => { println!("Done!") }
        Err(error) => { println!("Error: {}", error) }
    }
}
