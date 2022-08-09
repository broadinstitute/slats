mod error;
mod config;
mod meta;
mod read;

use error::Error;

fn run() -> Result<(), Error> {
    let files = config::get_files()?;
    println!("Metadata for {}", &files.sum_stats);
    meta::show_metadata(&files.sum_stats)?;
    println!("Metadata for {}", &files.covariances);
    meta::show_metadata(&files.covariances)?;
    read::read(&files)?;
    Ok(())
}

fn main() {
    match run() {
        Ok(_) => { println!("Done!") }
        Err(error) => { println!("Error: {}", error) }
    }
}
