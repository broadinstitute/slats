mod error;

use std::env;
use std::path::Path;
use error::Error;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;

fn get_file() -> Result<String, Error> {
    let mut next_is_file = false;
    for arg in env::args() {
        if next_is_file {
            return Ok(arg);
        } else if arg == "-f" {
            next_is_file = true;
        }
    }
    Err(Error::from("Missing argument '-f'."))
}

fn show_metadata(path: &Path) -> Result<(), Error> {
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();
    let n_row_groups = metadata.num_row_groups();
    println!("Number of row groups: {}", n_row_groups);
    for i_row_group in 0..n_row_groups {
        let row_group = metadata.row_group(i_row_group);
        let n_cols = row_group.num_columns();
        println!("Row group {} has {} columns.", i_row_group, n_cols);
        for i_col in 0..n_cols {
            let col = row_group.column(i_col);
            let col_name = col.column_descr().name();
            let phys_type_name = col.column_descr().physical_type().to_string();
            let log_type_name = col.column_descr().logical_type()
                .map(|log_type| { format!(" / {:?}", log_type) })
                .unwrap_or_else(|| String::from(""));
            println!("({}) {}: {}{}",
                     i_col, col_name, phys_type_name, log_type_name);
        }
    }
    Ok(())
}

fn run() -> Result<(), Error> {
    let db_file = get_file()?;
    let db_path = Path::new(&db_file);
    show_metadata(db_path)?;
    Ok(())
}

fn main() {
    match run() {
        Ok(_) => { println!("Done!") }
        Err(error) => { println!("Error: {}", error) }
    }
}
