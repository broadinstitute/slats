mod error;

use std::path::Path;
use error::Error;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;

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
        for i_col in 0 .. n_cols {
            let col = row_group.column(i_col);
            println!("Column {} of row group {} has name {}.", i_col, i_row_group,
                     col.column_descr().name());
        }
    }
    Ok(())
}

fn main() {
    let db_file = "app41189_20220119150724";
    let db_path = Path::new(db_file);
    match show_metadata(db_path) {
        Ok(_) => { println!("Done!") }
        Err(error) => { println!("Error: {}", error) }
    }
    println!("Done");
}
