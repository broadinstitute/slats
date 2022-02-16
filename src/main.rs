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
        for i_col in 0..n_cols {
            let col = row_group.column(i_col);
            let col_name = col.column_descr().name();
            let phys_type_name = col.column_descr().physical_type().to_string();
            let log_type_name = col.column_descr().logical_type()
                .map(|log_type| { format!("{:?}", log_type) })
                .unwrap_or_else(|| String::from("[none]"));
            println!("Column {} of row group {} has name {}, physical type {} and logical type {}.",
                     i_col, i_row_group, col_name, phys_type_name, log_type_name);
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
