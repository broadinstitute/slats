use std::fs::File;
use parquet::file::reader::{FileReader, SerializedFileReader};
use crate::config::MetaConfig;
use crate::Error;

pub(crate) fn meta(config: &MetaConfig) -> Result<(), Error> {
    show_metadata(&config.file)
}

pub(crate) fn show_metadata(path: &str) -> Result<(), Error> {
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
