use std::cmp::max;
use std::fs::File;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use crate::config::Files;
use crate::Error;

pub(crate) fn read(files: &Files) -> Result<(), Error> {
    let sum_stats_reader =
        SerializedFileReader::new(File::open(&files.sum_stats)?)?;
    let n_sum_stats = sum_stats_reader.get_row_iter(None)?.count();
    println!("Sum stats file has {} rows.", n_sum_stats);
    let covariances_reader =
        SerializedFileReader::new(File::open(&files.covariances)?)?;
    let mut n_matrix: u32 = 0;
    for row in covariances_reader.get_row_iter(None)? {
        let matrix_row = row.get_uint(0)?;
        let matrix_col = row.get_uint(1)?;
        n_matrix = max(max(n_matrix, matrix_row), matrix_col);
    }
    println!("Covariance matrix has size {}", n_matrix);
    Ok(())
}