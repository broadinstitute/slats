use core::cmp::max;
use core::option::Option::None;
use core::result::Result;
use core::result::Result::Ok;
use parquet::file::serialized_reader::SerializedFileReader;
use std::fs::File;
use parquet::file::reader::FileReader;
use crate::config::InspectConfig;
use crate::data::{Covariance, SumStat};
use crate::{Error, meta};

pub(crate) fn inspect(config: &InspectConfig) -> Result<(), Error> {
    if let Some(sum_stats) = &config.sum_stats {
        println!("Metadata for {}", sum_stats);
        meta::show_metadata(sum_stats)?;
        read_sum_stats(sum_stats)?;
    }
    if let Some(covariances) = &config.covariances {
        println!("Metadata for {}", covariances);
        meta::show_metadata(covariances)?;
        read_covariances(covariances)?;
    }
    Ok(())

}

pub(crate) fn read_sum_stats(sum_stats: &str) -> Result<(), Error> {
    let sum_stats_reader =
        SerializedFileReader::new(File::open(sum_stats)?)?;
    let n_sum_stats = sum_stats_reader.get_row_iter(None)?.count();
    println!("Sum stats file has {} rows.", n_sum_stats);
    let mut n_sum_stats: u32 = 0;
    for (i, row) in sum_stats_reader.get_row_iter(None)?.enumerate() {
        let sum_stat = SumStat::read(&row)?;
        if i < 30 {
            println!("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}", sum_stat.variant.chr,
                     sum_stat.variant.pos, sum_stat.variant.ref_allele,
                     sum_stat.variant.alt_allele, sum_stat.data.alt_ac, sum_stat.data.mac,
                     sum_stat.data.maf, sum_stat.data.n, sum_stat.data.u, sum_stat.data.v)
        }
        n_sum_stats = max(n_sum_stats, sum_stat.data.n);
    }
    println!("Max n in sum stats is {}", n_sum_stats);
    Ok(())
}

pub(crate) fn read_covariances(covariances: &str) -> Result<(), Error> {
    let covariances_reader =
        SerializedFileReader::new(File::open(covariances)?)?;
    let mut n_matrix: u32 = 0;
    for (i, row) in covariances_reader.get_row_iter(None)?.enumerate() {
        let covariance = Covariance::read(&row)?;
        if i < 30 {
            println!("{}\t{}\t{}", covariance.indices.row, covariance.indices.col,
                     covariance.value)
        }
        let matrix_row = covariance.indices.row;
        let matrix_col = covariance.indices.col;
        n_matrix = max(max(n_matrix, matrix_row), matrix_col);
    }
    println!("Covariance matrix has size {}", n_matrix);
    Ok(())
}
