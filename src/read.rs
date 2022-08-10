use std::cmp::max;
use std::fs::File;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Row, RowAccessor};
use crate::config::Files;
use crate::Error;

struct SumStat {
    chr: String,
    pos: u32,
    ref_allele: String,
    alt_allele: String,
    alt_ac: u32,
    mac: u32,
    maf: f64,
    n: u32,
    u: f64,
    v: f64,
    f1: f64,
    f2: f64,
    f3: f64,
    f4: f64,
}

impl SumStat {
    fn read(record: &Row) -> Result<Self, Error> {
        let chr = record.get_string(0)?.clone();
        let pos = record.get_uint(1)?;
        let ref_allele = record.get_string(2)?.clone();
        let alt_allele = record.get_string(3)?.clone();
        let alt_ac = record.get_uint(4)?;
        let mac = record.get_uint(5)?;
        let maf = record.get_double(6)?;
        let n = record.get_uint(7)?;
        let u = record.get_double(8)?;
        let v = record.get_double(9)?;
        let f1 = record.get_double(10)?;
        let f2 = record.get_double(11)?;
        let f3 = record.get_double(12)?;
        let f4 = record.get_double(13)?;
        Ok(SumStat { chr, pos, ref_allele, alt_allele, alt_ac, mac, maf, n, u, v, f1, f2, f3, f4 })
    }
}

struct Covariance {
    row: u32,
    col: u32,
    value: f64,
}

impl Covariance {
    fn read(record: &Row) -> Result<Self, Error> {
        let row = record.get_uint(0)?;
        let col = record.get_uint(1)?;
        let value = record.get_double(2)?;
        Ok(Covariance { row, col, value })
    }
}

// Metadata for /home/oliverr/metastaar/phenics/spirit0/sum_stats/summary_statistics.chr1.764.parquet
// Number of row groups: 1
// Row group 0 has 14 columns.
// (0) chr: BYTE_ARRAY / String
// (1) pos: INT32 / Integer { bit_width: 32, is_signed: false }
// (2) ref: BYTE_ARRAY / String
// (3) alt: BYTE_ARRAY / String
// (4) alt_AC: INT32 / Integer { bit_width: 32, is_signed: false }
// (5) MAC: INT32 / Integer { bit_width: 32, is_signed: false }
// (6) MAF: DOUBLE
// (7) N: INT32 / Integer { bit_width: 32, is_signed: false }
// (8) U: DOUBLE
// (9) V: DOUBLE
// (10) 1: DOUBLE
// (11) 2: DOUBLE
// (12) 3: DOUBLE
// (13) 4: DOUBLE
// Metadata for /home/oliverr/metastaar/phenics/spirit0/covariances/covariances.chr1.764.parquet
// Number of row groups: 1
// Row group 0 has 3 columns.
// (0) row: INT32 / Integer { bit_width: 32, is_signed: false }
// (1) col: INT32 / Integer { bit_width: 32, is_signed: false }
// (2) value: DOUBLE
// Sum stats file has 12949 rows.
// Max n in sum stats is 54035
// Covariance matrix has size 25995
// Done!

pub(crate) fn read(files: &Files) -> Result<(), Error> {
    let sum_stats_reader =
        SerializedFileReader::new(File::open(&files.sum_stats)?)?;
    let n_sum_stats = sum_stats_reader.get_row_iter(None)?.count();
    println!("Sum stats file has {} rows.", n_sum_stats);
    let mut n_sum_stats: u32 = 0;
    for (i, row) in sum_stats_reader.get_row_iter(None)?.enumerate() {
        let sum_stat = SumStat::read(&row)?;
        if i < 30 {
            println!("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}", sum_stat.chr, sum_stat.pos,
                     sum_stat.ref_allele, sum_stat.alt_allele, sum_stat.alt_ac, sum_stat.mac,
                     sum_stat.maf, sum_stat.n)
        }
        n_sum_stats = max(n_sum_stats, sum_stat.n);
    }
    println!("Max n in sum stats is {}", n_sum_stats);
    let covariances_reader =
        SerializedFileReader::new(File::open(&files.covariances)?)?;
    let mut n_matrix: u32 = 0;
    for (i, row) in covariances_reader.get_row_iter(None)?.enumerate() {
        let covariance = Covariance::read(&row)?;
        if i < 30 {
            println!("{}\t{}\t{}", covariance.row, covariance.col, covariance.value)
        }
        let matrix_row = covariance.row;
        let matrix_col = covariance.col;
        n_matrix = max(max(n_matrix, matrix_row), matrix_col);
    }
    println!("Covariance matrix has size {}", n_matrix);
    Ok(())
}