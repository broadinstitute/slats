use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Row;
use crate::config::JoinSumConfig;
use crate::data::{SumStat, Variant, VariantData};
use crate::Error;

pub(crate) fn join(config: &JoinSumConfig) -> Result<(), Error> {
    let mut unmatched1: HashMap<Variant, VariantData> = HashMap::new();
    let mut unmatched2: HashMap<Variant, VariantData> = HashMap::new();
    let reader1 =
        SerializedFileReader::new(File::open(&config.sum_stats1)?)?;
    let reader2 =
        SerializedFileReader::new(File::open(&config.sum_stats2)?)?;
    let mut rows1 = reader1.get_row_iter(None)?;
    let mut rows2 = reader2.get_row_iter(None)?;
    let mut writer = BufWriter::new(File::create(&config.out)?);
    loop {
        let row1 = rows1.next();
        let row2 = rows2.next();
        if let Some(row1) = &row1 {
            join_row(row1, &mut unmatched1, &mut unmatched2, &mut writer,
                     true)?;
        }
        if let Some(row2) = &row2 {
            join_row(row2, &mut unmatched2, &mut unmatched1, &mut writer,
                     false)?;
        }
        if row1.is_none() && row2.is_none() {
            break;
        }
    }
    writer.flush()?;
    Ok(())
}

fn join_row(record: &Row, these: &mut HashMap<Variant, VariantData>,
            those: &mut HashMap<Variant, VariantData>,
            writer: &mut BufWriter<File>, this_first: bool)
            -> Result<(), Error> {
    let SumStat { variant, data } = SumStat::read(record)?;
    match those.remove(&variant) {
        Some(that_data) => {
            if this_first {
                writeln!(writer, "{}\t{}\t{}\t{}\t{}\t{}", variant.chr, variant.pos,
                         variant.ref_allele, variant.alt_allele, data.beta(), that_data.beta())?;
            } else {
                writeln!(writer, "{}\t{}\t{}\t{}\t{}\t{}", variant.chr, variant.pos,
                         variant.ref_allele, variant.alt_allele, that_data.beta(), data.beta())?;
            }
        }
        None => {
            these.insert(variant, data);
        }
    }
    Ok(())
}