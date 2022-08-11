use parquet::record::{Row, RowAccessor};
use crate::Error;

#[derive(Eq, Hash, PartialEq)]
pub(crate) struct Variant {
    pub(crate) chr: String,
    pub(crate) pos: u32,
    pub(crate) ref_allele: String,
    pub(crate) alt_allele: String,
}

pub(crate) struct VariantData {
    pub(crate) alt_ac: u32,
    pub(crate) mac: u32,
    pub(crate) maf: f64,
    pub(crate) n: u32,
    pub(crate) u: f64,
    pub(crate) v: f64,
}

impl VariantData {
    pub(crate) fn beta(&self) -> f64 { self.u / self.v }
}

pub(crate) struct SumStat {
    pub(crate) variant: Variant,
    pub(crate) data: VariantData,
}

impl SumStat {
    pub(crate) fn read(record: &Row) -> Result<Self, Error> {
        let chr = record.get_string(0)?.clone();
        let pos = record.get_uint(1)?;
        let ref_allele = record.get_string(2)?.clone();
        let alt_allele = record.get_string(3)?.clone();
        let variant = Variant { chr, pos, ref_allele, alt_allele };
        let alt_ac = record.get_uint(4)?;
        let mac = record.get_uint(5)?;
        let maf = record.get_double(6)?;
        let n = record.get_uint(7)?;
        let u = record.get_double(8)?;
        let v = record.get_double(9)?;
        let data = VariantData { alt_ac, mac, maf, n, u, v };
        Ok(SumStat { variant, data })
    }
}

#[derive(Eq, Hash, PartialEq)]
pub(crate) struct Indices {
    pub(crate) row: u32,
    pub(crate) col: u32,
}

pub(crate) struct Covariance {
    pub(crate) indices: Indices,
    pub(crate) value: f64,
}

impl Covariance {
    pub(crate) fn read(record: &Row) -> Result<Self, Error> {
        let row = record.get_uint(0)?;
        let col = record.get_uint(1)?;
        let indices = Indices { row, col };
        let value = record.get_double(2)?;
        Ok(Covariance { indices, value })
    }
}
