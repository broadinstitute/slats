use std::fmt::{Display, Formatter};
use parquet::errors::ParquetError;

pub(crate) enum Error {
    Io(std::io::Error),
    Parquet(ParquetError)
}

impl From<std::io::Error> for Error {
    fn from(io_error: std::io::Error) -> Self {
        Error::Io(io_error)
    }
}

impl From<ParquetError> for Error {
    fn from(parquet_error: ParquetError) -> Self {
        Error::Parquet(parquet_error)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(io_error) => { writeln!(f, "{}", io_error) }
            Error::Parquet(parquet_error) => { writeln!(f, "{}", parquet_error) }
        }
    }
}