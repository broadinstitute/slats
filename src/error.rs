use std::fmt::{Display, Formatter};
use parquet::errors::ParquetError;

pub(crate) enum Error {
    String(String),
    Io(std::io::Error),
    Parquet(ParquetError),
}

impl From<&str> for Error {
    fn from(string: &str) -> Self { Error::from(String::from(string)) }
}

impl From<String> for Error {
    fn from(string: String) -> Self { Error::String(string) }
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
            Error::String(string) => { writeln!(f, "{}", string) }
            Error::Io(io_error) => { writeln!(f, "{}", io_error) }
            Error::Parquet(parquet_error) => { writeln!(f, "{}", parquet_error) }
        }
    }
}