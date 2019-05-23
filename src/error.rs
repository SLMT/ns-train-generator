
use std::io;
use std::fmt;
use std::result;
use std::num;

use toml::de;

pub type Result<T> = result::Result<T, GeneratorError>;

#[derive(Debug)]
pub enum GeneratorError {
    IoError(io::Error),
    TomlParseError(de::Error),
    CsvParseError(csv::Error),
    FloatParseError(num::ParseFloatError),
    IntParseError(num::ParseIntError),
    DbError(postgres::Error),
    NdArrayError(ndarray::ShapeError),
    Message(String)
}

impl fmt::Display for GeneratorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GeneratorError::Message(s) => write!(f, "Error: {}", s),
            _ => write!(f, "{}", self)
        }
    }
}

impl From<io::Error> for GeneratorError {
    fn from(error: io::Error) -> Self {
        GeneratorError::IoError(error)
    }
}

impl From<de::Error> for GeneratorError {
    fn from(error: de::Error) -> Self {
        GeneratorError::TomlParseError(error)
    }
}

impl From<csv::Error> for GeneratorError {
    fn from(error: csv::Error) -> Self {
        GeneratorError::CsvParseError(error)
    }
}

impl From<num::ParseFloatError> for GeneratorError {
    fn from(error: num::ParseFloatError) -> Self {
        GeneratorError::FloatParseError(error)
    }
}

impl From<num::ParseIntError> for GeneratorError {
    fn from(error: num::ParseIntError) -> Self {
        GeneratorError::IntParseError(error)
    }
}

impl From<postgres::Error> for GeneratorError {
    fn from(error: postgres::Error) -> Self {
        GeneratorError::DbError(error)
    }
}

impl From<ndarray::ShapeError> for GeneratorError {
    fn from(error: ndarray::ShapeError) -> Self {
        GeneratorError::NdArrayError(error)
    }
}