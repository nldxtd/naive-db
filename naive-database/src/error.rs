use std::error::Error;

pub type DBResult<T> = ::std::result::Result<T, Box<dyn Error>>;
