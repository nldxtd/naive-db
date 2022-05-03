use std::fs;

use crate::{config::BASE_DIR, dbms::database, error::DBResult, filesystem::page_manager};

pub fn init() {
    if fs::create_dir(BASE_DIR.as_path()).is_ok() {}
}

pub fn write_back() -> DBResult<()> {
    database::write_back()?;
    page_manager::flush_all()?;
    Ok(())
}
