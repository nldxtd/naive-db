use std::{fs::OpenOptions, path::PathBuf};

use lazy_static::lazy_static;

pub const PAGE_SIZE_IDX: u64 = 13;
pub const PAGE_SIZE: usize = 8192; // bytes, 1 << PAGE_SIZE_IDX

#[cfg(test)]
pub const LRU_SIZE: usize = 500;
#[cfg(not(test))]
pub const LRU_SIZE: usize = 60000; // total cache size = LRU_SIZE * PAGE_SIZE

pub const PAGE_HEADER_LEN: usize = 64; // bytes

pub const PAGE_NUM_ON_CREATE: u64 = 2;

pub const MAX_COMP_INDEX: usize = 3;

pub const DEFAULT_SIZE: u8 = 32;

pub const MAX_JOIN_TABLE: usize = 2;

pub const MAX_CHAR_LEN: usize = 255;

lazy_static! {
    pub static ref BASE_DIR: PathBuf = "data".into();
}

lazy_static! {
    pub static ref REPL_HISTORY: PathBuf = {
        let path = BASE_DIR.join("repl.history");
        if OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .is_err()
        {}
        path
    };
}
