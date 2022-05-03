#![allow(unused)]

use std::{
    fs::{self, DirEntry},
    path::Path,
    time::{Duration, Instant},
};

use crate::error::DBResult;

pub mod bitmap;
pub mod lru;
pub mod persistence;
pub mod serial_cell;
pub mod table;

pub use bitmap::*;
use chrono::NaiveDate;
use like::Like;

pub fn iter_dir_by<T>(
    dir: &Path,
    mut action: impl FnMut(&DirEntry) -> Option<T>,
) -> DBResult<impl Iterator<Item = T>> {
    let iter = fs::read_dir(dir)?;
    let iter = iter.filter_map(move |r| match r.ok() {
        Some(d) => action(&d),
        None => None,
    });
    Ok(iter)
}

pub fn parse_date(s: &str) -> Option<NaiveDate> {
    let alternatives = ["%Y-%m-%d", "%Y/%m/%d"];
    for date_format in alternatives {
        if let Ok(date) = NaiveDate::parse_from_str(s.trim_matches('\''), date_format) {
            return Some(date);
        }
    }
    None
}

#[inline(always)]
pub fn naive_timeit<T>(f: impl FnOnce() -> T) -> (T, Duration) {
    let now = Instant::now();
    let t = f();
    let elapsed = now.elapsed();
    (t, elapsed)
}

pub type Identity<T> = T;
