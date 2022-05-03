use std::{
    collections::HashMap,
    fs::File,
    io::{Error, ErrorKind, Result},
    path::{Path, PathBuf},
};

use lazy_static::lazy_static;
use memmap::{MmapMut, MmapOptions};

use crate::{config::PAGE_SIZE, defines::PageNum, page::Page, utils::serial_cell::SerialCell};

use super::file_manager::{fs_create_file, fs_open_file, fs_reserve_page};

fn not_found() -> Error {
    ErrorKind::NotFound.into()
}

fn already_exists() -> Error {
    ErrorKind::AlreadyExists.into()
}

struct MmapManager {
    map_record: HashMap<PathBuf, (File, MmapMut)>,
}

impl MmapManager {
    fn new() -> Self {
        Self {
            map_record: HashMap::new(),
        }
    }
}

lazy_static! {
    static ref MMAP_MANAGER: SerialCell<MmapManager> = SerialCell::new(MmapManager::new());
}

pub fn open_file(filepath: &Path) -> Result<()> {
    let file = fs_open_file(filepath).or_else(|_| fs_create_file(filepath))?;
    let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
    let record = &mut MMAP_MANAGER.borrow_mut().map_record;
    if record.contains_key(filepath) {
        Err(already_exists())
    } else {
        record.insert(filepath.to_owned(), (file, mmap));
        Ok(())
    }
}

pub fn close_file(filepath: &Path) -> Result<()> {
    MMAP_MANAGER
        .borrow_mut()
        .map_record
        .remove(filepath)
        .ok_or_else(not_found)?;
    Ok(())
}

pub fn read_page<T>(
    filepath: &Path,
    pagenum: PageNum,
    action: impl FnOnce(&Page) -> T,
) -> Result<T> {
    let mut inner = MMAP_MANAGER.borrow_mut();
    let (file, mmap) = inner.map_record.get_mut(filepath).ok_or_else(not_found)?;
    let start = pagenum as usize * PAGE_SIZE;
    let end = start + PAGE_SIZE;
    if end >= mmap.len() {
        file.set_len(end as u64)?;
        *mmap = unsafe { MmapOptions::new().map_mut(file)? };
    }
    let range = &mmap[start..end];
    Ok(action(unsafe { Page::from_ref_unchecked(range) }))
}

pub fn modify_page<T>(
    filepath: &Path,
    pagenum: PageNum,
    action: impl FnOnce(&mut Page) -> T,
) -> Result<T> {
    let mut inner = MMAP_MANAGER.borrow_mut();
    let (file, mmap) = inner.map_record.get_mut(filepath).ok_or_else(not_found)?;
    let start = pagenum as usize * PAGE_SIZE;
    let end = start + PAGE_SIZE;
    if end >= mmap.len() {
        file.set_len(end as u64)?;
        *mmap = unsafe { MmapOptions::new().map_mut(file)? };
    }
    let range = &mut mmap[start..end];
    Ok(action(unsafe { Page::from_mut_unchecked(range) }))
}

pub fn flush_all() -> Result<()> {
    for (_, mmap) in MMAP_MANAGER.borrow().map_record.values() {
        mmap.flush()?;
    }
    Ok(())
}

pub fn reserve_page(filepath: &Path, n: PageNum) -> Result<()> {
    let mut inner = MMAP_MANAGER.borrow_mut();
    let (file, mmap) = inner.map_record.get_mut(filepath).ok_or_else(not_found)?;
    fs_reserve_page(file, n)?;
    *mmap = unsafe { MmapOptions::new().map_mut(file)? };
    Ok(())
}
