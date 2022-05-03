#![allow(unused)]

use std::{
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Read, Result, Seek, SeekFrom, Write},
    path::Path,
};

use crate::{
    config::{PAGE_NUM_ON_CREATE, PAGE_SIZE, PAGE_SIZE_IDX},
    defines::PageNum,
};

use crate::page::{Page, PageBuf};

pub fn fs_create_file(filepath: &Path) -> Result<File> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .read(true)
        .open(filepath)?;
    file.set_len(PAGE_NUM_ON_CREATE << PAGE_SIZE_IDX);
    Ok(file)
}

pub fn fs_remove_file(filepath: &Path) -> Result<()> {
    fs::remove_file(filepath)
}

pub fn fs_ensure_file(filepath: &Path) -> Result<File> {
    if filepath.is_file() {
        OpenOptions::new().write(true).read(true).open(filepath)
    } else {
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(filepath)
    }
}

pub fn fs_ensure_remove(filepath: &Path) -> Result<()> {
    match fs::remove_file(filepath) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

pub fn fs_open_file(filepath: &Path) -> Result<File> {
    OpenOptions::new().write(true).read(true).open(filepath)
}

pub fn fs_read_page(file: &mut File, pagenum: PageNum) -> Result<PageBuf> {
    let mut page = PageBuf::new();
    fs_read_page_to(file, pagenum, &mut page[..])?;
    Ok(page)
}

pub fn fs_read_page_to(file: &mut File, pagenum: PageNum, buf: &mut [u8]) -> Result<()> {
    let seekfrom = SeekFrom::Start((pagenum as u64) << PAGE_SIZE_IDX);
    let len = buf.len();
    file.seek(seekfrom)?;
    match file.read_exact(&mut buf[..PAGE_SIZE.min(len)]) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
            file.set_len((pagenum as u64) << PAGE_SIZE_IDX)?;
            buf.fill(0);
            Ok(())
        }
        e @ Err(_) => e,
    }
}

pub fn fs_write_page(file: &mut File, pagenum: PageNum, buf: &Page) -> Result<()> {
    let seekfrom = SeekFrom::Start((pagenum as u64) << PAGE_SIZE_IDX);
    file.seek(seekfrom)?;
    file.write_all(buf)?;
    file.sync_data()?;
    Ok(())
}

pub fn fs_write_page_from(file: &mut File, pagenum: PageNum, buf: &[u8]) -> Result<()> {
    let seekfrom = SeekFrom::Start((pagenum as u64) << PAGE_SIZE_IDX);
    file.seek(seekfrom)?;
    let len = buf.len().min(PAGE_SIZE);
    file.write_all(buf)?;
    if len < PAGE_SIZE {
        file.write_all(&[0; PAGE_SIZE][..PAGE_SIZE - len])?;
    }
    file.sync_data()?;
    Ok(())
}

pub fn fs_page_count(file: &File) -> Result<u64> {
    Ok(file.metadata()?.len() / PAGE_SIZE as u64)
}

/// set file capacity to `n` pages when its capacity is lower than `n`
/// or keep its length when it already has larger length
/// `n-1` would be the greatest pagenum without setting a greater length
pub fn fs_reserve_page(file: &File, n: PageNum) -> Result<()> {
    let len = file.metadata()?.len().max((n as u64) << PAGE_SIZE_IDX);
    file.set_len(len)
}
