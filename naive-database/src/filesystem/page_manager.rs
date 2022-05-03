use std::{
    collections::HashMap,
    fs::File,
    io::{Error, ErrorKind, Result},
    path::{Path, PathBuf},
};

use bimap::{BiHashMap, Overwritten};
use fixedbitset::FixedBitSet;
use lazy_static::lazy_static;

use crate::{
    config::LRU_SIZE,
    defines::PageNum,
    utils::{
        lru::LruRecord,
        page::{Page, PageBuf},
        serial_cell::SerialCell,
    },
};

use super::file_manager::{
    fs_create_file, fs_open_file, fs_read_page_to, fs_reserve_page, fs_write_page_from,
};

fn not_found() -> Error {
    ErrorKind::NotFound.into()
}

fn already_exists() -> Error {
    ErrorKind::AlreadyExists.into()
}

type PageIndex = (PathBuf, PageNum);
type CacheIndex = usize;

struct PageManager {
    file_record: HashMap<PathBuf, File>,
    index_record: BiHashMap<PageIndex, CacheIndex>,
    page_cache: Vec<PageBuf>,
    lru: LruRecord,
    dirty: FixedBitSet,
}

impl PageManager {
    #[inline]
    fn new(cache_size: usize) -> Self {
        Self {
            file_record: HashMap::new(),
            page_cache: vec![PageBuf::new(); cache_size],
            index_record: BiHashMap::new(),
            lru: LruRecord::new(cache_size),
            dirty: FixedBitSet::with_capacity(cache_size),
        }
    }

    #[inline]
    fn write_back(&mut self, index: CacheIndex, file: &mut File, pagenum: PageNum) -> Result<()> {
        if self.dirty[index] {
            fs_write_page_from(file, pagenum, &mut self.page_cache[index])?;
            self.dirty.set(index, false);
        }
        Ok(())
    }

    #[inline]
    fn get_file(&self, filepath: &Path) -> Result<File> {
        let file = self
            .file_record
            .get(filepath)
            .ok_or(not_found())?
            .try_clone()?;
        Ok(file)
    }

    fn get_page(&mut self, filepath: &Path, pagenum: PageNum, dirty: bool) -> Result<&mut Page> {
        let (hit, cache_index) = match self
            .index_record
            .get_by_left(&(filepath.to_path_buf(), pagenum))
        {
            Some(&index) => (true, index),
            None => (false, self.lru.find_furthest()),
        };

        if !hit {
            let insert_result = self
                .index_record
                .insert((filepath.to_owned(), pagenum), cache_index);
            match insert_result {
                Overwritten::Right((victim, pagenum), _) => {
                    let mut victim = self.get_file(&victim)?;
                    self.write_back(cache_index, &mut victim, pagenum)?;
                }
                Overwritten::Neither => {}
                _ => unreachable!(),
            }
            let mut file = self.get_file(filepath)?;
            fs_read_page_to(&mut file, pagenum, &mut self.page_cache[cache_index])?;
        }

        self.lru.access(cache_index);
        if dirty {
            self.dirty.insert(cache_index);
        }
        Ok(&mut self.page_cache[cache_index])
    }

    fn open_file(&mut self, filepath: &Path) -> Result<()> {
        if self.file_record.contains_key(filepath) {
            Err(already_exists())
        } else {
            let file = fs_open_file(filepath).or_else(|_| fs_create_file(filepath))?;
            self.file_record.insert(filepath.to_path_buf(), file);
            Ok(())
        }
    }

    fn close_file(&mut self, filepath: &Path) -> Result<()> {
        if let Some(mut file) = self.file_record.remove(filepath) {
            let cache_indexes: Vec<_> = self
                .index_record
                .iter()
                .filter(|((name, _), _)| name == filepath)
                .map(|(_, cache_index)| *cache_index)
                .collect();
            for cache_index in cache_indexes {
                let ((_, pagenum), _) = self.index_record.remove_by_right(&cache_index).unwrap();
                self.write_back(cache_index, &mut file, pagenum)?;
            }
            Ok(())
        } else {
            Err(not_found())
        }
    }

    fn get_read(&mut self, filepath: &Path, pagenum: PageNum) -> Result<&Page> {
        self.get_page(filepath, pagenum, false).map(|page| &*page)
    }

    #[must_use]
    fn get_write(&mut self, filepath: &Path, pagenum: PageNum) -> Result<&mut Page> {
        self.get_page(filepath, pagenum, true)
    }

    fn flush_all(&mut self) -> Result<()> {
        for (&(ref filepath, pagenum), &index) in &self.index_record {
            let mut file = self.get_file(filepath)?;
            if self.dirty[index] {
                fs_write_page_from(&mut file, pagenum, &mut self.page_cache[index])?;
            }
        }
        self.index_record.clear();
        self.dirty.clear();
        Ok(())
    }
}

lazy_static! {
    static ref PAGE_MANAGER: SerialCell<PageManager> = SerialCell::new(PageManager::new(LRU_SIZE));
}

pub fn open_file(filepath: &Path) -> Result<()> {
    PAGE_MANAGER.borrow_mut().open_file(filepath)
}

pub fn close_file(filepath: &Path) -> Result<()> {
    PAGE_MANAGER.borrow_mut().close_file(filepath)
}

pub fn read_page<T>(
    filepath: &Path,
    pagenum: PageNum,
    action: impl FnOnce(&Page) -> T,
) -> Result<T> {
    let mut inner = PAGE_MANAGER.borrow_mut();
    let page = inner.get_read(filepath, pagenum)?;
    Ok(action(page))
}

pub fn modify_page<T>(
    filepath: &Path,
    pagenum: PageNum,
    action: impl FnOnce(&mut Page) -> T,
) -> Result<T> {
    let mut inner = PAGE_MANAGER.borrow_mut();
    let page = inner.get_write(filepath, pagenum)?;
    Ok(action(page))
}

pub fn flush_all() -> Result<()> {
    PAGE_MANAGER.borrow_mut().flush_all()
}

pub fn reserve_page(filepath: &Path, n: PageNum) -> Result<()> {
    let inner = PAGE_MANAGER.borrow();
    let file = inner.file_record.get(filepath).ok_or_else(not_found)?;
    fs_reserve_page(file, n)
}
