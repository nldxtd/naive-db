#![allow(unused)]

use std::{mem::transmute, path::Path};

use chrono::NaiveDate;

use crate::{
    defines::{PageNum, RowID},
    error::DBResult,
    filesystem::page_manager::{modify_page, read_page},
    page::{FixedPageHeader as Header, Page},
};

use super::{ColumnType, ColumnVal};

pub const fn rid2pagenum(rid: RowID) -> PageNum {
    rid / Header::max_slot()
}

/// (pagenum, slotnum)
/// slotnum * slotsize = slot offset
pub const fn rid2entry(rid: RowID) -> (PageNum, usize) {
    (rid2pagenum(rid), (rid % Header::max_slot()) as usize)
}

pub const fn pagenum2rid(pagenum: PageNum) -> RowID {
    pagenum * Header::max_slot()
}

pub const fn entry2rid(pagenum: PageNum, slot: u16) -> RowID {
    pagenum2rid(pagenum) + slot as u32
}

pub struct PageIter<'path> {
    pos: PageNum,
    path: &'path Path,
}

impl<'path> PageIter<'path> {
    pub fn new(start: PageNum, filepath: &'path Path) -> Self {
        Self {
            pos: start,
            path: filepath,
        }
    }

    pub fn pos(&self) -> PageNum {
        self.pos
    }

    /// Remove current
    ///
    /// `pos` would be previous `prev` after remove, unless `pos` is at the start of the linked-list
    /// , in which case it would be `next`
    ///
    /// It's better to panic when calling to this function fails
    pub fn remove(&mut self) -> DBResult<PageNum> {
        let path = self.path;
        let pos = self.pos;
        let (prev, next) = modify_page(path, pos, |page| {
            let header = page.header_mut();
            let (prev, next) = (header.prev_page, header.next_page);
            header.prev_page = pos;
            header.next_page = pos;
            (prev, next)
        })?;
        if prev != pos {
            modify_page(path, prev, |page| {
                page.header_mut().next_page = if next != pos { next } else { prev }
            })?;
        }
        if next != pos {
            modify_page(path, next, |page| {
                page.header_mut().prev_page = if prev != pos { prev } else { next }
            })?;
        }
        self.pos = if prev != pos { prev } else { next };
        Ok(pos)
    }

    /// Append sublist after current
    ///
    /// If current is not the end of the list, the rest would be replaced
    ///
    /// `pos` would remain the same
    ///
    /// Would panic if self is not at the end of a list
    /// , or start is actually not a start of a list
    ///
    /// It's better to panic when calling to this function fails
    pub fn append(&mut self, start: PageNum) -> DBResult<PageNum> {
        let path = self.path;
        let pos = self.pos;
        modify_page(path, pos, |page| {
            let header = page.header_mut();
            debug_assert_eq!(header.next_page, pos);
            header.next_page = start;
        })?;
        modify_page(path, start, |page| {
            let header = page.header_mut();
            debug_assert_eq!(header.prev_page, start);
            header.prev_page = pos;
        })?;
        Ok(pos)
    }

    /// Insert after current
    ///
    /// `pos` would remain the same
    ///
    /// It's better to panic when calling to this function fails
    pub fn insert(&mut self, pagenum: PageNum) -> DBResult<PageNum> {
        let path = self.path;
        let pos = self.pos;
        let next = modify_page(path, pos, |page| {
            let header = page.header_mut();
            let next = header.next_page;
            header.next_page = pagenum;
            next
        })?;
        modify_page(path, pagenum, |page| {
            let header = page.header_mut();
            header.prev_page = pos;
            header.next_page = if next != pos { next } else { pagenum };
        })?;
        if next != pos {
            modify_page(path, next, |page| page.header_mut().prev_page = pagenum)?;
        }
        Ok(pos)
    }

    pub fn next(&mut self) -> DBResult<Option<PageNum>> {
        let new_pos = read_page(self.path, self.pos, |page| page.header().next_page)?;
        if self.pos == new_pos {
            Ok(None)
        } else {
            let pos = self.pos;
            self.pos = new_pos;
            Ok(Some(pos))
        }
    }

    pub fn prev(&mut self) -> DBResult<Option<PageNum>> {
        let new_pos = read_page(self.path, self.pos, |page| page.header().prev_page)?;
        if self.pos == new_pos {
            Ok(None)
        } else {
            let pos = self.pos;
            self.pos = new_pos;
            Ok(Some(pos))
        }
    }

    pub fn read<T>(&self, action: impl FnOnce(&Page) -> T) -> DBResult<T> {
        read_page(self.path, self.pos, action).map_err(Into::into)
    }

    pub fn modify<T>(&self, action: impl FnOnce(&mut Page) -> T) -> DBResult<T> {
        modify_page(self.path, self.pos, action).map_err(Into::into)
    }
}

pub fn next_page(pagenum: PageNum, filepath: &Path) -> DBResult<Option<PageNum>> {
    let pagenum = read_page(filepath, pagenum, |page| {
        let next_page = page.header().next_page;
        if next_page == pagenum {
            None
        } else {
            Some(next_page)
        }
    })?;
    Ok(pagenum)
}

pub fn prev_page(pagenum: PageNum, filepath: &Path) -> DBResult<Option<PageNum>> {
    let pagenum = read_page(filepath, pagenum, |page| {
        let prev_page = page.header().prev_page;
        if prev_page == pagenum {
            None
        } else {
            Some(prev_page)
        }
    })?;
    Ok(pagenum)
}

pub fn parse_write_entry(val: &str, coltype: ColumnType, entry: &mut [u8]) -> DBResult<()> {
    use ColumnType::*;
    debug_assert_ne!(val, "NULL");
    debug_assert_ne!(val, "null");

    match coltype {
        Int => {
            let i: i32 = val.parse()?;
            bincode::serialize_into(entry, &i)?;
        }
        Float => {
            let f: f32 = val.parse()?;
            bincode::serialize_into(entry, &f)?;
        }
        Date => {
            let d: NaiveDate = val.parse()?;
            let i: i32 = unsafe { transmute(d) };
            bincode::serialize_into(entry, &i)?;
        }
        Char | Varchar => {
            let entrylen = entry.len();
            let vallen = val.len();
            (&mut entry[..vallen]).copy_from_slice(val.as_bytes());
            (&mut entry[vallen..entrylen]).fill(0);
        }
    }
    Ok(())
}

pub fn colval_write_entry(colval: &ColumnVal, entry: &mut [u8]) -> DBResult<()> {
    use ColumnVal::*;
    match colval {
        Int(val) => bincode::serialize_into(entry, val)?,
        Float(val) => bincode::serialize_into(entry, val)?,
        Date(val) => {
            let val: i32 = unsafe { transmute(*val) };
            bincode::serialize_into(entry, &val)?;
        }
        Char(val) | Varchar(val) => {
            let entrylen = entry.len();
            let vallen = val.len();
            (&mut entry[..vallen]).copy_from_slice(val.as_bytes());
            (&mut entry[vallen..entrylen]).fill(0);
        }
    }
    Ok(())
}
