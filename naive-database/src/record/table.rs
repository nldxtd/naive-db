use std::{
    cell::RefCell,
    collections::{BTreeSet, HashMap, HashSet},
    convert::identity,
    ffi::CStr,
    fs::{self},
    intrinsics::transmute,
    mem::size_of,
    ops::Range,
    path::{Path, PathBuf}, slice::SliceIndex,
};

use chrono::NaiveDate;
use like::Like;
use naive_sql_parser::{CompareOp, Expr};
use serde::Serialize;

use crate::{
    config::{MAX_COMP_INDEX, PAGE_HEADER_LEN, PAGE_SIZE},
    defines::{ColID, PageNum, RowID, TableID},
    error::DBResult,
    filesystem::{
        file_manager::fs_ensure_remove,
        page_manager::{self, modify_page, read_page, reserve_page},
    },
    index::{
        colindex::{ColIndex, EntryRef, data2fastcmp},
        fast_cmp::FastCmp,
    },
    page::FixedPageHeader,
    utils::{
        bit_at, clear_bit_at, iter_bits, parse_date, persistence::Persistence, set_bit_at,
        table::print_data_row,
    },
};

use super::{
    column::{Column, ColumnType, ColumnVal},
    pagemanip::{colval_write_entry, entry2rid, pagenum2rid, rid2entry, PageIter},
    Constraints,
};

type ColV = ColumnVal;
type NullColV = Option<ColumnVal>;

#[derive(Debug, Serialize, Deserialize)]
pub struct TableMeta {
    id: TableID,
    name: String,
    pub columns: Vec<Column>,
    // maybe we should make all constaints nameless
    pub named_constraint: HashMap<String, (ColID, Constraints)>,

    available_pages: Option<PageNum>, // [from, to]
    full_pages: Option<PageNum>,      // same
    max_pagenum: PageNum,
    pub rest_slot: u32,

    pub foreign_key: HashMap<Vec<ColID>, (TableID, Vec<ColID>)>,
    pub as_foreign_key: HashMap<Vec<ColID>, HashSet<(TableID, Vec<ColID>)>>,
    pub primary: Vec<ColID>,
    pub unique: HashSet<Vec<ColID>>,
    pub index_record: HashSet<([ColID; MAX_COMP_INDEX], u8)>,
}

pub fn vec_to_buf(col_vec: &[ColID]) -> [ColID; MAX_COMP_INDEX] {
    let mut col_buf = [0_u32; MAX_COMP_INDEX];
    for (i, col) in col_vec.iter().enumerate() {
        col_buf[i] = *col
    }
    col_buf
}

impl Persistence for TableMeta {
    fn filename(&self) -> String {
        Self::format_meta_filename(&self.name)
    }

    fn delete_self(self, dir: &Path) -> DBResult<()> {
        fs_ensure_remove(&dir.join(self.filename()))?;
        for (col, len) in self.index_record {
            let idx_name = ColIndex::format_filename(self.id, &col[..len as usize]);
            fs_ensure_remove(&dir.join(idx_name))?;
        }
        Ok(())
    }
}

impl TableMeta {
    pub fn new(id: TableID, name: String) -> Self {
        Self {
            id,
            name,
            columns: Vec::new(),
            named_constraint: HashMap::new(),
            foreign_key: HashMap::new(),
            as_foreign_key: HashMap::new(),
            available_pages: None,
            full_pages: None,
            max_pagenum: 0,
            rest_slot: 0,
            index_record: HashSet::new(),
            primary: Vec::new(),
            unique: HashSet::new(),
        }
    }

    pub fn id(&self) -> TableID {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn colnum(&self) -> ColID {
        self.columns.len() as _
    }

    // other table ref to self
    pub fn add_foreign_key(&mut self, cols: &Vec<ColID>, fkeys: (TableID, Vec<ColID>)) {
        if let Some(v) = self.as_foreign_key.get_mut(cols) {
            v.insert(fkeys);
        } else {
            let mut v = HashSet::new();
            v.insert(fkeys);
            self.as_foreign_key.insert(cols.to_vec(), v);
        }
    }

    pub fn get_column_id(&self, col_name: &str) -> Option<ColID> {
        for (pos, column) in self.columns.iter().enumerate() {
            if column.name == col_name {
                return Some(pos as u32);
            }
        }
        None
    }

    pub fn get_columns_id(&self, col_names: &[String]) -> Option<Vec<ColID>> {
        let mut col_ids = Vec::new();
        for col_name in col_names {
            if let Some(col_id) = self.get_column_id(col_name) {
                col_ids.push(col_id);
            } else {
                return None;
            }
        }
        Some(col_ids)
    }

    #[inline]
    pub fn nullbit_size(&self) -> u16 {
        let len = self.columns.len();
        (len / 8) as u16 + (len % 8 != 0) as u16
    }

    #[inline]
    pub fn slot_size(&self) -> u16 {
        self.nullbit_size()
            + self
                .columns
                .iter()
                .map(|col| self._colsize(col))
                .sum::<u16>()
    }

    #[inline]
    pub fn slot_pos(&self, rid: RowID) -> (PageNum, Range<usize>) {
        let (pagenum, slot) = rid2entry(rid);
        (pagenum, self.slot_range(slot))
    }

    #[inline]
    pub fn entry_pos(&self, rid: RowID, col: ColID) -> (PageNum, Range<usize>) {
        let (pagenum, slot) = self.slot_pos(rid);
        let offset = self.entry_offset(col) as usize;
        let len = self.actual_column_size(col) as usize;
        let start = slot.start;
        (pagenum, start + offset..start + offset + len)
    }

    #[inline]
    pub fn entry_range_within_slot(&self, col: ColID) -> Range<usize> {
        let offset = self.entry_offset(col) as usize;
        let len = self.actual_column_size(col) as usize;
        offset..offset + len
    }

    #[inline]
    fn slot_range(&self, slot: usize) -> Range<usize> {
        let size = self.slot_size() as usize;
        let start = slot * size;
        start..start + size
    }

    #[inline]
    fn entry_offset(&self, col: ColID) -> u16 {
        self.nullbit_size()
            + self
                .columns
                .iter()
                .take(col as _)
                .map(|col| self._colsize(col))
                .sum::<u16>()
    }

    #[inline]
    pub fn actual_column_size(&self, col: ColID) -> u16 {
        let col = &self.columns[col as usize];
        self._colsize(col)
    }

    #[inline]
    fn _colsize(&self, col: &Column) -> u16 {
        use ColumnType::*;
        let size = match col.coltype {
            Char | Varchar => 1 + col.colsize as usize,
            Int => size_of::<i32>(),
            Float => size_of::<f32>(),
            Date => size_of::<NaiveDate>(),
        };
        size as _
    }

    #[inline]
    pub fn max_slot(&self) -> u16 {
        let data_size = (PAGE_SIZE - PAGE_HEADER_LEN) as u16;
        let slot_size = self.slot_size();
        (data_size / slot_size).min(FixedPageHeader::max_slot() as _)
    }

    fn alloc_page(&mut self) -> PageNum {
        let pagenum = self.max_pagenum;
        self.max_pagenum += 1;
        self.rest_slot += self.max_slot() as u32;
        pagenum
    }

    pub fn format_data_filename(table_name: &str) -> String {
        format!("{}.data", table_name)
    }

    pub fn format_meta_filename(table_name: &str) -> String {
        format!("{}.metadata", table_name)
    }

    pub fn data_filename(&self) -> String {
        Self::format_data_filename(self.name.as_str())
    }

    pub fn meta_filename(&self) -> String {
        Self::format_meta_filename(self.name.as_str())
    }
}

#[derive(Debug)]
pub struct Table {
    pub meta: TableMeta,
    pub indices: HashMap<([ColID; MAX_COMP_INDEX], u8), RefCell<ColIndex>>,
    data_path: PathBuf,
}

impl Table {
    pub fn load_indices(
        &self,
    ) -> DBResult<HashMap<([ColID; MAX_COMP_INDEX], u8), RefCell<ColIndex>>> {
        let meta = &self.meta;
        let mut indices = HashMap::new();
        let dir = self.data_path.parent().unwrap();
        for &(col, len) in &meta.index_record {
            let index = ColIndex::load(
                &dir.join(ColIndex::format_filename(meta.id(), &col[..len as usize])),
            )?;
            indices.insert((col, len), RefCell::new(index));
        }
        Ok(indices)
    }

    pub fn load_no_index(dir: &Path, table_name: &str) -> DBResult<Self> {
        let meta = TableMeta::load(&dir.join(TableMeta::format_meta_filename(table_name)))?;
        let data_path = dir.join(TableMeta::format_data_filename(table_name));
        page_manager::open_file(&data_path)?;
        Ok(Self {
            meta,
            indices: HashMap::new(),
            data_path,
        })
    }

    pub fn write_back(self) -> DBResult<()> {
        let dir = self.data_path.parent().unwrap();
        self.meta.store(dir)?;
        for (_, index) in self.indices.into_iter() {
            let index = index.into_inner();
            index.store(dir)?;
        }
        page_manager::flush_all()?;
        page_manager::close_file(&self.data_path)?;
        Ok(())
    }

    pub fn delete_self(self) -> DBResult<()> {
        let dir = self.data_path.parent().unwrap();
        self.meta.delete_self(dir)?;
        for (_, index) in self.indices.into_iter() {
            let index = index.into_inner();
            index.delete_self(dir)?;
        }
        page_manager::close_file(&self.data_path)?;
        fs::remove_file(self.data_path)?;
        Ok(())
    }

    pub fn new(id: TableID, name: String, dir: &Path) -> DBResult<Self> {
        Self::from_meta(TableMeta::new(id, name), dir)
    }

    pub fn check_column_type(&self, expr: &Expr, col_id: ColID) -> DBResult<Option<u32>> {
        let col = self.meta.columns.get(col_id as usize).unwrap();
        let col_type = col.coltype;
        match expr {
            Expr::Binary(_, _, _) | Expr::ColumnRef(_) => {
                return Err("binary and columnref not supported here".into());
            }
            Expr::IntLit(_) => {
                if !((col_type == ColumnType::Float) | (col_type == ColumnType::Int)) {
                    return Err(format!("wrong type in column {}", col_id).into());
                }
            }
            Expr::FloatLit(_) => {
                if col_type != ColumnType::Float {
                    return Err(format!("wrong type in column {}", col_id).into());
                }
            }
            Expr::StringLit(content) => match col_type {
                ColumnType::Char | ColumnType::Varchar => {
                    if content.len() > col.colsize.into() {
                        return Err(format!("column {} longer than expected", col_id).into());
                    }
                }
                ColumnType::Date => {
                    if parse_date(content).is_none() {
                        return Err(format!("column {} is not a valid date", col_id).into());
                    }
                }
                _ => return Err(format!("wrong type in column {}", col_id).into()),
            },
            Expr::Null => {
                if col.constraints.is_not_null() || col.constraints.is_primary_key() {
                    return Err(format!("column {} cannot be null", col_id).into());
                }
                return Ok(Some(col_id));
            }
        }
        Ok(None)
    }

    pub fn check_type_insert(&self, record: &[Expr]) -> DBResult<()> {
        if record.len() != self.meta.columns.len() {
            return Err("value size not equal to column size".into());
        }
        let mut null_cols = Vec::new();
        for i in 0..self.meta.columns.len() {
            if let Some(record_type) = record.get(i) {
                if self.check_column_type(record_type, i as ColID)?.is_some() {
                    null_cols.push(i as ColID);
                }
            }
        }
        if self.meta.primary.is_empty() {
            return Ok(());
        } else {
            for col in &self.meta.primary {
                if !null_cols.contains(col) {
                    return Ok(());
                }
            }
        }
        Err("primary keys cannot be null".into())
    }

    pub fn check_data_exist(&self, row_data: &[Option<ColumnVal>], cols: &[ColID]) -> bool {
        let col_buf = vec_to_buf(cols);
        if let Some(index) = self.indices.get(&(col_buf, cols.len() as u8)) {
            let index = index.borrow();
            index.list.contains(&row_data.clone().into())
        } else {
            let rows = self.rows();
            for row in rows {
                let mut exist = true;
                let record_data = self.select_row(row).unwrap();
                for (i, col) in cols.iter().enumerate() {
                    if row_data.get(i) != record_data.get(*col as usize) {
                        exist = false;
                        break;
                    }
                }
                if exist {
                    return true;
                }
            }
            false
        }
    }

    pub fn filter_rows(
        &self,
        cols: &[ColID],
        op: CompareOp,
        colval: &[Option<ColumnVal>],
    ) -> DBResult<HashSet<RowID>> {
        let ret = match op {
            CompareOp::EQ => self.get_rows_by(
                colval,
                cols,
                |index| index.range_rows(colval.as_ref(), colval.as_ref()).collect(),
                |record_data| record_data == colval,
            ),
            CompareOp::NE => self.get_rows_by(
                colval,
                cols,
                |index| {
                    index
                        .out_range_rows(colval.as_ref(), colval.as_ref())
                        .collect()
                },
                |record_data| record_data != colval,
            ),
            CompareOp::GT => self.get_rows_by(
                colval,
                cols,
                |index| index.upper_range_rows(colval.as_ref()).collect(),
                |record_data| record_data > colval,
            ),
            CompareOp::LT => self.get_rows_by(
                colval,
                cols,
                |index| index.lower_range_rows(colval.as_ref()).collect(),
                |record_data| record_data < colval,
            ),
            CompareOp::GE => self.get_rows_by(
                colval,
                cols,
                |index| index.upper_eq_range_rows(colval.as_ref()).collect(),
                |record_data| record_data >= colval,
            ),
            CompareOp::LE => self.get_rows_by(
                colval,
                cols,
                |index| index.lower_eq_range_rows(colval.as_ref()).collect(),
                |record_data| record_data <= colval,
            ),
            CompareOp::LIKE => {
                debug_assert_eq!(colval.len(), 1);
                let colval = &colval[0];
                let pattern = match colval {
                    Some(ColumnVal::Char(s)) | Some(ColumnVal::Varchar(s)) => s,
                    _ => return Err("pattern used in `LIKE` or `NOT LIKE` must be a string".into()),
                };
                let col = cols[0];
                self.rows()
                    .filter_map(|rid| {
                        self.select(rid, col)
                            .ok()?
                            .map(|data| match data {
                                ColumnVal::Char(s) | ColumnVal::Varchar(s) => {
                                    Like::<true>::like(s.as_str(), pattern).ok()?.then(|| rid)
                                }
                                _ => unreachable!(),
                            })
                            .and_then(identity)
                    })
                    .collect()
            }
            CompareOp::NOTLIKE => {
                debug_assert_eq!(colval.len(), 1);
                let colval = &colval[0];
                let pattern = match colval {
                    Some(ColumnVal::Char(s)) | Some(ColumnVal::Varchar(s)) => s,
                    _ => return Err("pattern used in `LIKE` or `NOT LIKE` must be a string".into()),
                };
                let col = cols[0];
                self.rows()
                    .filter_map(|rid| {
                        self.select(rid, col)
                            .ok()?
                            .map(|data| match data {
                                ColumnVal::Char(s) | ColumnVal::Varchar(s) => {
                                    Like::<true>::not_like(s.as_str(), pattern)
                                        .ok()?
                                        .then(|| rid)
                                }
                                _ => unreachable!(),
                            })
                            .and_then(identity)
                    })
                    .collect()
            }
        };
        Ok(ret)
    }

    fn get_rows_by(
        &self,
        _cols_data: &[Option<ColumnVal>],
        cols: &[ColID],
        with_index: impl FnOnce(&ColIndex) -> HashSet<RowID>,
        is_match: impl Fn(&[NullColV]) -> bool,
    ) -> HashSet<RowID> {
        let mut filter_rows = HashSet::new();
        let col_buf = vec_to_buf(cols);
        if let Some(index) = self.indices.get(&(col_buf, cols.len() as u8)) {
            let index = index.borrow();
            filter_rows = with_index(&index);
        } else {
            for row in self.rows() {
                let record_data = self.select_cols(row, cols.iter().copied()).unwrap();
                if is_match(&record_data) {
                    filter_rows.insert(row);
                }
            }
        }
        filter_rows
    }

    // give the data on cols
    pub fn get_equal_rows(
        &self,
        cols_data: &[Option<ColumnVal>],
        cols: &[ColID],
    ) -> HashSet<RowID> {
        let mut filter_rows = HashSet::new();
        let col_buf = vec_to_buf(cols);
        if let Some(index) = self.indices.get(&(col_buf, cols.len() as u8)) {
            let index = index.borrow();
            filter_rows = index
                .range_rows(cols_data.as_ref(), cols_data.as_ref())
                .collect();
        } else {
            for row in self.rows() {
                let mut exist = true;
                let record_data = self.select_row(row).unwrap();
                for (i, col) in cols.iter().enumerate() {
                    if cols_data[i] != record_data[*col as usize] {
                        exist = false;
                        break;
                    }
                }
                if exist {
                    filter_rows.insert(row);
                }
            }
        }
        filter_rows
    }

    pub fn remove_index_at(&self, row_id: RowID, row_data: &[Option<ColumnVal>]) {
        for (_, index) in self.indices.iter() {
            let mut index = index.borrow_mut();
            index.remove_record(row_id, row_data);
        }
    }

    pub fn insert_index_at(&self, row_id: RowID, row_data: &[Option<ColumnVal>]) {
        for (_, index) in self.indices.iter() {
            let mut index = index.borrow_mut();
            index.insert_record(row_id, row_data);
        }
    }

    pub fn create_index(
        &self,
        cols: &[ColID],
        unique_required: bool
    ) -> DBResult<(([ColID; MAX_COMP_INDEX], u8), ColIndex)> {
        let len = cols.len();
        if len >= MAX_COMP_INDEX {
            return Err(format!(
                "only supports composite index that involves less than {} columns",
                MAX_COMP_INDEX
            )
            .into());
        }
        let mut colbuf = [0_u32; MAX_COMP_INDEX];
        for (i, col) in cols.iter().enumerate() {
            colbuf[i] = *col;
        }

        // get the index key and build a ColIndex
        // if need to be unique, check half way
        let mut list = BTreeSet::new();
        for rid in self.rows() {
            let row_data = self.select_cols(rid, cols.iter().cloned())?;

            if unique_required && list.contains(&row_data.clone()[..].into()) {
                return Err(format!("cols in table {} doesn't satisfy unique requirment", self.meta.name()).into());
            } 
            let (fast_cmp, is_null) = data2fastcmp(&row_data);
            list.insert(
                EntryRef {
                    col: colbuf,
                    len: len as _,
                    rid,
                    tbl: self.meta.id(),
                    fast_cmp,
                    is_null,
                }
                .into(),
            );
        }

        let col_index = ColIndex::new(self.meta.id(), len as _, colbuf, list);
        Ok(((colbuf, len as _), col_index))
    }

    pub fn insert_index(
        &mut self,
        ((colbuf, len), col_index): (([ColID; MAX_COMP_INDEX], u8), ColIndex),
    ) {
        self.indices
            .insert((colbuf, len as _), RefCell::new(col_index));
        self.meta.index_record.insert((colbuf, len as _));
    }

    /// If value of the deleted row is needed,
    /// select before delete
    pub fn delete(&mut self, rid: RowID) -> DBResult<()> {
        let (pagenum, slot) = rid2entry(rid);

        modify_page(self.data_path.as_path(), pagenum, |page| {
            let header = page.header_mut();
            let max_slot = self.meta.max_slot();
            let full = header.is_full(max_slot as _);
            clear_bit_at(&mut header.slot, slot);
            full
        })
        .map(|full| -> DBResult<_> {
            if full {
                let pos = {
                    let mut iter = PageIter::new(pagenum, &self.data_path);
                    iter.remove()?;
                    iter.pos()
                };
                match self.meta.full_pages {
                    Some(full_start) => {
                        if full_start == pagenum {
                            self.meta.full_pages = if pos == pagenum { None } else { Some(pos) };
                        }
                    }
                    None => unreachable!(),
                }
                match self.meta.available_pages {
                    Some(prev) => {
                        PageIter::new(pagenum, &self.data_path).append(prev)?;
                    }
                    None => {}
                }
                self.meta.available_pages = Some(pagenum);
            }
            Ok(())
        })
        .map_err(Into::into)
        .and_then(identity)
    }

    pub fn drop_index(&mut self, fields: &[String]) -> DBResult<()> {
        let len = fields.len();
        let mut colbuf = [0_u32; MAX_COMP_INDEX];
        for (i, field) in fields.iter().enumerate() {
            let col_id = self.meta.get_column_id(field);
            if let Some(col_id) = col_id {
                colbuf[i] = col_id;
            } else {
                return Err("no such column in table".into());
            }
        }
        // maybe this is enough
        if let Some(col_index) = self.indices.remove(&(colbuf, len as _)) {
            col_index
                .into_inner()
                .delete_self(self.data_path.parent().unwrap())?;
            self.meta.index_record.remove(&(colbuf, len as _));
            self.indices.remove(&(colbuf, len as _));
        } else {
            return Err("no such indexed in table".into());
        }
        Ok(())
    }

    pub fn from_meta(meta: TableMeta, dir: &Path) -> DBResult<Self> {
        let data_path = dir.join(TableMeta::format_data_filename(&meta.name));
        page_manager::open_file(&data_path)?;
        modify_page(&data_path, 0, |page| page.header_mut().clear())?;
        Ok(Self {
            meta,
            indices: HashMap::new(),
            data_path,
        })
    }

    pub fn find_useable_index(&self, col: ColID) -> Option<&RefCell<ColIndex>> {
        for (([first, ..], _), index) in &self.indices {
            if *first == col {
                return Some(index);
            }
        }
        None
    }

    fn get_available_start(&mut self) -> DBResult<PageNum> {
        let start = match self.meta.available_pages {
            Some(start) => start,
            None => {
                let new = self.meta.alloc_page();
                self.meta.available_pages = Some(new);
                modify_page(&self.data_path, new, |page| {
                    page.header_mut().clear_as_node(new)
                })?;
                new
            }
        };
        Ok(start)
    }

    #[inline]
    pub fn id(&self) -> TableID {
        self.meta.id
    }

    pub fn insert(&mut self, val: &[Option<ColumnVal>]) -> DBResult<RowID> {
        let pagenum = self.get_available_start()?;
        self.meta.rest_slot -= 1;

        modify_page(&self.data_path, pagenum, |page| -> DBResult<_> {
            let (header, data) = page.split_header_mut();

            let max_slot = self.meta.max_slot() as _;
            let slot = header.first_empty(max_slot).unwrap();
            set_bit_at(&mut header.slot, slot as _);
            let full = header.is_full(max_slot);

            let rid = entry2rid(pagenum, slot);
            let slot = &mut data[self.meta.slot_range(slot as _)];
            for (col, val) in val.iter().enumerate() {
                match val {
                    None => set_bit_at(&mut slot[..self.meta.nullbit_size() as _], col),
                    Some(expr) => colval_write_entry(
                        expr,
                        &mut slot[self.meta.entry_range_within_slot(col as _)],
                    )?,
                }
            }
            Ok((full, rid))
        })
        .map_err(Into::into)
        .and_then(identity)
        .map(|(full, rid)| {
            if full {
                let pos = {
                    let mut iter = PageIter::new(pagenum, &self.data_path);
                    iter.remove()?;
                    iter.pos()
                };
                match self.meta.available_pages {
                    Some(available_start) => {
                        if available_start == pagenum {
                            self.meta.available_pages =
                                if pos == pagenum { None } else { Some(pos) };
                        }
                    }
                    None => unreachable!(),
                }
                match self.meta.full_pages {
                    Some(prev) => {
                        PageIter::new(pagenum, &self.data_path).append(prev)?;
                    }
                    None => {}
                }
                self.meta.full_pages = Some(pagenum);
            }
            Ok(rid)
        })
        .and_then(identity)
    }

    pub fn reserve_for(&mut self, n_slots: usize) -> DBResult<()> {
        let rest_slot = self.meta.rest_slot as _;
        if n_slots > rest_slot {
            let slot_needed = n_slots - rest_slot;
            let max_slot = self.meta.max_slot() as usize;
            let remainder = slot_needed % max_slot;
            let page_needed = slot_needed / max_slot + (remainder != 0) as usize;
            let pagenum = self.meta.max_pagenum + page_needed as PageNum;
            self.meta.rest_slot = if remainder == 0 {
                0
            } else {
                (max_slot - remainder) as _
            };
            reserve_page(&self.data_path, pagenum)?;
        }
        Ok(())
    }

    fn interpret_entry(&self, rid: RowID, col: ColID) -> DBResult<Option<ColumnVal>> {
        use ColumnVal::*;
        let entry_range = self.meta.entry_range_within_slot(col);
        let nullbits = self.meta.nullbit_size() as usize;

        self.read_slot(rid, |slot| -> DBResult<Option<ColumnVal>> {
            let coltype = self.meta.columns[col as usize].coltype;
            let nullbits = &slot[..nullbits];
            if bit_at(nullbits, col as _) {
                return Ok(None);
            }

            let entry = &slot[entry_range];
            let colval = match coltype {
                ColumnType::Int => Int(bincode::deserialize(entry)?),
                ColumnType::Float => Float(bincode::deserialize(entry)?),
                ColumnType::Date => {
                    let i: i32 = bincode::deserialize(entry)?;
                    Date(unsafe { transmute(i) })
                }
                ColumnType::Char => {
                    let s = unsafe { CStr::from_ptr(entry as *const _ as *const _) };
                    Char(s.to_string_lossy().into())
                }
                ColumnType::Varchar => {
                    let s = unsafe { CStr::from_ptr(entry as *const _ as *const _) };
                    Varchar(s.to_string_lossy().into())
                }
            };
            Ok(Some(colval))
        })
        .and_then(identity)
    }

    fn read_entry<T>(
        &self,
        rid: RowID,
        col: ColID,
        action: impl FnOnce(&[u8]) -> T,
    ) -> DBResult<T> {
        let (pagenum, entry_range) = self.meta.entry_pos(rid, col);
        read_page(&self.data_path, pagenum, |page| {
            let data = page.data();
            let slot = &data[entry_range];
            action(slot)
        })
        .map_err(Into::into)
    }

    fn read_slot<T>(&self, rid: RowID, action: impl FnOnce(&[u8]) -> T) -> DBResult<T> {
        let (pagenum, slot_range) = self.meta.slot_pos(rid);
        read_page(&self.data_path, pagenum, |page| {
            let data = page.data();
            let slot = &data[slot_range];
            action(slot)
        })
        .map_err(Into::into)
    }

    pub fn rows(&self) -> Box<dyn Iterator<Item = RowID> + '_> {
        if let Some(idx_iter) = self.rows_by_index() {
            Box::new(idx_iter)
        } else {
            Box::new(self.rows_by_brute())
        }
    }

    pub fn rows_by_brute(&self) -> impl Iterator<Item = RowID> + '_ {
        let max_slot = self.meta.max_slot();
        (0..self.meta.max_pagenum).flat_map(move |pagenum| {
            read_page(&self.data_path, pagenum, |page| {
                let header = page.header();
                iter_bits(&header.slot)
                    .take(max_slot as _)
                    .enumerate()
                    .filter_map(|(i, exist)| exist.then(|| i as RowID))
                    .map(|slot| pagenum2rid(pagenum) + slot)
                    .collect::<Vec<_>>()
            })
            .unwrap()
        })
    }

    pub fn rows_by_index(&self) -> Option<impl DoubleEndedIterator<Item = RowID> + '_> {
        if let Some((_, first_idx)) = self.indices.iter().next() {
            let idx = first_idx.borrow();
            Some(idx.iter_rid().collect::<Vec<_>>().into_iter())
        } else {
            None
        }
    }

    fn check_rid_exist(&self, rid: RowID) -> DBResult<()> {
        let (pagenum, slot) = rid2entry(rid);
        if pagenum >= self.meta.max_pagenum {
            return Err("row does not exist".into());
        }
        let has_slot = read_page(&self.data_path, pagenum, |page| {
            bit_at(&page.header().slot, slot)
        })?;
        if !has_slot {
            dbg!(rid, pagenum, slot);
            return Err("row does not exist".into());
        }
        Ok(())
    }

    pub fn print_val(&self, rows: &[RowID], cols: &[ColID]) {
        if cols.is_empty() {
            return;
        }
        if rows.is_empty() {
            println!("No data found");
            return;
        }
        let header = self.meta.columns.iter().map(|col| col.name.as_str());
        let mut body = Vec::with_capacity(rows.len() * cols.len());
        for &rid in rows {
            let data = self.select_cols(rid, cols.iter().copied()).unwrap();
            body.extend(data);
        }
        print_data_row(header, body.chunks_exact(cols.len()));
        println!("{} items in total", rows.len());
    }

    pub fn select(&self, rid: RowID, col: ColID) -> DBResult<Option<ColumnVal>> {
        self.check_rid_exist(rid)?;
        let colval = self.interpret_entry(rid, col)?;
        Ok(colval)
    }

    pub fn select_cols(
        &self,
        rid: RowID,
        cols: impl Iterator<Item = ColID>,
    ) -> DBResult<Vec<Option<ColumnVal>>> {
        self.check_rid_exist(rid)?;
        cols.map(|col| self.interpret_entry(rid, col)).collect()
    }

    pub fn select_row(&self, rid: RowID) -> DBResult<Vec<Option<ColumnVal>>> {
        self.check_rid_exist(rid)?;
        (0..self.meta.colnum())
            .map(|col| self.interpret_entry(rid, col))
            .collect()
    }

    pub fn update(&mut self, rid: RowID, col: ColID, val: &Option<ColumnVal>) -> DBResult<()> {
        let (pagenum, slot_num) = rid2entry(rid);
        let (_, slot) = self.meta.slot_pos(rid);
        let (_, entry_range) = self.meta.entry_pos(rid, col);

        modify_page(self.data_path.as_path(), pagenum, |page| -> DBResult<_> {
            let (header, data) = page.split_header_mut();
            if !bit_at(&header.slot, slot_num) {
                return Err("row does not exist".into());
            }

            match val {
                Some(val) => {
                    let entry = &mut data[entry_range];
                    colval_write_entry(val, entry)?;
                    Ok(())
                }
                None => {
                    let slot = &mut data[slot];
                    set_bit_at(slot, col as _);
                    Ok(())
                }
            }
        })
        .map_err(Into::into)
        .and_then(identity)
    }

    pub fn update_row(&mut self, rid: RowID, val: &[Option<ColumnVal>]) -> DBResult<()> {
        let (pagenum, slot_num) = rid2entry(rid);

        modify_page(self.data_path.as_path(), pagenum, |page| -> DBResult<_> {
            let (header, data) = page.split_header_mut();
            if !bit_at(&header.slot, slot_num) {
                return Err("row does not exist".into());
            }

            for (col, val) in val.iter().enumerate() {
                match val {
                    Some(val) => {
                        let (_, entry_range) = self.meta.entry_pos(rid, col as _);
                        let entry = &mut data[entry_range];
                        colval_write_entry(val, entry)?;
                    }
                    None => {
                        let (_, slot) = self.meta.slot_pos(rid);
                        let slot = &mut data[slot];
                        set_bit_at(slot, col as _);
                    }
                }
            }
            Ok(())
        })
        .map_err(Into::into)
        .and_then(identity)
    }

    fn write_entry<T>(
        &mut self,
        rid: RowID,
        col: ColID,
        action: impl FnOnce(&mut [u8]) -> T,
    ) -> DBResult<T> {
        let (pagenum, entry_range) = self.meta.entry_pos(rid, col);
        modify_page(&self.data_path, pagenum, |page| {
            let data = page.data_mut();
            let slot = &mut data[entry_range];
            action(slot)
        })
        .map_err(Into::into)
    }

    fn write_slot<T>(&mut self, rid: RowID, action: impl FnOnce(&mut [u8]) -> T) -> DBResult<T> {
        let (pagenum, slot_range) = self.meta.slot_pos(rid);
        modify_page(&self.data_path, pagenum, |page| {
            let data = page.data_mut();
            let slot = &mut data[slot_range];
            action(slot)
        })
        .map_err(Into::into)
    }

    pub fn get_data_cols(
        &self,
        row_data: &[Option<ColumnVal>],
        cols: &[ColID],
    ) -> Vec<Option<ColumnVal>> {
        let mut slice_data = Vec::new();
        for col in cols {
            slice_data.push(row_data[*col as usize].clone())
        }
        slice_data
    }

    pub fn record2data(&self, record: &[Expr]) -> Vec<Option<ColumnVal>> {
        let mut row_data = Vec::new();
        for (i, col) in self.meta.columns.iter().enumerate() {
            row_data.push(Self::expr2colval(&record[i], col.coltype))
        }
        row_data
    }

    pub fn exprs2colval(&self, record: &[&Expr], cols: &[ColID]) -> Vec<Option<ColumnVal>> {
        let mut row_data = Vec::new();
        for (i, col) in cols.iter().enumerate() {
            row_data.push(Self::expr2colval(
                record[i],
                self.meta.columns[*col as usize].coltype,
            ))
        }
        row_data
    }

    pub fn expr2colval(expr: &Expr, coltype: ColumnType) -> Option<ColumnVal> {
        use ColumnVal::*;
        match expr {
            Expr::IntLit(i) => match coltype {
                ColumnType::Float => Some(Float(*i as _)),
                ColumnType::Int => Some(Int(*i)),
                _ => unreachable!("missing arg check"),
            },
            Expr::FloatLit(f) => Some(Float(*f)),
            Expr::StringLit(s) => match coltype {
                ColumnType::Char => Some(Char(s.clone())),
                ColumnType::Varchar => Some(Varchar(s.clone())),
                ColumnType::Date => Some(Date(parse_date(s).unwrap())),
                _ => unreachable!("missing arg check"),
            },
            Expr::Null => None,
            _ => unreachable!("missing arg check"),
        }
    }
}
