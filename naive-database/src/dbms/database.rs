use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    convert::TryInto,
    fs,
    path::PathBuf,
};

use bimap::BiHashMap;
use lazy_static::lazy_static;
use naive_sql_parser::{CreateTBField, NamedTBConstraint, TBConstraint::*};
use serde::{Deserialize, Serialize};

use crate::{
    config::{BASE_DIR, PAGE_NUM_ON_CREATE},
    defines::{ColID, TableID},
    error::DBResult,
    record::{Constraints, Table, TableMeta},
    utils::{iter_dir_by, persistence::Persistence, serial_cell::SerialCell},
};

#[derive(Debug)]
pub struct Database {
    current: PathBuf,
    current_tables: RefCell<HashMap<TableID, RefCell<Table>>>,
    id_record: BiHashMap<String, TableID>,
}

impl Database {
    #[inline]
    fn new() -> Self {
        Self {
            current: PathBuf::new(),
            current_tables: RefCell::new(HashMap::new()),
            id_record: BiHashMap::new(),
        }
    }

    pub fn current_database(&self) -> &str {
        self.current.iter().next_back().unwrap().to_str().unwrap()
    }

    pub fn is_ready(&self) -> bool {
        self.current.as_os_str() != ""
    }

    // discuss!
    pub fn create_database(&self, name: &str) -> DBResult<()> {
        let dir = BASE_DIR.join(name);
        let path = dir.join(self.filename());
        fs::create_dir(&dir)?;
        Self {
            current: path,
            ..Self::new()
        }
        .store(&dir)?;
        Ok(())
    }

    fn write_back(&mut self) -> DBResult<()> {
        let dir = self.current.as_path();
        if self.current.as_os_str() != "" {
            self.store(dir)?;
            for (_, table) in self.current_tables.take() {
                let table = table.into_inner();
                table.write_back()?;
            }
            self.current = "".into();
        }
        Ok(())
    }

    pub fn change_database(&mut self, name: &str) -> bool {
        let path = BASE_DIR.join(name);
        if path.is_dir() {
            let mut new_db = match Self::load(&path.join(self.filename())) {
                Ok(db) => db,
                Err(_e) => return false,
            };
            new_db.current = path;
            self.write_back().expect("serious error when writing back");
            *self = new_db;
            true
        } else {
            false
        }
    }

    pub fn drop_database(&self, name: &str) -> DBResult<()> {
        if name == self.current_database() {
            return Err("database already opened, try closing it before drop".into());
        }
        let path = BASE_DIR.join(name);
        fs::remove_dir_all(path)?;
        Ok(())
    }

    pub fn new_table(
        &mut self,
        name: &str,
        init: impl FnOnce(&mut TableMeta) -> DBResult<()>,
    ) -> DBResult<()> {
        if !self.is_ready() {
            return Err("no database in use".into());
        }
        let idr = &mut self.id_record;
        if idr.contains_left(name) {
            return Err("table already exists".into());
        }
        for i in 0..TableID::MAX {
            if !idr.contains_right(&i) {
                let mut meta = TableMeta::new(i, name.to_owned());
                init(&mut meta)?;
                let table = Table::from_meta(meta, &self.current)?;
                idr.insert(name.to_owned(), i);
                let mut current_tables = self.current_tables.borrow_mut();
                current_tables.insert(i, RefCell::new(table));
                return Ok(());
            }
        }
        Err("you've used up all available table ids, try delete some tables or recompile with a larger `TableID` type".into())
    }

    pub fn load_table(&self, id: TableID) -> DBResult<()> {
        if !self.is_ready() {
            return Err("no database in use".into());
        }
        let name = self
            .id_record
            .get_by_right(&id)
            .ok_or("no such table")?
            .to_owned();
        let table = Table::load_no_index(&self.current, &name)?;
        let id = table.id();

        let mut current_tables = self.current_tables.borrow_mut();
        current_tables.insert(id, RefCell::new(table));
        drop(current_tables);

        let current_tables = self.current_tables.borrow();
        let indices = current_tables[&id].borrow().load_indices()?;
        drop(current_tables);

        let mut current_tables = self.current_tables.borrow_mut();
        let table = current_tables.get_mut(&id).unwrap();
        table.borrow_mut().indices = indices;
        Ok(())
    }

    fn check_loaded(&self, id: TableID) -> bool {
        self.current_tables.borrow().contains_key(&id)
    }

    pub fn delete_table(&mut self, name: &str) -> DBResult<()> {
        if !self.is_ready() {
            return Err("no database in use".into());
        }

        let (_, id) = self
            .id_record
            .remove_by_left(name)
            .ok_or("table does not exist")?;
        let table = self.current_tables.borrow_mut().remove(&id).unwrap();
        table.into_inner().delete_self()?;
        Ok(())
    }

    pub fn list_databases(&self) -> DBResult<Vec<String>> {
        let databases = iter_dir_by(BASE_DIR.as_path(), |d| {
            if d.metadata().ok()?.is_dir() {
                d.file_name().to_str().map(ToOwned::to_owned)
            } else {
                None
            }
        })?
        .collect();
        Ok(databases)
    }

    pub fn list_tables(&self) -> DBResult<Vec<&str>> {
        if !self.is_ready() {
            return Err("no database in use".into());
        }

        Ok(self.id_record.left_values().map(|s| s.as_str()).collect())
    }

    pub fn get_table_id(&self, name: &str) -> Option<TableID> {
        self.id_record.get_by_left(name).cloned()
    }
}

impl Serialize for Database {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.id_record.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Database {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let id_record = BiHashMap::deserialize(deserializer)?;
        Ok(Self {
            id_record,
            ..Self::new()
        })
    }
}

impl Persistence for Database {
    fn filename(&self) -> String {
        "database.tablemeta".to_string()
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

lazy_static! {
    static ref DATABASE: SerialCell<Database> = SerialCell::new(Database::new());
}

pub fn get_table_id(name: &str) -> Option<TableID> {
    DATABASE.borrow().get_table_id(name)
}

pub fn load_table(name: &str) -> DBResult<TableID> {
    let inner = DATABASE.borrow();
    let id = inner.get_table_id(name).ok_or("no such table")?;
    if !inner.check_loaded(id) {
        inner.load_table(id).expect("error when loading table");
    }
    Ok(id)
}

pub fn ensure_table<T>(id: TableID, action: impl FnOnce(&Table) -> T) -> T {
    let inner = DATABASE.borrow();
    if !inner.check_loaded(id) {
        inner.load_table(id).expect("error when loading table");
    }
    let current_tables = inner.current_tables.borrow();
    let table = current_tables.get(&id).unwrap();
    let table = table.borrow();
    action(&table)
}

pub fn ensure_table_mut<T>(id: TableID, action: impl FnOnce(&mut Table) -> T) -> T {
    let inner = DATABASE.borrow();
    if !inner.check_loaded(id) {
        inner.load_table(id).expect("error when loading table");
    }
    let current_tables = inner.current_tables.borrow();
    let table = current_tables.get(&id).unwrap();
    let mut table = table.borrow_mut();
    action(&mut table)
}

pub fn get_table<T>(id: TableID, action: impl FnOnce(&Table) -> T) -> T {
    let inner = DATABASE.borrow();
    let current_tables = inner.current_tables.borrow();
    let table = current_tables.get(&id).unwrap();
    let table = table.borrow();
    action(&table)
}

pub fn modify_table<T>(id: TableID, action: impl FnOnce(&mut Table) -> T) -> T {
    let inner = DATABASE.borrow();
    let current_tables = inner.current_tables.borrow();
    let table = current_tables.get(&id).unwrap();
    let mut table = table.borrow_mut();
    action(&mut table)
}

pub fn create_database(db_name: &str) -> DBResult<()> {
    DATABASE.borrow().create_database(db_name)?;
    Ok(())
}

pub fn change_database(db_name: &str) -> bool {
    DATABASE.borrow_mut().change_database(db_name)
}

pub fn drop_database(db_name: &str) -> DBResult<()> {
    DATABASE.borrow().drop_database(db_name)?;
    Ok(())
}

pub fn create_table(tb_name: &str, fields: &[CreateTBField]) -> DBResult<()> {
    let mut inner = DATABASE.borrow_mut();
    let mut column_record = HashSet::new();
    let mut foreign = None;

    inner.new_table(tb_name, |meta| {
        for field in fields {
            match field {
                CreateTBField::Constraint(NamedTBConstraint {
                    name: _,
                    constraint,
                }) => match constraint {
                    Primary(cols) => {
                        if !meta.primary.is_empty() {
                            return Err("single primary key allowed".into());
                        } else if let Some(col_ids) = meta.get_columns_id(cols) {
                            meta.primary = col_ids.clone();
                            if cols.len() == 1 {
                                meta.columns
                                    .get_mut(meta.primary[0] as usize)
                                    .unwrap()
                                    .constraints |= Constraints::PRIMARY_KEY;
                            }
                            meta.unique.insert(col_ids);
                        } else {
                            return Err("no such column in table".into());
                        }
                    }
                    Unique(cols) => {
                        if let Some(col_ids) = meta.get_columns_id(cols) {
                            if cols.len() == 1 {
                                meta.columns
                                    .get_mut(col_ids[0] as usize)
                                    .unwrap()
                                    .constraints |= Constraints::UNIQUE;
                            }
                            meta.unique.insert(col_ids);
                        } else {
                            return Err("no such column in table".into());
                        }
                    }
                    Check { .. } => todo!(),
                    Foreign {
                        colname,
                        foreign_tb,
                        foreign_col,
                    } => {
                        let id = meta.id();
                        let table_cols = meta
                            .get_columns_id(colname)
                            .ok_or("no such column in current table")?;
                        foreign = Some(move || -> DBResult<_> {
                            if let Some(ftable_id) = get_table_id(foreign_tb) {
                                if colname.len() != foreign_col.len() {
                                    return Err("columns number should be the same".into());
                                }
                                let ftable_cols = modify_table(ftable_id, |table| -> Vec<ColID> {
                                    let ftable_cols = table
                                        .meta
                                        .get_columns_id(foreign_col)
                                        .ok_or("no such column in foreign table")
                                        .unwrap();
                                    if ftable_cols.len() == 1 {
                                        table
                                            .meta
                                            .columns
                                            .get_mut(table_cols[0] as usize)
                                            .unwrap()
                                            .constraints |= Constraints::AS_FOREIGN_KEY;
                                    }
                                    table.meta.unique.insert(ftable_cols.clone());
                                    table
                                        .meta
                                        .add_foreign_key(&ftable_cols.clone(), (id, table_cols.clone()));
                                    ftable_cols
                                });
                                modify_table(id, |table| {
                                    if table_cols.len() == 1 {
                                        table
                                            .meta
                                            .columns
                                            .get_mut(table_cols[0] as usize)
                                            .unwrap()
                                            .constraints |= Constraints::FOREIGN_KEY;
                                    }
                                    table
                                        .meta
                                        .foreign_key
                                        .insert(table_cols, (ftable_id, ftable_cols))
                                });
                            } else {
                                return Err("no such table in database".into());
                            }
                            Ok(())
                        });
                    }
                },
                CreateTBField::Column(column) => {
                    if column_record.contains(column.name.as_str()) {
                        return Err("".into());
                    }
                    column_record.insert(column.name.as_str());
                    meta.columns.push(column.try_into()?);
                }
            } // match
        } // for
        meta.rest_slot = meta.max_slot() as u32 * PAGE_NUM_ON_CREATE as u32;
        Ok(())
    })?;

    drop(inner);
    if let Some(foreign) = foreign {
        foreign()?;
    }

    Ok(())
}

pub fn drop_table(tb_name: &str) -> DBResult<()> {
    let mut inner = DATABASE.borrow_mut();
    let id = match inner.id_record.remove_by_left(tb_name) {
        Some((_, table_id)) => table_id,
        None => return Err("no such table in database".into()),
    };
    let mut current_tables = inner.current_tables.borrow_mut();
    match current_tables.remove(&id) {
        Some(table) => table.into_inner().delete_self(),
        _ => Table::load_no_index(inner.current.as_path(), tb_name)?.delete_self(),
    }
}

pub fn show_databases() -> DBResult<()> {
    let dbs = DATABASE.borrow().list_databases()?;
    if dbs.is_empty() {
        println!("No database yet.");
    } else {
        for db in dbs {
            println!("{}", db);
        }
    }
    Ok(())
}

pub fn show_tables() -> DBResult<()> {
    let database = DATABASE.borrow();
    let tables = database.list_tables()?;
    if tables.is_empty() {
        println!("No table currently in this database");
        return Ok(());
    }
    for table in tables {
        println!("{}", table);
    }
    Ok(())
}

pub fn write_back() -> DBResult<()> {
    let mut database = DATABASE.take();
    let dir = database.current.as_path();
    if dir.as_os_str() == "" {
        return Ok(());
    }
    database.write_back()?;
    Ok(())
}
