use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::dbms::aggregate::{avg, count, count_all, max, min, sum_float, sum_int};
use crate::defines::TableID;
use crate::defines::{ColID, RowID};
use crate::error::DBResult;
use crate::index::colindex::{data2fastcmp, ColIndex, EntryRef, IndexKey};
use crate::record::{Constraints, Table, ColumnType, vec_to_buf};
use crate::utils::naive_timeit;
use crate::utils::table::{check_constraint, get_coltype, print_join_table, print_vec};
use naive_sql_parser::{
    AddForeign, AddPrimary, Aggregator, Alter,
    ColumnRef::{self, *},
    CondExpr, CreateDB, CreateIdx, CreateTB, Delete, Desc, DropDB, DropForeign, DropIdx, DropTB,
    Insert, Select,
    Selectors::*,
    Show, SqlStmt, Update, UseDB,
};

use super::database as db;
use super::relation::{relation, Logic};

fn print_affected(n: usize) {
    println!("{} row(s) affected", n);
}

fn print_duration(action: &str, d: Duration, after: &str) {
    if d.as_secs() != 0 {
        println!(
            "Completed {} in {:.3} seconds{}",
            action,
            d.as_secs_f32(),
            after
        );
    } else {
        let microsec = d.as_micros();
        if microsec >= 1000 {
            println!(
                "Completed {} in {:.3} milliseconds{}",
                action,
                microsec as f32 / 1000.,
                after
            );
        } else {
            println!(
                "Completed {} in {:.3} microseconds{}",
                action, d.as_nanos() as f32 / 1000., after
            );
        }
    }
}

fn _print_time<T>(action: &str, f: impl FnOnce() -> T) -> T {
    let (ret, d) = naive_timeit(f);
    print_duration(action, d, " (including possible io)");
    ret
}

macro_rules! print_time {
    ( $func:tt ( $args:expr ) ) => {
        _print_time(stringify!($func), || $func($args))
    };
    ( $func:tt ( $args:expr ), $action:expr ) => {
        _print_time($action, || $func($args))
    };
}

fn check_colref(colref: &ColumnRef, table: &Table) -> DBResult<ColID> {
    let ret = match colref {
        Ident(ident) => table
            .meta
            .get_column_id(ident)
            .ok_or("no such column in table"),
        Attr { table_name, column } => {
            if table_name != table.meta.name() {
                return Err("no such column in table".into());
            }
            table
                .meta
                .get_column_id(column)
                .ok_or("no such column in table")
        }
    }?;
    Ok(ret)
}

fn check_colref_joined(
    colref: &ColumnRef,
    ltable: &Table,
    rtable: &Table,
) -> DBResult<(TableID, ColID)> {
    let lcol = check_colref(colref, ltable);
    let rcol = check_colref(colref, rtable);
    match (lcol, rcol) {
        (Ok(_), Ok(_)) => Err("column exists in both tables"),
        (Ok(col), Err(_)) => Ok((ltable.meta.id(), col)),
        (Err(_), Ok(col)) => Ok((rtable.meta.id(), col)),
        (Err(_), Err(_)) => Err("column doesn't in both tables"),
    }
    .map_err(Into::into)
}

fn get_aggr(
    aggr: &Aggregator,
    colref: &ColumnRef,
    rows: impl Iterator<Item = RowID>,
    id: TableID,
    col: ColID,
) -> DBResult<String> {
    let ret = match aggr {
        Aggregator::COUNT => {
            let count = count(rows, id, col)?;
            format!("COUNT({}): {}", colref, count)
        }
        Aggregator::AVG => {
            let avg = avg(rows, id, col)?;
            format!("AVG({}): {}", colref, avg)
        }
        Aggregator::MIN => {
            let min = min(rows, id, col)?;
            match min {
                Some(min) => format!("MIN({}): {}", colref, min),
                None => "NULL".to_owned(),
            }
        }
        Aggregator::MAX => {
            let max = max(rows, id, col)?;
            match max {
                Some(max) => format!("MAX({}): {}", colref, max),
                None => "NULL".to_owned(),
            }
        }
        Aggregator::SUM => {
            match db::get_table(id, |table| table.meta.columns[col as usize].coltype) {
                ColumnType::Int => {
                    let sum = sum_int(rows, id, col)?;
                    format!("SUM({}): {}", colref, sum)
                }
                ColumnType::Float => {
                    let sum = sum_float(rows, id, col)?;
                    format!("SUM({}): {}", colref, sum)
                }
                _ => {
                    return Err("column referenced in `SUM` must be of `INT` or `FLOAT` type".into())
                }
            }
        }
    };
    Ok(ret)
}

pub trait Exec {
    type Success;

    fn exec(&self) -> DBResult<Self::Success>;
}

impl Exec for SqlStmt {
    type Success = ();

    fn exec(&self) -> DBResult<Self::Success> {
        match self {
            SqlStmt::CreateDB(create_db_args) => print_time!(create_database(create_db_args)),
            SqlStmt::CreateTB(create_tb_args) => print_time!(create_table(create_tb_args)),
            SqlStmt::CreateIdx(create_idx_args) => print_time!(create_index(create_idx_args)),
            SqlStmt::DropDB(drop_db_args) => print_time!(drop_database(drop_db_args)),
            SqlStmt::DropTB(drop_tb_args) => print_time!(drop_table(drop_tb_args)),
            SqlStmt::DropIdx(drop_idx_args) => print_time!(drop_index(drop_idx_args)),
            SqlStmt::Select(select_args) => print_time!(select(select_args)),
            SqlStmt::Insert(insert_args) => print_time!(insert(insert_args)),
            SqlStmt::Update(update_args) => print_time!(update(update_args)),
            SqlStmt::Delete(delete_args) => print_time!(delete(delete_args)),
            SqlStmt::UseDB(use_db_args) => print_time!(use_database(use_db_args)),
            SqlStmt::Show(show_args) => print_time!(show(show_args)),
            SqlStmt::Desc(desc_args) => print_time!(describe(desc_args)),
            SqlStmt::Alter(alter_args) => print_time!(alter_table(alter_args)),
        }
    }
}

impl Exec for Vec<SqlStmt> {
    type Success = ();

    fn exec(&self) -> DBResult<Self::Success> {
        for sql in self {
            sql.exec()?;
        }
        Ok(())
    }
}

fn alter_table(args: &Alter) -> DBResult<()> {
    match args {
        Alter::CreateIdx(args) => create_index(args),
        Alter::DropIdx(args) => drop_index(args),
        Alter::AddPrimary(args) => add_primary(args),
        Alter::AddForeign(args) => add_foreign(args),
        Alter::DropForeign(args) => drop_foreign(args),
    }
}

fn create_database(args: &CreateDB) -> DBResult<()> {
    db::create_database(&args.0)
}

fn use_database(args: &UseDB) -> DBResult<()> {
    if db::change_database(&args.0) {
        Ok(())
    } else {
        Err("database does not exist".into())
    }
}

fn drop_database(args: &DropDB) -> DBResult<()> {
    db::drop_database(&args.0)
}

fn create_table(args: &CreateTB) -> DBResult<()> {
    db::create_table(&args.name, &args.fields)
}

fn create_index(args: &CreateIdx) -> DBResult<()> {
    let id = db::load_table(&args.table_name)?;
    let (colbuf, col_index) = db::ensure_table(id, |table| -> DBResult<_> {
        let cols = table
            .meta
            .get_columns_id(&args.fields)
            .ok_or(format!("no such columns in table {}", args.table_name))?;
        table.create_index(&cols, false)
    })?;
    db::modify_table(id, |table| {
        table.indices.insert(colbuf, col_index.into());
    });
    Ok(())
}

fn drop_table(args: &DropTB) -> DBResult<()> {
    db::drop_table(&args.0)
}

fn drop_index(args: &DropIdx) -> DBResult<()> {
    let id = db::get_table_id(&args.table_name).ok_or("table name not found")?;
    db::ensure_table_mut(id, |table| table.drop_index(&args.cols))
}

fn add_primary(args: &AddPrimary) -> DBResult<()> {
    let id = db::load_table(&args.table_name)?;
    db::ensure_table_mut(id, |table| -> DBResult<()> {
        if !table.meta.primary.is_empty() {
            return Err("a table cannot have more than one primary key".into());
        } else {
            let cols = table
                .meta
                .get_columns_id(&args.cols)
                .ok_or(format!("no such columns in table {}", args.table_name))?;
            let col_buf = vec_to_buf(&cols);
            if table.indices.get(&(col_buf, cols.len() as u8)).is_none() {
                let (colbuf, col_index) = table.create_index(&cols, true)?;
                table.indices.insert(colbuf, col_index.into());
                table.meta.primary = cols.clone();
                table.meta.unique.insert(cols.clone());
            }
            if cols.len() == 1 {
                table.meta.columns.get_mut(cols[0] as usize).unwrap().constraints |= Constraints::PRIMARY_KEY;
            }
        }
        Ok(())
    })?;
    Ok(())
}

fn add_foreign(args: &AddForeign) -> DBResult<()> {
    let table_id = db::load_table(&args.table_name)?;
    let ftable_id = db::load_table(&args.ftable_name)?;
    db::modify_table(table_id, |table| -> DBResult<()> {
        let cols = table
            .meta
            .get_columns_id(&args.cols)
            .ok_or(format!("no such column in table {}", args.table_name))?;
        let fcols = db::modify_table(ftable_id, |ftable| -> DBResult<Vec<ColID>> {
            let fcols = ftable
                .meta
                .get_columns_id(&args.fcols)
                .ok_or(format!("no such column in table {}", args.ftable_name))?;
            //check fcols is unique, maybe build a index here
            //dont repeatedly build index here
            let col_buf = vec_to_buf(&fcols);
            if ftable.indices.get(&(col_buf, fcols.len() as _)).is_none() {
                let (colbuf, col_index) = ftable.create_index(&fcols, true)?;
                ftable.indices.insert(colbuf, col_index.into());
                ftable.meta.unique.insert(fcols.clone());
            }
            //check every row exist in ftable
            for rid in table.rows() {
                let row_data = table.select_cols(rid, cols.iter().cloned())?;
                if !ftable.check_data_exist(&row_data, &fcols) {
                    return Err("foreign data cannot be found on foreign table".into());
                }
            }
            if fcols.len() == 1 {
                ftable
                    .meta
                    .columns
                    .get_mut(fcols[0] as usize)
                    .unwrap()
                    .constraints |= Constraints::AS_FOREIGN_KEY;
            }
            ftable
                .meta
                .add_foreign_key(&fcols.clone(), (table_id, cols.clone()));
            Ok(fcols)
        })?;
        if cols.len() == 1 {
            table
                .meta
                .columns
                .get_mut(cols[0] as usize)
                .unwrap()
                .constraints |= Constraints::FOREIGN_KEY;
        }
        table.meta.foreign_key.insert(cols, (ftable_id, fcols));
        Ok(())
    })?;
    Ok(())
}

fn drop_foreign(args: &DropForeign) -> DBResult<()> {
    let table_id = db::load_table(&args.table_name)?;
    let ftable_id = db::load_table(&args.ftable_name)?;
    db::modify_table(table_id, |table| -> DBResult<()> {
        let cols = table
            .meta
            .get_columns_id(&args.cols)
            .ok_or(format!("no such column in table {}", args.table_name))?;
        db::modify_table(ftable_id, |ftable| -> DBResult<()> {
            let fcols = ftable
                .meta
                .get_columns_id(&args.fcols)
                .ok_or(format!("no such column in table {}", args.ftable_name))?;
            if let Some(refs) = ftable.meta.as_foreign_key.get_mut(&fcols) {
                if !refs.remove(&(table_id, cols.clone())) {
                    return Err(format!(
                        "no such foreign relation between table {} ans table {}",
                        args.table_name, args.ftable_name
                    )
                    .into());
                }
            } else {
                return Err(format!(
                    "no such foreign relation between table {} ans table {}",
                    args.table_name, args.ftable_name
                )
                .into());
            }
            if ftable.meta.as_foreign_key.get(&fcols).unwrap().is_empty() && fcols.len() == 1 {
                ftable
                    .meta
                    .columns
                    .get_mut(fcols[0] as usize)
                    .unwrap()
                    .constraints &= !Constraints::AS_FOREIGN_KEY;
            }
            Ok(())
        })?;
        table.meta.foreign_key.remove(&cols);
        Ok(())
    })?;
    Ok(())
}

fn select(args: &Select) -> DBResult<()> {
    let mut table_ids = vec![];
    for table in &args.from {
        if let Some(id) = db::get_table_id(table) {
            table_ids.push(id);
        } else {
            return Err("no such table in database".into());
        }
    }
    for &table_id in &table_ids {
        db::ensure_table(table_id, |_| {});
    }
    let rows = match relation(
        args.condition.as_ref().unwrap_or(&CondExpr::True),
        &args.from,
    )? {
        Logic::Pos(x) => x,
        Logic::Neg(x) => {
            if table_ids.len() == 1 {
                let full: HashSet<_> = db::ensure_table(table_ids[0], |table| {
                    let mut ret = HashSet::new();
                    for rid in table.rows() {
                        ret.insert([rid, 0]);
                    }
                    ret
                });
                full.difference(&x).copied().collect()
            } else if table_ids.len() == 2 {
                let lrows: Vec<_> =
                    db::ensure_table(table_ids[0], |ltable| ltable.rows().collect());
                let rrows: Vec<_> =
                    db::ensure_table(table_ids[1], |rtable| rtable.rows().collect());
                let mut full = HashSet::new();
                for lrow in lrows {
                    for &rrow in &rrows {
                        full.insert([lrow, rrow]);
                    }
                }
                full.difference(&x).copied().collect()
            } else {
                unimplemented!();
            }
        }
    };

    let mut aggregates = vec![];

    // the print logic
    if table_ids.len() == 1 {
        let rows = rows.iter().map(|s| s[0]).collect::<Vec<_>>();
        let mut cols = Vec::new();
        match &args.selectors {
            Part(columns) => {
                db::get_table(table_ids[0], |table| -> DBResult<()> {
                    use naive_sql_parser::SingleSelector::*;
                    for col in columns {
                        match col {
                            Single(colref) => {
                                let col_id = check_colref(colref, table)?;
                                cols.push(col_id);
                            }
                            Aggregate(aggr, colref) => {
                                let col = check_colref(colref, table)?;
                                let id = table_ids[0];
                                let rows = rows.iter().cloned();
                                let aggr_str = get_aggr(aggr, colref, rows, id, col)?;
                                aggregates.push(aggr_str);
                            }
                            CountAll => {
                                let count = count_all(rows.iter().cloned())?;
                                aggregates.push(format!("Count(*): {}", count));
                            }
                        }
                    }
                    Ok(())
                })?;
            }
            All => db::get_table(table_ids[0], |table| -> _ {
                for i in 0..table.meta.columns.len() {
                    cols.push(i as ColID);
                }
            }),
        }

        db::get_table(table_ids[0], |table| {
            table.print_val(&rows, &cols);
        });
        println!("{}", aggregates.join("\n"));
    } else {
        // joined
        let mut lcols = Vec::new();
        let mut rcols = Vec::new();
        match &args.selectors {
            Part(columns) => {
                for column in columns {
                    use naive_sql_parser::SingleSelector::*;
                    match column {
                        Single(colref) => match colref {
                            Ident(ident) => {
                                let in_ltable = db::get_table(table_ids[0], |ltable| -> bool {
                                    if let Some(col_id) = ltable.meta.get_column_id(ident) {
                                        lcols.push(col_id);
                                        return true;
                                    }
                                    false
                                });
                                let in_rtable =
                                    db::get_table(table_ids[1], |rtable| -> DBResult<_> {
                                        if let Some(col_id) = rtable.meta.get_column_id(ident) {
                                            if !in_ltable {
                                                rcols.push(col_id);
                                                return Ok(true);
                                            } else {
                                                return Err(format!(
                                                    "column name {} exist in both tables",
                                                    ident
                                                )
                                                .into());
                                            }
                                        }
                                        Ok(false)
                                    })?;
                                if !in_ltable && !in_rtable {
                                    return Err(format!(
                                        "column name {} doesn't exist in both tables",
                                        ident
                                    )
                                    .into());
                                }
                            }
                            Attr { table_name, column } => {
                                if table_name == &args.from[0] {
                                    db::get_table(table_ids[0], |ltable| -> DBResult<()> {
                                        if let Some(col_id) = ltable.meta.get_column_id(column) {
                                            lcols.push(col_id);
                                            Ok(())
                                        } else {
                                            return Err(format!(
                                                "column {} doesn't exist in table {}",
                                                column, table_name
                                            )
                                            .into());
                                        }
                                    })?;
                                } else if table_name == &args.from[1] {
                                    db::get_table(table_ids[1], |rtable| -> DBResult<()> {
                                        if let Some(col_id) = rtable.meta.get_column_id(column) {
                                            rcols.push(col_id);
                                            Ok(())
                                        } else {
                                            return Err(format!(
                                                "column {} doesn't exist in table {}",
                                                column, table_name
                                            )
                                            .into());
                                        }
                                    })?;
                                } else {
                                    return Err(format!(
                                        "select column from unrelated table {}",
                                        table_name
                                    )
                                    .into());
                                }
                            }
                        },
                        Aggregate(aggr, colref) => {
                            let (id, col) = db::get_table(table_ids[0], |ltable| {
                                db::get_table(table_ids[1], |rtable| {
                                    check_colref_joined(colref, ltable, rtable)
                                })
                            })?;
                            let left = id == table_ids[0];
                            let rows = rows.iter().map(|&t| if left { t[0] } else { t[1] });
                            let aggr_str = get_aggr(aggr, colref, rows, id, col)?;
                            aggregates.push(aggr_str);
                        }
                        CountAll => {
                            aggregates.push(format!("Count(*): {}", rows.len()));
                        }
                    }
                }
            }
            All => {
                db::get_table(table_ids[0], |table| {
                    for i in 0..table.meta.columns.len() {
                        lcols.push(i as ColID);
                    }
                });
                db::get_table(table_ids[1], |table| {
                    for i in 0..table.meta.columns.len() {
                        rcols.push(i as ColID);
                    }
                })
            }
        }
        print_join_table(rows, table_ids[0], &lcols, table_ids[1], &rcols);
    } // joined
    Ok(())
}

fn insert(args: &Insert) -> DBResult<()> {
    let id = db::get_table_id(&args.table_name).ok_or("table name not found")?;
    let records = &args.values;
    for (i, record) in records.iter().enumerate() {
        let record_data = db::ensure_table(id, |table| -> DBResult<_> {
            table.check_type_insert(record)?;
            let record_data = table.record2data(record);
            for unique_cols in &table.meta.unique {
                let slice_data = table.get_data_cols(&record_data, unique_cols);
                if table.check_data_exist(&slice_data, unique_cols) {
                    return Err(format!("record {} doesn't satisfy unique requirment", i).into());
                }
            }
            for (table_cols, (ftable_id, ftable_cols)) in &table.meta.foreign_key {
                let slice_data = table.get_data_cols(&record_data, table_cols);
                db::ensure_table(*ftable_id, |ftable| -> DBResult<()> {
                    if !ftable.check_data_exist(&slice_data, ftable_cols) {
                        return Err(
                            format!("record {} doesn't satisfy foreign key requirment", i).into(),
                        );
                    }
                    Ok(())
                })?;
            }
            Ok(record_data)
        })?;
        let row = db::modify_table(id, |table| -> DBResult<RowID> {
            table.insert(&record_data)
        })?;
        db::get_table(id, |table| {
            table.insert_index_at(row, &record_data);
        })
    }
    Ok(())
}

fn update(args: &Update) -> DBResult<()> {
    let table_name = &args.table_name;
    let table_id = db::get_table_id(table_name).ok_or("table name not found")?;
    db::load_table(table_name)?;
    let col_name = match &args.column {
        Ident(col_name) => col_name,
        Attr {
            table_name: table,
            column,
        } => {
            if table != table_name {
                return Err("cannot reference a column from a different table when update".into());
            } else {
                column
            }
        }
    };

    let rows = match relation(&args.condition, &[table_name.clone(); 1])? {
        Logic::Pos(x) => x,
        Logic::Neg(x) => {
            let full: HashSet<_> = db::ensure_table(table_id, |table| {
                let mut ret = HashSet::new();
                for rid in table.rows() {
                    ret.insert([rid, 0]);
                }
                ret
            });
            full.difference(&x).copied().collect()
        }
    };
    let rows = rows.iter().map(|s| s[0]).collect::<Vec<_>>();

    let mut foreign_update: HashMap<TableID, Vec<_>> = HashMap::new();

    let (col_id, new_col_val) = db::ensure_table(table_id, |table| -> DBResult<_> {
        let col_id = table
            .meta
            .get_column_id(col_name)
            .ok_or("no such column in table")?;

        let val = Table::expr2colval(&args.value, table.meta.columns[col_id as usize].coltype);
        table.check_column_type(&args.value, col_id)?;
        Ok((col_id, val))
    })?;

    for &row in &rows {
        let (row_data, new_row_data) = db::get_table(table_id, |table| -> DBResult<_> {
            let row_data = table.select_row(row)?;
            let mut new_row_data = row_data.clone();
            new_row_data[col_id as usize] = new_col_val.clone();

            for unique_cols in &table.meta.unique {
                if unique_cols.contains(&col_id) {
                    let slice_data = table.get_data_cols(&new_row_data, unique_cols);
                    if table.check_data_exist(&slice_data, unique_cols) {
                        return Err(format!(
                            "row {} doesn't satisfy unique requirment after update",
                            row
                        )
                        .into());
                    }
                }
            }
            for (table_cols, (ftable_id, ftable_cols)) in &table.meta.foreign_key {
                if table_cols.contains(&col_id) {
                    let slice_data = table.get_data_cols(&new_row_data, table_cols);
                    db::ensure_table(*ftable_id, |ftable| -> DBResult<()> {
                        if !ftable.check_data_exist(&slice_data, ftable_cols) {
                            return Err(format!(
                                "record {} doesn't satisfy foreign key requirment after update",
                                row
                            )
                            .into());
                        }
                        Ok(())
                    })?;
                }
            }
            // initial foreign_update
            for table_ref_cols in table.meta.as_foreign_key.values() {
                for (ref_table_id, ref_cols) in table_ref_cols.iter() {
                    foreign_update.insert(*ref_table_id, Vec::new());
                }
            }
            for (ftable_cols, table_ref_cols) in &table.meta.as_foreign_key {
                if ftable_cols.contains(&col_id) {
                    for (ref_table_id, ref_cols) in table_ref_cols.iter() {
                        db::ensure_table(*ref_table_id, |ref_table| -> DBResult<()> {
                            let rows = ref_table.get_equal_rows(
                                &table.get_data_cols(&row_data, ftable_cols),
                                ref_cols
                            );
                            // maybe several cols to update?
                            let index = ftable_cols.iter().position(|&col| col == col_id).unwrap();
                            for row in rows {
                                if let Some(affected) = foreign_update.get_mut(ref_table_id) {
                                    affected.push((row, ref_cols[index]))
                                }
                            }
                            Ok(())
                        })?;
                    }
                }
            }
            Ok((row_data, new_row_data))
        })?;

        db::get_table(table_id, |table| -> DBResult<_> {
            table.remove_index_at(row, &row_data);
            Ok(())
        })?;
        db::modify_table(table_id, |table| -> DBResult<_> {
            table.update(row, col_id, &new_col_val)?;
            Ok(())
        })?;
        db::get_table(table_id, |table| {
            table.insert_index_at(row, &new_row_data);
        });
    }

    // maybe we need update record here
    for (refid, affected) in foreign_update {
        for (row, col) in affected {
            db::modify_table(refid, |table| -> DBResult<_> {
                table.update(row, col, &new_col_val)?;
                Ok(())
            })?;
        }
    }

    print_affected(rows.len());
    Ok(())
}

fn delete(args: &Delete) -> DBResult<()> {
    let table_name = &args.table_name;
    let table_id = db::get_table_id(table_name).ok_or("table name not found")?;
    db::load_table(&table_name)?;

    let rows = match relation(&args.condition, &[table_name.clone(); 1])? {
        Logic::Pos(x) => x,
        Logic::Neg(x) => {
            let mut ret = HashSet::new();
            let full: HashSet<_> = db::ensure_table(table_id, |table| {
                for rid in table.rows() {
                    ret.insert([rid, 0]);
                }
                ret
            });
            full.difference(&x).copied().collect()
        }
    };
    let rows = rows.iter().map(|s| s[0]).collect::<Vec<_>>();

    db::get_table(table_id, |table| -> DBResult<_> {
        for row in &rows {
            let row_data = table.select_row(*row)?;
            table.remove_index_at(*row, &row_data)
        }
        Ok(())
    })?;

    let mut ref_tables = Vec::new();
    db::get_table(table_id, |table| {
        for (k, v) in table
            .meta
            .as_foreign_key
            .iter() {
                for (ftable, _) in v.iter() {
                    ref_tables.push(*ftable);
                }   
            }
    }); 
    for ftable in ref_tables {
        db::ensure_table(ftable, |_| {});
    }
    db::modify_table(table_id, |table| -> DBResult<()> {
        for row in &rows {
            let row_data = table.select_row(*row)?;
            for (ftable_cols, table_ref_cols) in &table.meta.as_foreign_key {
                let slice_data = table.get_data_cols(&row_data, ftable_cols);
                for (ref_table_id, ref_cols) in table_ref_cols.iter() {
                    db::modify_table(*ref_table_id, |ref_table| -> DBResult<()> {
                        let rids = ref_table.get_equal_rows(
                            &slice_data,
                            ref_cols
                        );
                        for rid in rids {
                            ref_table.delete(rid)?;
                        }
                        Ok(())
                    })?;
                }
            }
            table.delete(*row)?;
        }
        Ok(())
    })?;
    print_affected(rows.len());
    Ok(())
}

fn show(args: &Show) -> DBResult<()> {
    match args {
        Show::Databases => db::show_databases()?,
        Show::Tables => db::show_tables()?,
        _ => unreachable!(),
    }
    Ok(())
}

fn describe(args: &Desc) -> DBResult<()> {
    let id = db::get_table_id(&args.0).ok_or("table name not found")?;
    let header = [
        "Name",
        "Type",
        "Not Null",
        "Primary",
        "Unique",
        "Foreign",
        "AsForeign",
    ];
    db::ensure_table(id, |table| {
        let columns = &table.meta.columns;
        let coltypes = columns
            .iter()
            .map(|col| get_coltype(col.coltype, col.colsize))
            .collect::<Vec<_>>();
        let mut body = Vec::with_capacity(columns.len() * header.len());
        for (i, col) in columns.iter().enumerate() {
            body.push(col.name.as_str());
            body.push(&coltypes[i]);
            body.push(check_constraint(col.constraints.is_not_null()));
            body.push(check_constraint(col.constraints.is_primary_key()));
            body.push(check_constraint(col.constraints.is_unique()));
            body.push(check_constraint(col.constraints.is_foreign_key()));
            body.push(check_constraint(col.constraints.as_foreign_key()));
        }
        print_vec(header.iter().copied(), body.chunks_exact(header.len()));
    });
    Ok(())
}
