use num_bigint::BigInt;

use crate::{
    defines::{ColID, RowID, TableID},
    error::DBResult,
    record::{ColumnType, ColumnVal},
};

use super::database::get_table;

pub fn count(rows: impl Iterator<Item = RowID>, table: TableID, col: ColID) -> DBResult<u32> {
    let count = get_table(table, |table| -> DBResult<_> {
        let count = rows
            .filter_map(|rid| table.select(rid, col).unwrap())
            .count();
        Ok(count)
    })?;
    Ok(count as _)
}

pub fn count_all(rows: impl Iterator<Item = RowID>) -> DBResult<u32> {
    Ok(rows.count() as _)
}

pub fn avg(rows: impl Iterator<Item = RowID>, table: TableID, col: ColID) -> DBResult<f64> {
    let avg = get_table(table, |table| -> DBResult<_> {
        match table.meta.columns[col as usize].coltype {
            ColumnType::Int | ColumnType::Float => {}
            _ => return Err("column referenced in `AVG` must be of `INT` or `FLOAT` type".into()),
        }

        use ColumnVal::*;
        let vals = rows.filter_map(|rid| {
            table.select(rid, col).unwrap().map(|val| match val {
                Int(i) => i as f64,
                Float(f) => f as _,
                _ => unreachable!(),
            })
        });
        let mut sum = 0f64;
        let mut count = 0;
        for (i, val) in vals.enumerate() {
            count = i;
            sum += val;
        }
        Ok(sum / (count + 1) as f64)
    })?;
    Ok(avg)
}

pub fn min(
    rows: impl Iterator<Item = RowID>,
    table: TableID,
    col: ColID,
) -> DBResult<Option<ColumnVal>> {
    let min = get_table(table, |table| -> DBResult<_> {
        let min = rows
            .filter_map(|rid| table.select(rid, col).unwrap())
            .min_by(|x, y| x.partial_cmp(y).unwrap());
        Ok(min)
    })?;
    Ok(min)
}

pub fn max(
    rows: impl Iterator<Item = RowID>,
    table: TableID,
    col: ColID,
) -> DBResult<Option<ColumnVal>> {
    let min = get_table(table, |table| -> DBResult<_> {
        let min = rows
            .filter_map(|rid| table.select(rid, col).unwrap())
            .max_by(|x, y| x.partial_cmp(y).unwrap());
        Ok(min)
    })?;
    Ok(min)
}

pub fn sum_float(rows: impl Iterator<Item = RowID>, table: TableID, col: ColID) -> DBResult<f64> {
    let sum = get_table(table, |table| -> DBResult<_> {
        let sum = rows
            .filter_map(|rid| {
                table.select(rid, col).unwrap().map(|val| match val {
                    ColumnVal::Float(f) => f as f64,
                    _ => unreachable!(),
                })
            })
            .sum::<f64>();
        Ok(sum)
    })?;
    Ok(sum)
}

pub fn sum_int(rows: impl Iterator<Item = RowID>, table: TableID, col: ColID) -> DBResult<BigInt> {
    let sum = get_table(table, |table| -> DBResult<_> {
        let sum = rows
            .filter_map(|rid| {
                table.select(rid, col).unwrap().map(|val| match val {
                    ColumnVal::Int(i) => i,
                    _ => unreachable!(),
                })
            })
            .sum::<BigInt>();
        Ok(sum)
    })?;
    Ok(sum)
}
