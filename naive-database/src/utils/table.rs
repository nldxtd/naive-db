use std::{
    collections::HashSet,
    fmt::Display,
    io::{stdout, BufWriter},
};

use lazy_static::lazy_static;
use prettytable::{format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR, Attr, Cell, Row, Table};

use regex::Regex;

use crate::{
    config::MAX_JOIN_TABLE,
    dbms::database::get_table,
    defines::{ColID, RowID, TableID},
    error::DBResult,
    record::{ColumnType, ColumnVal},
};

fn format_row<'a, T: Display + 'a>(row: impl Iterator<Item = &'a T>) -> Row {
    Row::new(row.map(|val| Cell::new(val.to_string().as_str())).collect())
}

fn format_data_row(row: &[Option<ColumnVal>]) -> Row {
    lazy_static! {
        static ref NULL: String = "NULL".to_owned();
    };

    Row::new(
        row.iter()
            .map(|val| match val {
                Some(val) => Cell::new(&val.to_string()),
                None => Cell::new(&NULL),
            })
            .collect(),
    )
}

pub fn print_vec<'header, 'body>(
    header: impl Iterator<Item = &'header str>,
    body: impl Iterator<Item = &'body [&'body str]>,
) {
    let mut table = Table::new();
    table.set_format(*FORMAT_NO_BORDER_LINE_SEPARATOR);

    let header = Row::new(
        header
            .map(|s| Cell::new(s).with_style(Attr::Bold))
            .collect(),
    );
    table.set_titles(header);

    for row in body {
        table.add_row(format_row(row.iter()));
    }
    let out = stdout();
    let out = out.lock();
    let mut out = BufWriter::new(out);
    table.print(&mut out);
}

pub fn print_data_row<'header, 'body>(
    header: impl Iterator<Item = &'header str>,
    body: impl Iterator<Item = &'body [Option<ColumnVal>]>,
) {
    let mut table = Table::new();
    table.set_format(*FORMAT_NO_BORDER_LINE_SEPARATOR);

    let header = Row::new(
        header
            .map(|s| Cell::new(s).with_style(Attr::Bold))
            .collect(),
    );
    table.set_titles(header);

    for row in body {
        table.add_row(format_data_row(row));
    }
    let out = stdout();
    let out = out.lock();
    let mut out = BufWriter::new(out);
    table.print(&mut out);
}

pub fn get_coltype(coltype: ColumnType, colsize: u8) -> String {
    match coltype {
        ColumnType::Int => "Int".to_string(),
        ColumnType::Float => "Float".to_string(),
        ColumnType::Char => format!("Char({})", colsize),
        ColumnType::Varchar => format!("VarChar({})", colsize),
        ColumnType::Date => "Date".to_string(),
    }
}

pub fn check_constraint(is_match: bool) -> &'static str {
    if is_match {
        "Yes"
    } else {
        ""
    }
}

pub fn parse_colval(coltype: ColumnType, val: &str) -> DBResult<Option<ColumnVal>> {
    use ColumnVal::*;
    lazy_static! {
        static ref NULL: Regex = Regex::new(r"(?i)null").unwrap();
    };

    let val = if NULL.is_match(val) {
        return Ok(None);
    } else {
        match coltype {
            ColumnType::Int => Int(val.parse()?),
            ColumnType::Float => Float(val.parse()?),
            ColumnType::Char => Char(val.to_owned()),
            ColumnType::Varchar => Varchar(val.to_owned()),
            ColumnType::Date => Date(val.parse()?),
        }
    };

    Ok(Some(val))
}

pub fn print_join_table(
    rows: HashSet<[RowID; MAX_JOIN_TABLE]>,
    lid: TableID,
    lcols: &[ColID],
    rid: TableID,
    rcols: &[ColID],
) {
    if rows.is_empty() {
        println!("No data found");
        return;
    }
    get_table(lid, |ltable| {
        get_table(rid, |rtable| {
            let header = lcols
                .iter()
                .map(|&lcol| ltable.meta.columns[lcol as usize].name.as_str())
                .chain(
                    rcols
                        .iter()
                        .map(|&rcol| rtable.meta.columns[rcol as usize].name.as_str()),
                );
            let mut body = Vec::with_capacity((lcols.len() + rcols.len()) * rows.len());
            for &[lrow, rrow] in rows.iter() {
                let ldata = ltable.select_cols(lrow, lcols.iter().copied()).unwrap();
                body.extend(ldata);
                let rdata = rtable.select_cols(rrow, rcols.iter().copied()).unwrap();
                body.extend(rdata);
            }
            print_data_row(header, body.chunks_exact(lcols.len() + rcols.len()));
            println!("{} items in total", rows.len());
        })
    })
}
