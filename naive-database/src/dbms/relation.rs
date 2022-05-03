use std::{collections::HashSet, vec};

use like::Like;
use naive_sql_parser::{CalcExpr, ColumnRef, CompareOp, CondExpr, Expr, LogicOp};

use crate::{
    config::MAX_JOIN_TABLE,
    dbms::database::{ensure_table, get_table, get_table_id},
    defines::{ColID, RowID, TableID},
    error::DBResult,
    record::{vec_to_buf, ColumnVal},
};

#[derive(Debug)]
pub enum Logic<T> {
    Pos(T),
    Neg(T),
}

impl<T> Logic<T> {
    fn not(self) -> Self {
        match self {
            Self::Pos(x) => Self::Neg(x),
            Self::Neg(x) => Self::Pos(x),
        }
    }

    fn get(self) -> T {
        match self {
            Self::Pos(x) => x,
            Self::Neg(x) => x,
        }
    }
}

fn comp_colval(lhs: &Option<ColumnVal>, op: CompareOp, rhs: &Option<ColumnVal>) -> DBResult<bool> {
    macro_rules! check_like {
        ( $( $name:ident )* ) => {
        $(
            let $name = match $name {
                Some(Char(s)) | Some(Varchar(s)) => s,
                None => return Ok(false),
                _ => return Err("columns used in `LIKE` expression must be of type `CHAR` or `VARCHAR`".into()),
            };
        )*
        }
    }

    use CompareOp::*;
    let ret = match op {
        EQ => lhs == rhs,
        NE => lhs != rhs,
        GT => lhs > rhs,
        LT => lhs < rhs,
        GE => lhs >= rhs,
        LE => lhs <= rhs,
        LIKE => {
            use ColumnVal::*;
            check_like! { lhs rhs };
            return Like::<true>::like(lhs.as_str(), rhs).map_err(Into::into);
        }
        NOTLIKE => {
            use ColumnVal::*;
            check_like! { lhs rhs };
            return Like::<true>::not_like(lhs.as_str(), rhs).map_err(Into::into);
        }
    };
    Ok(ret)
}

fn calc_term(expr: &CalcExpr, tables: &[String]) -> DBResult<HashSet<[RowID; MAX_JOIN_TABLE]>> {
    let compare = |lhs: &Expr, op, rhs: &Expr| -> DBResult<_> {
        let (ltable, lcol) = match lhs {
            Expr::ColumnRef(colref) => match colref {
                ColumnRef::Ident(ident) => (table_of_column(ident, tables)?, ident),
                ColumnRef::Attr {
                    table_name: table,
                    column,
                } => (table.as_str(), column),
            },
            _ => {
                return Err(
                    "expect column on the left-hand side when comparing in where clause".into(),
                )
            }
        };
        let lid = get_table_id(ltable).ok_or(format!("table {} does not exist", ltable))?;
        let rows = ensure_table(lid, |table| -> DBResult<_> {
            let ret = match rhs {
                Expr::IntLit(_) | Expr::FloatLit(_) | Expr::StringLit(_) | Expr::Null => {
                    let col = [table.meta.get_column_id(lcol).unwrap()];
                    let expr = &[rhs];
                    let col_val = table.exprs2colval(expr, &col);
                    let rows = table.filter_rows(&col, op, &col_val)?;
                    if tables.len() > 1 {
                        if tables[0] == ltable {
                            get_cartesian(rows.iter().cloned(), &tables[1], false)?
                        } else {
                            get_cartesian(rows.iter().cloned(), &tables[0], true)?
                        }
                    } else {
                        let mut ret = HashSet::new();
                        for rid in rows {
                            ret.insert([rid, 0]);
                        }
                        ret
                    }
                }
                Expr::ColumnRef(rcolref) => {
                    let (rtable, rcol) = match rcolref {
                        ColumnRef::Ident(ident) => (table_of_column(ident, tables)?, ident),
                        ColumnRef::Attr {
                            table_name: table,
                            column,
                        } => (table.as_str(), column),
                    };
                    let rid =
                        get_table_id(rtable).ok_or(format!("table {} does not exist", ltable))?;
                    let lcol = get_table(lid, |table| -> ColID {
                        table.meta.get_column_id(lcol).unwrap()
                    });
                    let rcol = get_table(rid, |table| -> ColID {
                        table.meta.get_column_id(rcol).unwrap()
                    });
                    if ltable == rtable {
                        let ret = get_table(lid, |table| -> DBResult<_> {
                            let mut ret = vec![];
                            for rid in table.rows() {
                                let cols = [lcol, rcol];
                                let cols = cols.iter().cloned();
                                let vals = table.select_cols(rid, cols)?;
                                let result = unsafe {
                                    comp_colval(vals.get_unchecked(0), op, vals.get_unchecked(1))?
                                };
                                if result {
                                    ret.push(rid);
                                }
                            }

                            Ok(ret)
                        })?;
                        if tables.len() == 1 {
                            ret.iter().map(|&rid| [rid, 0]).collect()
                        } else if tables[0] == ltable {
                            get_cartesian(ret.iter().cloned(), &tables[1], false)?
                        } else {
                            get_cartesian(ret.iter().cloned(), &tables[0], true)?
                        }
                    } else if tables[0] == ltable {
                        get_match_rows(lid, lcol, rid, rcol, op)?
                    } else {
                        get_match_rows(rid, rcol, lid, lcol, op.rev())?
                    }
                }
                Expr::Binary(_, _, _) => todo!(),
            };
            Ok(ret)
        })?;
        Ok(rows)
    };

    let rows = match expr {
        CalcExpr::In(_, _) => todo!(),
        CalcExpr::Compare(lhs, op, rhs) => compare(lhs, *op, rhs)?,
        CalcExpr::IsNull(_) => todo!(),
    };
    Ok(rows)
}

pub fn relation(
    cond: &CondExpr,
    ctx: &[String],
) -> DBResult<Logic<HashSet<[RowID; MAX_JOIN_TABLE]>>> {
    let binary = |lhs: &CondExpr,
                  op: &LogicOp,
                  rhs: &CondExpr|
     -> DBResult<Logic<HashSet<[RowID; MAX_JOIN_TABLE]>>> {
        let lhs = relation(lhs, ctx)?;
        let rhs = relation(rhs, ctx)?;
        let ret = match op {
            naive_sql_parser::LogicOp::OR => match (lhs, rhs) {
                (Pos(lhs), Pos(rhs)) => Pos(lhs.union(&rhs).copied().collect()),
                (Neg(lhs), Neg(rhs)) => Neg(lhs.intersection(&rhs).copied().collect()),
                (Pos(lhs), Neg(rhs)) | (Neg(rhs), Pos(lhs)) => {
                    Neg(rhs.difference(&lhs).copied().collect())
                }
            },
            naive_sql_parser::LogicOp::AND => match (lhs, rhs) {
                (Pos(lhs), Pos(rhs)) => Pos(lhs.intersection(&rhs).copied().collect()),
                (Neg(lhs), Neg(rhs)) => Neg(lhs.union(&rhs).copied().collect()),
                (Pos(lhs), Neg(rhs)) | (Neg(rhs), Pos(lhs)) => {
                    Pos(lhs.difference(&rhs).copied().collect())
                }
            },
        };
        Ok(ret)
    };

    use Logic::*;
    let ret = match cond {
        CondExpr::True => Neg(HashSet::new()),
        CondExpr::False => Pos(HashSet::new()),
        CondExpr::Binary(lhs, op, rhs) => binary(lhs, op, rhs)?,
        CondExpr::Not(expr) => relation(expr, ctx)?.not(),
        CondExpr::Term(expr) => Pos(calc_term(expr, ctx)?),
    };
    Ok(ret)
}

pub fn table_of_column<'t>(col_name: &str, tables: &'t [String]) -> DBResult<&'t str> {
    let mut ret = None;
    for tbl in tables {
        let id = get_table_id(tbl).unwrap();
        if get_table(id, |table| table.meta.get_column_id(col_name)).is_some() {
            if let Some(prev_tbl) = ret {
                return Err(format!(
                    "column {} appears simultaneously in table {} and {} ambiguously",
                    col_name, prev_tbl, tbl
                )
                .into());
            } else {
                ret = Some(tbl);
            }
        }
    }
    if let Some(ret) = ret {
        Ok(ret)
    } else {
        Err(format!("no table has the column name {}", col_name).into())
    }
}

pub fn get_cartesian(
    rows: impl Iterator<Item = RowID>,
    table_name: &str,
    on_left: bool,
) -> DBResult<HashSet<[RowID; MAX_JOIN_TABLE]>> {
    let table_id =
        get_table_id(table_name).ok_or(format!("table {} does not exist", table_name))?;

    let ret = ensure_table(table_id, |table| {
        let table_rows: Vec<_> = table.rows().collect();
        if on_left {
            let mut ret = HashSet::new();
            for rrid in rows {
                for &lrid in &table_rows {
                    ret.insert([lrid, rrid]);
                }
            }
            ret
        } else {
            let mut ret = HashSet::new();
            for lrid in rows {
                for &rrid in &table_rows {
                    ret.insert([lrid, rrid]);
                }
            }
            ret
        }
    });
    Ok(ret)
}

fn get_match_rows(
    ltable_id: TableID,
    lcol: ColID,
    rtable_id: TableID,
    rcol: ColID,
    op: CompareOp,
) -> DBResult<HashSet<[RowID; MAX_JOIN_TABLE]>> {
    let lhas_index = get_table(ltable_id, |table| -> bool {
        let col_buf = vec_to_buf(&[lcol]);
        table.indices.get(&(col_buf, 1)).is_some()
    });
    let mut ret = HashSet::new();
    if lhas_index {
        ret = get_table(
            rtable_id,
            |rtable| -> DBResult<HashSet<[RowID; MAX_JOIN_TABLE]>> {
                let ret = get_table(
                    ltable_id,
                    |ltable| -> DBResult<HashSet<[RowID; MAX_JOIN_TABLE]>> {
                        let mut ret = HashSet::new();
                        for rrid in rtable.rows() {
                            let data = rtable.select(rrid, rcol)?;
                            let lrows = ltable.filter_rows(&[lcol], op, &[data])?;
                            for lrid in lrows {
                                ret.insert([lrid, rrid]);
                            }
                        }
                        Ok(ret)
                    },
                )?;
                Ok(ret)
            },
        )?;
    } else {
        ret = get_table(
            ltable_id,
            |ltable| -> DBResult<HashSet<[RowID; MAX_JOIN_TABLE]>> {
                let ret = get_table(
                    rtable_id,
                    |rtable| -> DBResult<HashSet<[RowID; MAX_JOIN_TABLE]>> {
                        let mut ret = HashSet::new();
                        for lrid in ltable.rows() {
                            let data = ltable.select(lrid, lcol)?;
                            let rrows = rtable.filter_rows(&[rcol], op, &[data])?;
                            for rrid in rrows {
                                ret.insert([lrid, rrid]);
                            }
                        }
                        Ok(ret)
                    },
                )?;
                Ok(ret)
            },
        )?;
    }
    Ok(ret)
}
