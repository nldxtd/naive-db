use std::fmt::Display;

use chrono::NaiveDate;

#[derive(Debug, Clone, Copy)]
pub enum ColumnType {
    Int,
    Float,
    Char,
    Varchar,
    Date,
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub coltype: ColumnType,
    pub colsize: Option<u8>,
    pub notnull: bool,
    pub unique: bool,
    pub primary: bool,
    pub foreign: Option<(String, String)>,
}

#[derive(Debug)]
pub struct NamedTBConstraint {
    pub name: Option<String>,
    pub constraint: TBConstraint,
}

#[derive(Debug)]
pub enum TBConstraint {
    Primary(Vec<String>),
    Unique(Vec<String>),
    Check {
        colname: String,
        exprs: Vec<Expr>,
    },
    Foreign {
        colname: Vec<String>,
        foreign_tb: String,
        foreign_col: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum CompareOp {
    EQ,
    NE,
    GT,
    LT,
    GE,
    LE,
    LIKE,
    NOTLIKE,
}

impl CompareOp {
    pub fn rev(self) -> Self {
        use CompareOp::*;
        match self {
            EQ => EQ,
            NE => NE,
            GT => LE,
            LT => GE,
            GE => LT,
            LE => GT,
            LIKE => LIKE,
            NOTLIKE => NOTLIKE,
        }
    }
}

#[derive(Debug)]
pub enum LogicOp {
    AND,
    OR,
}

#[derive(Debug)]
pub enum CondExpr {
    True,
    False,
    Binary(Box<CondExpr>, LogicOp, Box<CondExpr>),
    Not(Box<CondExpr>),
    Term(CalcExpr),
}

#[derive(Debug)]
pub enum CalcExpr {
    In(Box<Expr>, Vec<Expr>),
    Compare(Box<Expr>, CompareOp, Box<Expr>),
    IsNull(Box<Expr>),
}

#[derive(Debug)]
pub enum BinaryOp {
    ADD,
    SUB,
    MUL,
    DIV,
}

#[derive(Debug)]
pub enum Expr {
    Binary(Box<Expr>, BinaryOp, Box<Expr>),
    ColumnRef(ColumnRef),
    IntLit(i32),
    FloatLit(f32),
    StringLit(String),
    Null,
}

#[derive(Debug)]
pub enum ColumnRef {
    Ident(String),
    Attr { table_name: String, column: String },
}

impl Display for ColumnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnRef::Ident(ident) => write!(f, "{}", ident),
            ColumnRef::Attr { table_name, column } => write!(f, "{}.{}", table_name, column),
        }
    }
}

#[derive(Debug)]
pub enum Aggregator {
    COUNT,
    AVG,
    MIN,
    MAX,
    SUM,
}

#[derive(Debug)]
pub enum SingleSelector {
    Single(ColumnRef),
    Aggregate(Aggregator, ColumnRef),
    CountAll,
}

#[derive(Debug)]
pub enum Selectors {
    Part(Vec<SingleSelector>),
    All,
}
