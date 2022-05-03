use std::{
    convert::TryFrom,
    error::Error,
    fmt::{self, Display, Formatter},
};

use bitflags::bitflags;
use chrono::NaiveDate;
use naive_sql_parser::{Column as ASTColumn, ColumnType as ASTColumnType};
use serde::Serialize;

use crate::{config::DEFAULT_SIZE, utils::Identity};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u8)]
pub enum ColumnType {
    Int,
    Float,
    Char,
    Varchar,
    Date,
}

impl From<ASTColumnType> for ColumnType {
    fn from(coltype: ASTColumnType) -> Self {
        use ASTColumnType::*;
        macro_rules! map_enum {
            ( $e:expr; $( $x:ident )* ) => {
                match $e {
                    $(
                        $x => Self::$x,
                    )*
                }
            }
        }
        map_enum!(coltype; Int Float Char Varchar Date)
    }
}

macro_rules! map_enum {
    ( $e: expr; $( $x: ident )* ) => {
        match $e {
        $( &Self::$x(_) => $x, )*
        }
    }
}

macro_rules! cmp_enum {
    ( $e: expr; $( $x: ident )* ) => {
        match $e {
        $( ($x(l), $x(r)) => l.partial_cmp(r), )*
        _ => None,
        }
    }
}

macro_rules! impl_colval {
    ( $( $hkt:ident $name:ident ),* ) => {
$(
// Null is expressed through `Option`
#[derive(Debug, Clone, PartialEq)]
pub enum $name {
    Int($hkt<i32>),
    Float($hkt<f32>),
    Char($hkt<String>),
    Varchar($hkt<String>),
    Date($hkt<NaiveDate>),
}

impl $name {
    #[inline]
    pub fn coltype(&self) -> ColumnType {
        use ColumnType::*;
        map_enum!(self; Int Float Char Varchar Date)
    }
}

impl AsRef<Self> for $name {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl From<$hkt<i32>> for $name {
    #[inline]
    fn from(data: $hkt<i32>) -> Self {
        Self::Int(data)
    }
}

impl From<$hkt<f32>> for $name {
    #[inline]
    fn from(data: $hkt<f32>) -> Self {
        Self::Float(data)
    }
}

impl From<$hkt<NaiveDate>> for $name {
    #[inline]
    fn from(data: $hkt<NaiveDate>) -> Self {
        Self::Date(data)
    }
}

impl PartialOrd for $name {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use $name::*;
        cmp_enum!((self, other); Int Float Char Varchar Date)
    }
}
)*
}}

impl_colval! {
    Identity ColumnVal,
    Vec ColumnValVec
}

impl Display for ColumnVal {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ColumnVal::Int(i) => write!(formatter, "{}", i),
            ColumnVal::Float(f) => write!(formatter, "{}", f),
            ColumnVal::Char(s) | ColumnVal::Varchar(s) => write!(formatter, "'{}'", s),
            ColumnVal::Date(d) => write!(formatter, "'{}'", d),
        }
    }
}

bitflags! {
    #[derive(Serialize, Deserialize)]
    pub struct Constraints: u8 {
        const EMPTY = 0b0000_0000;
        const NOT_NULL = 0b0000_0001;
        const UNIQUE = 0b0000_0010;
        // primary key imply notnull and unique
        // which should be checked
        const PRIMARY_KEY = 0b0000_0100;
        const FOREIGN_KEY = 0b0000_1000;
        const AS_FOREIGN_KEY = 0b0001_0000;
    }
}

impl Constraints {
    pub fn is_not_null(&self) -> bool {
        self.contains(Self::NOT_NULL)
    }

    pub fn is_unique(&self) -> bool {
        self.contains(Self::UNIQUE)
    }

    pub fn is_primary_key(&self) -> bool {
        self.contains(Self::PRIMARY_KEY)
    }

    pub fn is_foreign_key(&self) -> bool {
        self.contains(Self::FOREIGN_KEY)
    }

    pub fn as_foreign_key(&self) -> bool {
        self.contains(Self::AS_FOREIGN_KEY)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub coltype: ColumnType,
    pub colsize: u8,
    pub constraints: Constraints,
}

impl Column {}

impl TryFrom<&ASTColumn> for Column {
    type Error = Box<dyn Error>;

    fn try_from(col: &ASTColumn) -> Result<Self, Self::Error> {
        use ASTColumnType::{Char, Varchar};
        let colsize = match col.colsize {
            Some(i) => i as _,
            None if matches!(col.coltype, Char | Varchar) => {
                return Err("type `char` or `varchar` must have size provided".into())
            }
            _ => DEFAULT_SIZE,
        };
        let mut constraints = Constraints::EMPTY;
        if col.notnull {
            constraints |= Constraints::NOT_NULL
        }
        if col.unique {
            constraints |= Constraints::UNIQUE
        }
        if col.primary {
            constraints |= Constraints::PRIMARY_KEY
        }
        if col.foreign.is_some() {
            constraints |= Constraints::FOREIGN_KEY
        }
        Ok(Self {
            name: col.name.clone(),
            coltype: col.coltype.into(),
            colsize,
            constraints,
        })
    }
}
