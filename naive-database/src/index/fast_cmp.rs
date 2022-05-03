#![allow(unused)]

use chrono::NaiveDate;
use serde::Serialize;
use std::{cmp::Ordering, mem};

use crate::record::{ColumnType, ColumnVal};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct FastCmp {
    coltype: ColumnType,
    data: i32,
}

impl FastCmp {
    #[inline]
    pub unsafe fn new(coltype: ColumnType, data: [u8; 4]) -> Self {
        Self {
            coltype,
            data: mem::transmute(data),
        }
    }

    pub fn from_colval(colval: &ColumnVal) -> Self {
        use ColumnVal::*;
        let data = match colval {
            Char(s) | Varchar(s) => {
                let mut data = 0;
                for c in s.as_bytes().iter().take(4) {
                    data <<= 8;
                    data += *c as i32;
                }
                data
            }
            Int(i) => *i,
            Float(f) => f.to_bits() as _,
            Date(d) => unsafe {
                debug_assert_eq!(mem::size_of::<NaiveDate>(), 4);
                mem::transmute(*d)
            },
            _ => 0,
        };
        Self {
            coltype: colval.coltype(),
            data,
        }
    }

    #[inline]
    pub fn coltype(&self) -> ColumnType {
        self.coltype
    }
}

impl<T: AsRef<ColumnVal>> From<T> for FastCmp {
    fn from(colval: T) -> Self {
        Self::from_colval(colval.as_ref())
    }
}

impl PartialOrd for FastCmp {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FastCmp {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        use ColumnType::*;
        match self.coltype {
            Int | Char | Varchar => self.data.cmp(&other.data),
            Date => unsafe {
                let lhs: NaiveDate = mem::transmute(self.data);
                let rhs: NaiveDate = mem::transmute(other.data);
                lhs.cmp(&rhs)
            },
            Float => {
                let lhs = f32::from_bits(self.data as _);
                let rhs = f32::from_bits(other.data as _);
                lhs.partial_cmp(&rhs)
                    .expect("I'm not expecting an NaN here")
            }
        }
    }
}
