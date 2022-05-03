#![allow(unused)]

use std::{
    cmp::Ordering, collections::BTreeSet, intrinsics::transmute, mem::size_of, ops::Bound::*,
};

use serde::{Deserialize, Serialize};

use crate::{
    config::MAX_COMP_INDEX,
    dbms::database::{ensure_table, get_table},
    defines::{ColID, RowID, TableID},
    record::ColumnVal,
    utils::persistence::Persistence,
};

use super::fast_cmp::FastCmp;

macro_rules! assert_field {
    ( $self:ident $other:ident; $( $field:ident ),* ) => {
        $( debug_assert_eq!($self.$field, $other.$field, concat!("comparing indexes of different", stringify!($field))); )*
    };
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq)]
pub struct EntryRef {
    pub len: u8,
    pub rid: RowID,                   // for "row id"
    pub col: [ColID; MAX_COMP_INDEX], // for "column index"
    pub tbl: TableID,                 // table id
    pub fast_cmp: [FastCmp; MAX_COMP_INDEX],
    pub is_null: u8,
}

impl EntryRef {
    fn nullat(&self, idx: u8) -> bool {
        (self.is_null & (1 << idx)) != 0
    }

    // still need check here
    fn comp_at(&self, other: &Self, idx: u8) -> Ordering {
        debug_assert!(idx < self.len);
        // null treated as the greatest elem
        match (self.nullat(idx), other.nullat(idx)) {
            (true, true) => return self.rid.cmp(&other.rid),
            (true, false) => return Ordering::Less,
            (false, true) => return Ordering::Greater,
            _ => {}
        }

        match self.fast_cmp[idx as usize].cmp(&other.fast_cmp[idx as usize]) {
            Ordering::Equal => {}
            e => return e,
        }

        ensure_table(self.tbl, |table| {
            let l = table.select(self.rid, self.col[idx as usize]).unwrap();
            let r = table.select(other.rid, other.col[idx as usize]).unwrap();
            l.partial_cmp(&r).unwrap()
        })
    }

    fn comp_with_data_at(&self, colval: &Option<ColumnVal>, idx: u8) -> Ordering {
        let colval = match (colval, self.nullat(idx)) {
            (Some(colval), false) => colval,
            (Some(_), true) => return Ordering::Less,
            (None, false) => return Ordering::Greater,
            _ => return Ordering::Equal,
        };

        match self.fast_cmp[idx as usize].cmp(&FastCmp::from_colval(colval)) {
            Ordering::Equal => {}
            e => return e,
        }

        get_table(self.tbl, |table| {
            let l = table
                .select(self.rid, self.col[idx as usize])
                .unwrap()
                .unwrap();
            l.partial_cmp(colval).unwrap()
        })
    }
}

impl PartialEq for EntryRef {
    fn eq(&self, other: &Self) -> bool {
        assert_field!(self other; tbl, col, len);
        self.cmp(other).is_eq()
    }
}

impl Ord for EntryRef {
    fn cmp(&self, other: &Self) -> Ordering {
        assert_field!(self other; tbl, col, len);
        let mut cmp_res = Ordering::Equal;
        for i in 0..self.len {
            cmp_res = self.comp_at(other, i);
            if !cmp_res.is_eq() {
                return cmp_res;
            }
        }
        self.rid.cmp(&other.rid)
    }
}

impl PartialOrd for EntryRef {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug)]
pub enum IndexKey {
    Ref(EntryRef),
    Data([Option<ColumnVal>; MAX_COMP_INDEX]),
}

impl IndexKey {
    pub fn to_ref(&self) -> &EntryRef {
        match self {
            Self::Ref(r) => r,
            _ => panic!("not ref"),
        }
    }
}

impl From<&[Option<ColumnVal>]> for IndexKey {
    fn from(colval: &[Option<ColumnVal>]) -> Self {
        use std::mem::MaybeUninit;
        let len;
        let mut buf: [Option<ColumnVal>; MAX_COMP_INDEX] = unsafe {
            let mut buf: [MaybeUninit<Option<ColumnVal>>; MAX_COMP_INDEX] =
                MaybeUninit::uninit().assume_init();
            len = colval.len();
            for (i, val) in colval.iter().enumerate() {
                buf[i].write(val.clone());
            }
            transmute(buf)
        };
        (&mut buf[len..]).fill(None);
        Self::Data(buf)
    }
}

impl From<EntryRef> for IndexKey {
    fn from(eref: EntryRef) -> Self {
        Self::Ref(eref)
    }
}

impl Serialize for IndexKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if let Self::Ref(eref) = self {
            eref.serialize(serializer)
        } else {
            unreachable!("a non-ref index key shall not be serialized");
        }
    }
}

impl<'de> Deserialize<'de> for IndexKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let eref = EntryRef::deserialize(deserializer)?;
        Ok(Self::Ref(eref))
    }
}

impl PartialEq for IndexKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for IndexKey {}

impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexKey {
    fn cmp(&self, other: &Self) -> Ordering {
        use IndexKey::*;

        match (self, other) {
            (Ref(eref), Data(data)) => {
                let mut cmp_res = Ordering::Equal;
                for idx in 0..eref.len {
                    cmp_res = eref.comp_with_data_at(&data[idx as usize], idx);
                    if !cmp_res.is_eq() {
                        return cmp_res;
                    }
                }
                cmp_res
            }
            (Data(data), Ref(eref)) => {
                let mut cmp_res = Ordering::Equal;
                for idx in 0..eref.len {
                    cmp_res = eref.comp_with_data_at(&data[idx as usize], idx);
                    if !cmp_res.is_eq() {
                        return cmp_res.reverse();
                    }
                }
                cmp_res
            }

            (Ref(l), Ref(r)) => l.cmp(r),
            (Data(l), Data(r)) => l.partial_cmp(r).unwrap(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColIndex {
    pub tbl: TableID,
    pub len: u8,
    pub col: [ColID; MAX_COMP_INDEX],
    pub list: BTreeSet<IndexKey>,
}

impl ColIndex {
    pub fn new(
        id: TableID,
        len: u8,
        col: [ColID; MAX_COMP_INDEX],
        list: BTreeSet<IndexKey>,
    ) -> Self {
        Self {
            tbl: id,
            col,
            list,
            len,
        }
    }

    pub fn insert_record(&mut self, row_id: RowID, data: &[Option<ColumnVal>]) {
        let mut fastcmp_buf: [FastCmp; MAX_COMP_INDEX] =
            unsafe { transmute([0u8; MAX_COMP_INDEX * size_of::<FastCmp>()]) };
        let mut is_null = 0u8;
        for i in 0..self.len {
            if let Some(colval) = &data[self.col[i as usize] as usize] {
                fastcmp_buf[i as usize] = colval.into();
            } else {
                is_null |= 1 << i;
            }
        }
        let index_key = EntryRef {
            col: self.col,
            fast_cmp: fastcmp_buf,
            is_null,
            len: self.len as _,
            rid: row_id,
            tbl: self.tbl,
        }
        .into();
        self.list.insert(index_key);
    }

    pub fn remove_record(&mut self, row_id: RowID, data: &[Option<ColumnVal>]) {
        let mut fastcmp_buf: [FastCmp; MAX_COMP_INDEX] =
            unsafe { transmute([0u8; MAX_COMP_INDEX * size_of::<FastCmp>()]) };
        let mut is_null = 0u8;
        for i in 0..self.len {
            if let Some(colval) = data.get(self.col[i as usize] as usize).unwrap() {
                fastcmp_buf[i as usize] = colval.into();
            } else {
                is_null |= 1 << i;
            }
        }
        let index_key: IndexKey = EntryRef {
            col: self.col,
            fast_cmp: fastcmp_buf,
            is_null,
            len: self.len as _,
            rid: row_id,
            tbl: self.tbl,
        }
        .into();
        self.list.remove(&index_key);
    }

    #[inline]
    pub fn format_filename(tbl: TableID, col: &[ColID]) -> String {
        format!(
            "tb{}-col{}.bp.index",
            tbl,
            col.iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("_")
        )
    }

    #[inline]
    pub fn first_rid(&self) -> Option<RowID> {
        self.list.iter().next().map(|key| key.to_ref().rid)
    }

    #[inline]
    pub fn last_rid(&self) -> Option<RowID> {
        self.list.iter().next_back().map(|key| key.to_ref().rid)
    }

    #[inline]
    pub fn lower_bound<T: Into<IndexKey>>(&self, key: T) -> Option<&EntryRef> {
        self.list
            .range((Excluded(key.into()), Unbounded))
            .next()
            .map(|key| key.to_ref())
    }

    #[inline]
    pub fn upper_bound<T: Into<IndexKey>>(&self, key: T) -> Option<&EntryRef> {
        self.list
            .range(..key.into())
            .next_back()
            .map(|key| key.to_ref())
    }

    #[inline]
    pub fn lower_bound_eq<T: Into<IndexKey>>(&self, key: T) -> Option<&EntryRef> {
        self.list.range(key.into()..).next().map(|key| key.to_ref())
    }

    #[inline]
    pub fn upper_bound_eq<T: Into<IndexKey>>(&self, key: T) -> Option<&EntryRef> {
        self.list
            .range(..=key.into())
            .next_back()
            .map(|key| key.to_ref())
    }

    #[inline]
    pub fn range_rows<T: Into<IndexKey>>(
        &self,
        lower_key: T,
        upper_key: T,
    ) -> impl Iterator<Item = RowID> + '_ {
        self.list
            .range(lower_key.into()..=upper_key.into())
            .map(|key| key.to_ref().rid)
    }

    #[inline]
    pub fn out_range_rows<T: Into<IndexKey>>(
        &self,
        lower_key: T,
        upper_key: T,
    ) -> impl Iterator<Item = RowID> + '_ {
        self.list
            .range(..lower_key.into())
            .map(|key| key.to_ref().rid)
            .chain(
                self.list
                    .range((Excluded(upper_key.into()), Unbounded))
                    .map(|key| key.to_ref().rid),
            )
    }

    #[inline]
    pub fn lower_range_rows<T: Into<IndexKey>>(&self, key: T) -> impl Iterator<Item = RowID> + '_ {
        self.list
            .range((Unbounded, Excluded(key.into())))
            .map(|key| key.to_ref().rid)
    }

    #[inline]
    pub fn lower_eq_range_rows<T: Into<IndexKey>>(
        &self,
        key: T,
    ) -> impl Iterator<Item = RowID> + '_ {
        self.list
            .range((Unbounded, Included(key.into())))
            .map(|key| key.to_ref().rid)
    }

    #[inline]
    pub fn upper_range_rows<T: Into<IndexKey>>(&self, key: T) -> impl Iterator<Item = RowID> + '_ {
        self.list
            .range((Excluded(key.into()), Unbounded))
            .map(|key| key.to_ref().rid)
    }

    #[inline]
    pub fn upper_eq_range_rows<T: Into<IndexKey>>(
        &self,
        key: T,
    ) -> impl Iterator<Item = RowID> + '_ {
        self.list
            .range((Included(key.into()), Unbounded))
            .map(|key| key.to_ref().rid)
    }

    #[inline]
    pub fn iter_rid(&self) -> impl DoubleEndedIterator<Item = RowID> + '_ {
        self.list.iter().map(|key| key.to_ref().rid)
    }
}

pub fn data2fastcmp(data: &[Option<ColumnVal>]) -> ([FastCmp; MAX_COMP_INDEX], u8) {
    let mut fastcmp_buf: [FastCmp; MAX_COMP_INDEX] =
        unsafe { transmute([0u8; MAX_COMP_INDEX * size_of::<FastCmp>()]) };
    let mut is_null = 0u8;
    for i in 0..data.len() {
        if let Some(colval) = data.get(i).unwrap() {
            fastcmp_buf[i as usize] = colval.into();
        } else {
            is_null |= 1 << i;
        }
    }
    (fastcmp_buf, is_null)
}

impl Persistence for ColIndex {
    fn filename(&self) -> String {
        Self::format_filename(self.tbl, &self.col[..self.len as usize])
    }
}
