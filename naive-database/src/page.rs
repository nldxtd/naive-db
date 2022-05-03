use crate::{
    config::{PAGE_HEADER_LEN, PAGE_SIZE},
    defines::PageNum,
    utils::iter_bits,
};
use std::{
    borrow::{Borrow, BorrowMut},
    convert::identity,
    fmt::Debug,
    mem::{self, size_of},
    ops::{Deref, DerefMut},
    slice,
};

type Buf = [u8; PAGE_SIZE];

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(align(4096))]
pub struct PageBuf {
    data: Buf,
}

impl PageBuf {
    #[inline]
    pub const fn new() -> Self {
        Self {
            data: [0; PAGE_SIZE],
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Page {
    data: [u8],
}

impl Page {
    #[inline]
    pub fn from_ref<T: AsRef<[u8]> + ?Sized>(data: &T) -> Option<&Self> {
        let data = data.as_ref();
        if data.len() >= PAGE_SIZE {
            Some(unsafe { mem::transmute(&data[..PAGE_SIZE]) })
        } else {
            None
        }
    }

    #[inline]
    pub unsafe fn from_ref_unchecked<T: AsRef<[u8]> + ?Sized>(data: &T) -> &Self {
        let data = data.as_ref();
        mem::transmute(slice::from_raw_parts(data.as_ptr(), data.len()))
    }

    #[inline]
    pub fn from_mut<T: AsMut<[u8]> + ?Sized>(data: &mut T) -> Option<&mut Self> {
        let data = data.as_mut();
        if data.len() >= PAGE_SIZE {
            Some(unsafe { mem::transmute(&mut data[..PAGE_SIZE]) })
        } else {
            None
        }
    }

    #[inline]
    pub unsafe fn from_mut_unchecked<T: AsMut<[u8]> + ?Sized>(data: &mut T) -> &mut Self {
        let data = data.as_mut();
        mem::transmute(slice::from_raw_parts_mut(data.as_mut_ptr(), data.len()))
    }

    #[inline]
    pub fn write(&mut self, data: &[u8]) {
        self.copy_from_slice(&PageBuf::from(data))
    }

    pub fn header(&self) -> &FixedPageHeader {
        debug_assert_eq!(size_of::<FixedPageHeader>(), PAGE_HEADER_LEN);
        unsafe { &*(self.as_ptr() as *const _) }
    }

    pub fn header_mut(&mut self) -> &mut FixedPageHeader {
        debug_assert_eq!(size_of::<FixedPageHeader>(), PAGE_HEADER_LEN);
        unsafe { &mut *(self.as_mut_ptr() as *mut _) }
    }

    pub fn data(&self) -> &[u8] {
        &self[PAGE_HEADER_LEN..]
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self[PAGE_HEADER_LEN..]
    }

    pub fn split_header(&self) -> (&FixedPageHeader, &[u8]) {
        debug_assert_eq!(size_of::<FixedPageHeader>(), PAGE_HEADER_LEN);
        (
            unsafe { &*(self.as_ptr() as *const _) },
            &self[PAGE_HEADER_LEN..],
        )
    }

    pub fn split_header_mut(&mut self) -> (&mut FixedPageHeader, &mut [u8]) {
        debug_assert_eq!(size_of::<FixedPageHeader>(), PAGE_HEADER_LEN);
        (
            unsafe { &mut *(self.as_mut_ptr() as *mut _) },
            &mut self[PAGE_HEADER_LEN..],
        )
    }
}

impl From<&[u8]> for PageBuf {
    #[inline]
    fn from(buf: &[u8]) -> Self {
        let mut page = Self::new();
        let len = buf.len().min(PAGE_SIZE);
        page[..len].copy_from_slice(&buf[..len]);
        page
    }
}

impl Default for PageBuf {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for PageBuf {
    type Target = Page;

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { Page::from_ref_unchecked(&self.data) }
    }
}

impl DerefMut for PageBuf {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { Page::from_mut_unchecked(&mut self.data) }
    }
}

impl Borrow<Page> for PageBuf {
    #[inline]
    fn borrow(&self) -> &Page {
        self.deref()
    }
}

impl BorrowMut<Page> for PageBuf {
    #[inline]
    fn borrow_mut(&mut self) -> &mut Page {
        self.deref_mut()
    }
}

impl Deref for Page {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl DerefMut for Page {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl From<&Page> for [u8; PAGE_SIZE] {
    #[inline]
    fn from(page: &Page) -> Self {
        let mut arr = [0; PAGE_SIZE];
        arr.copy_from_slice(page);
        arr
    }
}

impl Borrow<Buf> for Page {
    #[inline]
    fn borrow(&self) -> &Buf {
        unsafe { &*(self.as_ptr() as *const Buf) }
    }
}

impl BorrowMut<Buf> for Page {
    #[inline]
    fn borrow_mut(&mut self) -> &mut Buf {
        unsafe { &mut *(self.as_mut_ptr() as *mut Buf) }
    }
}

impl AsRef<Buf> for Page {
    #[inline]
    fn as_ref(&self) -> &Buf {
        self.borrow()
    }
}

impl AsMut<Buf> for Page {
    #[inline]
    fn as_mut(&mut self) -> &mut Buf {
        self.borrow_mut()
    }
}

impl AsRef<Page> for Buf {
    #[inline]
    fn as_ref(&self) -> &Page {
        unsafe { mem::transmute(&self[..]) }
    }
}

impl AsMut<Page> for Buf {
    #[inline]
    fn as_mut(&mut self) -> &mut Page {
        unsafe { mem::transmute(&mut self[..]) }
    }
}

impl AsRef<Page> for PageBuf {
    #[inline]
    fn as_ref(&self) -> &Page {
        self.deref()
    }
}

impl AsMut<Page> for PageBuf {
    #[inline]
    fn as_mut(&mut self) -> &mut Page {
        self.deref_mut()
    }
}

#[repr(C)]
pub struct FixedPageHeader {
    pub prev_page: PageNum,
    pub next_page: PageNum,
    pub slot: [u8; 56], // 64 -4 -4
    _private: (),
}

impl FixedPageHeader {
    pub const fn max_slot() -> u32 {
        56 * 8
    }

    fn new() -> Self {
        Self {
            _private: (),
            next_page: 0,
            prev_page: 0,
            slot: [0; 56],
        }
    }

    pub fn clear(&mut self) {
        *self = Self::new();
    }

    pub fn clear_as_node(&mut self, pagenum: PageNum) {
        self.clear();
        self.prev_page = pagenum;
        self.next_page = pagenum;
    }

    #[inline]
    pub fn first_empty(&self, max_slot: usize) -> Option<u16> {
        iter_bits(&self.slot)
            .take(max_slot)
            .position(|b| !b)
            .map(|i| i as _)
    }

    #[inline]
    pub fn total(&self) -> u16 {
        iter_bits(&self.slot).map(|b| b as u16).sum()
    }

    #[inline]
    pub fn rest_empty(&self, max_slot: usize) -> u16 {
        max_slot as u16 - self.total()
    }

    #[inline]
    pub fn is_full(&self, max_slot: usize) -> bool {
        iter_bits(&self.slot).take(max_slot).all(identity)
    }

    #[inline]
    pub fn from_page(page: &Page) -> &Self {
        page.header()
    }

    #[inline]
    pub fn from_page_mut(page: &mut Page) -> &mut Self {
        page.header_mut()
    }
}

impl AsRef<FixedPageHeader> for Page {
    fn as_ref(&self) -> &FixedPageHeader {
        self.header()
    }
}

impl AsMut<FixedPageHeader> for Page {
    fn as_mut(&mut self) -> &mut FixedPageHeader {
        self.header_mut()
    }
}

impl Debug for FixedPageHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FixedPageHeader")
            .field("prev", &self.prev_page)
            .field("next", &self.next_page)
            .finish()
    }
}
