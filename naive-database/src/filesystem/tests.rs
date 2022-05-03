use std::{io::Result, iter::from_fn};

use rand::prelude::*;
use tempfile::tempdir;

use crate::{
    config::{LRU_SIZE, PAGE_SIZE},
    defines::PageNum,
    page::PageBuf,
};

use super::{file_manager::*, page_manager::*};

#[test]
fn simple_cache_test() -> Result<()> {
    let file_num = 20;
    let max_pagenum = (LRU_SIZE / file_num) as PageNum;

    let tempdir = tempdir()?;
    let files: Vec<_> = (0..file_num)
        .map(|i| {
            tempdir
                .path()
                .join("testfile".to_owned() + i.to_string().as_ref())
        })
        .collect();
    let mut seq: Vec<_> = from_fn(|| {
        Some((
            files.choose(&mut thread_rng()).unwrap(),
            thread_rng().next_u64() as PageNum % max_pagenum,
        ))
    })
    .take(LRU_SIZE * 2)
    .collect();

    for filepath in &files {
        open_file(filepath)?;
    }
    for &(filepath, pagenum) in &seq {
        modify_page(filepath, pagenum, |page| {
            page.copy_from_slice(&PageBuf::from(
                pagenum.to_string().as_bytes().repeat(PAGE_SIZE).as_ref(),
            ))
        })?;
    }
    // flush_all()?;
    for &(filepath, pagenum) in &seq {
        read_page(filepath, pagenum, |page| {
            assert_eq!(
                page,
                PageBuf::from(pagenum.to_string().as_bytes().repeat(PAGE_SIZE).as_ref()).as_ref()
            )
        })?;
    }
    for filepath in &files {
        close_file(filepath)?;
    }

    seq.shuffle(&mut thread_rng());
    for filepath in &files {
        open_file(filepath)?;
    }
    for &(filepath, pagenum) in &seq {
        read_page(filepath, pagenum, |page| {
            assert_eq!(
                page,
                PageBuf::from(pagenum.to_string().as_bytes().repeat(PAGE_SIZE).as_ref()).as_ref()
            )
        })?;
    }
    flush_all()?;

    Ok(())
}

#[test]
fn test_simple_io() -> Result<()> {
    let tempdir = tempdir()?;
    let filepath = tempdir.path().join("testfile");

    let mut file = fs_create_file(&filepath)?;
    let buf = *b"gdastgewrtdagasdfgaedrhearhretqgtqertadfsfhgearfdgateqrhq45u 136 1 5124641214b342";
    fs_write_page_from(&mut file, 16, &buf)?;

    let page = fs_read_page(&mut file, 16)?;
    assert_eq!(&buf, &page[..buf.len()]);
    assert_eq!(page.as_ref(), PageBuf::from(buf.as_ref()).as_ref());
    assert_eq!(
        fs_read_page(&mut file, 10)?.as_ref(),
        PageBuf::default().as_ref()
    );

    drop(file);
    let mut file = fs_open_file(&filepath)?;
    let page = fs_read_page(&mut file, 16)?;
    assert_eq!(&buf, &page[..buf.len()]);
    assert_eq!(page.as_ref(), PageBuf::from(buf.as_ref()).as_ref());
    assert_eq!(
        fs_read_page(&mut file, 10)?.as_ref(),
        PageBuf::default().as_ref()
    );

    fs_remove_file(&filepath)?;
    assert!(!filepath.exists());

    Ok(())
}
