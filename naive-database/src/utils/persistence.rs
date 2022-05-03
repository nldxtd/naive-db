use std::{fs, path::Path};

use serde::{Deserialize, Serialize};

use crate::{error::DBResult, filesystem::file_manager::fs_ensure_remove};

pub trait Persistence
where
    for<'de> Self: Serialize + Deserialize<'de>,
{
    fn filename(&self) -> String;

    #[inline]
    fn store(&self, dir: &Path) -> DBResult<()> {
        let file = dir.join(self.filename());
        let file = fs::OpenOptions::new().write(true).create(true).open(file)?;
        bincode::serialize_into(file, self)?;
        Ok(())
    }

    #[inline]
    fn load(file: &Path) -> DBResult<Self> {
        let file = fs::File::open(file)?;
        Ok(bincode::deserialize_from(file)?)
    }

    #[inline]
    fn delete_self(self, dir: &Path) -> DBResult<()> {
        fs_ensure_remove(&dir.join(self.filename()))?;
        Ok(())
    }
}
