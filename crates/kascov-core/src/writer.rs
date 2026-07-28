use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use fs2::FileExt;

use crate::{Error, Result};

pub(crate) struct WriterLease {
    _file: File,
}

impl WriterLease {
    pub(crate) fn acquire(database: &Path) -> Result<Self> {
        let lock_path = lock_path(database)?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|error| lease_error(&lock_path, error.to_string()))?;
        file.try_lock_exclusive()
            .map_err(|error| lease_error(&lock_path, error.to_string()))?;
        Ok(Self { _file: file })
    }
}

fn lock_path(database: &Path) -> Result<PathBuf> {
    let file_name = database.file_name().ok_or_else(|| Error::Invalid {
        what: "writer lease path",
        value: database.display().to_string(),
    })?;
    let mut lock_name = file_name.to_os_string();
    lock_name.push(".writer.lock");
    Ok(database.with_file_name(lock_name))
}

fn lease_error(path: &Path, detail: String) -> Error {
    Error::Invalid {
        what: "writer lease",
        value: format!("{}: {detail}", path.display()),
    }
}
