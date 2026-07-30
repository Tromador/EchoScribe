//! Exclusive ownership for offline operations which mutate one session.
//!
//! The lock lives in the transcription directory because the Python worker
//! inherits its handle. It nevertheless protects every offline command which
//! can publish session authority or a session-declared derived artefact.

use std::{
    fs::{self, File, OpenOptions, TryLockError},
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};

use crate::artifacts::TRANSCRIPTION_DIRECTORY_NAME;

const OPERATION_LEASE_FILE_NAME: &str = "worker.lock";
const TRANSIENT_LOCK_RETRY_DELAY: Duration = Duration::from_millis(5);

#[derive(Debug)]
pub(crate) struct SessionOperationLease {
    file: File,
    path: PathBuf,
}

impl SessionOperationLease {
    pub(crate) fn acquire(session_directory: &Path) -> Result<Self> {
        let transcription_directory = session_directory.join(TRANSCRIPTION_DIRECTORY_NAME);
        // The directory and lock are coordination infrastructure, not
        // session-declared workflow artefacts. Creating them cannot publish or
        // alter workflow authority.
        fs::create_dir_all(&transcription_directory).with_context(|| {
            format!(
                "failed to prepare session operation lease directory {}",
                transcription_directory.display()
            )
        })?;
        let path = transcription_directory.join(OPERATION_LEASE_FILE_NAME);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .with_context(|| {
                format!("failed to open session operation lease {}", path.display())
            })?;
        for attempt in 0..=1 {
            match file.try_lock() {
                Ok(()) => return Ok(Self { file, path }),
                // A concurrently spawning child can briefly inherit an
                // unrelated close-on-exec descriptor. One bounded retry avoids
                // mistaking that fork-to-exec window for durable ownership;
                // the operating-system lock remains the sole authority.
                Err(TryLockError::WouldBlock) if attempt == 0 => {
                    thread::sleep(TRANSIENT_LOCK_RETRY_DELAY);
                }
                Err(TryLockError::WouldBlock) => bail!(
                    "another mutating operation is already active for session {}; lease {} is held",
                    session_directory.display(),
                    path.display()
                ),
                Err(TryLockError::Error(error)) => {
                    return Err(error).with_context(|| {
                        format!(
                            "failed to acquire session operation lease {}",
                            path.display()
                        )
                    });
                }
            }
        }
        unreachable!("the bounded session operation lease loop always returns")
    }

    /// Duplicate the locked handle so an orphaned Python child retains
    /// ownership even if its Rust parent terminates.
    pub(crate) fn inherited_handle(&self) -> Result<File> {
        self.file.try_clone().with_context(|| {
            format!(
                "failed to duplicate session operation lease {} for worker",
                self.path.display()
            )
        })
    }
}
