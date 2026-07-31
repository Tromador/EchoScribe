//! Classification for failures at an offline workflow stage boundary.
//!
//! Validation refusals leave workflow authority untouched. Once a coordinator
//! has accepted a stage and begun publication, failures are eligible for the
//! durable stop-and-wait handling used by the one-stop pipeline.

use std::{error::Error, fmt};

use anyhow::Error as AnyhowError;

#[derive(Debug)]
pub(crate) struct StageError {
    accepted: bool,
    source: AnyhowError,
}

impl StageError {
    pub(crate) fn refused(source: AnyhowError) -> Self {
        Self {
            accepted: false,
            source,
        }
    }

    pub(crate) fn accepted(source: AnyhowError) -> Self {
        Self {
            accepted: true,
            source,
        }
    }

    pub(crate) fn was_accepted(&self) -> bool {
        self.accepted
    }

    pub(crate) fn into_anyhow(self) -> AnyhowError {
        self.source
    }
}

impl fmt::Display for StageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(formatter)
    }
}

impl Error for StageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.source()
    }
}

pub(crate) type StageResult<T> = Result<T, StageError>;
