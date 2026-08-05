//! Explicit migration of transcription policy in a completed session snapshot.
//!
//! Routine processing never refreshes immutable participant context from the
//! operator's mutable source file. This command is the narrow, leased route for
//! changing only the transcription-admission Boolean of a historical session.

use std::{fs, path::Path};

use anyhow::{Context, Result, bail};

use crate::{
    operation_lease::SessionOperationLease,
    participants::ParticipantContext,
    session::{SessionStore, WorkflowState, replace_participant_snapshot},
};

pub(crate) fn run(session_directory: &Path, discord_user_id: u64, transcribe: bool) -> Result<()> {
    let session_directory = fs::canonicalize(session_directory).with_context(|| {
        format!(
            "failed to resolve session directory {}",
            session_directory.display()
        )
    })?;
    let _lease = SessionOperationLease::acquire(&session_directory)?;
    let session = SessionStore::load(&session_directory).with_context(|| {
        format!(
            "failed to load workflow state from {}",
            session_directory.display()
        )
    })?;
    if session.record().state != WorkflowState::Complete {
        bail!(
            "set-transcription-policy requires session state complete; found {}",
            session.record().state.as_str()
        );
    }

    let snapshot_path = session_directory.join(&session.record().files.participants.path);
    let mut participants = ParticipantContext::load(&snapshot_path).with_context(|| {
        format!(
            "failed to validate participant snapshot {}",
            snapshot_path.display()
        )
    })?;
    participants.set_transcribe(discord_user_id, transcribe);
    replace_participant_snapshot(&session_directory, &participants).with_context(|| {
        format!(
            "failed to publish participant transcription policy in {}",
            snapshot_path.display()
        )
    })?;

    println!(
        "Session {} participant {} transcription policy set to {}.",
        session.record().session_id,
        discord_user_id,
        transcribe
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        env, process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::{
        participants::ParticipantContext,
        session::{NewSession, SessionStore},
    };

    #[test]
    fn completed_session_policy_migration_preserves_context() {
        let directory = test_directory("policy");
        let source = directory.join("source.toml");
        fs::write(
            &source,
            concat!(
                "version = 1\n",
                "[participants.\"11\"]\n",
                "character = \"A Character\"\n",
                "role = \"gm\"\n",
            ),
        )
        .unwrap();
        let participants = ParticipantContext::load(&source).unwrap();
        let mut session = SessionStore::create(
            &directory,
            NewSession {
                session_id: "session-policy",
                started_at_unix_millis: 1,
                configuration_version: 1,
                guild_id: "123",
                channel_id: "456",
                participants: &participants,
            },
        )
        .unwrap();
        session
            .transition(crate::session::WorkflowState::RecordedClean, 2)
            .unwrap();
        session
            .transition(crate::session::WorkflowState::ReadyForTranscription, 3)
            .unwrap();
        // This focused fixture bypasses audio stages but retains a structurally
        // valid complete workflow authority for the policy boundary.
        let mut record = session.record().clone();
        record.format = crate::session::SESSION_FORMAT_VERSION;
        record.state = WorkflowState::Complete;
        record.files.work_items = Some(crate::session::FileDescription {
            path: crate::artifacts::WORK_ITEM_MANIFEST_PATH.to_owned(),
            format: crate::artifacts::LEGACY_WORK_ITEM_MANIFEST_FORMAT_VERSION,
        });
        record.files.results = Some(crate::session::FileDescription {
            path: crate::artifacts::TRANSCRIPTION_RESULTS_PATH.to_owned(),
            format: crate::artifacts::LEGACY_TRANSCRIPTION_RESULT_FORMAT_VERSION,
        });
        record.checkpoints.push(crate::session::CheckpointRecord {
            completed_at_unix_millis: 3,
            stage: "work_manifest_built".to_owned(),
        });
        fs::write(
            directory.join("session.json"),
            serde_json::to_vec_pretty(&record).unwrap(),
        )
        .unwrap();

        run(&directory, 11, false).unwrap();
        run(&directory, 22, false).unwrap();

        let snapshot = ParticipantContext::load(&directory.join("participants.toml")).unwrap();
        let existing = snapshot.get(11).unwrap();
        assert_eq!(existing.character.as_deref(), Some("A Character"));
        assert_eq!(existing.role, "gm");
        assert!(!existing.transcribe);
        let added = snapshot.get(22).unwrap();
        assert_eq!(added.role, "player");
        assert_eq!(added.character, None);
        assert!(!added.transcribe);

        fs::remove_dir_all(directory).unwrap();
    }

    fn test_directory(label: &str) -> std::path::PathBuf {
        let directory = env::temp_dir().join(format!(
            "echoscribe-participant-policy-{label}-{}-{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&directory).unwrap();
        directory
    }
}
