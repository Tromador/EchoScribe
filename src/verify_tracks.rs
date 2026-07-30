//! Explicit full-decode verification for routine live FLAC products.
//!
//! Normal shutdown deliberately avoids this whole-file pass. The operator can
//! request it independently without regenerating audio or changing session
//! workflow state.

use std::path::Path;

use anyhow::{Context, Result, bail};
use flac_codec::{
    decode::{FlacSampleReader, Verified, verify},
    metadata::Metadata,
};

use crate::{
    session::SessionStore,
    track_manifest::{TrackManifest, TrackState},
};

const DECODE_BUFFER_SAMPLES: usize = 16 * 1024;

pub(crate) fn run(session_directory: &Path) -> Result<()> {
    let session = SessionStore::load(session_directory).with_context(|| {
        format!(
            "failed to load session workflow record from {}",
            session_directory.display()
        )
    })?;
    let manifest_path = session_directory.join(&session.record().files.tracks.path);
    let manifest = TrackManifest::read(&manifest_path)
        .with_context(|| format!("failed to read track manifest {}", manifest_path.display()))?;
    if manifest.session_id != session.record().session_id {
        bail!(
            "track manifest session ID {:?} does not match session record {:?}",
            manifest.session_id,
            session.record().session_id
        );
    }

    let verified_tracks = verify_complete_manifest(session_directory, &manifest)?;

    println!(
        "Verified {verified_tracks} routine FLAC track(s) for session {}.",
        manifest.session_id
    );
    Ok(())
}

/// Fully decode and validate every routine track described as complete.
///
/// Explicit `verify` and operator-controlled `continue` share this check so a
/// continuation cannot apply a weaker meaning of “healthy”.
pub(crate) fn verify_complete_manifest(
    session_directory: &Path,
    manifest: &TrackManifest,
) -> Result<u64> {
    let mut incomplete_users = Vec::new();
    let mut verified_tracks = 0_u64;
    for track in &manifest.tracks {
        if track.state == TrackState::Incomplete {
            incomplete_users.push(track.discord_user_id.clone());
            continue;
        }

        let path = session_directory.join(&track.path);
        let integrity = verify(&path)
            .map_err(anyhow::Error::new)
            .with_context(|| format!("failed to decode-verify {}", path.display()))?;
        match integrity {
            Verified::MD5Match => {}
            Verified::MD5Mismatch => {
                bail!("FLAC PCM MD5 verification failed for {}", path.display())
            }
            Verified::NoMD5 => bail!("FLAC has no PCM MD5 for {}", path.display()),
        }

        let mut reader = FlacSampleReader::open(&path)
            .map_err(anyhow::Error::new)
            .with_context(|| format!("failed to open {}", path.display()))?;
        if reader.sample_rate() != track.sample_rate
            || u16::from(reader.channel_count()) != track.channels
            || u16::try_from(reader.bits_per_sample()).ok() != Some(track.bits_per_sample)
        {
            bail!(
                "FLAC stream format does not match tracks.json for {}",
                path.display()
            );
        }

        let mut decoded_samples = 0_u64;
        let mut buffer = [0_i32; DECODE_BUFFER_SAMPLES];
        loop {
            let read = reader
                .read(&mut buffer)
                .map_err(anyhow::Error::new)
                .with_context(|| format!("failed while decoding {}", path.display()))?;
            if read == 0 {
                break;
            }
            decoded_samples = decoded_samples
                .checked_add(read as u64)
                .ok_or_else(|| anyhow::anyhow!("decoded sample count overflow"))?;
        }
        if decoded_samples != track.length_samples {
            bail!(
                "decoded {} samples from {}, but tracks.json records {}",
                decoded_samples,
                path.display(),
                track.length_samples
            );
        }

        println!(
            "Verified routine FLAC for Discord user {}: {} samples, PCM MD5 matched.",
            track.discord_user_id, decoded_samples
        );
        verified_tracks += 1;
    }

    if !incomplete_users.is_empty() {
        bail!(
            "cannot verify an incomplete recording: tracks for Discord users {} require recovery",
            incomplete_users.join(", ")
        );
    }

    Ok(verified_tracks)
}
