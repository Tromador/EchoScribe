use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter},
    path::{Path, PathBuf},
};

use flac_codec::{
    decode::{Verified, verify},
    encode::{FlacSampleWriter, Options},
};

use crate::diagnostics::{CHANNELS, DecodedFrame, SAMPLE_RATE, SAMPLES_PER_TICK, TrackSummary};

const BITS_PER_SAMPLE: u32 = 16;
const SILENCE_BLOCK_SAMPLES: usize = 4096;
const SILENCE_BLOCK: [i32; SILENCE_BLOCK_SAMPLES] = [0; SILENCE_BLOCK_SAMPLES];

pub(crate) struct FlacTrackWriter {
    directory: PathBuf,
    tracks: HashMap<u32, Track>,
}

struct Track {
    writer: FlacSampleWriter<BufWriter<File>>,
    path: PathBuf,
    first_tick: u64,
    next_tick: u64,
    frames: u64,
    source_samples: u64,
    inserted_silence_samples: u64,
    nonstandard_frames: u64,
    sample_buffer: Vec<i32>,
}

impl FlacTrackWriter {
    pub(crate) fn new(session_directory: &Path) -> io::Result<Self> {
        let directory = session_directory.join("tracks");
        fs::create_dir(&directory)?;

        Ok(Self {
            directory,
            tracks: HashMap::new(),
        })
    }

    pub(crate) fn write_frame(&mut self, frame: DecodedFrame) -> io::Result<()> {
        if !self.tracks.contains_key(&frame.ssrc) {
            let track = Track::create(&self.directory, frame.ssrc, frame.tick)?;
            self.tracks.insert(frame.ssrc, track);
        }

        self.tracks
            .get_mut(&frame.ssrc)
            .expect("track was inserted above")
            .write_frame(frame)
    }

    pub(crate) fn finalize(self) -> io::Result<Vec<TrackSummary>> {
        let mut summaries = Vec::with_capacity(self.tracks.len());

        for (ssrc, track) in self.tracks {
            summaries.push(track.finalize(ssrc)?);
        }

        summaries.sort_unstable_by_key(|summary| summary.ssrc);
        Ok(summaries)
    }
}

impl Track {
    fn create(directory: &Path, ssrc: u32, first_tick: u64) -> io::Result<Self> {
        let path = directory.join(format!("ssrc-{ssrc}.flac"));
        let writer = FlacSampleWriter::create(
            &path,
            Options::default(),
            SAMPLE_RATE,
            BITS_PER_SAMPLE,
            CHANNELS as u8,
            None,
        )
        .map_err(flac_error)?;

        Ok(Self {
            writer,
            path,
            first_tick,
            next_tick: 0,
            frames: 0,
            source_samples: 0,
            inserted_silence_samples: 0,
            nonstandard_frames: 0,
            sample_buffer: Vec::with_capacity(SAMPLES_PER_TICK as usize),
        })
    }

    fn write_frame(&mut self, frame: DecodedFrame) -> io::Result<()> {
        if frame.tick < self.next_tick {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "audio tick {} for SSRC {} arrived after tick {}",
                    frame.tick,
                    frame.ssrc,
                    self.next_tick - 1
                ),
            ));
        }

        let missing_ticks = frame.tick - self.next_tick;
        let silence_samples = missing_ticks
            .checked_mul(SAMPLES_PER_TICK)
            .ok_or_else(|| io::Error::other("FLAC silence length overflow"))?;
        self.write_silence(silence_samples)?;

        self.sample_buffer.clear();
        self.sample_buffer
            .extend(frame.samples.iter().map(|sample| i32::from(*sample)));
        self.writer.write(&self.sample_buffer).map_err(flac_error)?;

        self.next_tick = frame
            .tick
            .checked_add(1)
            .ok_or_else(|| io::Error::other("FLAC tick counter overflow"))?;
        self.frames += 1;
        self.source_samples += frame.samples.len() as u64;
        self.inserted_silence_samples += silence_samples;
        if frame.samples.len() as u64 != SAMPLES_PER_TICK {
            self.nonstandard_frames += 1;
        }

        Ok(())
    }

    fn write_silence(&mut self, mut samples: u64) -> io::Result<()> {
        while samples > 0 {
            let block_length = usize::try_from(samples.min(SILENCE_BLOCK_SAMPLES as u64))
                .expect("silence block length always fits usize");
            self.writer
                .write(&SILENCE_BLOCK[..block_length])
                .map_err(flac_error)?;
            samples -= block_length as u64;
        }

        Ok(())
    }

    fn finalize(self, ssrc: u32) -> io::Result<TrackSummary> {
        let path = self.path.clone();
        self.writer.finalize().map_err(flac_error)?;
        OpenOptions::new().write(true).open(&path)?.sync_data()?;
        match verify(&path).map_err(flac_error)? {
            Verified::MD5Match => {}
            Verified::MD5Mismatch => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("FLAC MD5 verification failed for {}", path.display()),
                ));
            }
            Verified::NoMD5 => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("FLAC has no PCM MD5 for {}", path.display()),
                ));
            }
        }

        Ok(TrackSummary {
            ssrc,
            path: self.path,
            first_tick: self.first_tick,
            frames: self.frames,
            source_samples: self.source_samples,
            inserted_silence_samples: self.inserted_silence_samples,
            nonstandard_frames: self.nonstandard_frames,
        })
    }
}

fn flac_error(error: flac_codec::Error) -> io::Error {
    io::Error::other(error)
}

#[cfg(test)]
mod tests {
    use std::{
        env, process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use flac_codec::decode::FlacSampleReader;

    use super::*;

    #[test]
    fn aligned_flac_round_trips_without_changing_pcm() {
        let session_directory = env::temp_dir().join(format!(
            "echoscribe-flac-track-test-{}-{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&session_directory).unwrap();
        let mut tracks = FlacTrackWriter::new(&session_directory).unwrap();

        tracks
            .write_frame(DecodedFrame {
                elapsed_nanos: 40_000_000,
                tick: 2,
                ssrc: 4326,
                samples: vec![100; SAMPLES_PER_TICK as usize],
            })
            .unwrap();

        let summaries = tracks.finalize().unwrap();
        assert_eq!(summaries[0].first_tick, 2);
        assert_eq!(summaries[0].inserted_silence_samples, 2 * SAMPLES_PER_TICK);

        let mut reader = FlacSampleReader::open(&summaries[0].path).unwrap();
        let mut samples = Vec::new();
        reader.read_to_end(&mut samples).unwrap();
        assert_eq!(samples.len(), 3 * SAMPLES_PER_TICK as usize);
        assert!(samples[..1920].iter().all(|sample| *sample == 0));
        assert!(samples[1920..].iter().all(|sample| *sample == 100));

        fs::remove_dir_all(session_directory).unwrap();
    }
}
