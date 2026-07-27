use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter},
    path::{Path, PathBuf},
};

use hound::{SampleFormat, WavSpec, WavWriter};

pub(crate) const SAMPLE_RATE: u32 = 48_000;
pub(crate) const CHANNELS: u16 = 1;
pub(crate) const SAMPLES_PER_TICK: u64 = 960;

pub(crate) struct DecodedFrame {
    pub(crate) tick: u64,
    pub(crate) ssrc: u32,
    pub(crate) samples: Vec<i16>,
}

pub(crate) struct DiagnosticWriter {
    directory: PathBuf,
    tracks: HashMap<u32, Track>,
}

pub(crate) struct TrackSummary {
    pub(crate) ssrc: u32,
    pub(crate) path: PathBuf,
    pub(crate) first_tick: u64,
    pub(crate) frames: u64,
    pub(crate) source_samples: u64,
    pub(crate) inserted_silence_samples: u64,
    pub(crate) nonstandard_frames: u64,
}

struct Track {
    writer: WavWriter<BufWriter<File>>,
    path: PathBuf,
    first_tick: u64,
    next_tick: u64,
    frames: u64,
    source_samples: u64,
    inserted_silence_samples: u64,
    nonstandard_frames: u64,
}

impl DiagnosticWriter {
    pub(crate) fn new(session_directory: &Path) -> io::Result<Self> {
        let directory = session_directory.join("diagnostics");
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

    pub(crate) fn checkpoint(&mut self) -> io::Result<()> {
        for track in self.tracks.values_mut() {
            track.checkpoint()?;
        }

        Ok(())
    }

    pub(crate) fn sync_data(&self) -> io::Result<()> {
        for track in self.tracks.values() {
            track.sync_data()?;
        }

        Ok(())
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
        let path = directory.join(format!("ssrc-{ssrc}.wav"));
        let writer = WavWriter::create(
            &path,
            WavSpec {
                channels: CHANNELS,
                sample_rate: SAMPLE_RATE,
                bits_per_sample: 16,
                sample_format: SampleFormat::Int,
            },
        )
        .map_err(hound_error)?;

        Ok(Self {
            writer,
            path,
            first_tick,
            next_tick: first_tick,
            frames: 0,
            source_samples: 0,
            inserted_silence_samples: 0,
            nonstandard_frames: 0,
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
            .ok_or_else(|| io::Error::other("diagnostic silence length overflow"))?;

        for _ in 0..silence_samples {
            self.writer.write_sample(0_i16).map_err(hound_error)?;
        }

        for sample in &frame.samples {
            self.writer.write_sample(*sample).map_err(hound_error)?;
        }

        self.next_tick = frame
            .tick
            .checked_add(1)
            .ok_or_else(|| io::Error::other("diagnostic tick counter overflow"))?;
        self.frames += 1;
        self.source_samples += frame.samples.len() as u64;
        self.inserted_silence_samples += silence_samples;
        if frame.samples.len() as u64 != SAMPLES_PER_TICK {
            self.nonstandard_frames += 1;
        }

        Ok(())
    }

    fn checkpoint(&mut self) -> io::Result<()> {
        self.writer.flush().map_err(hound_error)
    }

    fn sync_data(&self) -> io::Result<()> {
        sync_path(&self.path)
    }

    fn finalize(self, ssrc: u32) -> io::Result<TrackSummary> {
        let path = self.path.clone();
        self.writer.finalize().map_err(hound_error)?;
        sync_path(&path)?;

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

fn hound_error(error: hound::Error) -> io::Error {
    io::Error::other(error)
}

fn sync_path(path: &Path) -> io::Result<()> {
    OpenOptions::new().write(true).open(path)?.sync_data()
}

#[cfg(test)]
mod tests {
    use std::{
        env, process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use hound::WavReader;

    use super::*;

    #[test]
    fn mono_wav_preserves_frames_and_inserts_tick_gaps() {
        let session_directory = env::temp_dir().join(format!(
            "echoscribe-diagnostic-test-{}-{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&session_directory).unwrap();
        let mut diagnostics = DiagnosticWriter::new(&session_directory).unwrap();

        diagnostics
            .write_frame(DecodedFrame {
                tick: 10,
                ssrc: 4326,
                samples: vec![100; SAMPLES_PER_TICK as usize],
            })
            .unwrap();
        diagnostics
            .write_frame(DecodedFrame {
                tick: 12,
                ssrc: 4326,
                samples: vec![200; SAMPLES_PER_TICK as usize],
            })
            .unwrap();

        let summaries = diagnostics.finalize().unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].first_tick, 10);
        assert_eq!(summaries[0].frames, 2);
        assert_eq!(summaries[0].source_samples, 2 * SAMPLES_PER_TICK);
        assert_eq!(summaries[0].inserted_silence_samples, SAMPLES_PER_TICK);
        assert_eq!(summaries[0].nonstandard_frames, 0);

        let mut reader = WavReader::open(&summaries[0].path).unwrap();
        let spec = reader.spec();
        assert_eq!(spec.channels, CHANNELS);
        assert_eq!(spec.sample_rate, SAMPLE_RATE);
        assert_eq!(spec.bits_per_sample, 16);
        assert_eq!(spec.sample_format, SampleFormat::Int);

        let samples = reader
            .samples::<i16>()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(samples.len(), 3 * SAMPLES_PER_TICK as usize);
        assert!(samples[..960].iter().all(|sample| *sample == 100));
        assert!(samples[960..1920].iter().all(|sample| *sample == 0));
        assert!(samples[1920..].iter().all(|sample| *sample == 200));

        fs::remove_dir_all(session_directory).unwrap();
    }

    #[test]
    fn checkpoint_makes_in_progress_wav_readable() {
        let session_directory = env::temp_dir().join(format!(
            "echoscribe-checkpoint-test-{}-{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&session_directory).unwrap();
        let mut diagnostics = DiagnosticWriter::new(&session_directory).unwrap();
        let wav_path = session_directory.join("diagnostics/ssrc-4326.wav");

        diagnostics
            .write_frame(DecodedFrame {
                tick: 10,
                ssrc: 4326,
                samples: vec![100; SAMPLES_PER_TICK as usize],
            })
            .unwrap();
        diagnostics.checkpoint().unwrap();
        diagnostics.sync_data().unwrap();

        {
            let mut reader = WavReader::open(&wav_path).unwrap();
            let samples = reader
                .samples::<i16>()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(samples, vec![100; SAMPLES_PER_TICK as usize]);
        }

        diagnostics
            .write_frame(DecodedFrame {
                tick: 11,
                ssrc: 4326,
                samples: vec![200; SAMPLES_PER_TICK as usize],
            })
            .unwrap();
        diagnostics.finalize().unwrap();

        let mut reader = WavReader::open(wav_path).unwrap();
        let samples = reader
            .samples::<i16>()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(samples.len(), 2 * SAMPLES_PER_TICK as usize);
        assert!(samples[..960].iter().all(|sample| *sample == 100));
        assert!(samples[960..].iter().all(|sample| *sample == 200));

        fs::remove_dir_all(session_directory).unwrap();
    }
}
