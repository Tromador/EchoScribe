# Discord Audio Transcript Deduplication Pipeline

This document outlines the complete processing pipeline used to transform Discord voice session recordings into a clean, deduplicated transcript. It includes toolchain descriptions, code usage, and reasoning behind each processing step.
I have been using it to capture a TTRPG session, then using an LLM to create an "after action report", but it could be adapted for other purposes. If this helps you with something, that's just fantastic. It's all presented "as is" and 
will almost certainly require some work to configure for your circumstances. The pipeline uses faster-whisper on top of CTranslate2, and today you can install standard binary wheels from PyPI
(no custom local CTranslate2 build required). PyPI currently publishes modern CTranslate2 releases (for example 4.7.1), so this project can target standard package installation. The transcription script supports both CUDA/GPU and CPU modes via CLI flags, so the same project can run on high-end RTX cards or a CPU-only machine. All other modules were 
"off the shelf", but some did require some configuration and persuasion. Have fun!

---

## 🎙 Phase 0: Discord Audio Capture

**Script Used**: `index.ts`

Captures individual `.wav` files from a live Discord voice channel using Discord.js and `@discordjs/voice`.

### Features:
- Captures audio **per user**, identified by Discord `user_id`
- Uses `start_speaking` and `stop_speaking` events for stream control
- Converts Opus streams to WAV:
  - Decodes with `prism-media` into PCM
  - Encodes into WAV with `wav.Writer` at 48kHz mono, 16-bit
- Writes each audio stream to a timestamped `.wav` file in a daily subdirectory
- Records session metadata into a `session_log.jsonl` per day:
  - `user` (name mapping if available)
  - `user_id`
  - `start` and `end` timestamps (ISO format)
  - `filename` (relative to day directory)
- **Handles multiple streams per user** (supports overlapping or simultaneous speaking events)
- **Failsafe for unresolved pipelines** (timeouts to prevent orphaned streams)
- **Graceful shutdown on SIGINT**:
  - Cleans up voice connection and Discord client
  - Closes all active listeners
  - Forces process exit after a delay if needed
- Configured via `.env`:
  - `DISCORD_TOKEN`
  - `GUILD_ID`
  - `CHANNEL_ID`
- Static mapping for friendly user names based on known Discord `user_id`s
- Logs key events (start/stop speaking, recording success/failure)

### Example Output:

```plaintext
audio/2025-05-04/
├── user_340336807222837270_16-22-12.wav
├── user_..._.wav
└── session_log.jsonl

This log is the first input into the pipeline via `dedupe_audit.py`.
```

---

## ✅ Phase 1: Audio Validation and Filtering

**Script Used**: `dedupe_audit.py`  
**Helper Module**: `burst_scope.py`

Purpose: Clean up raw `.wav` files exported from Discord (one per user per utterance) and discard files with no usable content. Performs detailed validation, including duplicate detection, silence filtering, VAD-based speech detection, and burst-based rescue of very short utterances.

### Key Logic:
- **SHA-256 Fingerprinting**: Detects exact duplicate audio files via hash comparison, ensuring files aren't redundantly processed.
- **Filename Duplicate Check**: Flags files that appear more than once based on name.
- **Duration Extraction**: Reads duration directly from the WAV header using `soundfile`.
- **RMS Energy Filtering**: Files with total RMS energy below a threshold (0.003) are discarded as effectively silent.
- **Silero VAD Detection**: Uses the Silero VAD model to detect speech within the audio file:
  - Files with no detected speech are rejected.
  - Files shorter than `--vad-min-duration` (default 2.0s) can be rescued.
- **Burst Scope Rescue** (`burst_scope.py`):
  - Short, sharp energy bursts (e.g., "Yes", "Okay") bypass VAD rejection if they meet burst criteria.
  - Files passing the `is_bursty_candidate()` test are tagged with an override flag `"override": "burst_scope"`.
- **Legacy Text Similarity Filter** (currently commented out):
  - Originally filtered out generic filler transcriptions (e.g., "uh", "yes"), but is now inactive.
- **Mode Handling**:
  - `--mode` controls whether the script actually writes the audit log (`default="dry-run"`).
  - If not in dry-run mode, writes results to `dedupe_audit.jsonl`.
- **Detailed Audit Summary**:
  - Reports total files, accepted files, SHA-256 and filename duplicates, RMS rejects, VAD rejections (short/long), and burst rescues.
  - Prints the final audit log output path.

### Example Command:
    python3 dedupe_audit.py \
      --log-file audio/2025-05-04/session_log.jsonl \
      --audio-dir audio/2025-05-04 \
      --model-dir models/silero-vad \
      --output dedupe_audit_2025-05-04.v2.jsonl \
      --vad-min-duration 2.0

- `--mode` can be omitted (defaults to dry-run) or set to `full` to write the output.
- `--vad-min-duration` controls the short utterance threshold (default 2.0s).

---

### 🧙 Helper Module: `burst_scope.py`

Used by `dedupe_audit.py` to rescue very short utterances that were rejected by VAD but show bursty characteristics. Provides an RMS-based burstiness analysis for audio files.

#### Features:
- Reads `.wav` files and computes:
  - **Total RMS**: Overall energy level of the file.
  - **Frame-based RMS**: Divides audio into small frames (default 20ms) and computes RMS for each.
  - **Standard Deviation of RMS**: Measures how much the energy fluctuates between frames.
- **Burstiness Criteria**:
  - File passes if `std_rms > 0.03` (default) and `total RMS > 0.003`.
  - Indicates sharp, dynamic bursts (e.g., “Yes”, “Okay”) rather than continuous hum.
- **Import-safe**: `is_bursty_candidate(filepath)` can be called from other scripts.
- **Standalone CLI Usage**:
      python3 burst_scope.py path/to/audio.wav

  - Prints duration, total RMS, frame RMS standard deviation, and a summary.
  - Optionally displays an RMS plot using `matplotlib` (if available).

#### Example Diagnostic Output:
    📄 File: user_340336807222837270_16-22-12.wav
    ⏱ Duration: 1.23 seconds
    🔋 Total RMS: 0.005432
    📈 Frame RMS std: 0.045321
       Max Frame RMS: 0.091200
       Min Frame RMS: 0.000000
    💥 Bursty Signal? YES
    ✅ Candidate for Whisper rescue

Used in `dedupe_audit.py` to override short VAD-rejected utterances when `is_bursty_candidate()` returns `True`.

---

## 🧠 Phase 2: Whisper Transcription

**Script Used**: `transcribe_accepted.py`

Reads the filtered metadata from `dedupe_audit.py` and performs Whisper transcription on the retained audio files.

### Features:
- Loads entries from JSONL where `"status": "accepted"`
- Transcribes each `.wav` file using `faster-whisper` (CTranslate2 backend)
  - Beam search with beam size 5
  - Joins segment texts with spaces
- Adds a `"text"` field to each entry with the transcription result
- Skips missing audio files with warnings
- Outputs an updated `.jsonl` with the transcribed text included
- Supports `--device` (`auto`/`cpu`/`cuda`) and configurable `--compute-type`
- Allows `--cpu-threads` tuning for CPU runs
- Keeps rejected/non-accepted rows in output JSONL and only enriches accepted rows

### Example Command:
    python3 transcribe_accepted.py \
      --input-log dedupe_audit_2025-05-04.v2.jsonl \
      --audio-dir ../discord_audio_bot/audio/2025-05-04 \
      --model-dir models/whisper-large-v3-f16 \
      --output-jsonl session_log.transcribed.jsonl \
      --device auto \
      --compute-type auto

### Notes:
- `--input-log` points to the filtered log output by `dedupe_audit.py`
- `--audio-dir` contains the audio files (`.wav`) referenced in the log
- `--model-dir` specifies the directory containing the faster-whisper model
- `--output-jsonl` is where the transcribed log with text will be saved
- Use `--device cpu` to force CPU inference when no compatible CUDA GPU/runtime is available
- Use `--compute-type` (for example `int8` on CPU or `float16` on CUDA) to match your hardware

### Example JSONL Entry (after transcription):
    {
      "user": "Stef",
      "user_id": "881203221593464864",
      "start": "2025-05-04T16:22:12.345Z",
      "end": "2025-05-04T16:22:15.678Z",
      "filename": "user_881203221593464864_16-22-12.wav",
      "status": "accepted",
      "text": "Here is the transcribed text of the audio."
    }

---

## 🧹 Phase 3: Deduplication by Clustering

**Script Used**: `dedupe_transcript.py` 

### Clustering Features:
- **Token-based matching** with Jaccard and Cosine similarity
- **Fragment rule** collapses short utterances that are strict subsets of longer ones
- **Canonical form comparison**:
  - Lowercase normalisation
  - Punctuation stripping
  - Specific contraction expansion (e.g., "we're" → "we are")
- **Cluster best pick** selection via scoring:
  - Weighted length, punctuation count, initial capitalisation bonus
  - Penalties for filler words (e.g., "uh", "um")
- **Per-speaker clustering** to prevent cross-user confusion
- **Final pass** deduplication:
  - Merges entries with high Jaccard (≥0.85) or Cosine similarity (≥0.92) even if not clustered initially
- **Optional containment filtering** (enabled with `--filter-contained`):
  - Removes entries fully contained in longer utterances by the same speaker, based on start/end times and durations
- **Debug output** (enabled with `--debug-clusters`):
  - Detailed per-speaker cluster breakdowns with best pick entries and cluster sizes
- **Deduplication summary**:
  - Total users processed
  - Total and deduplicated utterance counts
  - Per-user cluster reduction ratios

```
python3 dedupe_transcript.py \
  --input-jsonl session_log.transcribed.jsonl \
  --output-text transcript_2025-05-04.deduped.txt \
  --debug-clusters cluster_debug.txt \
  --filter-contained
```

Optional:
- Debug output: cluster breakdowns to review merge quality

---

## 📝 Output

A final `.txt` file with a per-line speaker label:

```plaintext
Trish: First mate. First mate.
Stef: subject to an appropriate skill roll.
Rich: I wonder...
```

Preserves:
- Character
- Flow
- Cleaned dialogue
- Session integrity

---

## 🗂 Script Inventory

| Script                  | Purpose                                                   |
|--------------------------|-----------------------------------------------------------|
| `index.ts`               | Captures Discord voice as per-user .wav files             |
| `dedupe_audit.py`        | Filters raw audio: silence, noise, duplicates, duration   |
| `burst_scope.py`         | Rescues short sharp utterances from false VAD rejection   |
| `transcribe_accepted.py` | Transcribes accepted .wav files into enriched JSONL       |
| `dedupe_transcript.py`  | Deduplicates transcribed JSONL using clustering           |
