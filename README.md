# EchoScribe

EchoScribe was created to record Discord tabletop role-playing sessions and
produce reliable, speaker-attributed source transcripts for later after-action
reports (AARs). It retains durable journals, aligned per-participant FLAC tracks,
and chronological structured and human-readable transcripts.

Although designed around TTRPG sessions, the recording and transcription
pipeline can be used for any Discord voice conversation where recoverability,
speaker identity, and a shared timeline matter.

The operator-facing application is Rust. It owns recording, recovery, workflow
state and Python process orchestration. Faster-whisper runs as a subordinate
worker and never modifies `session.json`.

## Repository layout

```text
Cargo.toml, src/                  Rust application
workers/faster-whisper/          Python transcription worker and dependencies
docs/                            User and developer guides
archive/legacy-pipeline/         Historical pre-rewrite implementation
echoscribe.ps1, echoscribe.sh    Convenience launchers
```

## Documentation

- [User Guide](docs/USER_GUIDE.md) — installation, configuration, operation,
  command reference, recovery, and troubleshooting.
- [Developer Guide](docs/DEVELOPER_GUIDE.md) — architecture, source layout,
  authority, data flow, failure semantics, and extension guidance.

## Python environment

Create the shared repository-root virtual environment and install the worker
dependencies through the root convenience requirements file.

POSIX:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
```

Windows PowerShell:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

EchoScribe uses `ECHOSCRIBE_PYTHON` when explicitly set. Otherwise it prefers
the root `.venv` interpreter and then falls back to the platform Python command.

## Transcription vocabulary

Copy `vocabulary.example.txt` to `vocabulary.txt` and replace the examples with
names, places, jargon, specialist terminology, or other phrases relevant to
the conversation being recorded. Use one phrase per line. Blank lines and
full-line `#` comments are ignored; inline comments are not supported because
a `#` appearing later in a line belongs to the phrase.

The configured vocabulary path is relative to `echoscribe.toml`. The real
`vocabulary.txt` remains ignored by Git so each deployment or project can
maintain its own terms without changing the repository example.

## Running EchoScribe

The launchers rebuild the release executable when required, preserve the
caller's working directory, and forward all arguments to the application.

```powershell
.\echoscribe.ps1
.\echoscribe.ps1 record
.\echoscribe.ps1 inspect recordings\session-...
.\echoscribe.ps1 build-work-items recordings\session-... echoscribe.toml
.\echoscribe.ps1 transcribe recordings\session-... echoscribe.toml
.\echoscribe.ps1 continue recordings\session-...
.\echoscribe.ps1 continue recordings\session-... echoscribe.toml
.\echoscribe.ps1 rebuild-transcript recordings\session-...
```

```sh
./echoscribe.sh
./echoscribe.sh record
./echoscribe.sh inspect recordings/session-...
./echoscribe.sh build-work-items recordings/session-... echoscribe.toml
./echoscribe.sh transcribe recordings/session-... echoscribe.toml
./echoscribe.sh continue recordings/session-...
./echoscribe.sh continue recordings/session-... echoscribe.toml
./echoscribe.sh rebuild-transcript recordings/session-...
```

With no command, EchoScribe records, finalises, builds the work manifest,
transcribes and publishes the final transcript. The `record` command stops
after clean recording finalisation.

The unconfigured `continue` form only validates recording recovery. Supplying
the configuration resumes the one-stop pipeline at the stage established by
durable session authority. It never performs track recovery automatically.
`rebuild-transcript` reconstructs only the final human-readable transcript from
complete structured results. Successful transcription publishes
`transcription/transcript.txt` inside the session directory.

For development, invoke Cargo directly:

```sh
cargo run --
cargo run -- record
cargo run -- transcribe recordings/session-... echoscribe.toml
cargo run --release -- transcribe recordings/session-... echoscribe.toml
```

Build without running:

```sh
cargo build --release
```

Cargo writes the executable to `target/release/echoscribe` on Linux and
`target/release/echoscribe.exe` on Windows. Debug builds use the corresponding
path beneath `target/debug/`; generated executables are not copied into the
repository root.

## Legacy pipeline

The archived pipeline is the previous Discord.js/Python capture,
transcription, VAD-rescue, and deduplication stack. It is preserved for
historical reference and is not executed by the current application. See its
[archival note](archive/legacy-pipeline/ARCHIVE.md),
[original README](archive/legacy-pipeline/README.md), and
[detailed pipeline document](archive/legacy-pipeline/discord_transcript_pipeline.md).

---

EchoScribe was developed with substantial assistance from OpenAI Codex in
ChatGPT, under human direction and review. This is disclosed so users and
contributors can make their own informed choice about AI-assisted software.

[![AI-assisted with ChatGPT](https://img.shields.io/badge/AI_assisted-ChatGPT-74AA9C?logo=openai&logoColor=white)](https://openai.com/chatgpt/overview/)
[![Rust 2024](https://img.shields.io/badge/Rust-2024_edition-000000?logo=rust&logoColor=white)](https://doc.rust-lang.org/edition-guide/rust-2024/index.html)
[![Discord voice: Songbird](https://img.shields.io/badge/Discord_voice-Songbird-5865F2?logo=discord&logoColor=white)](https://docs.rs/songbird/latest/songbird/)
[![Transcription: faster-whisper](https://img.shields.io/badge/Transcription-faster--whisper-3776AB?logo=python&logoColor=white)](https://github.com/SYSTRAN/faster-whisper)
[![License: BSD 3-Clause](https://img.shields.io/badge/License-BSD_3--Clause-3A7D44)](https://spdx.org/licenses/BSD-3-Clause.html)
