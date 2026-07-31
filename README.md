# EchoScribe

EchoScribe records Discord tabletop sessions into durable journals and aligned
per-participant FLAC tracks, then produces chronological structured and
human-readable transcripts.

The operator-facing application is Rust. It owns recording, recovery, workflow
state and Python process orchestration. Faster-whisper runs as a subordinate
worker and never modifies `session.json`.

## Repository layout

```text
Cargo.toml, src/                  Rust application
workers/faster-whisper/          Python transcription worker and dependencies
docs/                            Normative specification and architecture
archive/legacy-pipeline/         Historical pre-rewrite implementation
echoscribe.ps1, echoscribe.sh    Convenience launchers
```

The archived pipeline is retained for reference and is not executed by the
current application.

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
