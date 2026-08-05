# EchoScribe User Guide

EchoScribe records conversations from a Discord voice channel and produces
per-participant audio, a structured transcript, and a readable plain-text
transcript. It works for tabletop role-playing, meetings, lectures, symposia,
interviews, and other spoken sessions.

This guide describes the current application as an operator uses it. It is
standalone documentation: it does not depend on the project specification,
architecture record, or implementation history.

## 1. What EchoScribe does

For normal use, one command performs the complete workflow:

```text
join the configured Discord voice channel
    -> record until Ctrl-C
    -> finalise one aligned FLAC track per Discord user
    -> build chronological transcription work items
    -> run faster-whisper locally
    -> publish structured and plain-text transcripts
```

EchoScribe also retains packet, playout, identity, and workflow evidence. If a
derived FLAC track is damaged, it can normally be regenerated without asking
Discord to provide the session again.

Known failures stop at a safe boundary. Recovery and continuation are explicit
operator actions; EchoScribe does not respond to a fault by automatically
applying more load or silently skipping data.

## 2. Current scope

The current application provides:

- Discord voice capture through Serenity and Songbird;
- durable recording journals;
- live, session-aligned, per-user FLAC tracks;
- deterministic track recovery from the journals;
- chronological faster-whisper transcription;
- resumable transcription with durable progress;
- complete, atomic retranscription of a finished session;
- per-participant transcription exclusion without suppressing recording;
- offline replay of selected transcription ranges for diagnosis;
- JSONL structured results and a readable text transcript.

It does not currently provide live transcription, relevance filtering,
automated report or minutes generation, live facilitation assistance, a
graphical interface, or cloud transcription.

## 3. Requirements

### 3.1 Discord

You need a Discord bot which can see and connect to the chosen voice channel.
The configuration requires:

- the bot token;
- the guild (server) ID;
- the voice-channel ID.

EchoScribe requests guild and guild-voice-state gateway intents. It does not
request message content.

Discord IDs are decimal numbers, but they are written as quoted strings in the
TOML configuration. Discord's Developer Mode exposes the Copy ID commands used
to obtain them.

### 3.2 Rust

The repository is a Rust 2024-edition Cargo application. Install a current
stable Rust toolchain, including Cargo. Build products are generated beneath
`target/`; no executable is copied into the repository root.

### 3.3 Python and faster-whisper

Transcription uses a subordinate Python worker. Create one virtual environment
at the repository root and install the pinned worker dependencies.

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

The installed runtime includes faster-whisper, CTranslate2, SoundFile, and the
Hugging Face Hub client. The Hub client supplies the `hf` command; it is an
indirect faster-whisper dependency rather than a separate EchoScribe
requirement. Confirm that it is available after activating the virtual
environment with:

```sh
hf version
```

SoundFile extracts each bounded FLAC range supplied to Whisper; it is not an
alternative recorder. The same environment runs the optional diagnostic replay
helper described in section 10.15.

With `device = "cuda"`, CTranslate2 also needs a compatible local NVIDIA/CUDA
runtime. CPU operation can instead use settings such as `device = "cpu"` and a
suitable CPU compute type. EchoScribe passes model, device, compute type, and
beam settings to faster-whisper rather than trying to repair an incompatible
runtime.

The public model is downloaded into the normal Hugging Face cache when
faster-whisper first needs it, unless it is already cached. A Hugging Face login
is not normally required for the public model used by the example. To fetch the
example's `large-v3` model before the first transcription, use:

```sh
hf download Systran/faster-whisper-large-v3
```

This populates the same cache used by faster-whisper. See Appendix A for model
selection, download-size checks, and alternative model names.

### 3.4 Choosing the Python interpreter

EchoScribe selects the worker interpreter in this order:

1. `ECHOSCRIBE_PYTHON`, when set to a non-empty value;
2. `.venv/Scripts/python.exe` on Windows or `.venv/bin/python` on POSIX;
3. `python` on Windows or `python3` on POSIX.

The root virtual environment and worker script are resolved from the
application repository, independently of the shell's current directory. An
explicitly empty `ECHOSCRIBE_PYTHON` is an error.

## 4. Build and launch

Build the release executable with:

```sh
cargo build --release
```

The result is:

```text
target/release/echoscribe       # POSIX
target/release/echoscribe.exe   # Windows
```

The root launchers are the normal convenient entry points:

```powershell
.\echoscribe.ps1
```

```sh
./echoscribe.sh
```

Both launchers preserve the caller's working directory, forward every argument
unchanged, preserve the application exit status, and invoke the release profile
through Cargo. Cargo rebuilds when the source is newer than the executable.

During development, the equivalent direct command is:

```sh
cargo run --release -- <arguments>
```

Examples in the rest of this guide use the logical executable name
`echoscribe`. Substitute the PowerShell launcher, shell launcher, or generated
binary as appropriate.

## 5. Configuration

Copy the examples before the first run.

POSIX:

```sh
cp echoscribe.example.toml echoscribe.toml
cp participants.example.toml participants.toml
cp vocabulary.example.txt vocabulary.txt
```

Windows PowerShell:

```powershell
Copy-Item echoscribe.example.toml echoscribe.toml
Copy-Item participants.example.toml participants.toml
Copy-Item vocabulary.example.txt vocabulary.txt
```

`echoscribe.toml`, `participants.toml`, and `vocabulary.txt` are ignored by
Git. The examples remain tracked.

### 5.1 Main configuration

The complete version-1 schema is:

```toml
version = 1

[discord]
token = "replace-with-discord-bot-token"
guild_id = "replace-with-discord-guild-id"
channel_id = "replace-with-discord-voice-channel-id"

[recording]
output_directory = "recordings"
diagnostic_wav = false

[participants]
file = "participants.toml"

[transcription]
model = "large-v3"
language = "en"
device = "cuda"
compute_type = "float16"
beam_size = 5
vocabulary_file = "vocabulary.txt"
resume_rewind_seconds = 120
lexical_no_speech_threshold = 0.60

[segmentation]
vad_enabled = false
merge_gap_ms = 750
```

Unknown or misspelt fields are rejected instead of being silently ignored.

| Field | Meaning |
|---|---|
| `version` | Configuration format. The current value is `1`. |
| `discord.token` | Bot token used only by live recording. Keep it private. |
| `discord.guild_id` | Guild containing the voice channel. |
| `discord.channel_id` | Voice channel EchoScribe joins. |
| `recording.output_directory` | Parent directory for new session directories. |
| `recording.diagnostic_wav` | Opt-in SSRC-keyed live WAV diagnostics. Defaults to `false` if omitted. |
| `participants.file` | Operator-maintained participant context TOML. |
| `transcription.model` | faster-whisper model name or model path understood by faster-whisper. |
| `transcription.language` | Language passed to faster-whisper. |
| `transcription.device` | CTranslate2 device, normally `cuda` or `cpu`. |
| `transcription.compute_type` | CTranslate2 compute type, such as `float16` for the configured GPU. |
| `transcription.beam_size` | Whisper beam size; must be greater than zero. |
| `transcription.vocabulary_file` | Session-specific hotword phrase file. |
| `transcription.resume_rewind_seconds` | Amount of committed transcription reconsidered after a known worker failure. `0` disables rewind. |
| `transcription.lexical_no_speech_threshold` | Threshold for the unprompted lexical-speech qualification pass. Defaults to `0.60`. |
| `segmentation.vad_enabled` | When true, admit complete work-item ranges through bundled Silero VAD before lexical qualification. When false, send every work item directly to lexical qualification. |
| `segmentation.merge_gap_ms` | Playout gaps no larger than this are merged for the same user. Must be greater than zero. |

`output_directory`, the participant file, and the vocabulary file are resolved
relative to the directory containing the named main configuration—not relative
to the shell's working directory.

The current `merge_gap_ms = 750` is provisional tuning rather than an
architectural promise.

`lexical_no_speech_threshold` controls whether an unprompted Whisper segment
is accepted as lexical speech before configured hotwords are used. Higher
values are more permissive; lower values are more restrictive. The `0.60`
default is tuned for EchoScribe's source-transcript use case: it retains
useful short responses such as “okay” while deliberately allowing low-value
acknowledgements, abandoned utterances, and speech-like non-verbal sounds to
be rejected.

Offline work-item and transcription commands still require a structurally
complete main TOML file. They load only the settings needed for their stage and
do not connect to Discord. They also use the immutable participant snapshot
inside the session rather than rereading the mutable participant source.

### 5.2 Participant context

The participant file adds descriptive context keyed by Discord user ID:

```toml
version = 2
transcript_name_source = "discord"

[participants."881203221593464864"]
name = "Stefan"
role = "speaker"

[participants."123456789012345678"]
role = "chair"
transcribe = false
```

EchoScribe obtains the server display name, global display name, and username
from Discord. This observed identity is retained as provenance. The participant
file supplies:

- optional `name`, which may be a real name, character name, professional name,
  or any other useful alternative to the Discord handle;
- optional `role`, accepting any non-empty single-line text and defaulting to
  `participant`;
- optional `transcribe`, defaulting to `true`.

The top-level `transcript_name_source` selects line attribution. `"discord"`
is the default and uses the observed Discord name. `"name"` uses the configured
`name`, falling back to the Discord name when a participant has no configured
name. The choice does not alter Discord identity evidence.

Empty names or roles, multiline values, and invalid or zero Discord IDs are
rejected.

A participant with `transcribe = false` remains fully recorded in journals,
identity evidence and the routine FLAC track, but is omitted from transcription
work-item generation. This is an operator policy, not a role. A participant who
is absent from the file is recorded and transcribed with default `participant`
context and Discord-name attribution.

At session creation, EchoScribe writes a canonical `participants.toml` snapshot
inside the session directory. Later edits to the configured participant file do
not rewrite old sessions. Canonical snapshots always write the resolved Boolean;
older snapshots without it load as `transcribe = true`. Historical version-1
snapshots retain their original `character`, `player`/`gm`, and Discord-name
semantics; they do not need migration for reading, rebuilding, or
retranscription.

### 5.3 Transcription vocabulary

The vocabulary file gives faster-whisper names, places, jargon, scientific
terms, and other specialist phrases which ordinary language modelling may
miss.

- Use UTF-8 text.
- Put one complete word or phrase on each line.
- Leading and trailing whitespace is removed.
- Blank lines are ignored.
- A line whose first non-whitespace character is `#` is a comment.
- Inline comments are not supported; a later `#` is part of the phrase.

A missing, empty, or comment-only file produces a warning and transcription
continues without hotwords. Other read errors and invalid UTF-8 stop the stage.

See `vocabulary.example.txt` for a commented example.

## 6. Normal operation

### 6.1 One-stop recording and transcription

From the directory containing `echoscribe.toml`:

```sh
echoscribe
```

Or name a different configuration explicitly:

```sh
echoscribe path/to/session.toml
```

EchoScribe creates a session directory, connects to Discord, and joins the
configured voice channel. Press Ctrl-C once when the session is over.

Normal shutdown then:

1. leaves the voice channel;
2. closes and drains accepted capture records;
3. finalises healthy routine FLAC encoders;
4. publishes `tracks.json` and recording state;
5. builds or reuses the work manifest;
6. runs one Python transcription worker;
7. validates every committed result;
8. publishes the final transcript and marks the session complete.

If any required recording or transcription integrity check fails, the command
returns non-zero and stops at the durable boundary described in `session.json`.
Do not assume that a `.flac` or transcript-looking file alone proves completion.

### 6.2 Recording only

To record and finalise without immediately transcribing:

```sh
echoscribe record
echoscribe record path/to/session.toml
```

A healthy recording-only session ends in `ready_for_transcription`. Advance it
later with:

```sh
echoscribe continue recordings/session-... echoscribe.toml
```

### 6.3 Choosing a configuration from another directory

The launchers preserve the current directory. Therefore, a no-argument launch
looks for `echoscribe.toml` in the directory from which it was invoked. Pass an
explicit configuration path when running elsewhere.

## 7. Session contents

A complete current session resembles:

```text
recordings/session-.../
├── session.json
├── participants.toml
├── packets.dat
├── playout.dat
├── events.ndjson
├── tracks.json
├── tracks/
│   ├── user-<discord-id>.flac
│   └── ...
└── transcription/
    ├── worker.lock
    ├── work-items.jsonl
    ├── results.jsonl
    └── transcript.txt
```

After supported retranscription, `session.json` instead references one complete
generation under `transcription/retranscriptions/<generation>/`. The manifest,
results and readable transcript in that directory become authoritative together;
the previous complete files are not overwritten during staging.

Optional or failure-related output can include:

```text
tracks/user-<discord-id>.flac.part
diagnostics/ssrc-<ssrc>.wav
recovered/ssrc-<ssrc>.wav
transcript.partial.txt
```

The important roles are:

| Artefact | Role |
|---|---|
| `session.json` | Current workflow authority, artefact references, failures, and checkpoints. |
| `participants.toml` | Immutable session-local participant context. |
| `packets.dat` | Decrypted packet journal used for recording recovery. |
| `playout.dat` | Authoritative playout and loss decisions. |
| `events.ndjson` | SSRC mapping, identity, disconnect, and routing-failure evidence. |
| `tracks.json` | Current per-user routine-track manifest. |
| `tracks/user-<id>.flac` | Complete session-aligned routine audio for one Discord user. |
| `*.flac.part` | Incomplete track. It must not be treated as healthy merely because it decodes. |
| `work-items.jsonl` | Deterministic chronological transcription plan. |
| `results.jsonl` | Authoritative committed structured transcript. |
| `transcript.partial.txt` | Rebuildable incomplete human-readable view. |
| `transcript.txt` | Final rebuildable human-readable transcript. |
| `worker.lock` | Incidental operating-system coordination file, not evidence that a worker is currently alive. |

Every routine FLAC is mono, 48 kHz, and sourced from 16-bit PCM. Silence is
inserted so tracks share one session timeline.

Do not delete the journals merely because the FLACs or transcript exist if you
want to retain recording-recovery capability.

## 8. Transcript output

The current format-2 readable transcript begins with a participant roster,
followed by one completed work item per line:

```text
Participants:
- Discord: Tromador | Name: Stefan | Role: attendee

Transcript:
[00:09:26] Tromador: What were you saying? I completely missed it.
```

The roster lists transcribed participants in order of first appearance and
shows the observed Discord name, the configured name (or the Discord name as
fallback), and the role. Participants excluded from transcription have no work
items and therefore do not appear in the roster.

Historical format-1 transcripts remain valid as timestamped lines without a
roster. `rebuild-transcript` preserves the grammar declared by session
authority, so rebuilding format 1 remains line-only. A successful replacement
retranscription publishes format 2 with the roster. When that replacement uses
historical format-1 work items, the old `character` value appears in the
roster's `Name` column without changing Discord-based line attribution.

Lines are ordered by session-relative start time. Overlapping speakers remain
separate lines. The text view omits SSRCs, confidence scores, word-level timing,
and relevance judgements.

The `results.jsonl` path declared by `session.json` is the durable transcript
authority. Each result
retains work-item identity, sequence, Discord user, speaker metadata, timing,
source range, text, and completion status. If text and JSONL disagree after a
crash, EchoScribe rebuilds the text from the validated JSONL prefix.

The master transcript includes formal contributions, side discussions, jokes,
social chatter, and anything else captured. Relevance filtering belongs to a
later downstream process.

### 8.1 Known model-generated transcription artefacts

Faster Whisper is a probabilistic speech-recognition model. Marginal,
ambiguous, noisy, or very short audio can occasionally produce plausible but
incorrect text.

EchoScribe supplies the configured vocabulary to Faster Whisper as
hotword context. This materially improves names, places, jargon, and specialist
terminology. On an uncertain range, however, the same context can sometimes
turn a short uncertain transcription into a fluent stock phrase learned by the
model, such as:

- `Thank you for watching.`
- `Thank you for your support.`
- generic video-outro or customer-support language.

EchoScribe does not insert these phrases. Their presence alone does not mean
that the recording is damaged, session authority is broken, or transcription
files are corrupt. Nor should the audio be assumed to contain only silence:
the initial decode may itself have produced a plausible but incorrect short
hypothesis from uncertain audio.

EchoScribe deliberately does not blacklist stock phrases. A phrase may
occasionally be genuine speech, and removing visible text would not establish
what the audio actually contained. Disabling vocabulary assistance would also
materially reduce accuracy for proper names and specialist terminology.

Participant recordings are retained so the source audio can be reviewed by a
person, considered by later language-model processing while producing minutes
or another report, or retranscribed in future with an improved Faster Whisper
model or a different transcription backend. This is a known limitation of the
current transcription backend, not a session-recovery procedure or a
data-integrity fault.

## 9. Workflow states

`session.json` records one of these states:

| State | Meaning |
|---|---|
| `recording` | Live capture was started and has not published final recording disposition. |
| `recorded_clean` | Track finalisation completed; the later ready-state publication is still pending. |
| `recorded_incomplete` | Recording stopped with incomplete routine output. |
| `awaiting_operator` | A known fault or explicit recovery boundary requires operator action. |
| `ready_for_transcription` | Recording artefacts are healthy enough for work-item generation/transcription. |
| `transcribing` | Result authority is published; a worker may be active or eligible for controlled restart. |
| `transcription_failed` | Worker failure evidence is durable, but the second transition to operator state did not complete. |
| `complete` | Every work item has a matching result and the final transcript was published. |

Use the state and failure records together. Historical failure entries remain
as evidence after successful recovery; their continued presence does not by
itself mean that the fault remains unresolved.

## 10. Command reference

### 10.1 `echoscribe [config]`

Runs the normal one-stop workflow. With no argument, the configuration is
`echoscribe.toml` in the current directory.

### 10.2 `echoscribe record [config]`

Runs live recording and finalisation only. It never builds work items or starts
Python.

### 10.3 `echoscribe inspect <session>`

Reads session metadata and the packet, playout, and event journals without
modifying them. It reports journal tails, packet continuity, playout losses,
identity mappings, disconnects, and unresolved SSRC abandonments.

Inspection recognises supported legacy format-2 sessions as well as current
workflow formats. A clean inspection means the checks it performed passed; it
does not independently certify every derived FLAC or transcription artefact.

### 10.4 `echoscribe verify <session>`

Fully decodes every routine track marked complete in `tracks.json`, checks the
FLAC PCM MD5, validates its audio format, and compares decoded length with the
manifest. It is read-only and can be expensive for long sessions.

Routine clean shutdown deliberately does not pay for this whole-file pass.

### 10.5 `echoscribe recover <session> [user-id ...]`

Regenerates routine per-user FLAC from the authoritative journals.

- With no user IDs, it selects every track currently marked incomplete.
- With IDs, it rebuilds exactly those users, including a currently healthy
  track when explicitly requested.

Recovery requires `awaiting_operator`. It writes through the normal
`.flac.part` to `.flac` publication lifecycle, updates `tracks.json`, records
durable recovery evidence, and remains in `awaiting_operator`.

It never continues the pipeline automatically.

### 10.6 `echoscribe continue <session>`

Validates recording recovery only. It requires a format-3 or format-4
`awaiting_operator` session without transcription results.

The command replays authoritative activity, requires every decoded frame to be
safely attributable, verifies complete routine tracks, and refuses unresolved
recording-integrity faults. On success it advances to
`ready_for_transcription`; it does not transcribe because no configuration was
provided.

### 10.7 `echoscribe continue <session> <config>`

Continues the one-stop pipeline from the stage established by current durable
authority:

- recovered recording: validate recovery, then proceed;
- `ready_for_transcription`: reuse or build work items, then transcribe;
- `transcribing`: controlled restart from the contiguous committed prefix,
  without rewind;
- known transcription failure: apply the configured durable rewind and resume;
- accepted manifest or pre-results orchestration failure: retry that boundary
  without unnecessarily repeating recording recovery validation.

This command never performs track recovery. Run `recover` first when routine
tracks are incomplete.

### 10.8 `echoscribe build-work-items <session> <config>`

Builds `transcription/work-items.jsonl` without running Whisper. It requires
`ready_for_transcription`, complete routine tracks, healthy required session
artefacts, and safe attribution of all decoded activity.

The command reads `segmentation.merge_gap_ms`, uses the immutable session-local
participant snapshot, sorts items globally and deterministically, and
atomically replaces any previous manifest. Workflow state remains
`ready_for_transcription`.

### 10.9 `echoscribe transcribe <session> <config>`

Runs the explicit transcription stage.

- First invocation requires `ready_for_transcription` and a published work
  manifest.
- Explicit controlled restart accepts `transcribing`, validates the contiguous
  result prefix, rebuilds partial text, and resumes at the next item without
  rewind.

After a known worker failure which has moved to `awaiting_operator` or
`transcription_failed`, use configured `continue` so the durable rewind and
failure protocol is applied.

When `segmentation.vad_enabled = true`, the worker checks each complete
extracted range with the Silero implementation bundled by the pinned
faster-whisper package. A detected speech range is admitted in full to lexical
qualification: VAD does not trim its beginning or end. A Silero-negative range
is not sent to Whisper. It still receives a normal complete `results.jsonl`
record with empty text so restart ordering remains intact, but no blank
`Speaker:` line is written to the human transcript.

A Silero-positive range next receives the unprompted lexical qualification
described by `lexical_no_speech_threshold`. Configured hotwords are used only
after that qualification accepts the range. The worker prints aggregate VAD
accepted/rejected and lexical decision summaries on successful completion.
VAD loading or inference failure stops the worker rather than silently
disabling the gate.

### 10.10 `echoscribe retranscribe <session> <config>`

Runs a complete replacement transcription of a healthy `complete` session.
It validates the existing complete transcript and recording evidence, rebuilds
work items with the current `merge_gap_ms` and immutable session participant
snapshot, and starts the normal production worker at sequence 1 with the current
transcription settings.

The new manifest, results and readable transcript are staged in a separate
generation. One atomic `session.json` replacement publishes all three paths
together only after every result and the final transcript validate. A worker,
validation or publication failure leaves the old complete set authoritative and
the workflow state remains `complete`; retranscription is not continuation or
failure recovery.

To exclude someone from an existing completed session, apply the explicit
historical-policy migration and then retranscribe:

```sh
echoscribe set-transcription-policy \
  recordings/session-... \
  123456789012345678 false
echoscribe retranscribe \
  recordings/session-... \
  echoscribe.toml
```

Do not edit the session-local `participants.toml` by hand. The policy command
acquires the session lease and changes only that participant's materialised
`transcribe` value. Editing the operator's source participant file affects
future sessions only.

### 10.11 `echoscribe set-transcription-policy <session> <user-id> <true|false>`

Explicitly migrates only a completed session snapshot's transcription policy.
It does not rebuild work items or start transcription; run `retranscribe`
afterwards. If the user has no snapshot entry, EchoScribe adds one with normal
format-appropriate defaults and the requested policy. This exceptional command is the
supported route for historical snapshots, which otherwise remain immutable.

### 10.12 `echoscribe rebuild-transcript <session>`

Reconstructs the session-declared readable transcript atomically for a complete
format-5 or format-6 session. It validates the complete work and result
authorities, does not start Python, does not change structured results, and does
not change workflow state.

Because structured transcript authority is sufficient for rendering, old
journals, participant snapshots, track manifests, and FLACs are not required by
this command.

### 10.13 `echoscribe recover-wav <session>`

Replays packet and playout journals into SSRC-keyed diagnostic WAV files under
`recovered/`. This is diagnostic reconstruction, not routine user-track
recovery. The output directory must not already exist.

### 10.14 `echoscribe export <session>`

Runs the older diagnostic/migration path which replays journals into
SSRC-keyed, fully decode-verified FLAC files and writes its export manifest.
It is not the routine recovery command and is not suitable for writing over a
current session's existing `tracks/` directory. Use `recover` for current
Discord-user-keyed routine tracks.

### 10.15 Python-only diagnostic replay

`workers/faster-whisper/diagnose_ranges.py` replays selected existing work
items without changing workflow state, transcript authority, or published
session artefacts. It is a diagnostic helper rather than an `echoscribe`
subcommand.

Use the same Python environment as the production worker. Repeat `--sequence`
to inspect several items in one model load:

```sh
./.venv/bin/python workers/faster-whisper/diagnose_ranges.py \
  --config echoscribe.toml \
  --session recordings/session-... \
  --sequence 17 \
  --sequence 42 \
  --output diagnostics/session-ranges.jsonl
```

PowerShell:

```powershell
.\.venv\Scripts\python.exe workers\faster-whisper\diagnose_ranges.py `
  --config echoscribe.toml `
  --session recordings\session-... `
  --sequence 17 `
  --sequence 42 `
  --output diagnostics\session-ranges.jsonl
```

For each range, the helper prints acoustic measurements, padded and unpadded
Silero results, and five decode comparisons in this output order:

1. `current_echoscribe` uses the current hotword-assisted decode settings on
   the original complete range.
2. `no_hotword_control` uses the same original range without hotwords.
3. `craig_like_internal_vad` enables selected Craig-like internal-VAD
   behaviour and word timestamps while retaining hotwords.
4. `internal_vad_no_hotwords` uses the original complete range with internal
   VAD and word timestamps enabled, but hotwords disabled.
5. `explicit_silero_trimmed` explicitly decodes the concatenated unpadded
   Silero speech spans and remains distinct from Faster Whisper's internal VAD.

The Craig-like comparison tests selected behaviour from
[CraigChat/runpod-worker-faster_whisper](https://github.com/CraigChat/runpod-worker-faster_whisper)
only; it is not an exact reproduction of Craig's private production
configuration. Likewise, the result labelled `current_echoscribe` is the
current prompted decode stage, not a replay of the complete Silero and lexical
admission pipeline. These comparisons gather evidence; none is presented as a
recommended production replacement.

`--output` is optional. When supplied, it is overwritten with one JSON object
per requested work item. The repository-root `diagnostics/` directory is
ignored by Git and is a suitable destination; create it first if it does not
exist. Diagnostic JSONL is evidence for investigation, not session or
transcript authority. The helper loads the model once, performs one untimed
warm-up, and cleans its temporary bounded WAV files.

## 11. Recovery playbooks

### 11.1 Incomplete recording track

1. Read the command error and shutdown telemetry.
2. Inspect the session:

   ```sh
   echoscribe inspect recordings/session-...
   ```

3. Correct the external cause first, such as disk capacity or resource
   pressure.
4. Recover all incomplete tracks or selected users:

   ```sh
   echoscribe recover recordings/session-...
   echoscribe recover recordings/session-... 881203221593464864
   ```

5. Optionally perform full FLAC verification:

   ```sh
   echoscribe verify recordings/session-...
   ```

6. Validate and resume the pipeline:

   ```sh
   echoscribe continue recordings/session-... echoscribe.toml
   ```

Authoritative journal loss or corruption cannot be repaired by regenerating a
FLAC. Continuation will continue to refuse while that fault remains.

### 11.2 Transcription worker failure

Committed results are preserved. After correcting the cause—such as the Python
environment, model cache, CUDA runtime, or available storage—run:

```sh
echoscribe continue recordings/session-... echoscribe.toml
```

EchoScribe validates the result prefix and applies
`resume_rewind_seconds`. The rewind removes one contiguous suffix beginning at
the earliest committed result which overlaps the rewind window. Crash-safe
prepared/applied checkpoints prevent repeated attempts from progressively
discarding older results when no new progress was made.

There is no automatic retry.

### 11.3 Interrupted transcription without a published failure

If durable state remains `transcribing`, configured `continue` performs the
stage-aware controlled restart. The explicit equivalent is:

```sh
echoscribe transcribe recordings/session-... echoscribe.toml
```

This route retains the valid contiguous prefix and resumes at the next item
without failure rewind.

### 11.4 Damaged or missing final text only

If the session is `complete` and `work-items.jsonl` plus `results.jsonl` remain
intact:

```sh
echoscribe rebuild-transcript recordings/session-...
```

No audio is decoded or retranscribed.

## 12. Concurrency and `worker.lock`

Only one mutating offline operation may own a session at a time. Recovery,
continuation, work-item generation, transcription, retranscription, participant
policy migration, and transcript rebuilding share an operating-system file lock
at `transcription/worker.lock`.

If EchoScribe reports that another mutating operation owns the session:

- check whether a Rust or Python EchoScribe process is still running;
- wait for it to finish or investigate it;
- do not delete `worker.lock` as a remedy.

The file can remain after a clean exit. Ownership is the live operating-system
lock, not the file's existence. A Python worker inherits the locked handle, so
it continues to exclude a second writer even if its Rust parent dies. The lock
becomes available when the final owning process terminates.

Read-only `inspect` and `verify` do not take exclusive ownership.

## 13. Operational telemetry

At shutdown, EchoScribe reports:

- capture queue accepted/consumed records and drops;
- packet, event, playout, audio, and routing-tick counts;
- capture queue high-water mark;
- journal checkpoint and storage-sync counts;
- identity-routing resolutions and abandonments;
- live FLAC queue depth/high-water/failures;
- per-user FLAC frame, sample, silence, and SSRC information;
- RTP sequence gaps, duplicates, and late/out-of-order packets.

Zero drops and a low high-water mark are healthy evidence. Any authoritative
queue drop, decoded-audio ingress drop, unresolved identity abandonment, FLAC
queue rejection, or writer failure prevents a clean recording disposition.

Diagnostic WAV is optional and failure-isolated. Enabling it may help codec or
alignment investigation, but it is not required for normal recording or
recovery.

## 14. Troubleshooting

### Configuration parse failure

EchoScribe rejects missing and unknown fields. Compare the live file with
`echoscribe.example.toml`; do not assume a successful `cargo check` validates
operator configuration.

### Participant parse failure

Check the top-level `version`, `transcript_name_source`, quoted Discord-ID table
keys, and non-empty single-line names and roles. Version 2 accepts arbitrary
roles; version-1 historical files retain their `player`/`gm` validation.

### Missing vocabulary warning

This is non-fatal. Copy `vocabulary.example.txt`, create an empty
`vocabulary.txt`, or deliberately continue without hotwords.

### Python worker is missing or cannot launch

Confirm the repository still contains
`workers/faster-whisper/transcription_worker.py`, then check the interpreter
selection described in section 3.4. An explicitly empty
`ECHOSCRIBE_PYTHON` is rejected.

### `faster-whisper` or `soundfile` is not installed

Activate the root `.venv` and install `requirements.txt`. Confirm EchoScribe is
selecting that interpreter rather than a different system Python.

### CUDA or model load failure

The worker exits non-zero and EchoScribe retains committed progress. Correct
the model cache, driver, CUDA libraries, device, or compute-type setting, then
use configured `continue`. Do not edit `results.jsonl` to make the error vanish.

### Session is awaiting operator action

Read `session.json` failures and checkpoints, inspect journal health, and check
`tracks.json`. Use recording recovery only for derived track faults. Use
configured continuation for transcription or post-recording stage failures.

### A `.flac.part` file can be played

Playability is not completeness. The `.part` suffix and `tracks.json` state are
the relevant disposition; recover the track before continuation.

### Transcript and JSONL disagree

JSONL is authoritative. Controlled restart reconstructs partial text. For a
complete session, use `rebuild-transcript`.

### Expected speech is absent from the transcript

An empty structured result may have been rejected by Silero or by the
unprompted lexical qualification pass. Identify the corresponding sequence in
the session-declared work manifest and use the diagnostic replay helper in
section 10.15. Do not edit the result or transcript files to force acceptance.

## 15. Data handling and backups

The main configuration contains a Discord bot token and should remain private.
Participant snapshots and event journals contain Discord user IDs and display
identity evidence. Transcripts contain everything captured, including social
conversation which may be unrelated to the game.

Diagnostic replay JSONL can contain the same participant identity, source
paths, decoded text, confidence evidence, and timing information. Handle it as
session data even when it is written outside the session directory.

For a recoverable backup, preserve the entire session directory. In particular,
retain `session.json`, all three journals, the participant snapshot, and event
evidence together. Copying only the final transcript preserves the readable
product but not recording recovery or structured provenance.

Session artefact paths recorded in `session.json` are relative to the session
directory. EchoScribe rejects absolute paths and paths which escape it.

## 16. Legacy pipeline

The pre-rewrite Discord.js/Python capture, filtering, VAD-rescue,
transcription, and deduplication stack is preserved under
`archive/legacy-pipeline/`. It is historical reference material and is not
invoked by the current Rust application.

See the legacy directory's own README and detailed pipeline document for its
operation. Do not install or invoke legacy requirements as part of the current
workflow.

## Appendix A. Faster Whisper models

### Choosing a model

The example configuration uses `large-v3`. It is a sound starting point when
the available GPU and acceptable processing time can accommodate it. Smaller
models require fewer resources and generally run faster, but transcription
accuracy may fall. The practical result depends on the language, recording,
hardware, and CTranslate2 compute type, so test alternatives against
representative EchoScribe recordings before adopting one.

Common faster-whisper model names include:

| `transcription.model` | Intended use |
|---|---|
| `large-v3` | Full multilingual model and the EchoScribe example setting. |
| `medium`, `small`, `base`, `tiny` | Progressively smaller multilingual models. |
| `medium.en`, `small.en`, `base.en`, `tiny.en` | English-only variants; the benefit is greatest for the smallest models. |
| `turbo` | Faster large-v3-derived model with a possible accuracy trade-off. |
| `distil-large-v3` | Faster distilled English model; evaluate it as a different model rather than assuming identical output. |

The [Faster Whisper project](https://github.com/SYSTRAN/faster-whisper#usage)
documents the model names it supports and its own performance measurements.
The [OpenAI Whisper model table](https://github.com/openai/whisper#available-models-and-languages)
gives the underlying model families, approximate scale, language coverage, and
relative speed. Its memory figures describe OpenAI Whisper rather than
CTranslate2, so use them as model-selection guidance rather than an exact
EchoScribe hardware requirement.

`transcription.model` may also name a Hugging Face repository containing a
CTranslate2-converted Whisper model, or a local converted-model directory.
An arbitrary original Transformers checkpoint is not necessarily directly
loadable by faster-whisper; see its
[model-conversion guidance](https://github.com/SYSTRAN/faster-whisper#model-conversion).

### Downloading the example model

Activate EchoScribe's virtual environment first. Installing the pinned Python
requirements installs `huggingface-hub`, which provides `hf`:

```sh
hf version
```

Faster-whisper downloads a recognised model name automatically on first use.
For the example configuration, it maps `large-v3` to the public
`Systran/faster-whisper-large-v3` repository. Pre-download it with:

```sh
hf download Systran/faster-whisper-large-v3
```

The command downloads the repository into the normal Hugging Face cache and
prints the cached snapshot path. Repeating it reuses files already present.
Authentication is not normally required for this public repository.

To inspect the transfer without downloading anything, including the amount not
already cached, use:

```sh
hf download Systran/faster-whisper-large-v3 --dry-run
```

To inspect or verify the cache later, use:

```sh
hf cache ls
hf cache verify Systran/faster-whisper-large-v3
```

The [Hugging Face CLI guide](https://huggingface.co/docs/huggingface_hub/en/guides/cli)
documents download, authentication, cache-location, and cache-management
options. Avoid copying cached snapshot files by hand: the cache is
version-aware and should be managed through faster-whisper or `hf`.
