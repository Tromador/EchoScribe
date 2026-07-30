# EchoScribe Architecture

## Status and authority

**Status: Normative architecture**

This document defines the approved technical shape of EchoScribe.

Where the current implementation differs, the difference is implementation work to reconcile. Existing code does not override this architecture.

## 1. System overview

```text
Discord voice
    |
    v
Serenity + Songbird
    |
    +--> decrypted RTP packets -----------------------+
    |                                                 |
    +--> playout decisions ---------------------------+--> authoritative capture queue
    |                                                 |         |
    +--> speaker mapping events ----------------------+         +--> packets.dat
    |                                                           +--> playout.dat
    +--> decoded per-SSRC PCM ---------------------------------> +--> events.ndjson
                                                                  |
                                                                  +--> PCM fan-out
                                                                         |
                                                                         +--> bounded live FLAC queue
                                                                         |       |
                                                                         |       +--> per-user .flac.part writers
                                                                         |
                                                                         +--> optional diagnostic WAV
                                                                         |
                                                                         +--> future utterance assembler
```

The authoritative capture consumer remains fast and bounded.

Derived audio work is downstream of the authoritative consumer and may not block it.

## 2. Approved technology baseline

- Root Cargo application package and generated binary named `echoscribe`.
- Rust application runtime.
- Serenity for Discord gateway integration.
- Songbird for Discord voice receive and decoded PCM.
- Tokio bounded channels for asynchronous stage boundaries.
- Existing versioned packet and playout journal formats unless an approved slice requires a versioned extension.
- `flac-codec` as the approved Rust FLAC encoder baseline.
- Python for faster-whisper integration.
- faster-whisper with CTranslate2 and local CUDA.
- TOML for human-edited configuration.
- JSON/JSONL for durable machine-readable session and transcription records.

A change to these choices is a material course change.

The repository root is the Cargo package root. Songbird is the Discord voice
dependency and recording subsystem; it is not the package or application name.
Cargo-generated binaries remain under `target/debug/` and `target/release/`.
The root `echoscribe.ps1` and `echoscribe.sh` launchers are thin convenience
entry points around `cargo run --release` and introduce no workflow authority.

## 3. Authoritative and derived artefacts

| Artefact | Created | Authority | Required normal output | Regenerable |
|---|---|---:|---:|---:|
| `session.json` | live and updated by stages | workflow authority | yes | no |
| `participants.toml` | session creation | immutable session participant context | yes | no |
| `packets.dat` | live | recording authority | yes | no |
| `playout.dat` | live | recording authority | yes | no |
| `events.ndjson` | live | identity/event evidence | yes | no |
| `tracks/user-<id>.flac.part` | live | derived, incomplete | temporary | yes |
| `tracks/user-<id>.flac` | clean finalisation | derived routine product | yes | yes |
| diagnostic WAV | live when enabled | derived diagnostic | no | yes |
| `tracks.json` | live/finalisation | derived track manifest | yes | yes from evidence plus session context |
| `transcription/work-items.jsonl` | post-recording | derived processing plan | yes | yes |
| `transcription/results.jsonl` | transcription | structured transcript authority | yes | by retranscription |
| `transcript.partial.txt` | transcription | human view, incomplete | temporary | yes |
| `transcription/transcript.txt` | successful completion | human product | yes | yes |

The journals and session state are not deleted merely because routine tracks and transcripts exist.

## 4. Capture boundary

Songbird callbacks may:

- extract the required record;
- copy or move bounded data;
- attempt a non-blocking enqueue;
- update lightweight atomic telemetry.

Callbacks may not:

- write files;
- encode FLAC;
- wait for queue capacity;
- invoke Python;
- perform unbounded allocation or buffering;
- retry indefinitely.

Failure to enqueue authoritative packet, playout, or identity records is a recording-integrity event and must be reported distinctly from derived PCM/FLAC failure.

## 5. Authoritative capture consumer

The authoritative consumer owns:

- packet journal writing;
- playout journal writing;
- session event writing;
- checkpoint and durability synchronisation;
- SSRC-to-user mapping state required for downstream routing;
- forwarding decoded PCM into derived consumers.

Its work takes priority over derived audio output.

The consumer forwards decoded frames to the FLAC stage without waiting for FLAC encoding.

## 6. Identity model

### 6.1 Stable routine key

The routine track key is the Discord user ID.

An SSRC is a transport identifier and may change during a session.

The capture layer maintains timestamped SSRC-to-user mapping evidence. Decoded frames from several SSRCs belonging to the same Discord user feed the same session-aligned routine track.

Track filenames use the stable user ID rather than a display name:

```text
tracks/user-881203221593464864.flac.part
tracks/user-881203221593464864.flac
```

Display names remain metadata and transcript presentation.

### 6.2 Unknown mapping

Decoded PCM must never be guessed onto the wrong participant.

A small bounded pending-mapping mechanism may retain early frames until the SSRC mapping arrives.

If the mapping cannot be resolved within the bounded policy, the affected live track is marked incomplete and abandoned; the journals preserve recovery evidence.

The exact buffer duration is an implementation and tuning detail, but it must be bounded and observable.

### 6.3 Display name

The transcript speaker name uses:

1. Discord server display name;
2. Discord global display name where available;
3. Discord username;
4. numeric Discord user ID.

The participant context file does not override the observed server display name.

## 7. Participant context file

The main configuration references a separate TOML file:

```toml
[participants]
file = "participants.toml"
```

Example:

```toml
version = 1

[participants."881203221593464864"]
character = "Example Character"
role = "player"

[participants."123456789012345678"]
role = "gm"
```

`character` is optional.

`role` is optional and defaults to `player`.

More than one participant may be `gm`.

Missing entries produce warnings only.

Participant context remains a separate TOML artefact throughout the workflow.

At session creation, EchoScribe writes a resolved canonical `participants.toml`
snapshot inside the session directory. The snapshot:

- retains the Discord-user-ID keyed mapping;
- materialises defaults, including `role = "player"`;
- contains the participant context resolved from the configured source file;
- is immutable session context and is not regenerated from the configured source.

`session.json` references the session-local snapshot by path and format. It must
not embed participant entries.

Later stages read the session-local snapshot rather than the mutable configured
source file.

## 8. Live FLAC stage

### 8.1 Queue topology

The FLAC encoder is a separate bounded stage downstream of authoritative capture.

The FLAC stage receives records containing at least:

- Discord user ID;
- session tick or equivalent sample offset;
- mono PCM samples;
- source SSRC for evidence and diagnostics.

The queue exposes:

- current depth;
- capacity;
- high-water mark;
- enqueue failures;
- sustained backlog indication;
- abandonment count and reasons.

The queue is never unbounded.

### 8.2 Writer ownership

One live writer exists per active Discord user track.

The writer inserts silence to maintain the common session timeline.

SSRC changes do not create a new routine track.

The stage may use per-user tasks or a managed writer loop. That is an internal implementation choice provided it preserves:

- bounded resources;
- one logical track per user;
- deterministic shutdown;
- failure isolation;
- no process or task restart storms.

### 8.3 File lifecycle

Writer creation:

```text
tracks/user-<id>.flac.part
```

Clean shutdown:

1. stop accepting new PCM;
2. drain accepted FLAC queue records;
3. finalise each healthy encoder;
4. flush and synchronise storage;
5. atomically rename `.flac.part` to `.flac`;
6. update track and session state.

A `.flac` filename means finalisation succeeded.

A `.flac.part` file is never treated as a healthy routine track merely because a decoder can open it.

### 8.4 Verification

Routine shutdown does not perform a mandatory complete FLAC decode.

Normal finalisation relies on encoder finalisation, available encoder integrity data, successful flush/synchronisation, and atomic rename.

Full decode verification remains available for:

- targeted tests;
- diagnosis;
- recovery validation;
- explicit operator request.

## 9. FLAC failure and backlog

If any decoded PCM record is rejected at the capture ingress boundary and the affected user cannot be identified safely, every produced or routed routine user track is conservatively marked incomplete with reason `capture_audio_drop`. Those tracks retain their `.flac.part` names, the aggregate drop count is recorded durably, and the session enters `awaiting_operator`.

### 9.1 Encoder failure

On writer error:

- mark the user track abandoned and incomplete;
- stop sending further PCM to that writer;
- do not create a replacement writer;
- do not restart the encoder repeatedly;
- continue authoritative capture;
- record the reason and time in durable state.

### 9.2 Backlog

Backlog is a behavioural fault to diagnose, not an accepted operating mode.

Warnings may be emitted before queue exhaustion.

If the stage loses continuity:

- abandon the affected track;
- preserve journals;
- avoid further load from the failed writer;
- record diagnostics.

If a shared cause affects all writers, each affected track becomes incomplete without spawning additional workers or processes.

## 10. Diagnostic WAV stage

Diagnostic WAV uses a separate optional consumer or explicitly enabled writer path.

It is disabled for normal sessions.

Diagnostic failure must not affect authoritative capture or routine FLAC.

## 11. Session metadata and state

`session.json` is a versioned durable workflow record. New recording sessions
begin in format 4. Format 3 remains readable for sessions created before the
work-item artefact reference was introduced. Format 5 is introduced when the
authoritative transcription-results artefact is published.

It contains:

- session ID;
- start and stop times;
- guild and channel IDs;
- configuration version;
- authoritative packet, playout, and event journal descriptions;
- participant snapshot path and format;
- track manifest path and format;
- optional transcription work-manifest path and format;
- optional transcription results path and format according to session version;
- current workflow state;
- failure records;
- completed stage checkpoints.

The participant snapshot and track manifest references are recorded in the
`files` section alongside the authoritative journals. Participant entries
remain in the separate session-local `participants.toml`.

New sessions use format 4 with no work-manifest reference until the artefact has
been published. Format-3 sessions imply no work-manifest reference and must not
contain the field. Successful work-item generation upgrades a format-3 session
to format 4.

Format 4 must not contain a results description. Format 5 requires both the
work-items description and a results description. The results description uses
the fixed relative path `transcription/results.jsonl` and independently
versioned result-record format 1.

Minimum states:

```text
recording
recorded_clean
recorded_incomplete
awaiting_operator
ready_for_transcription
transcribing
transcription_failed
complete
```

Additional internal states may be used, but commands must expose a clear stable state.

Important transitions:

```text
recording
    -> recorded_clean
        -> ready_for_transcription
            -> transcribing
                -> complete

recorded_clean
    -> awaiting_operator
```

The `recorded_clean -> awaiting_operator` transition is reserved for a failure
encountered after `recorded_clean` was durably published but before the later
clean workflow state could be published.

```text
recording
    -> recorded_incomplete
        -> awaiting_operator
            -> explicit recover
                -> awaiting_operator
                    -> explicit continue
                        -> ready_for_transcription

transcribing
    -> transcription_failed
        -> awaiting_operator
            -> explicit continue
                -> transcribing
```

`recover` never silently invokes `continue`.

## 12. Track manifest

`tracks.json` records one entry per Discord user.

Minimum fields:

```json
{
  "format": 1,
  "session_id": "session-...",
  "tracks": [
    {
      "discord_user_id": "881203221593464864",
      "display_name": "Tromador",
      "role": "player",
      "character": "Example Character",
      "path": "tracks/user-881203221593464864.flac",
      "state": "complete",
      "sample_rate": 48000,
      "channels": 1,
      "bits_per_sample": 16,
      "start_sample": 0,
      "length_samples": 123456789,
      "source_ssrcs": [4326, 7386]
    }
  ]
}
```

An incomplete track records its `.flac.part` path, abandonment reason, and last contiguous session position.

## 13. Post-session range builder

The range builder reads playout activity and identity evidence.

It emits candidate speech ranges per user, then sorts them globally.

Nearby ranges for the same user are merged across a configurable silence gap.

The range builder is an explicit component with a composable refinement interface. A future VAD implementation may:

- adjust boundaries;
- reject non-speech;
- split candidates;
- rescue short speech.

The initial implementation uses playout activity without VAD.

Tuning values are configuration, not architecture.

Before normal one-stop orchestration is introduced, the range builder is
invoked explicitly:

```text
build-work-items <session> <config>
```

The command requires `ready_for_transcription`, validates the session-local
artefacts and complete routine tracks, and reads only `merge_gap_ms` from the
named main configuration. Participant metadata comes from the immutable
session-local snapshot, not the configured participant source.

## 14. Transcription work manifest

`transcription/work-items.jsonl` contains one item per candidate range in global chronological order.

Minimum record shape:

```json
{
  "format": 1,
  "id": "session-...:000001",
  "session_id": "session-...",
  "sequence": 1,
  "discord_user_id": "881203221593464864",
  "speaker": "Tromador",
  "role": "player",
  "character": "Example Character",
  "start_ms": 9260,
  "end_ms": 11960,
  "source": "tracks/user-881203221593464864.flac",
  "source_start_ms": 9260,
  "source_end_ms": 11960
}
```

The manifest is retained for replay and diagnosis.

Work item IDs are stable for the generated manifest.

Publication uses a synchronised temporary file followed by atomic replacement
of `transcription/work-items.jsonl`. After publication, the work-item file
description and `work_manifest_built` checkpoint are committed together in one
atomic `session.json` replacement. Repeated generation replaces the manifest
deterministically, leaves the workflow state unchanged, and does not append.

## 15. Rust/Python boundary

Rust is the session orchestrator.

For post-session transcription, Rust starts one Python worker process and passes:

- main configuration path;
- session directory;
- work manifest path;
- output paths;
- resume position where applicable.

Before the first worker launch, Rust:

1. validates the published work manifest, complete routine tracks, and required
   session-local artefacts;
2. creates and synchronises an empty `transcription/results.jsonl`;
3. atomically upgrades format 4 to format 5, publishes the results reference,
   and transitions `ready_for_transcription` to `transcribing`;
4. launches the worker only after that durable replacement succeeds.

An explicit `transcribe <session> <config>` invocation may also enter from
`transcribing` as a controlled restart. Rust validates a contiguous result
prefix beginning at sequence 1, truncates only an incomplete final record back
to the last validated newline, rebuilds and synchronises
`transcript.partial.txt`, and resumes at the next item without rewind.

Rust acquires an exclusive per-session transcription lease before result-prefix
repair or partial-transcript reconstruction and retains it for the complete
worker lifetime. The lock is held through an operating-system file lock on
`transcription/worker.lock`; the persistent filename is not evidence of current
ownership. A duplicated locked handle is inherited by the Python worker so an
orphaned child continues to exclude another invocation after its Rust parent
terminates. The operating system releases the lease after the last owning
Rust/Python handle closes, permitting controlled restart without PID files,
manual lock deletion, or elapsed-time guesses.
`worker.lock` is an incidental coordination artefact and is not added to the
session artefact manifest.

All other `transcribe` entry states are rejected. Slice 9 records a known
worker failure atomically with `transcribing -> transcription_failed`, then
publishes `transcription_failed -> awaiting_operator`. If that second
replacement fails, explicit transcription continuation also accepts the
stranded `transcription_failed` state.

The repository-owned worker is
`workers/faster-whisper/transcription_worker.py`. It is subordinate to the
Rust application, which remains the sole workflow and session-state authority.
Rust resolves it from the compile-time Cargo application root independently of
the caller's working directory.

The interpreter is selected from `ECHOSCRIBE_PYTHON` when set. An empty
explicit value is an error. Otherwise EchoScribe prefers
`.venv/Scripts/python.exe` on Windows or `.venv/bin/python` on POSIX, resolved
from the application root, before falling back to `python` or `python3`
respectively.

The Python worker:

1. loads faster-whisper once;
2. processes manifest items sequentially;
3. commits one JSONL result per completed item;
4. writes the corresponding plain-text line;
5. exits zero only when all required items complete;
6. exits non-zero on persistent item or worker failure.

For a 48 kHz source range, the worker calculates the requested end frame before
considering physical EOF. It may clamp only an overrun of at most 47 frames,
which covers the final millisecond conversion discrepancy. A larger shortfall
fails the item before either JSONL or text output is committed.

The initial boundary is file manifests plus process exit status.

A future persistent Python worker may replace process-per-session orchestration for live transcription without changing the logical work-item contract.

## 16. Transcription results and text output

`transcription/results.jsonl` is append-oriented and retained.

Minimum completed record:

```json
{
  "format": 1,
  "work_item_id": "session-...:000001",
  "session_id": "session-...",
  "sequence": 1,
  "discord_user_id": "881203221593464864",
  "speaker": "Tromador",
  "role": "player",
  "character": "Example Character",
  "start_ms": 9260,
  "end_ms": 11960,
  "text": "What were you saying? I completely missed it.",
  "status": "complete"
}
```

Timing is sufficient to derive overlaps. Explicit overlap references may be added by deterministic transcript assembly logic.

Commit ordering:

1. append and synchronise the structured result;
2. append and synchronise the human-readable line.

The JSONL result is authoritative if a crash occurs between those writes.

On resume, EchoScribe rebuilds `transcript.partial.txt` from the retained contiguous JSONL prefix before processing new work.

Each retained result repeats the work item's required provenance: session and
work-item identity, global sequence, Discord user and speaker metadata,
session-relative timing, source track and range, text, and completed status.
Controlled restart rejects gaps, duplicates, mismatches, and malformed interior
records. Missing participant context remains represented by the defaults
already materialised in the work item and never blocks transcription.

## 17. Transcription ordering

The manifest and worker use one global sequence ordered by:

1. session-relative start time;
2. a deterministic tie-breaker.

The tie-breaker may use Discord user ID and work-item ID. It must be stable.

Overlapping speakers remain separate units.

The plain transcript does not pretend the conversation was non-overlapping.

## 18. Failure and continuation protocol

### 18.1 Recording failure

Any incomplete required track causes normal orchestration to stop after recording finalisation.

No recovery or transcription starts automatically.

### 18.2 Explicit recovery

`recover <session>`:

- validates authoritative journals;
- with no user IDs, regenerates every track currently marked incomplete;
- with one or more Discord user IDs, regenerates exactly those tracks;
- treats naming a healthy track as the explicit request required to rebuild it;
- writes Discord-user-keyed routine FLAC through the normal
  `.flac.part` then `.flac` lifecycle;
- sizes Opus packet-loss concealment from the authoritative playout
  `decoded_samples` duration rather than retained packet decode capacity;
- rolls a published `.flac` back to `.flac.part` if final directory
  synchronisation fails, and synchronises the rollback;
- records recovery results;
- leaves the session awaiting explicit continuation.

The former diagnostic WAV recovery remains separately available as
`recover-wav <session>`. Historical failure records remain durable evidence;
they are not removed or treated as permanent blockers merely because they
remain in `session.json`.

### 18.3 Explicit continuation

`continue <session>`:

- validates session state;
- validates required journals and session artefacts;
- derives the complete required Discord-user set by resolving every decoded
  frame during authoritative replay;
- refuses unattributed decoded PCM and any attributable user missing from the
  complete track manifest;
- requires every routine track to be currently complete and healthy;
- requires recovery attempts and results to have been recorded durably;
- refuses to proceed while known recording faults remain;
- resumes from the next valid stage.

Successfully regenerated derived-track faults may cease to block continuation.
Authoritative journal loss or corruption cannot be repaired by regenerating
FLAC and continues to block while unresolved.

For transcription failure it:

1. acquires the exclusive transcription lease;
2. validates and repairs only a truncated final JSONL record;
3. reuses an unapplied prepared resume target, if one exists;
4. otherwise finds the last globally contiguous committed result and calculates
   the configured rewind target;
5. durably records `transcription_resume_prepared_<sequence>`;
6. atomically replaces and synchronises results with the exact retained prefix;
7. rebuilds and synchronises the partial text from that prefix;
8. atomically records `transcription_resume_applied_<sequence>` while
   transitioning `awaiting_operator -> transcribing`;
9. launches one new Python worker in global chronological order.

The command forms are:

```text
continue <session>
continue <session> <config>
```

The form without configuration is recording recovery only and requires a
format-3 or format-4 `awaiting_operator` session without a results description.
The configured form is transcription continuation only and requires a format-5
session with work and result descriptions, durable transcription-failure
evidence, and state `awaiting_operator` or `transcription_failed`. Arguments and
validated session structure select the route; state alone does not.

For positive rewind, `committed_end` is the final contiguous result's `end_ms`
and the saturated boundary is `committed_end - resume_rewind_seconds * 1000`.
The discarded suffix begins at the earliest committed result for which
`start_ms < committed_end` and `end_ms > boundary`. Zero rewind retains the
entire prefix. If no results are committed, sequence 1 is the target.

Prepared and applied checkpoints make replacement idempotent across crashes. A
retry reuses an unapplied target. After an applied attempt, failure without
forward progress reuses that attempt boundary; new committed results permit a
later continuation to calculate one new rewind from the new authority.

After worker termination, failure diagnostics are derived from the authority
then present, not from the originally supplied start sequence. Launch failure,
non-zero exit, and signal termination are distinguished. Failure evidence
records the attempted start sequence, next uncommitted sequence and work-item
ID, and process diagnostic.

After a zero worker exit, Rust verifies that every work item has exactly one
matching result, deterministically rebuilds the partial transcript, atomically
renames it to `transcription/transcript.txt`, synchronises the directory,
records completion, and transitions `transcribing -> complete` while retaining
the lease. A transcript left by a failed session update is derived output and
is safely replaced by a later explicit retry.

No automatic retry occurs.

## 19. Main configuration

One TOML file configures the stack.

Required conceptual sections:

```toml
version = 1

[discord]
token = "..."
guild_id = "..."
channel_id = "..."

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

[segmentation]
vad_enabled = false
# merge_gap_ms is configurable; its initial value is selected during tuning
```

`merge_gap_ms` is required configuration, but this architecture does not assign a normative default. Its initial value must be documented as provisional and tuned against representative recordings.

CLI options may override configuration for explicit diagnosis or reprocessing.

The offline transcription loader reads only the transcription settings needed
by the worker. It does not validate Discord credentials or IDs and does not
read the mutable configured participant file.

The vocabulary path is resolved relative to the named main configuration.
Missing and empty or whitespace-only files produce a warning and no hotwords.
A readable UTF-8 file contributes one phrase per trimmed non-blank line, with
no comment syntax. Other I/O errors and invalid UTF-8 fail clearly.

## 20. Future live path

The live extension attaches another bounded consumer to PCM fan-out:

```text
PCM fan-out
    -> utterance assembler
        -> chronological transcription queue
            -> persistent Python worker
                -> structured running transcript
                    -> optional GM-assist
```

Future segmentation may combine:

- playout activity;
- silence timing;
- speaking-state events;
- VAD.

No current component may require future live transcription to replace the capture or recording architecture.
