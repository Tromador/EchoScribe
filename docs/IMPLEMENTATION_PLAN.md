# EchoScribe Implementation Plan

## Status and authority

**Status: Normative implementation sequence**

Codex implements one slice at a time.

Approval of a slice does not authorise the next slice.

At the start of each slice, Codex must restate:

- intended result;
- implementation route;
- files expected to change;
- relevant checks;
- explicit non-goals.

At the end of each slice, Codex stops and reports:

- changes made;
- deviations;
- checks run;
- checks not run;
- newly discovered material decisions.

Codex does not run live Discord tests.

## Current reconciliation baseline

The current Rust implementation already provides:

- Serenity and Songbird connection;
- decoded RTP and PCM reception;
- packet, playout, and event journals;
- bounded capture queue telemetry;
- diagnostic WAV writing;
- offline inspection;
- exact PCM recovery;
- aligned offline FLAC export.

The current implementation differs from the approved architecture in important ways:

- live PCM currently feeds diagnostic WAV rather than routine live FLAC;
- FLAC output is currently offline;
- FLAC tracks are keyed by SSRC;
- final FLAC filenames are created immediately;
- FLAC finalisation performs a full decode verification;
- session metadata is not yet a durable workflow state machine;
- transcription orchestration is not implemented.

Useful code should be reused where it fits. It must not be treated as approval of the current workflow.

## Application repository layout

EchoScribe is the root Cargo application package and generated binary. The
package is named `echoscribe`; Songbird remains its voice dependency and
recording subsystem rather than the application identity.

Current Rust source lives under root `src/`. The subordinate faster-whisper
component lives under `workers/faster-whisper/`. Cargo-generated debug and
release binaries remain under root `target/`. Root PowerShell and POSIX
launchers provide `cargo run --release` convenience only and preserve Rust as
the sole workflow authority.

---

## Slice 0 — Reconciliation map

### Authority

Read-only inspection.

### Intended result

Produce a precise implementation map from the current repository to the approved architecture.

### Required output

Identify:

- current decoded PCM production and ownership;
- current capture queue boundaries;
- SSRC mapping availability and timing;
- current shutdown order;
- reusable FLAC encoding and alignment code;
- reusable recovery/export code;
- session metadata changes required;
- proposed files/modules for the live FLAC stage;
- proposed bounded pending-mapping policy;
- proposed targeted tests.

### Prohibited

Do not edit code, dependencies, tests, documentation, Git state, or recordings.

### Stop condition

Stop after reporting the map.

---

## Slice 1 — Configuration and participant context

### Intended result

Extend the configuration model without changing recording behaviour.

### Route

- retain one main TOML configuration;
- add recording, participants, transcription, and segmentation settings;
- load the separate participants TOML;
- validate formats and paths;
- default missing participant roles to `player`;
- preserve missing participant mappings as warnings rather than errors.

### Required tests

- valid configuration;
- relative path resolution;
- invalid version;
- invalid Discord IDs;
- participant file absent when configured;
- missing participant entry is non-fatal;
- multiple GMs accepted;
- `resume_rewind_seconds` parsing;
- diagnostic WAV default false.

### Non-goals

- no Discord name lookup;
- no live FLAC;
- no transcription;
- no session state transition work.

### Stop condition

Configuration and participant context parse correctly under targeted tests.

---

## Slice 2 — Durable session state foundation

### Intended result

Make `session.json` a versioned durable workflow record.

### Route

- introduce explicit workflow states;
- record authoritative artefacts;
- write a resolved canonical `participants.toml` snapshot inside the session
  directory;
- preserve the Discord-user-ID keyed participant mapping and materialise
  defaults such as `role = "player"`;
- reference the session-local participant snapshot by path and format from the
  `session.json` `files` section; do not embed participant entries;
- make later stages read the session-local snapshot rather than the configured
  source file;
- record the track-manifest path and format in the `files` section;
- use session format 3 with Discord IDs represented as JSON strings (the
  format current at Slice 2; Slice 7 introduces the approved format-4
  successor);
- record failures and stage checkpoints;
- update state through atomic file replacement or another crash-safe method;
- keep existing journal formats unchanged unless a versioned metadata reference is required.

### Required tests

- initial `recording` state;
- clean state transition;
- incomplete-track transition;
- failure record persistence;
- invalid transition rejection;
- crash-safe metadata replacement;
- canonical participant snapshot with materialised defaults;
- participant snapshot remains separate and is referenced from `session.json`;
- old session inspection remains possible or fails with a clear version message.

### Non-goals

- no live FLAC;
- no recovery orchestration;
- no transcription;
- no transition to `recorded_clean` from the current recording path before
  later track-finalisation work exists.

### Stop condition

Session state transitions are durable and independently testable.

---

## Slice 3 — Stable Discord user identity

### Intended result

Resolve routine output identity by Discord user rather than SSRC.

### Route

- capture Discord server display names;
- retain timestamped SSRC-to-user evidence;
- maintain current mapping state for downstream routing;
- implement a bounded pending-mapping mechanism;
- expose user identity and display name to the future FLAC stage;
- never guess an unknown mapping.

### Required tests

- one SSRC maps to one user;
- two sequential SSRCs map to one user;
- display-name fallback order;
- bounded pending frames resolve when mapping arrives;
- unresolved mapping produces an incomplete/abandonment signal rather than misattribution;
- missing participant context is warning-only.

### Non-goals

- no FLAC writer yet;
- no character-based transcript attribution;
- no utterance segmentation.

### Live verification

Tromador verifies real Discord display-name and SSRC mapping behaviour.

### Stop condition

A decoded frame can be routed deterministically to a Discord user identity.

---

## Slice 4 — Separate bounded live FLAC stage

### Intended result

Write one aligned `.flac.part` per Discord user during recording without delaying authoritative capture.

### Route

- create a separate bounded FLAC queue downstream of the authoritative capture consumer;
- adapt the existing FLAC alignment/encoding code for live user-keyed writers;
- retain one logical writer per Discord user;
- merge frames across SSRC changes;
- expose queue depth, high-water, failures, and abandonment;
- keep diagnostic WAV disabled by default and opt-in.

### Required tests

- per-user track creation;
- SSRC change continues one user track;
- leading silence;
- internal silence;
- nonstandard frame accounting;
- queue high-water metrics;
- queue-full abandonment;
- encoder-error abandonment;
- no replacement writer after abandonment;
- authoritative consumer remains non-blocking;
- diagnostic WAV only when enabled.

### Non-goals

- no automatic recovery;
- no transcription;
- no removal of offline export;
- no routine full-file decode verification.

### Live verification

Tromador verifies:

- FLAC grows during the session;
- journals remain healthy;
- no authoritative queue drops;
- SSRC change behaviour where practical;
- diagnostic WAV configuration.

### Stop condition

Routine aligned FLAC is produced live and isolated from authoritative capture.

---

## Slice 5 — Clean finalisation and failure stop

### Intended result

Finalise healthy tracks safely and stop orchestration after any known recording fault.

### Route

- drain accepted FLAC records;
- finalise healthy encoders;
- synchronise files;
- atomically rename `.flac.part` to `.flac`;
- record track manifests and state;
- leave abandoned tracks incomplete;
- if any decoded-audio record is rejected at the capture ingress queue, mark
  every produced or routed routine user track incomplete with reason
  `capture_audio_drop`, retain `.flac.part`, record the aggregate drop count
  durably, and leave the session awaiting operator action;
- stop before transcription if any required track is incomplete;
- allow `recorded_clean -> awaiting_operator` only when a failure occurs after
  `recorded_clean` was durably published but before the later clean workflow
  state could be published;
- remove mandatory full decode verification from routine shutdown while retaining explicit verification tooling.

### Required tests

- successful atomic rename;
- final name never appears before successful finalisation;
- sync/finalise error leaves `.part`;
- incomplete session state;
- healthy track manifest;
- incomplete track manifest;
- decoded-audio ingress drop leaves all produced/routed routine tracks
  incomplete with durable aggregate evidence;
- one-shot failure publishing `ready_for_transcription` records the failure and
  moves `recorded_clean` to `awaiting_operator`;
- shutdown does not start offline export;
- shutdown does not start transcription after a recording fault;
- explicit verification still works.

### Non-goals

- no recovery command;
- no transcription worker.

### Live verification

Tromador verifies prompt normal shutdown and accurate track playback.

### Stop condition

Clean sessions become ready for transcription; faulty sessions wait for operator action.

---

## Slice 6 — Explicit recovery and continuation commands

### Intended result

Allow controlled recovery without automatic continuation.

### Route

- retain current inspect/recover capabilities;
- make `recover <session>` recover every currently incomplete user track;
- make `recover <session> <user-id>...` recover exactly the named tracks,
  including a healthy track when explicitly named;
- retain diagnostic WAV recovery as `recover-wav <session>`;
- retain `export`, `inspect`, and `verify`;
- produce normal `.flac.part` then clean `.flac` lifecycle;
- record recovery state and results;
- implement `continue` state validation;
- `recover` must not invoke `continue`;
- `continue` must refuse while required recording faults remain.
- treat historical failure records as durable evidence rather than permanent
  blockers when current derived tracks have been successfully repaired and
  validated;
- continue to block on unresolved authoritative journal loss or corruption.

### Required tests

- recover one selected track;
- recover several failed tracks;
- healthy tracks are not rebuilt without explicit request;
- Opus PLC uses the exact authoritative loss duration even when retained packet
  decode capacity is larger;
- continuation rejects unattributed replayed PCM;
- continuation rejects an attributable user omitted from `tracks.json`;
- failed final publication sync rolls `.flac` back to `.flac.part`;
- failed recovery leaves awaiting-operator state;
- successful recovery still waits;
- continue refuses incomplete state;
- continue accepts healthy recovered state.

### Non-goals

- no transcription implementation beyond a placeholder next-stage transition.

### Stop condition

Operator-controlled recording recovery is complete and state-safe.

---

## Slice 7 — Track manifest and playout range builder

### Intended result

Produce chronological, time-ranged transcription work items from healthy aligned tracks.

### Route

- finalise `tracks.json`;
- read playout activity by user;
- merge nearby same-user activity using configurable `merge_gap_ms`;
- create stable work-item IDs;
- sort globally by start time with deterministic tie-breaking;
- expose `build-work-items <session> <config>` as the explicit offline command;
- acquire the shared per-session operation lease before loading workflow
  authority and retain it through manifest and session publication;
- require `ready_for_transcription` and validate session-local artefacts plus
  complete routine tracks;
- load `merge_gap_ms` without depending on Discord connectivity, the Discord
  token, or the mutable configured participant file;
- obtain participant metadata from the immutable session-local snapshot;
- atomically replace retained `transcription/work-items.jsonl`;
- introduce session format 4 with an optional work-item file description;
- keep format-3 sessions readable with no work-item description, reject that
  field in format 3, and upgrade format 3 to format 4 after successful
  work-manifest publication;
- publish the work-item description and `work_manifest_built` checkpoint
  together in one atomic `session.json` replacement;
- leave `ready_for_transcription` unchanged;
- define a refinement interface that later VAD can implement.

### Required tests

- same-user nearby runs merge;
- long gaps remain separate;
- short speech is retained;
- overlapping speakers produce separate ordered items;
- SSRC changes remain one speaker;
- stable deterministic ordering;
- stable work-item IDs;
- source ranges match aligned FLAC;
- optional VAD interface can be substituted in a test without changing callers.

### Non-goals

- no VAD model;
- no transcription;
- no automatic post-recording orchestration;
- no tuning claim beyond documented provisional defaults.

### Stop condition

A healthy session produces a replayable globally ordered work manifest.

---

## Slice 8 — Python transcription worker

### Intended result

Transcribe one session through one Python process and one faster-whisper model load.

### Route

- expose `transcribe <session> <config>`;
- accept `ready_for_transcription` for first invocation and `transcribing` for
  an explicit controlled restart; reject other states;
- validate the published work manifest, complete routine tracks, and required
  session-local artefacts;
- introduce session format 5 with required work-items and results descriptions;
- keep new recording sessions in format 4 and keep formats 3 and 4 readable
  under their existing compatibility rules;
- create and synchronise an empty `transcription/results.jsonl`, then publish
  the format-5 results reference and transition to `transcribing` together
  before launching the worker;
- Rust launches the repository-owned Python worker with configuration,
  manifest, result, output, and resume paths;
- Python loads faster-whisper once;
- work items are processed sequentially in global order;
- each item is initially independent of previous Whisper text;
- successful complete results append and synchronise to `results.jsonl`;
- corresponding lines append and synchronise to `transcript.partial.txt`;
- controlled restart accepts only a matching contiguous result prefix,
  discards only a truncated final record, rebuilds the partial text from that
  prefix, and resumes at the next item without rewind;
- acquire the shared per-session operation lease before loading workflow
  authority, prefix repair, or text reconstruction and retain it for the
  complete worker lifetime;
- pass a duplicated locked handle into the worker so an orphaned Python child
  continues to exclude another invocation, while operating-system release
  after the final handle closes provides stale/crash recovery;
- reject a physical source which ends more than 47 frames before the requested
  48 kHz range end, and commit no output for that item;
- successful completion leaves the session in `transcribing`;
- Python exits non-zero on failure;
- no automatic retry.

The application worker is
`workers/faster-whisper/transcription_worker.py`. Rust resolves it from the
compile-time root Cargo manifest directory independently of the caller's
working directory. It selects `ECHOSCRIBE_PYTHON` first, then the platform
interpreter in the repository-root `.venv` when present, and finally `python`
on Windows or `python3` elsewhere. An explicitly empty override is an error.

The offline transcription configuration loader does not validate Discord
credentials or IDs and does not read the configured participant file.
Vocabulary phrases are trimmed non-blank UTF-8 lines. Lines whose first
non-whitespace character is `#` are comments; `#` elsewhere is retained as
part of the phrase. Missing, empty and comment-only vocabulary is warning-only;
other I/O failures and invalid UTF-8 are errors.

### Required tests

Use mocked or deliberately small local audio where possible.

Test:

- manifest parsing;
- one model load per process;
- chronological processing;
- time-range extraction;
- committed JSONL result;
- matching text line;
- worker non-zero exit on item failure;
- retained prior results;
- no duplicate output after controlled restart;
- a blocking worker prevents a second invocation from repairing outputs or
  launching another worker;
- the lease remains held by a duplicated worker handle after the parent handle
  closes and becomes acquirable after the final handle closes;
- a final range overrun of at most 47 frames is accepted;
- a materially truncated source commits neither a JSONL result nor a text line;
- missing mapping metadata does not block transcription;
- all captured conversation categories are preserved.

### Non-goals

- no live transcription;
- no GM queue;
- no relevance filtering;
- no AAR generation;
- no cross-item Whisper conditioning.
- no durable transcription-failure transition, rewind continuation, final
  transcript rename, or transition to `complete` (Slice 9).

### External verification

Tromador runs CUDA/faster-whisper acceptance on Zen.

### Stop condition

A healthy recorded session produces incremental structured and human transcript output.

---

## Slice 9 — Transcription failure and resumable continuation

### Intended result

Resume failed transcription without restarting the entire session.

### Route

- retain `continue <session>` for recording recovery of format-3 and format-4
  sessions with no results description;
- add `continue <session> <config>` for format-5 transcription continuation
  from `awaiting_operator` or a stranded `transcription_failed`;
- reject command, configuration, format, state, and artefact mismatches before
  mutation;
- expose `resume_rewind_seconds` through the Discord-independent offline
  transcription loader;
- after worker termination, validate the complete contiguous result prefix and
  derive the next uncommitted sequence and item;
- distinguish launch failure, non-zero exit, and signal termination;
- atomically record attempted start, next item, and process diagnostics while
  setting `transcription_failed`, then transition to `awaiting_operator`;
- use the shared per-session operation lease for `recover`, recording
  `continue`, `build-work-items`, `transcribe`, and configured transcription
  `continue`;
- acquire that lease before loading `session.json`, route/state validation, or
  resolving any session-declared path for every mutating offline command;
- keep session artefact validation, result/text mutation, worker execution,
  final publication, and workflow updates within the protected region;
- for positive rewind, find the earliest result intersecting the configured
  window ending at the last committed result's `end_ms`;
- for zero rewind, retain the complete committed prefix;
- keep JSONL contiguous by atomically replacing it with the retained prefix;
- durably record `transcription_resume_prepared_<sequence>` before replacement;
- reapply an unmatched prepared target after a crash instead of calculating a
  second rewind;
- rebuild and synchronise `transcript.partial.txt`;
- atomically record `transcription_resume_applied_<sequence>` while
  transitioning to `transcribing`;
- after failure without forward progress, reuse the previous applied boundary;
- after new committed progress, permit one new rewind calculation;
- launch one new worker and resume in global chronological order;
- after zero exit, require exactly one matching result for every work item,
  deterministically rebuild text, atomically publish
  `transcription/transcript.txt`, record completion, and transition to
  `complete`;
- retain the shared session-operation lease through final publication and state
  update;
- route every post-worker result-authority validation error through durable
  transcription failure publication;
- retain newline-terminated malformed or mismatched records unchanged and
  record their safely validated prefix plus earliest unsafe work item;
- continue to repair only a truncated final byte tail automatically.

### Required tests

- failure after several committed items;
- delayed stale observations in both transcription command paths reload
  authority after lease acquisition and cannot overwrite a completed session;
- a delayed work-manifest builder reloads completed authority after lease
  acquisition and cannot alter session, work, result, partial, or final
  transcript artefacts;
- a blocking transcription worker excludes work-manifest publication;
- recording recovery and recording continuation cannot publish while another
  mutating session operation owns the lease;
- next-uncommitted diagnostics derived after worker exit;
- launch failure;
- signal or status-less termination;
- stranded `transcription_failed` continuation;
- zero-second rewind;
- 120-second rewind;
- rewind crossing several work items;
- overlapping work items preserving a contiguous prefix;
- truncated final result repair;
- valid prefix followed by a complete malformed result and non-zero exit;
- mismatched result followed by zero exit;
- durable operator state for both result-integrity failures;
- failed second result-integrity state publication leaves a recoverable
  `transcription_failed` session;
- no duplicate JSONL authority;
- deterministic text reconstruction;
- crash after prepared checkpoint;
- crash after result replacement but before applied checkpoint;
- repeated failure without progress does not rewind again;
- later failure after new progress calculates a new rewind;
- invalid command/config/session combinations;
- refusal to continue from invalid state;
- successful final rename;
- failure of the completion session update remains recoverable;
- persistent second failure returns to operator state.

### Non-goals

- no automatic retry;
- no heuristic diagnosis of CUDA faults;
- no live worker persistence.

### Stop condition

Operator-controlled transcription continuation is deterministic and replayable.

---

## Slice 10 — One-stop normal orchestration

### Intended result

Provide one normal invocation while preserving separately callable stages.

### Route

The normal command is `echoscribe [config]` and coordinates:

```text
record
    -> finalise
    -> build work manifest
    -> transcribe
    -> finalise transcript
```

At every known failure it:

- records durable state;
- stops at the safe boundary;
- reports the required operator action.

Keep explicit commands for at least:

- inspect;
- recover;
- continue;
- transcribe or equivalent reprocessing;
- transcript rebuild or equivalent.

`echoscribe record [config]` retains recording and finalisation as an explicit
stage and leaves a healthy session in `ready_for_transcription`.

Configured `continue <session> <config>` is stage-aware. It validates completed
recording recovery without performing recovery. A format-3 or format-4 session
whose latest durable failure records an accepted work-manifest or pre-results
transcription-orchestration stop from `ready_for_transcription` resumes directly
at that boundary rather than repeating recording validation. Healthy
`ready_for_transcription` authority builds a missing work manifest or reuses a
valid published one. Format-5 `transcribing` uses controlled restart, and
transcription failure uses durable rewind continuation. Incompatible and
complete sessions are refused before mutation. Unconfigured `continue
<session>` retains recording-recovery validation only.

The coordinator acquires the shared session-operation lease after live
recording and holds it continuously through manifest construction, results
publication, worker execution, transcript publication, and completion.
Internal stage functions accept the already-held lease.

Add `rebuild-transcript <session>` for a complete format-5 session. It validates
complete work and result authority and atomically rebuilds only
`transcription/transcript.txt`, without Python, retranscription, result changes,
or workflow transition. Rendering depends only on those completed structured
transcription authorities; recording journals, participant context, the track
manifest and source FLACs need not remain present.

Accepted-stage manifest or orchestration publication failure may be recorded
durably with `ready_for_transcription -> awaiting_operator`. CLI/config errors,
lease contention, incompatible state, and pre-acceptance validation refusal do
not mutate authority.

### Required tests

- clean full state transition with mocked external boundaries;
- recording failure stops before manifest/transcription;
- transcription failure stops with partial outputs;
- recovery does not continue;
- continue resumes at correct stage;
- completed session refuses accidental duplicate normal run or requires an explicit reprocess option.

### Non-goals

- no GUI;
- no service manager;
- no live transcription;
- no GM-assist;
- no AAR generation.

### Live acceptance

Tromador performs an end-to-end RPG-session acceptance run.

### Stop condition

Normal operation requires one invocation, and all failure paths remain explicitly operator-controlled.

---

## Accepted correction — atomic completed-session retranscription

### Intended result

Replace a healthy completed transcript using current segmentation,
participant-admission and Whisper settings without risking the existing
complete authority.

### Route

- add `retranscribe <session> <config>` with strict two-path parsing and no
  Discord connection;
- acquire the shared session-operation lease before loading session authority;
- require and validate a healthy complete format-5 or format-6 session;
- rebuild work items from authoritative playout evidence using current
  `merge_gap_ms` and the immutable session-local participant snapshot;
- add participant `transcribe`, defaulting and materialising to `true`;
- exclude false participants before work-item sequencing and ID allocation
  without changing recording evidence or routine tracks;
- run the normal production worker from sequence 1 against staged generation
  paths using current transcription settings;
- require one exact staged result per staged work item and render the readable
  transcript from those validated results;
- introduce complete-only session format 6, whose work-item, result and
  readable-transcript references share one retranscription generation;
- publish the replacement set through one atomic `session.json` switch only
  after the complete generation is synchronised and validated;
- retain the old complete authority and `complete` state on every failure;
- add explicit leased `set-transcription-policy` migration for changing only
  one historical snapshot Boolean without rereading mutable role, character or
  identity data.

### Required checks

- CLI exact-argument parsing;
- complete-state refusal without mutation;
- participant default, explicit false and older-snapshot compatibility;
- exclusion before contiguous sequencing with retained included provenance;
- fresh sequence-1 worker invocation and staged output paths;
- exact successful format-6 generation publication;
- worker, staged-integrity and session-publication failures preserving prior
  complete authority;
- operation-lease exclusion;
- repeated safe deterministic regeneration.

### Non-goals

- no recording, merge, VAD, lexical qualification or prompted-decode changes;
- no continuation or recovery semantics;
- no mutable participant-source reread during work generation;
- no result, work-item or participant snapshot format bump;
- no deletion of prior complete transcription generations.

---

## Accepted correction — post-recording speech-presence gate

### Intended result

Prevent Whisper hallucinations on decoded comfort noise without changing
published work-item ranges or result-prefix authority.

### Route

- load `segmentation.vad_enabled` through the Discord-independent offline
  transcription configuration;
- pass the validated value explicitly from Rust to Python;
- when disabled, retain the existing transcription path for every work item;
- when enabled, analyse the complete extracted range with faster-whisper's
  bundled Silero implementation;
- send a Silero-positive range to unprompted lexical qualification in full,
  without VAD trimming;
- treat a Silero miss as final and commit a normal complete empty result
  without invoking Whisper;
- qualify lexical speech when at least one non-empty unprompted segment has
  `no_speech_prob` below the validated threshold;
- run the configured hotword-assisted decode only after lexical acceptance,
  or reuse accepted unprompted text when no hotwords are configured;
- omit empty results from Python and Rust human-readable transcript rendering
  while retaining them in the contiguous JSONL authority;
- emit aggregate VAD decision telemetry at worker completion.

The current `RangeRefiner` remains a no-op. This correction is a worker-side
speech-presence gate, not range splitting, boundary refinement, live VAD, or a
new persistent format.

### Required checks

- disabled-path compatibility;
- Silero-positive full-range and quiet-speech acceptance;
- comfort-noise rejection;
- Silero-negative rejection without Whisper invocation;
- empty and high-no-speech lexical rejection;
- prompted decoding only after lexical acceptance;
- complete empty-result commitment and restart behaviour;
- Rust configuration propagation and empty-result rendering;
- Discord-independent offline configuration loading.

---

## Deferred tuning programme

After real recordings exist, tune without changing architecture:

- `merge_gap_ms`;
- pending SSRC mapping grace window;
- FLAC queue capacity and warning thresholds;
- faster-whisper model and decoding settings;
- vocabulary/hotwords;
- cross-item conditioning;
- Silero acoustic gating and lexical qualification threshold;
- possible future VAD range refinement;
- overlap annotation option.

Tuning evidence must come from representative session recordings.

---

## Deferred future programme

Not authorised by this plan:

- live utterance assembly;
- persistent Python transcription worker;
- GM/player queue classification;
- GM priority;
- code-word handling;
- AI assistant integration;
- live transcript delivery;
- AAR generation.

Those features must attach to the existing PCM fan-out and structured transcript contracts rather than replace the recording architecture.
