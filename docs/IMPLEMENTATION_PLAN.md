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
- use session format 3 with Discord IDs represented as JSON strings;
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
- write retained `transcription/work-items.jsonl`;
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
- no tuning claim beyond documented provisional defaults.

### Stop condition

A healthy session produces a replayable globally ordered work manifest.

---

## Slice 8 — Python transcription worker

### Intended result

Transcribe one session through one Python process and one faster-whisper model load.

### Route

- Rust launches the Python worker with configuration and manifest paths;
- Python loads faster-whisper once;
- work items are processed sequentially in global order;
- each item is initially independent of previous Whisper text;
- successful results append to `results.jsonl`;
- corresponding lines append to `transcript.partial.txt`;
- Python exits non-zero on failure;
- no automatic retry.

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
- missing mapping metadata does not block transcription;
- all captured conversation categories are preserved.

### Non-goals

- no live transcription;
- no GM queue;
- no relevance filtering;
- no AAR generation;
- no cross-item Whisper conditioning.

### External verification

Tromador runs CUDA/faster-whisper acceptance on Zen.

### Stop condition

A healthy recorded session produces incremental structured and human transcript output.

---

## Slice 9 — Transcription failure and resumable continuation

### Intended result

Resume failed transcription without restarting the entire session.

### Route

- record the failed item and worker diagnostics;
- set `transcription_failed` and await operator action;
- find the last contiguous committed sequence;
- apply configured `resume_rewind_seconds` default 120;
- supersede results intersecting the rewind window;
- rebuild `transcript.partial.txt` from valid JSONL;
- launch one new worker;
- resume in global chronological order;
- finalise `transcript.txt` only after all items commit.

### Required tests

- failure after several committed items;
- zero-second rewind;
- 120-second rewind;
- rewind crossing several work items;
- no duplicate JSONL authority;
- deterministic text reconstruction;
- refusal to continue from invalid state;
- successful final rename;
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

The normal command coordinates:

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

The exact top-level normal command name is a CLI naming detail. It must be documented and unambiguous.

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

## Deferred tuning programme

After real recordings exist, tune without changing architecture:

- `merge_gap_ms`;
- pending SSRC mapping grace window;
- FLAC queue capacity and warning thresholds;
- faster-whisper model and decoding settings;
- vocabulary/hotwords;
- cross-item conditioning;
- VAD refinement and short-speech rescue;
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
