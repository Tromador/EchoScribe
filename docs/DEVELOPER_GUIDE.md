# EchoScribe Developer Guide

This guide explains the current EchoScribe implementation to someone who wants
to understand, maintain, or extend it. It describes the application as built,
without relying on development-history or implementation-plan documents.

For installation, configuration, command syntax, and recovery procedures, see
the [User Guide](USER_GUIDE.md).

## 1. Architectural intent

EchoScribe is a recoverable recording system first and a transcription system
second.

The central rule is:

> A failure in a derived product must not silently destroy or misrepresent the
> authoritative recording evidence.

That produces a clear priority order:

1. packet, playout, event, and workflow authority;
2. routine per-user FLAC;
3. transcription;
4. future downstream consumers.

The design also preserves stable identities and timelines:

- Discord user ID identifies a logical participant track;
- SSRC is timestamped transport evidence and may change;
- every routine FLAC starts at session time zero;
- every transcription work item refers to a bounded range of an aligned track;
- JSONL results, rather than display text, are transcript authority.

## 2. Technology and component boundary

The repository root is one Cargo application named `echoscribe`.

- Rust owns the CLI, Discord lifecycle, live capture, recording authority,
  recovery, workflow state, offline orchestration, and process lifetime.
- Serenity supplies Discord gateway events and member identity evidence.
- Songbird supplies voice connectivity, decrypted RTP, playout decisions, and
  decoded mono PCM.
- Rust writes routine FLAC through `flac-codec`.
- A repository-owned Python worker uses faster-whisper/CTranslate2 for local
  transcription.
- Python never edits `session.json` and is never workflow authority.

The current Python boundary is one process per transcription invocation and one
model load per worker process. A normal session uses one worker; an explicit
restart starts a new one. The manifest/process contract is designed so a later
persistent worker can replace that process lifetime without changing recording
authority.

## 3. Repository layout

```text
EchoScribe/
├── Cargo.toml
├── Cargo.lock
├── src/
├── workers/
│   └── faster-whisper/
│       ├── transcription_worker.py
│       ├── requirements.txt
│       └── tests/
├── docs/
├── archive/
│   └── legacy-pipeline/
├── echoscribe.example.toml
├── participants.example.toml
├── vocabulary.example.txt
├── requirements.txt
├── echoscribe.ps1
└── echoscribe.sh
```

`Cargo.lock` is tracked because EchoScribe is an application. The root Python
`requirements.txt` delegates to the worker-specific requirements file.

The root launchers contain no application logic. They resolve `Cargo.toml`
relative to themselves, preserve the caller's working directory, and invoke:

```text
cargo run --release --manifest-path <root>/Cargo.toml -- <arguments>
```

## 4. Rust module map

| Module | Responsibility |
|---|---|
| `main.rs` | CLI parsing, live Discord lifecycle, handler registration, shutdown, and entry into one-stop orchestration. |
| `config.rs` | Strict TOML schemas, live configuration, offline stage projections, relative path resolution, and vocabulary parsing. |
| `participants.rs` | Participant TOML validation, case-insensitive roles, defaults, and canonical session snapshots. |
| `artifacts.rs` | Canonical session artefact names and independent format versions. |
| `session.rs` | Versioned `session.json`, state transition table, event records, validation, and atomic authority replacement. |
| `telemetry.rs` | Songbird callback adapter, non-blocking capture submission, and RTP continuity telemetry. |
| `capture.rs` | Session startup transaction, bounded ingress queue, authoritative consumer, fan-out, shutdown, finalisation, and recording disposition. |
| `journal.rs` | Versioned packet journal read/write format. |
| `playout.rs` | Versioned playout journal and Opus payload/loss evidence. |
| `identity.rs` | Bounded SSRC-to-user resolution, display-name fallback, disconnect handling, and terminal continuity poisoning. |
| `diagnostics.rs` | Optional live and recovered SSRC-keyed WAV writers. |
| `live_flac.rs` | Bounded downstream encoder stage and one live writer per Discord user. |
| `track_manifest.rs` | Version-1 routine track manifest validation and atomic publication. |
| `flac_tracks.rs` | Older explicit SSRC-keyed offline FLAC export and full-decode verification. |
| `recover.rs` | Authoritative journal replay, Opus decode/PLC, diagnostic WAV recovery, and legacy export. |
| `routine_recovery.rs` | User-keyed routine FLAC regeneration, mapping replay, publication, and durable recovery evidence. |
| `continuation.rs` | Recording-recovery validation and transition back to `ready_for_transcription`. |
| `work_items.rs` | Playout-derived candidate ranges, merge policy, refinement interface, stable global work manifest. |
| `operation_lease.rs` | Exclusive per-session operating-system lock shared by all mutating offline operations. |
| `stage.rs` | Distinguishes pre-acceptance refusal from failure after a pipeline stage has begun publication. |
| `transcription.rs` | Rust worker orchestration, result-prefix validation, failure publication, rewind continuation, and transcript rendering. |
| `orchestration.rs` | One-stop post-recording coordinator and stage-aware configured continuation. |
| `retranscription.rs` | Completed-session replacement staging and atomic generation publication. |
| `participant_policy.rs` | Explicit leased migration of one historical snapshot's transcription Boolean. |
| `inspect.rs` | Read-only current/legacy session and authoritative-journal inspection. |
| `verify_tracks.rs` | Explicit full-decode verification of complete routine FLAC tracks. |

The Python worker owns only:

- strict work-manifest parsing;
- bounded source-range extraction with SoundFile;
- one faster-whisper model instance;
- sequential inference;
- synchronised append of one result and one text line per item.

## 5. System data flow

### 5.1 Live path

```text
Discord gateway                     Discord voice
       |                                  |
       | member and voice state           | RTP, playout, decoded PCM, ticks
       +-------------------+--------------+
                           v
                  Songbird callbacks
                           |
                    non-blocking send
                           v
             bounded capture ingress queue
                           |
                authoritative consumer
           +---------------+----------------+
           |               |                |
           v               v                v
      journals       identity router   optional WAV
                           |
                    resolved user PCM
                           |
                    non-blocking send
                           v
                 bounded live FLAC queue
                           |
                  per-user FLAC writers
```

The initial callback queue carries packet, event, playout, decoded-audio, and
routing-tick records. It is bounded at 4,096 records. Callbacks use `try_send`;
they never wait for capacity or perform file/codec work.

The single capture consumer owns authoritative ordering and all journal
writers. Derived FLAC encoding is separated behind another queue after the
consumer has recorded and routed the input.

### 5.2 Post-recording path

```text
authoritative journals + events + tracks.json + participant snapshot
                              |
                              v
                    chronological range builder
                              |
                    transcription/work-items.jsonl
                              |
                              v
                   Rust transcription orchestrator
                              |
                   one faster-whisper worker
                              |
                 +------------+-------------+
                 v                          v
        transcription/results.jsonl  session-root transcript.partial.txt
                 |                          |
                 +------------+-------------+
                              v
                  transcription/transcript.txt
```

Normal one-stop orchestration acquires one session operation lease after live
recording finishes and holds it continuously through the entire diagram.

## 6. Authority and regenerability

Not every file which looks useful has equal standing.

| Artefact | Authority | Regenerable? |
|---|---|---|
| `session.json` | Workflow authority and internal artefact manifest | No |
| session `participants.toml` | Immutable participant context for that session | No |
| `packets.dat` | Decrypted packet recording authority | No |
| `playout.dat` | Playout/loss and decoded-duration authority | No |
| `events.ndjson` | Identity, mapping, disconnect, and continuity-failure evidence | No |
| routine FLAC | Derived aligned audio product | Yes, when journals/evidence remain healthy |
| `tracks.json` | Derived track disposition and provenance | Reconstructable only with retained evidence/context |
| `work-items.jsonl` | Deterministic derived transcription plan | Yes from healthy recording artefacts and the chosen segmentation setting |
| `results.jsonl` | Structured transcript authority | Only by retranscription |
| partial/final text | Human-readable derived view | Yes from validated results |

This distinction drives failure behaviour. A playable `.flac.part` is not
promoted to complete. A plausible `transcript.txt` is not accepted over
contradictory JSONL authority.

## 7. Persistent formats

### 7.1 Session formats

Current `session.json` readers accept formats 3, 4, 5, and 6:

- format 3 predates the work-manifest description;
- format 4 is the format used when a new recording session starts and may
  optionally describe `work-items.jsonl`;
- format 5 requires both work-item and result descriptions and belongs to the
  transcription workflow;
- format 6 is complete-only and explicitly references a work manifest, results
  and readable transcript in one published retranscription generation.

Format 2 remains available only through the read-only inspector.

The `files` object is an internal manifest for authoritative or cross-stage
artefacts. Format-3 to format-5 paths are fixed. Format-6 transcription paths
must share one directory below `transcription/retranscriptions/`. All paths are
relative and independently versioned; readers reject absolute paths, parent
traversal, and unexpected paths/formats.

Adding a field to a `deny_unknown_fields` structure without a format bump breaks
older readers even if the new field is optional. Treat persistent schema
changes as versioned compatibility decisions, not ordinary struct edits.

### 7.2 Independent artefact versions

Current independent versions are:

- participant snapshot: 1;
- routine track manifest: 1;
- work-item records: 1;
- transcription result records: 1;
- event journal: 2, with event format 1 still readable;
- packet and playout versions declared by their owning modules.

The event journal is newline-delimited JSON. Work and result authorities are
also JSONL. A newline-terminated malformed record is durable bad evidence and
must not be silently skipped. Only a non-newline-terminated final byte tail is
eligible for narrowly defined crash repair.

### 7.3 Workflow states

The transition table is explicit rather than ordinal:

```text
recording
    -> recorded_clean
        -> ready_for_transcription
            -> transcribing
                -> complete

recording
    -> recorded_incomplete
        -> awaiting_operator
            -> ready_for_transcription       # after explicit recovery validation

transcribing
    -> transcription_failed
        -> awaiting_operator
            -> transcribing                  # explicit configured continuation

recorded_clean -> awaiting_operator          # failed ready publication
ready_for_transcription -> awaiting_operator # accepted post-recording stage failure
```

Every transition is validated before atomic persistence. Failure records are
append-only evidence; successful repair does not delete history.

## 8. Session creation as a transaction

`capture::start` treats startup as a small transaction:

1. allocate a new session directory under a cleanup guard;
2. create and initialise the packet, event, and playout writers;
3. create fallible downstream capture components;
4. write and synchronise the canonical participant snapshot;
5. publish `session.json` as the final startup commit point;
6. disarm cleanup and spawn the consumer.

If any pre-publication step fails, the newly allocated directory is removed.
No valid-looking `recording` session is left behind with missing journals.

## 9. Live capture in detail

### 9.1 Gateway identity

Serenity supplies identity evidence in two routes:

- `guild_create` seeds members already present in the configured voice channel;
- `voice_state_update` handles joins, changes, and departures.

Bots are ignored. No HTTP member lookup is needed. Display-name preference is:

1. server display name/nickname;
2. global display name;
3. username;
4. numeric Discord user ID.

The operator participant file does not override this human speaker name.

### 9.2 Songbird evidence

`VoiceTelemetry` registers one global handler for:

- speaking-state updates, which carry SSRC-to-user mapping evidence;
- RTP packets, retained as decrypted packet records;
- voice ticks, which supply decoded PCM, playout decisions, losses, and a
  global clock for pending identity expiry.

Callback telemetry is observational. Authoritative persistence happens in the
capture consumer.

### 9.3 Journal durability

The consumer uses buffered writers. Current tuning is:

- 256 KiB writer buffers;
- structural flush/checkpoint every 5 seconds;
- storage data synchronisation every 30 seconds;
- final flush and synchronisation during shutdown.

These intervals bound ordinary buffered loss; they are not permission to treat
the journals casually. Any enqueue rejection on the shared capture ingress is
a recording-integrity fault.

### 9.4 Identity routing

Decoded PCM is received by SSRC but routine tracks are keyed by Discord user ID.
The identity router never guesses.

Current pending limits are:

- 250 voice ticks of age, nominally five seconds;
- 96,000 retained PCM samples per unresolved SSRC;
- 32 concurrently pending unresolved SSRCs.

Limits are checked before retaining the triggering frame. Global routing ticks
expire a silent unresolved SSRC even if it sends no further frames.

When a late mapping identifies an abandoned SSRC, the logical user track is
poisoned. Later PCM from a replacement SSRC cannot make the same user appear
healthy after known missing audio. Disconnect evidence revokes all current
SSRC mappings for the user while preserving display identity for reconnection.

### 9.5 Diagnostic WAV

When enabled, decoded SSRC audio is sent to the optional diagnostic writer
without waiting for user mapping. Diagnostic output therefore remains useful
for identity investigation. Its errors abandon diagnostics without damaging
journals or routine FLAC.

### 9.6 Live FLAC stage

The live FLAC queue is bounded at 1,024 routed PCM frames. The capture consumer
uses a strictly non-blocking enqueue.

Queue telemetry includes accepted frames, high-water mark, enqueue failures,
warning crossings, and abandoned users. A warning is emitted at 75% and
re-armed only after depth falls below 50%.

One managed encoder loop owns writers keyed by Discord user. Each writer:

- starts as `tracks/user-<id>.flac.part`;
- inserts leading and internal silence from session ticks;
- accepts successive SSRCs belonging to the same user;
- retains SSRC provenance;
- is never restarted after terminal continuity or encoder failure.

Queue-full rejection abandons only the user whose frame was rejected. A
decoded-audio rejection at the earlier shared ingress cannot safely attribute
the loss, so every produced/routed routine track is conservatively incomplete
with reason `capture_audio_drop`.

## 10. Shutdown and track publication

Ctrl-C and gateway termination converge on the same top-level route:

1. request voice leave;
2. close capture acceptance;
3. drain all accepted capture records;
4. drain all accepted FLAC records;
5. finalise encoders;
6. flush and synchronise journals/tracks;
7. publish track dispositions;
8. publish workflow state;
9. shut down the gateway.

For each healthy routine track, finalisation performs:

```text
finish encoder
    -> synchronise .flac.part
    -> rename to .flac
    -> synchronise tracks directory
```

If the directory sync after rename fails, publication attempts to roll back to
`.flac.part` and synchronise again. Incomplete tracks remain `.part` and carry
abandonment metadata in `tracks.json`.

Normal shutdown does not fully decode every FLAC. Encoder finalisation,
integrity metadata, synchronisation, and atomic rename define routine
publication. `verify` provides the explicit expensive whole-file check.

## 11. Authoritative replay and recovery

`recover.rs` is the common decoder for current recovery and diagnostic export:

1. index packet records by SSRC, RTP sequence, and timestamp;
2. replay playout decisions in recorded order;
3. select the exact recorded Opus payload for packet decisions;
4. ask the per-SSRC Opus decoder for packet-loss concealment using the
   authoritative `decoded_samples` duration for loss decisions;
5. require decoded sample counts to agree with playout authority;
6. expose session tick and elapsed-time evidence to the selected writer.

Routine recovery adds the timestamped mapping timeline from `events.ndjson` and
merges every selected user's SSRCs into one aligned FLAC. It records started and
completed checkpoints per user, publishes recovered files through the normal
`.part` lifecycle, updates `tracks.json`, and deliberately remains in
`awaiting_operator`.

Recording continuation independently validates:

- current state and format;
- authoritative journal health;
- safe attribution of every decoded frame;
- the exhaustive set of users implied by replay;
- complete manifest coverage;
- full FLAC decode/MD5 health;
- durable evidence of required recovery attempts/results;
- absence of unresolved authoritative faults.

This avoids trusting a derived manifest to prove that its own user set is
complete.

## 12. Work-item generation

The current range builder derives candidate activity from playout decisions,
not from audio-energy VAD.

For each safely attributed decoded frame it builds a sample-domain range on the
user's aligned track. Nearby same-user ranges are merged when the intervening
gap is no greater than `merge_gap_ms`. Different speakers remain separate even
when they overlap.

The builder then:

1. validates track/session alignment;
2. passes candidates through the `RangeRefiner` interface;
3. materialises participant role, character and transcription policy from the
   session snapshot;
4. removes `transcribe = false` participants;
5. sorts globally by start time with stable tie-breakers;
6. assigns `session-id:000001` style IDs only after exclusion;
7. atomically replaces `transcription/work-items.jsonl`;
8. publishes the file description and `work_manifest_built` checkpoint in one
   `session.json` replacement.

`NoopRefiner` remains the current implementation. The active speech-presence
gate is deliberately later, over the exact extracted range in the Python
worker, so it cannot change the published work-item timestamps, IDs, ordering,
or attribution. A future boundary-refining VAD may still implement this seam
when range adjustment or splitting is explicitly required.

Repeated work-item generation while still ready replaces rather than appends.
Stable input and settings produce stable ordering and IDs.

## 13. Transcription process boundary

### 13.1 Before launch

Rust validates current session authority, complete tracks, the work manifest,
and source ranges. On first transcription it:

1. creates and synchronises empty `results.jsonl`;
2. atomically upgrades the session from format 4 to format 5;
3. publishes the result description and transition to `transcribing` together;
4. launches Python only after that durable update succeeds.

This guarantees that a running worker never writes results which workflow
authority does not know about.

### 13.2 Worker invocation

Rust resolves `workers/faster-whisper/transcription_worker.py` from the
compile-time Cargo root. It passes explicit paths, the next global sequence,
model settings, the validated `vad_enabled` Boolean, the validated lexical
no-speech threshold, and repeated hotword arguments. Python does not parse the
main TOML.

The worker loads the model once and processes work items sequentially. For each
item it:

1. verifies the strict manifest record;
2. resolves the source inside the session directory;
3. extracts only the requested mono 48 kHz range to a temporary WAV;
4. when enabled, decodes the complete temporary range to 16 kHz and calls
   faster-whisper's bundled Silero `get_speech_timestamps` API;
5. rejects a Silero-negative range without invoking Whisper;
6. decodes an admitted range in full without hotwords and accepts lexical
   speech when at least one non-empty segment has `no_speech_prob` below the
   validated threshold;
7. after lexical acceptance, decodes again with configured hotwords or reuses
   the unprompted text when no hotwords are configured;
8. normalises output to one physical text line;
9. appends and synchronises the JSONL result;
10. appends and synchronises the corresponding partial-text line only when the
   normalised text is non-empty.

Silero rejection is final. It commits the normal complete result with empty
text, skips both Whisper passes, and emits no human-readable transcript line.
Silero acceptance changes no range boundary and proceeds to lexical
qualification over the same complete temporary audio.

The default analyser uses the API and `silero_vad_v6.onnx` asset shipped by
pinned faster-whisper 1.2.1. It introduces no Torch/TorchHub model, download,
or runtime dependency. Loading or inference errors propagate as worker
failure. Aggregate decision counts are emitted only at worker completion.

The requested end is rounded from milliseconds to frames. Clamping to physical
EOF is allowed only for the maximum 47-frame 48 kHz rounding discrepancy. A
larger source shortfall fails before either output is committed.

### 13.3 Result authority

Rust requires a contiguous global prefix beginning at sequence 1. Each result
must exactly match its work item's provenance and have completed status.

A VAD rejection is represented by a matching complete result with `text = ""`.
That record advances the prefix normally and is not retranscribed on restart.
Both Python incremental rendering and Rust partial/final rebuilding omit its
human-readable line.

The validator rejects:

- gaps or duplicates;
- unexpected fields or formats;
- wrong session/item identity;
- mismatched speaker, timing, or source provenance;
- newline-terminated malformed records;
- complete records after the earliest unsafe record.

Only an incomplete final byte tail may be truncated to the last validated
newline.

## 14. Transcription failure, restart, and completion

### 14.1 Controlled restart

An explicit `transcribe` entry from `transcribing` validates the prefix,
repairs only a truncated final tail, rebuilds partial text from JSONL, and starts
at the next sequence without rewind.

### 14.2 Known worker failure

After launch failure, non-zero exit, signal termination, or post-worker result
integrity failure, Rust derives diagnostics from authority as it exists after
the worker stopped. The worker may have committed more items before failing, so
the sequence originally passed at launch is not treated as current truth.

Failure evidence includes the attempted start, next uncommitted item, process
diagnostic, and—where authority is unsafe—the safe prefix length and earliest
unsafe item. Rust atomically publishes `transcription_failed`, then separately
moves to `awaiting_operator`. If the second write fails, the stranded explicit
failure state remains continuable.

### 14.3 Rewind continuation

Configured continuation applies `resume_rewind_seconds` only after explicit
operator action.

For a positive interval:

```text
committed_end = end_ms of final contiguous result
boundary = max(0, committed_end - rewind_ms)
```

The discarded suffix begins at the earliest committed result intersecting that
window. Every later result is also removed so JSONL remains one global prefix,
including when overlapping speakers have non-monotonic end times.

Before replacement, Rust records
`transcription_resume_prepared_<sequence>`. After exact result replacement and
partial-text reconstruction, it records
`transcription_resume_applied_<sequence>` while returning to `transcribing`.
An interrupted retry reapplies the same target rather than calculating a second
rewind. Failure without forward progress reuses the previous attempt boundary;
new progress permits one new calculation.

### 14.4 Completion and display repair

After zero worker exit, Rust requires one exact completed result for every work
item, deterministically rebuilds partial text from JSONL, atomically renames it
to `transcription/transcript.txt`, synchronises the directory, and transitions
to `complete` while retaining the lease.

`rebuild-transcript` validates only complete work and result authority. It
deliberately does not return to recording journals, participant snapshots,
track manifests, or audio.

### 14.5 Complete replacement retranscription

`retranscribe` validates an existing complete format-5 or format-6 transcript,
then calls the normal work-item builder over authoritative recording evidence
with the current merge setting and immutable session snapshot. The production
worker receives unreferenced generation paths and always starts at sequence 1.

After exact result validation, Rust renders the final text into the same staged
generation. `SessionStore::publish_retranscription_complete` then performs the
only authority mutation: one atomic format-6 `session.json` replacement names
all three files. A worker, validation, crash or metadata-publication failure
therefore leaves the old complete paths and `complete` state intact. Failed
generation directories are non-authoritative.

Historical snapshots are not refreshed from configured participant data.
`set-transcription-policy` is the narrow exception: under the same lease it
atomically changes only one materialised `transcribe` Boolean. Missing entries
are inserted with the normal player defaults. The operator must invoke
`retranscribe` separately to publish a replacement transcript.

## 15. Exclusive offline ownership

Every offline command which mutates session authority or a declared artefact
uses `SessionOperationLease`:

- routine recovery;
- recording continuation;
- work-item generation;
- transcription and transcription continuation;
- complete replacement retranscription;
- completed-session transcription-policy migration;
- stage-aware configured continuation;
- final transcript rebuilding.

The lease is acquired before loading `session.json` or resolving any
session-declared path and is held through validation, file publication, worker
lifetime, and workflow updates. This prevents a delayed operation from carrying
stale in-memory authority across another command's successful update.

The lock is an operating-system file lock on `transcription/worker.lock`. The
file itself is incidental and is not listed in `session.json`. Rust duplicates
the locked handle into the Python child's stdin, so an orphan child retains
ownership. Process death releases ownership without PID records, stale-time
guesses, or manual lock deletion.

Read-only inspection and verification do not acquire the lease.

## 16. One-stop stage coordination

The live period is outside the offline operation lease. After clean recording,
`orchestration.rs` acquires the lease, reloads authority, and retains ownership
through work construction, result publication, Python, transcript publication,
and completion.

`StageError` distinguishes two failure classes:

- **refused**: configuration, state, or input validation failed before the
  stage accepted publication; workflow authority remains untouched;
- **accepted**: the stage passed validation and failed while publishing or
  executing; the coordinator records a durable stage failure and moves a
  ready session to `awaiting_operator` where possible.

Configured continuation routes by current format, state, artefact descriptions,
checkpoints, and matching latest failure evidence. It does not route solely on
historical failures. A stopped manifest/pre-results stage resumes directly at
that boundary; a real recording failure still undergoes full recording
continuation validation.

Internal stage functions accept an already-held lease. Do not call a public
entry point which tries to reacquire the same lease from inside the coordinator.

## 17. Durability patterns

Use these established patterns for persistent changes.

### 17.1 Atomic authority replacement

For JSON/TOML/JSONL files which are replaced:

1. create a session-local temporary file;
2. write the complete new value;
3. flush language/runtime buffering;
4. synchronise file data;
5. rename over the final path;
6. synchronise the containing directory.

Do not publish a checkpoint separately from the file reference it certifies.
For example, the work-item description and `work_manifest_built` checkpoint
are one `session.json` replacement.

### 17.2 Append authority

For transcription results:

1. append one complete JSON line;
2. flush;
3. synchronise;
4. only then append and synchronise the human text line.

The structured result wins after a crash between those steps.

### 17.3 Incomplete filename lifecycle

Derived audio uses `.part` until encoder finalisation and storage publication
are complete. Do not infer completeness by opening a partial file.

### 17.4 Startup and failure publication

Publish workflow claims last. A component must not leave `session.json`
claiming that a stage started or completed when prerequisite file creation
failed.

Durable failure recording is itself subject to storage failure. Error messages
must distinguish the original fault from failure to publish its durable record;
code must not claim durability which was not achieved.

## 18. How to extend EchoScribe safely

### 18.1 Add a configuration setting

1. Decide whether the setting belongs to live configuration, an offline stage,
   or both.
2. Add it to the strict on-disk `File*` schema in `config.rs`.
3. Validate it explicitly and expose only the resolved value needed by each
   consumer.
4. Preserve offline loaders' independence from Discord credentials and the
   mutable participant source.
5. Update `echoscribe.example.toml`, this guide, and focused parsing tests.
6. Tell operators that deployed root TOML files need updating; changing the
   example alone does not update their ignored local configuration.

Unknown fields are denied and missing required fields fail. A default is a
behaviour decision, not merely a Serde convenience.

### 18.2 Add or change a persistent artefact

1. Decide whether it is authority, a cross-stage product, or incidental output.
2. Add canonical path/format constants to `artifacts.rs` where appropriate.
3. Use session-relative paths and reject traversal.
4. Version the owning format when older readers would reject the new shape.
5. Publish file data before adding its description/checkpoint to
   `session.json`.
6. Define recovery, compatibility, and deletion semantics before coding.

Do not expand the session manifest to list every temporary, cache, lock, or
diagnostic file merely because it exists.

### 18.3 Add a mutating offline command

1. Parse all operator-controlled arguments/configuration which can be rejected
   cleanly.
2. Canonicalise the session directory.
3. Acquire `SessionOperationLease`.
4. Load and validate `session.json` only after ownership.
5. Keep every declared-path read, derived write, and workflow update inside the
   lease.
6. Split public acquisition from an internal `*_with_lease` entry if the
   one-stop coordinator may call it.
7. Classify pre-acceptance refusal separately from an accepted-stage failure.

Never treat synchronised append writes as a substitute for exclusive session
ownership.

### 18.4 Add a live derived consumer

Attach it downstream of the authoritative consumer through a bounded queue.
The authoritative path must use a non-blocking enqueue and retain distinct
metrics. Define queue-full, worker-error, shutdown-drain, and abandonment
semantics before allowing the output to influence workflow state.

Do not add filesystem, codec, Python, HTTP, or unbounded buffering work to a
Songbird callback.

### 18.5 Extend identity or event evidence

SSRC evidence must remain timestamped. New events need clear replay semantics,
including how disconnect or replacement evidence affects current mappings.

If a new JSON event shape would not be accepted by an older strict reader,
bump the event format and retain explicit compatibility for supported earlier
formats. Numeric Discord IDs remain decimal strings in JSON authorities.

### 18.6 Add a transcription result field

The same field must be considered in:

- work-item/result format versions;
- Rust Serde structures and exact provenance validation;
- Python strict schema and result construction;
- restart-prefix validation;
- transcript rebuild where relevant;
- compatibility tests.

Do not silently add a field under the same strict format number.

### 18.7 Add future VAD boundary refinement

The current worker-side VAD is an accept/reject presence gate and deliberately
does not alter ranges. If later evidence justifies boundary adjustment or
splitting, implement `RangeRefiner` rather than bypassing the playout-derived
candidate pipeline. Preserve:

- per-user aligned sample ranges;
- global deterministic ordering;
- source bounds.

Do not call archived scripts from the application.

### 18.8 Add future live transcription

The intended seam is another bounded consumer after resolved user PCM fan-out:

```text
resolved user PCM
    -> utterance assembler
    -> chronological queue
    -> persistent Python worker
    -> structured running transcript
```

It must not replace packet/playout/event authority, routine FLAC, stable user
identity, or the retained structured transcript contract. GM assistance is a
later consumer of structured output, not a reason to filter the master record.

## 19. Testing and verification

Tests live beside Rust modules under `#[cfg(test)]`; Python worker tests live in
`workers/faster-whisper/tests/`.

Prefer focused checks during development:

```sh
cargo test config::tests
cargo test identity::tests
cargo test capture::tests
cargo test transcription::tests
cargo test orchestration::tests
cargo check
cargo fmt --all --check
```

Run Python worker tests without loading a real model:

```sh
./.venv/bin/python -m unittest discover \
  -s workers/faster-whisper/tests -p 'test_*.py'
```

Windows PowerShell:

```powershell
.\.venv\Scripts\python.exe -m unittest discover `
  -s workers\faster-whisper\tests -p "test_*.py"
```

The worker tests use fake models/range extractors and deliberately small local
audio. Unit tests must not install dependencies, contact Discord, download a
model, invoke CUDA, or mutate real recordings.

Live Discord and GPU acceptance are separate operator-run checks. Useful live
evidence includes queue drops/high-water, SSRC mapping timing, track growth,
shutdown disposition, FLAC playback, model load, and final transcript quality.

Passing tests establish the exercised behaviour. They do not replace review of
authority, failure, shutdown, or recovery semantics.

## 20. Code style and review landmarks

Comments should explain boundaries, ownership, invariants, failure semantics,
persistent formats, and non-obvious reasoning. Avoid narrating self-explanatory
syntax.

When reviewing a change, ask:

- Which artefact is authoritative?
- Can this work block a callback or the authoritative consumer?
- Is every queue bounded, observable, and failure-defined?
- Is user identity proven rather than guessed?
- Can a stale command overwrite newer session authority?
- Does a filename claim more health than was durably established?
- Can a crash repeat a destructive rewind or publish an impossible state?
- Are historical evidence and current blocking faults kept distinct?
- Does the change preserve deterministic replay and ordering?
- Are persistent compatibility and path containment explicit?

## 21. Legacy implementation

`archive/legacy-pipeline/` preserves the former Discord.js/Python stack,
including its detailed pipeline document. It is not imported, executed, or
installed by the current application.

The archive remains valuable as historical evidence and as a source for the
short-utterance burst-rescue policy. Current code must reimplement useful ideas
through present interfaces rather than creating runtime dependencies on the
archive.

## 22. Deliberate current boundaries

The following are extension points, not partially hidden features:

- `vad_enabled` controls a post-extraction speech-presence gate; the current
  range refiner remains a no-op and published ranges are unchanged;
- transcription begins after recording, not live;
- one Python worker handles one session;
- work items do not condition Whisper on previous item text;
- the master transcript performs no relevance filtering;
- there is no AAR or GM-assist stage;
- diagnostic WAV and SSRC-keyed export remain separate from routine user FLAC;
- automatic recording recovery and automatic transcription retry are forbidden.

Build new functionality onto the existing authority and fan-out contracts. Do
not make routine output faster or more convenient by weakening the evidence
needed to know whether a session is complete.
