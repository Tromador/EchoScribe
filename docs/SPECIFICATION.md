# EchoScribe Product Specification

## Status and authority

**Status: Normative project specification**

This document defines the required product behaviour of EchoScribe.

Implementation details are governed by `ARCHITECTURE.md`. Work sequencing is governed by `IMPLEMENTATION_PLAN.md`.

## 1. Purpose

EchoScribe records tabletop role-playing sessions conducted over Discord and produces a complete, readable transcript suitable for automated downstream processing, including after-action reports.

The immediate product is a reliable recording and transcription device.

EchoScribe must also preserve a clean extension path towards future live transcription and GM-assist features. Those features are deliberately outside the current implementation scope.

## 2. Primary workflow

For a normal session, one operator invocation must coordinate:

```text
join configured Discord voice channel
    -> record until explicitly stopped
    -> finalise routine speaker tracks
    -> transcribe in chronological order
    -> produce structured and human-readable transcripts
```

Each stage must also remain separately invocable for diagnosis, recovery, or reprocessing.

A known recording or transcription failure stops the normal workflow and waits for operator action.

## 3. Recording requirements

EchoScribe must:

- use Serenity for the Discord gateway;
- use Songbird for Discord voice acquisition;
- support current Discord voice encryption and DAVE through Songbird;
- receive decoded mono PCM for each speaker;
- preserve speaker identity information where Discord provides it;
- record until explicitly stopped;
- leave the configured voice channel cleanly on normal shutdown;
- keep live event callbacks bounded and non-blocking;
- make queue pressure, dropped work, and writer failure observable.

Songbird is the approved voice implementation. No further Discord voice-library evaluation is required unless evidence demonstrates that Songbird cannot meet a specific requirement.

## 4. Durable capture

EchoScribe must preserve sufficient durable evidence to recover a session if a derived audio track is damaged, incomplete, or unavailable.

The authoritative recoverable recording consists of:

- decrypted packet records;
- playout decisions;
- speaker-mapping events;
- session metadata.

A crash may lose a bounded amount of recently buffered data. It must not render the complete session unrecoverable merely because a derived FLAC or WAV file was not finalised.

Derived tracks are products of the recording. They are not the sole recording authority.

## 5. Routine speaker tracks

EchoScribe must write routine speaker tracks incrementally during the live session.

Requirements:

- one routine track per stable Discord user ID;
- lossless FLAC;
- mono;
- 48 kHz;
- 16-bit PCM source precision;
- session-aligned timing;
- leading and internal silence where required to preserve the shared timeline;
- clean finalisation on normal shutdown;
- regeneration from the durable journals when required.

SSRC remains transport evidence. A participant who changes SSRC during the session must remain one routine speaker track.

Live tracks are written with an incomplete name ending in `.flac.part`. A track is renamed atomically to `.flac` only after successful finalisation and storage synchronisation.

Routine track creation must not require this normal post-session path:

```text
complete session
    -> decode all journals
    -> encode all speaker tracks
```

Offline export remains available for recovery, regeneration, migration, and diagnosis. It is not the routine producer of session FLAC tracks.

## 6. Diagnostic WAV output

Live diagnostic WAV output is opt-in.

Normal recording produces:

- durable journals;
- routine live FLAC tracks;
- session metadata.

Diagnostic mode may additionally produce WAV files for capture, alignment, or codec investigation.

WAV recovery from the journals remains available regardless of diagnostic mode.

## 7. Participants and character context

Transcript attribution represents the player, not the character.

The primary human speaker name is the Discord server display name observed during the session. Fallbacks may use other Discord identity fields and finally the numeric Discord user ID.

A separate human-readable TOML file supplies optional downstream context:

- Discord user ID;
- current character name;
- role such as `player` or `gm`.

The main EchoScribe TOML file references this participant file.

The participant file is not a campaign database, lore store, or historical state system. It describes the context needed to process the current session.

Multiple participants may have the `gm` role.

A participant missing from the mapping file is recorded and transcribed normally. Missing character or role context is a warning, not a recording or transcription failure. An unspecified role defaults to `player`.

## 8. Transcription

EchoScribe transcribes locally on Zen using faster-whisper and CTranslate2.

The initial implementation uses:

- one Python worker process per transcription session;
- one model load per worker process;
- globally chronological work-item processing;
- time-ranged work items referencing aligned per-user FLAC tracks;
- retained JSONL work and result records;
- simultaneous human-readable transcript output.

The initial implementation may begin transcription after recording has stopped and all required tracks have finalised successfully.

The transcription design must not assume that completed whole-session files are the only possible source of transcription audio.

## 9. Transcription range generation

Post-session transcription candidates are derived from recorded playout activity.

Nearby activity from the same participant is merged across a configurable silence gap to avoid arbitrary fragmentation of continuous thought.

The exact merge threshold is a tuning parameter, not a permanent architectural constant.

Range generation must be composable so that VAD can later:

- refine candidate boundaries;
- reject non-speech;
- rescue short speech;
- supplement playout-derived activity.

VAD is not required in the first implementation, but the architecture must never make it difficult to introduce.

## 10. Transcription content and ordering

The master transcription records everything captured:

- in-character dialogue;
- out-of-character game discussion;
- rules discussion;
- jokes and social conversation;
- unrelated chatter.

EchoScribe does not decide what is relevant to the game or AAR.

Relevance filtering belongs to downstream AAR processing, which consumes the structured transcript without altering the source record.

Work items are processed globally by session-relative start time. Per-speaker batch processing that destroys conversational ordering is not permitted.

Initial work items are transcribed independently. Cross-item Whisper conditioning may be evaluated later using real recordings.

## 11. Structured transcript

EchoScribe retains machine-readable JSONL records for:

- transcription work items;
- committed transcription results.

The structured results are the durable authority for transcript reconstruction and automated downstream processing.

Each committed result must include enough information to identify:

- session;
- work item;
- global sequence;
- Discord user;
- human speaker name;
- optional role and character;
- session-relative start and end time;
- source track and source range;
- transcription text;
- overlap information or sufficient timing to derive it.

The structured transcript is suitable for later AAR processing and future GM-assist.

## 12. Human-readable transcript

EchoScribe writes a UTF-8 plain-text transcript while transcription proceeds.

Format:

```text
[HH:MM:SS] Speaker: speech
```

Requirements:

- chronological order;
- elapsed session timestamps;
- player attribution;
- one completed transcription unit per line;
- no SSRC identifiers;
- no confidence scores;
- no word-by-word timestamps;
- no karaoke-style annotation;
- no relevance filtering.

Overlapping speakers are represented as separate lines ordered by start time. The normal text transcript does not add overlap labels. Exact timing and overlap remain available in JSONL.

During incomplete processing, the file is named:

```text
transcript.partial.txt
```

When all required work items have committed successfully, it is finalised as:

```text
transcript.txt
```

If a crash leaves JSONL and text output inconsistent, the text file is reconciled from committed JSONL results. Audio is not retranscribed merely to repair the display file.

## 13. Failure behaviour

### 13.1 Recording authority

Priority order under resource pressure is:

1. authoritative journals and session state;
2. routine FLAC tracks;
3. transcription;
4. future GM-assist.

A lower-priority failure must not damage the authoritative recording path.

### 13.2 FLAC encoder failure

If a live FLAC encoder fails:

- continue authoritative journal capture;
- abandon the affected live track for the remainder of the session;
- leave or mark the `.flac.part` incomplete;
- record the failure durably;
- do not start replacement writers;
- do not repeatedly retry;
- do not silently produce a gapped file as though it were complete.

The track may be regenerated later from the journals after operator investigation.

### 13.3 FLAC backlog

Sustained FLAC queue growth is a performance defect requiring investigation.

EchoScribe must record:

- current queue depth;
- high-water mark;
- sustained growth or threshold crossings;
- abandonment reason;
- affected track or tracks.

Queues must remain bounded.

If continuity is lost because the FLAC queue fills, the affected writer is abandoned as for encoder failure. Emergency containment is not accepted as normal operation.

### 13.4 End of session after recording failure

When recording stops with any required FLAC incomplete:

- stop the normal pipeline;
- do not recover automatically;
- do not start transcription;
- report the affected tracks and relevant recorded diagnostics;
- wait for operator instructions.

The operator must be able to investigate disk capacity, hung processes, CPU pressure, shared encoder failure, or other causes before applying further load.

### 13.5 Transcription failure

There is no automatic retry after a transcription worker failure.

EchoScribe must:

- retain committed JSONL results;
- retain `transcript.partial.txt`;
- record the failed work item and worker diagnostics;
- mark the session as requiring operator action;
- stop the normal pipeline.

## 14. Recovery and continuation

Recovery and continuation are separate explicit operations.

Conceptually:

```text
echoscribe recover <session>
echoscribe continue <session>
```

`recover` regenerates selected failed or incomplete derived tracks from the authoritative journals. It does not implicitly start transcription.

`continue` validates durable session state and required artefacts before resuming the normal workflow.

For transcription continuation:

- resume from the last globally contiguous committed work item;
- rewind by a configurable interval;
- invalidate or supersede results intersecting the rewind window;
- rebuild the partial text transcript from retained committed JSONL;
- resume chronological processing without duplicating transcript lines.

The main TOML setting is:

```toml
[transcription]
resume_rewind_seconds = 120
```

The documented default is 120 seconds. `0` disables rewind. A one-off CLI override may be provided.

## 15. Persistent session state

EchoScribe must record durable session state explicitly.

Commands must validate that state rather than infer the entire workflow solely from filenames.

The state model must distinguish at least:

- recording;
- recording stopped cleanly;
- recording stopped with incomplete tracks;
- awaiting operator action;
- ready for transcription;
- transcribing;
- transcription failed;
- complete.

Failures and recovery actions must be recorded with enough information to explain why the session stopped and what action is required.

## 16. Normal orchestration

Normal use is one-stop.

The application coordinates recording, finalisation, transcription, and transcript completion without requiring several terminal windows or manually chained commands.

Individual stages remain callable for:

- diagnostics;
- recovery;
- continuation;
- retranscription;
- transcript rebuilding.

A known failure always stops automatic orchestration at the safe boundary.

## 17. Future live transcription and GM-assist

The current implementation does not include:

- live utterance assembly;
- live faster-whisper processing;
- GM and player queues;
- GM queue priority;
- code-word detection;
- AI assistant interaction;
- Puppeteer or replacement browser automation;
- GM-assist response delivery.

The architecture must nevertheless allow the future path:

```text
live per-user PCM
    -> utterance assembly
    -> chronological transcription queue
    -> running transcript
    -> optional GM-assist consumer
```

Adding those features must not require replacement of:

- Songbird capture;
- authoritative journals;
- live PCM fan-out;
- routine FLAC recording;
- structured transcript records.

## 18. Operating environment

The primary operating environment is Zen:

- local execution;
- RTX 5090 with 32 GB VRAM;
- 64 GB system RAM;
- sufficient local storage for long sessions and recovery data.

Avoiding a long mandatory post-session audio-encoding phase is a product requirement.

Mort is outside EchoScribe's deployment and processing scope.

## 19. Current non-goals

The current build does not include:

- another Discord voice-library evaluation;
- cloud transcription;
- Craig or Runpod integration;
- live transcription;
- GM-assist;
- AAR generation;
- word-level timestamps;
- automatic relevance filtering;
- a graphical interface;
- deployment to Mort;
- automatic recovery after a known failure;
- automatic transcription retry;
- enterprise infrastructure.

## 20. Acceptance criteria for the first complete version

The first complete version must demonstrate that it can:

1. record a real multi-person Discord RPG session through Songbird;
2. retain healthy authoritative journals under expected load;
3. merge SSRC changes into one routine track per Discord user;
4. write aligned FLAC tracks incrementally during recording;
5. finalise healthy tracks by atomic `.flac.part` to `.flac` rename;
6. isolate FLAC failure or backlog from authoritative journal capture;
7. stop for operator action after known recording or transcription failure;
8. recover selected tracks explicitly from the journals;
9. resume explicitly through durable session state;
10. generate chronological time-ranged transcription work items;
11. run one local faster-whisper Python worker per session;
12. write retained JSONL and plain-text output together;
13. produce the specified complete transcript without a mandatory whole-session audio-encoding pass;
14. preserve a documented path to future live chronological transcription.
