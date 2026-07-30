# EchoScribe — Repository Instructions

## Authority

The global Tromador Codex Working Agreement applies in full.

The following repository documents are normative and must be read before planning or changing EchoScribe:

1. `docs/SPECIFICATION.md` — required product behaviour and scope.
2. `docs/ARCHITECTURE.md` — approved technical architecture and failure semantics.
3. `docs/IMPLEMENTATION_PLAN.md` — authorised implementation sequence and slice boundaries.

Where source code differs from those documents, the source code describes current implementation state; it does not override the approved design.

Do not use historical notes, old pipeline documentation, comments, tests, existing dependencies, or currently implemented workflows as authority to change the approved design.

## Project baseline

- Serenity remains the Discord gateway library.
- Songbird remains the Discord voice library.
- The durable packet, playout, and event journals remain the recoverable recording authority.
- Routine per-user FLAC tracks are written incrementally during recording.
- Routine FLAC creation must not require a mandatory whole-session post-recording export.
- Faster-whisper runs locally through a separate Python worker process.
- The current build must remain extensible to future live transcription and GM-assist, but neither is implemented now.
- The master transcript records everything captured. Relevance filtering belongs to later AAR processing.
- Mort is outside EchoScribe's deployment and processing scope.

## Control of implementation

Implement only one approved slice from `docs/IMPLEMENTATION_PLAN.md` at a time.

Before editing, state:

1. the slice being implemented;
2. the intended externally observable result;
3. the implementation route;
4. the files expected to change;
5. the tests or checks expected to run;
6. the explicit non-goals.

Stop when the slice acceptance criteria have been addressed. Do not begin the next slice.

If implementation reveals a conflict with the specification or architecture, stop and report it. Do not silently select a replacement design.

## Live and external operations

- Codex must not connect to Discord.
- Tromador runs all live Discord tests.
- Do not deploy or copy work to Mort.
- Do not invoke cloud transcription or Runpod.
- Do not install dependencies, push, or publish without explicit permission for that action.

## Commit workflow

When an authorised implementation slice or correction is complete and locally
verified, commit the scoped changes without waiting for separate permission.
Tromador normally pushes the commit. Do not push unless he explicitly asks.

## Verification

Safe, understood, narrowly targeted local checks may be run during an authorised slice.

Live acceptance evidence is supplied by Tromador.

Passing tests do not prove that the architecture is correct. Verification must cover the requirements and failure semantics of the active slice.

## Code comments

Use concise, useful comments where they help a human or agentic developer understand,
review, or extend the code. Prioritise architectural boundaries, ownership, invariants,
failure semantics, persistent formats, and non-obvious reasoning. Do not turn the source
into extensive inline documentation or narrate code which is already self-explanatory.
