# EchoScribe — Repository Instructions

## Authority

The global Tromador Codex Working Agreement applies in full.

`docs/DEVELOPER_GUIDE.md` is the primary repository guide for current
architecture, ownership, authority, formats, failure semantics, and extension
guidance.

`docs/USER_GUIDE.md` describes current user-visible installation,
configuration, operation, recovery, and troubleshooting behaviour.

Inspect the relevant source code and tests before changing behaviour.
Historical notes and archived material are not authority for current
behaviour. If maintained documentation and implementation conflict, report the
conflict and resolve it explicitly rather than silently choosing one.

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

## Control of changes

Work only within the explicitly authorised scope.

Before editing, state:

1. the intended externally observable result;
2. the implementation route;
3. the files expected to change;
4. the tests or checks expected to run;
5. the explicit non-goals.

Stop when the scoped change is complete. Do not begin adjacent work.

If implementation reveals a conflict with maintained documentation or current
behaviour, stop and report it. Do not silently select a replacement design.

## Live and external operations

- Codex must not connect to Discord.
- Tromador runs all live Discord tests.
- Do not deploy or copy work to Mort.
- Do not invoke cloud transcription or Runpod.
- Do not install dependencies, push, or publish without explicit permission for that action.

## Commit workflow

When an authorised scoped change or correction is complete and locally
verified, commit the scoped changes without waiting for separate permission.
Tromador normally pushes the commit. Do not push unless he explicitly asks.

## Verification

Safe, understood, narrowly targeted local checks may be run during an authorised slice.

Live acceptance evidence is supplied by Tromador.

Passing tests do not prove that the architecture is correct. Verification must
cover the requirements and failure semantics of the scoped change.

## Code comments

Use concise, useful comments where they help a human or agentic developer understand,
review, or extend the code. Prioritise architectural boundaries, ownership, invariants,
failure semantics, persistent formats, and non-obvious reasoning. Do not turn the source
into extensive inline documentation or narrate code which is already self-explanatory.
