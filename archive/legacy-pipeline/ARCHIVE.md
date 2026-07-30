# Legacy EchoScribe pipeline

This directory preserves the complete pre-rewrite EchoScribe stack as a
historical unit:

- Discord.js voice capture in `index.ts`;
- per-utterance WAV validation and filtering in `dedupe_audit.py`;
- short-utterance energy analysis in `burst_scope.py`;
- the original faster-whisper transcriber;
- transcript similarity deduplication;
- the Node and Python dependency descriptions;
- the original README and detailed pipeline document.

The code is retained for reference and is not part of the current Rust/Songbird
recording and transcription workflow.

## Short-utterance rescue

The `dedupe_audit.py` and `burst_scope.py` combination contains a useful policy
which must not be lost when optional VAD refinement is introduced:

```text
VAD rejects a short candidate
    -> inspect its overall and frame-by-frame energy
    -> rescue plausible bursty speech
```

The current implementation deliberately retains short playout-derived work
items without VAD. A future VAD implementation should adapt and test this
rescue policy through the approved range-refinement boundary rather than
calling the archived scripts directly.
