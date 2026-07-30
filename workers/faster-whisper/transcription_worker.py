#!/usr/bin/env python3
"""Sequential faster-whisper worker for one EchoScribe session."""

import argparse
import json
import os
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path


WORK_ITEM_FORMAT = 1
RESULT_FORMAT = 1
SOURCE_SAMPLE_RATE = 48_000
MAX_END_ROUNDING_FRAMES = 47
WORK_ITEM_FIELDS = {
    "format",
    "id",
    "session_id",
    "sequence",
    "discord_user_id",
    "speaker",
    "role",
    "character",
    "start_ms",
    "end_ms",
    "source",
    "source_start_ms",
    "source_end_ms",
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Transcribe one EchoScribe work manifest in chronological order."
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--session", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--transcript", type=Path, required=True)
    parser.add_argument("--start-sequence", type=int, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--language", required=True)
    parser.add_argument("--device", required=True)
    parser.add_argument("--compute-type", required=True)
    parser.add_argument("--beam-size", type=int, required=True)
    parser.add_argument("--hotword", action="append", default=[])
    return parser.parse_args(argv)


def load_manifest(path):
    items = []
    with path.open("rb") as manifest:
        for line_number, line in enumerate(manifest, start=1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise ValueError(
                    f"malformed work manifest record at line {line_number}: {error}"
                ) from error
            validate_work_item(item, len(items) + 1)
            items.append(item)
    return items


def validate_work_item(item, expected_sequence):
    if not isinstance(item, dict) or set(item) != WORK_ITEM_FIELDS:
        raise ValueError(
            f"work item sequence {expected_sequence} has an unexpected schema"
        )
    if (
        item["format"] != WORK_ITEM_FORMAT
        or item["sequence"] != expected_sequence
        or not isinstance(item["id"], str)
        or not item["id"]
        or not isinstance(item["session_id"], str)
        or not item["session_id"]
        or not isinstance(item["discord_user_id"], str)
        or not item["discord_user_id"].isdigit()
        or not isinstance(item["speaker"], str)
        or not item["speaker"].strip()
        or item["role"] not in ("player", "gm")
        or (
            item["character"] is not None
            and not isinstance(item["character"], str)
        )
        or not isinstance(item["start_ms"], int)
        or not isinstance(item["end_ms"], int)
        or item["start_ms"] >= item["end_ms"]
        or not isinstance(item["source_start_ms"], int)
        or not isinstance(item["source_end_ms"], int)
        or item["source_start_ms"] >= item["source_end_ms"]
        or not isinstance(item["source"], str)
        or not item["source"]
    ):
        raise ValueError(f"invalid work item sequence {expected_sequence}")


def default_model_factory(args):
    try:
        from faster_whisper import WhisperModel
    except ImportError as error:
        raise RuntimeError(
            "faster-whisper is not installed in the selected Python environment"
        ) from error

    return WhisperModel(
        args.model,
        device=args.device,
        compute_type=args.compute_type,
    )


@contextmanager
def extract_audio_range(source_path, start_ms, end_ms):
    """Materialise one bounded source range so Whisper never sees other items."""
    try:
        import soundfile
    except ImportError as error:
        raise RuntimeError(
            "soundfile is not installed in the selected Python environment"
        ) from error

    temporary_path = None
    try:
        with soundfile.SoundFile(source_path) as source:
            sample_rate = source.samplerate
            if sample_rate != SOURCE_SAMPLE_RATE or source.channels != 1:
                raise ValueError(
                    f"routine source {source_path} is not mono 48 kHz audio"
                )
            start_frame = start_ms * sample_rate // 1_000
            requested_end_frame = (end_ms * sample_rate + 999) // 1_000
            end_overrun = max(0, requested_end_frame - len(source))
            if end_overrun > MAX_END_ROUNDING_FRAMES:
                raise ValueError(
                    f"source {source_path} is {end_overrun} frames shorter than "
                    f"the requested range ending at {end_ms} ms"
                )
            # Work-item milliseconds may round the physical final sample
            # upwards by 47 frames, but a larger shortfall is damaged input.
            end_frame = min(requested_end_frame, len(source))
            if start_frame >= end_frame:
                raise ValueError(
                    f"source range {start_ms}..{end_ms} ms is outside {source_path}"
                )
            source.seek(start_frame)
            audio = source.read(
                end_frame - start_frame, dtype="float32", always_2d=True
            )

        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as temporary:
            temporary_path = Path(temporary.name)
        soundfile.write(
            temporary_path,
            audio,
            sample_rate,
            format="WAV",
            subtype="PCM_16",
        )
        yield temporary_path
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def committed_result_count(path):
    data = path.read_bytes()
    if data and not data.endswith(b"\n"):
        raise ValueError("results authority has an unreconciled truncated record")
    return sum(1 for line in data.splitlines() if line.strip())


def normalise_text(segments):
    # One work item must remain one physical transcript line.
    parts = []
    for segment in segments:
        text = " ".join(segment.text.split())
        if text:
            parts.append(text)
    return " ".join(parts)


def result_from_item(item, text):
    return {
        "format": RESULT_FORMAT,
        "work_item_id": item["id"],
        "session_id": item["session_id"],
        "sequence": item["sequence"],
        "discord_user_id": item["discord_user_id"],
        "speaker": item["speaker"],
        "role": item["role"],
        "character": item["character"],
        "start_ms": item["start_ms"],
        "end_ms": item["end_ms"],
        "source": item["source"],
        "source_start_ms": item["source_start_ms"],
        "source_end_ms": item["source_end_ms"],
        "text": text,
        "status": "complete",
    }


def transcript_line(result):
    elapsed_seconds = result["start_ms"] // 1_000
    hours = elapsed_seconds // 3_600
    minutes = elapsed_seconds % 3_600 // 60
    seconds = elapsed_seconds % 60
    return (
        f"[{hours:02d}:{minutes:02d}:{seconds:02d}] "
        f"{result['speaker']}: {result['text']}\n"
    )


def append_and_sync(path, data):
    with path.open("ab") as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())


def process(args, model_factory=default_model_factory, range_extractor=extract_audio_range):
    items = load_manifest(args.manifest)
    if args.start_sequence < 1 or args.start_sequence > len(items) + 1:
        raise ValueError("start sequence is outside the work manifest")
    if committed_result_count(args.results) != args.start_sequence - 1:
        raise ValueError("results prefix does not match the requested start sequence")

    session_directory = args.session.resolve()
    model = model_factory(args)
    hotwords = ", ".join(args.hotword) or None

    for item in items[args.start_sequence - 1 :]:
        source_path = (session_directory / item["source"]).resolve()
        try:
            source_path.relative_to(session_directory)
        except ValueError as error:
            raise ValueError(
                f"work item {item['id']} source escapes the session directory"
            ) from error

        with range_extractor(
            source_path, item["source_start_ms"], item["source_end_ms"]
        ) as ranged_audio:
            segments, _ = model.transcribe(
                str(ranged_audio),
                beam_size=args.beam_size,
                language=args.language,
                condition_on_previous_text=False,
                hotwords=hotwords,
            )
            text = normalise_text(segments)

        result = result_from_item(item, text)
        encoded = (
            json.dumps(result, ensure_ascii=False, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        append_and_sync(args.results, encoded)
        append_and_sync(args.transcript, transcript_line(result).encode("utf-8"))


def main(argv=None):
    args = parse_args(argv)
    try:
        process(args)
    except Exception as error:  # Worker diagnostics belong on the process boundary.
        print(f"EchoScribe transcription worker failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
