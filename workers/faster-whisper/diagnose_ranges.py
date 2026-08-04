#!/usr/bin/env python3
"""Replay selected EchoScribe work items for transcription diagnosis."""

import argparse
import json
import math
import sys
import tempfile
import time
import tomllib
from contextlib import contextmanager
from pathlib import Path

import transcription_worker as production_worker


SESSION_FILE_NAME = "session.json"
SUPPORTED_CONFIG_VERSION = 1
SUPPORTED_WORK_MANIFEST_FORMAT = 1
VAD_SAMPLE_RATE = 16_000
FRAME_RMS_MILLISECONDS = 20

CURRENT_DECODE = "current_echoscribe"
NO_HOTWORD_DECODE = "no_hotword_control"
CRAIG_LIKE_DECODE = "craig_like_internal_vad"
EXPLICIT_TRIM_DECODE = "explicit_silero_trimmed"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Replay selected EchoScribe work-item ranges through diagnostic "
            "faster-whisper configurations."
        )
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--session", type=Path, required=True)
    parser.add_argument("--sequence", type=int, action="append", required=True)
    parser.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def load_diagnostic_config(path):
    """Load production transcription settings without Discord validation."""
    try:
        config = tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ValueError(f"configuration file is missing: {path}") from error
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as error:
        raise ValueError(
            f"failed to read configuration file {path}: {error}"
        ) from error

    if (
        not isinstance(config, dict)
        or config.get("version") != SUPPORTED_CONFIG_VERSION
    ):
        raise ValueError(
            "unsupported or missing configuration version; expected 1"
        )
    transcription = config.get("transcription")
    if not isinstance(transcription, dict):
        raise ValueError("configuration is missing [transcription]")

    settings = {}
    for field in ("model", "language", "device", "compute_type"):
        value = transcription.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"transcription.{field} must not be empty")
        settings[field] = value

    beam_size = transcription.get("beam_size")
    if (
        not isinstance(beam_size, int)
        or isinstance(beam_size, bool)
        or beam_size <= 0
    ):
        raise ValueError("transcription.beam_size must be greater than zero")
    settings["beam_size"] = beam_size

    vocabulary_value = transcription.get("vocabulary_file")
    if not isinstance(vocabulary_value, str) or not vocabulary_value:
        raise ValueError("transcription.vocabulary_file must not be empty")
    vocabulary_path = Path(vocabulary_value)
    if not vocabulary_path.is_absolute():
        vocabulary_path = path.parent / vocabulary_path
    hotwords, warning = load_vocabulary(vocabulary_path)
    settings["hotwords"] = hotwords
    settings["vocabulary_warning"] = warning
    return settings


def load_vocabulary(path):
    """Faithfully reproduce production vocabulary parsing and warnings."""
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return (
            [],
            f"vocabulary file {path} is missing; continuing without hotwords",
        )
    except OSError as error:
        raise ValueError(
            f"failed to read vocabulary file {path}: {error}"
        ) from error

    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError(
            f"vocabulary file {path} is not valid UTF-8"
        ) from error
    hotwords = [
        line
        for line in (line.strip() for line in text.splitlines())
        if line and not line.startswith("#")
    ]
    warning = None
    if not hotwords:
        warning = (
            f"vocabulary file {path} contains no vocabulary phrases; "
            "continuing without hotwords"
        )
    return hotwords, warning


def load_session_manifest(session_directory):
    if not session_directory.is_dir():
        raise ValueError(f"session directory is missing: {session_directory}")

    session_path = session_directory / SESSION_FILE_NAME
    try:
        session = json.loads(session_path.read_bytes())
    except FileNotFoundError as error:
        raise ValueError(
            f"session metadata is missing: {session_path}"
        ) from error
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(
            f"malformed session metadata {session_path}: {error}"
        ) from error

    try:
        description = session["files"]["work_items"]
        manifest_value = description["path"]
        manifest_format = description["format"]
    except (KeyError, TypeError) as error:
        raise ValueError(
            "session does not declare an existing work manifest"
        ) from error
    if (
        not isinstance(manifest_value, str)
        or not manifest_value
        or manifest_format != SUPPORTED_WORK_MANIFEST_FORMAT
    ):
        raise ValueError("session work-manifest description is malformed")

    manifest_path = resolve_within_session(
        session_directory,
        manifest_value,
        "work manifest",
    )
    try:
        items = production_worker.load_manifest(manifest_path)
    except FileNotFoundError as error:
        raise ValueError(
            f"work manifest is missing: {manifest_path}"
        ) from error
    except OSError as error:
        raise ValueError(
            f"failed to read work manifest {manifest_path}: {error}"
        ) from error
    return items


def select_work_items(items, sequences):
    by_sequence = {item["sequence"]: item for item in items}
    selected = []
    for sequence in sequences:
        item = by_sequence.get(sequence)
        if item is None:
            raise ValueError(f"unknown work-item sequence {sequence}")
        selected.append(item)
    return selected


def resolve_within_session(session_directory, path_value, label):
    session_directory = session_directory.resolve()
    resolved = (session_directory / path_value).resolve()
    try:
        resolved.relative_to(session_directory)
    except ValueError as error:
        raise ValueError(f"{label} escapes the session directory") from error
    return resolved


def resolve_source(session_directory, item):
    source_path = resolve_within_session(
        session_directory,
        item["source"],
        f"work item {item['id']} source",
    )
    if not source_path.is_file():
        raise ValueError(
            f"work item {item['id']} source is missing: {source_path}"
        )
    return source_path


def default_model_factory(settings):
    try:
        from faster_whisper import WhisperModel
    except ImportError as error:
        raise RuntimeError(
            "faster-whisper is not installed in the selected Python "
            "environment"
        ) from error
    return WhisperModel(
        settings["model"],
        device=settings["device"],
        compute_type=settings["compute_type"],
    )


def read_ranged_audio(path):
    try:
        import soundfile
    except ImportError as error:
        raise RuntimeError(
            "soundfile is not installed in the selected Python environment"
        ) from error

    try:
        with soundfile.SoundFile(path) as source:
            if (
                source.samplerate != production_worker.SOURCE_SAMPLE_RATE
                or source.channels != 1
            ):
                raise ValueError(
                    f"diagnostic range {path} is not mono 48 kHz audio"
                )
            samples = source.read(dtype="float32", always_2d=False)
            return samples, source.samplerate
    except ValueError:
        raise
    except Exception as error:
        raise ValueError(
            f"failed to read diagnostic range {path}: {error}"
        ) from error


def amplitude_dbfs(amplitude):
    if amplitude <= 0.0:
        return None
    return 20.0 * math.log10(amplitude)


def root_mean_square(samples):
    if len(samples) == 0:
        return 0.0
    return math.sqrt(
        sum(float(sample) * float(sample) for sample in samples) / len(samples)
    )


def acoustic_measurements(samples, sample_rate):
    if sample_rate <= 0:
        raise ValueError("audio sample rate must be greater than zero")
    sample_count = len(samples)
    peak = max((abs(float(sample)) for sample in samples), default=0.0)
    rms = root_mean_square(samples)

    frame_size = sample_rate * FRAME_RMS_MILLISECONDS // 1_000
    frame_values = [
        root_mean_square(samples[offset:offset + frame_size])
        for offset in range(0, sample_count, frame_size)
    ]
    if frame_values:
        frame_mean = sum(frame_values) / len(frame_values)
        frame_variance = sum(
            (value - frame_mean) ** 2 for value in frame_values
        ) / len(frame_values)
        frame_standard_deviation = math.sqrt(frame_variance)
        frame_maximum = max(frame_values)
    else:
        frame_mean = 0.0
        frame_standard_deviation = 0.0
        frame_maximum = 0.0

    return {
        "sample_rate_hz": sample_rate,
        "sample_count": sample_count,
        "duration_seconds": sample_count / sample_rate,
        "peak_amplitude": peak,
        "peak_dbfs": amplitude_dbfs(peak),
        "rms_amplitude": rms,
        "rms_dbfs": amplitude_dbfs(rms),
        "frame_rms_20ms": {
            "frame_count": len(frame_values),
            "mean": frame_mean,
            "standard_deviation": frame_standard_deviation,
            "maximum": frame_maximum,
        },
    }


def default_vad_analyser(path):
    try:
        from faster_whisper.audio import decode_audio
        from faster_whisper.vad import VadOptions, get_speech_timestamps
    except ImportError as error:
        raise RuntimeError(
            "faster-whisper VAD support is not installed in the selected "
            "Python environment"
        ) from error

    try:
        audio = decode_audio(str(path), sampling_rate=VAD_SAMPLE_RATE)
        production_default = get_speech_timestamps(
            audio,
            VadOptions(),
            sampling_rate=VAD_SAMPLE_RATE,
        )
        unpadded = get_speech_timestamps(
            audio,
            VadOptions(speech_pad_ms=0),
            sampling_rate=VAD_SAMPLE_RATE,
        )
    except Exception as error:
        raise RuntimeError(f"Silero VAD failed for {path}: {error}") from error
    return audio, VAD_SAMPLE_RATE, production_default, unpadded


def normalise_speech_timestamps(timestamps, audio_sample_count):
    normalised = []
    for timestamp in timestamps:
        try:
            start = int(timestamp["start"])
            end = int(timestamp["end"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(
                "Silero returned a malformed speech timestamp"
            ) from error
        if start < 0 or end <= start or end > audio_sample_count:
            raise ValueError(
                "Silero returned an out-of-range speech timestamp "
                f"{start}..{end}"
            )
        normalised.append({"start": start, "end": end})
    return normalised


def vad_measurements(timestamps, audio_sample_count, sample_rate):
    timestamps = normalise_speech_timestamps(timestamps, audio_sample_count)
    total_voiced_samples = sum(
        timestamp["end"] - timestamp["start"] for timestamp in timestamps
    )
    return {
        "sample_rate_hz": sample_rate,
        "speech_timestamps": [
            {
                "start_sample": timestamp["start"],
                "end_sample": timestamp["end"],
                "start_seconds": timestamp["start"] / sample_rate,
                "end_seconds": timestamp["end"] / sample_rate,
            }
            for timestamp in timestamps
        ],
        "total_voiced_duration_seconds": total_voiced_samples / sample_rate,
        "voiced_occupancy_fraction": (
            total_voiced_samples / audio_sample_count
            if audio_sample_count
            else 0.0
        ),
    }


@contextmanager
def materialise_voiced_audio(audio, timestamps, sample_rate):
    """Materialise concatenated unpadded Silero spans as a temporary WAV."""
    try:
        import numpy
        import soundfile
    except ImportError as error:
        raise RuntimeError(
            "NumPy and soundfile are required for diagnostic VAD trimming"
        ) from error

    temporary_path = None
    try:
        clips = [audio[item["start"]:item["end"]] for item in timestamps]
        voiced_audio = numpy.concatenate(clips)
        with tempfile.NamedTemporaryFile(
            suffix=".wav", delete=False
        ) as temporary:
            temporary_path = Path(temporary.name)
        soundfile.write(
            temporary_path,
            voiced_audio,
            sample_rate,
            format="WAV",
            subtype="PCM_16",
        )
        yield temporary_path, len(voiced_audio) / sample_rate
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def configured_hotwords(settings, enabled=True):
    phrases = list(settings["hotwords"]) if enabled else []
    return {
        "enabled": bool(phrases),
        "phrases": phrases,
    }


def transcribe_options(
    settings,
    hotwords,
    internal_vad,
    word_timestamps=False,
):
    options = {
        "beam_size": settings["beam_size"],
        "language": settings["language"],
        "condition_on_previous_text": False,
        "hotwords": hotwords,
        "vad_filter": internal_vad,
    }
    if word_timestamps:
        options["word_timestamps"] = True
    return options


def optional_float(value):
    return None if value is None else float(value)


def serialise_word(word):
    return {
        "start_seconds": optional_float(getattr(word, "start", None)),
        "end_seconds": optional_float(getattr(word, "end", None)),
        "text": getattr(word, "word", ""),
        "probability": optional_float(getattr(word, "probability", None)),
    }


def serialise_segment(segment, include_words):
    words = None
    if include_words:
        words = [
            serialise_word(word)
            for word in (getattr(segment, "words", None) or [])
        ]
    return {
        "start_seconds": optional_float(getattr(segment, "start", None)),
        "end_seconds": optional_float(getattr(segment, "end", None)),
        "text": getattr(segment, "text", ""),
        "temperature": optional_float(getattr(segment, "temperature", None)),
        "average_log_probability": optional_float(
            getattr(segment, "avg_logprob", None)
        ),
        "compression_ratio": optional_float(
            getattr(segment, "compression_ratio", None)
        ),
        "no_speech_probability": optional_float(
            getattr(segment, "no_speech_prob", None)
        ),
        "words": words,
    }


def decode_result(
    model,
    audio_path,
    options,
    settings,
    hotwords_enabled,
    input_duration_seconds,
    retained_source_spans,
    clock=time.perf_counter,
):
    started = clock()
    segment_iterator, _ = model.transcribe(str(audio_path), **options)
    segments = list(segment_iterator)
    elapsed = clock() - started
    include_words = bool(options.get("word_timestamps"))
    serialised_segments = [
        serialise_segment(segment, include_words) for segment in segments
    ]
    segment_ends = [
        segment["end_seconds"]
        for segment in serialised_segments
        if segment["end_seconds"] is not None
    ]
    maximum_end = max(segment_ends, default=0.0)
    return {
        "elapsed_seconds": elapsed,
        "text": production_worker.normalise_text(segments),
        "segment_count": len(segments),
        "configured_hotwords": configured_hotwords(
            settings,
            enabled=hotwords_enabled,
        ),
        "internal_vad_enabled": bool(options["vad_filter"]),
        "word_timestamps_enabled": include_words,
        "whisper_invoked": True,
        "input_duration_seconds": input_duration_seconds,
        "retained_source_spans": retained_source_spans,
        "segments": serialised_segments,
        "maximum_segment_end_overrun_seconds": max(
            0.0,
            maximum_end - input_duration_seconds,
        ),
    }


def empty_trimmed_decode(settings):
    return {
        "elapsed_seconds": 0.0,
        "text": "",
        "segment_count": 0,
        "configured_hotwords": configured_hotwords(settings),
        "internal_vad_enabled": False,
        "word_timestamps_enabled": False,
        "whisper_invoked": False,
        "input_duration_seconds": 0.0,
        "retained_source_spans": [],
        "segments": [],
        "maximum_segment_end_overrun_seconds": 0.0,
    }


def retained_source_spans(item, timestamps, sample_rate):
    retained = []
    output_start = 0.0
    for timestamp in timestamps:
        duration = (timestamp["end"] - timestamp["start"]) / sample_rate
        relative_start = timestamp["start"] / sample_rate
        relative_end = timestamp["end"] / sample_rate
        retained.append(
            {
                "vad_start_sample": timestamp["start"],
                "vad_end_sample": timestamp["end"],
                "range_start_seconds": relative_start,
                "range_end_seconds": relative_end,
                "source_start_ms": (
                    item["source_start_ms"] + relative_start * 1_000
                ),
                "source_end_ms": (
                    item["source_start_ms"] + relative_end * 1_000
                ),
                "trimmed_input_start_seconds": output_start,
                "trimmed_input_end_seconds": output_start + duration,
            }
        )
        output_start += duration
    return retained


def run_decode_configurations(
    model,
    ranged_audio,
    full_duration_seconds,
    settings,
    vad_audio,
    vad_sample_rate,
    unpadded_timestamps,
    item,
    voiced_materialiser=materialise_voiced_audio,
    clock=time.perf_counter,
):
    hotwords = ", ".join(settings["hotwords"]) or None
    current_options = transcribe_options(
        settings, hotwords, internal_vad=False
    )
    no_hotword_options = transcribe_options(
        settings, None, internal_vad=False
    )
    craig_like_options = transcribe_options(
        settings,
        hotwords,
        internal_vad=True,
        word_timestamps=True,
    )
    results = {
        CURRENT_DECODE: decode_result(
            model,
            ranged_audio,
            current_options,
            settings,
            True,
            full_duration_seconds,
            [],
            clock,
        ),
        NO_HOTWORD_DECODE: decode_result(
            model,
            ranged_audio,
            no_hotword_options,
            settings,
            False,
            full_duration_seconds,
            [],
            clock,
        ),
        CRAIG_LIKE_DECODE: decode_result(
            model,
            ranged_audio,
            craig_like_options,
            settings,
            True,
            full_duration_seconds,
            [],
            clock,
        ),
    }

    normalised_timestamps = normalise_speech_timestamps(
        unpadded_timestamps,
        len(vad_audio),
    )
    if not normalised_timestamps:
        results[EXPLICIT_TRIM_DECODE] = empty_trimmed_decode(settings)
        return results

    spans = retained_source_spans(item, normalised_timestamps, vad_sample_rate)
    with voiced_materialiser(
        vad_audio,
        normalised_timestamps,
        vad_sample_rate,
    ) as (trimmed_path, trimmed_duration):
        trimmed_options = transcribe_options(
            settings,
            hotwords,
            internal_vad=False,
        )
        results[EXPLICIT_TRIM_DECODE] = decode_result(
            model,
            trimmed_path,
            trimmed_options,
            settings,
            True,
            trimmed_duration,
            spans,
            clock,
        )
    return results


def warm_up_model(model, ranged_audio, settings):
    hotwords = ", ".join(settings["hotwords"]) or None
    options = transcribe_options(settings, hotwords, internal_vad=False)
    segments, _ = model.transcribe(str(ranged_audio), **options)
    list(segments)


def work_item_metadata(item):
    return {
        "sequence": item["sequence"],
        "work_item_id": item["id"],
        "session_id": item["session_id"],
        "discord_user_id": item["discord_user_id"],
        "speaker": item["speaker"],
        "role": item["role"],
        "character": item["character"],
        "work_item_start_ms": item["start_ms"],
        "work_item_end_ms": item["end_ms"],
        "work_item_duration_ms": item["end_ms"] - item["start_ms"],
        "source_path": item["source"],
        "source_start_ms": item["source_start_ms"],
        "source_end_ms": item["source_end_ms"],
        "source_duration_ms": (
            item["source_end_ms"] - item["source_start_ms"]
        ),
    }


def model_configuration(settings):
    return {
        "model": settings["model"],
        "language": settings["language"],
        "device": settings["device"],
        "compute_type": settings["compute_type"],
        "beam_size": settings["beam_size"],
    }


def diagnose_item(
    item,
    source_path,
    model,
    settings,
    range_extractor,
    audio_reader,
    vad_analyser,
    voiced_materialiser,
    clock,
):
    with range_extractor(
        source_path,
        item["source_start_ms"],
        item["source_end_ms"],
    ) as ranged_audio:
        samples, sample_rate = audio_reader(ranged_audio)
        acoustics = acoustic_measurements(samples, sample_rate)
        (
            vad_audio,
            vad_sample_rate,
            production_timestamps,
            unpadded_timestamps,
        ) = vad_analyser(ranged_audio)
        vad = {
            "production_default": vad_measurements(
                production_timestamps,
                len(vad_audio),
                vad_sample_rate,
            ),
            "speech_pad_ms_0": vad_measurements(
                unpadded_timestamps,
                len(vad_audio),
                vad_sample_rate,
            ),
        }
        decodes = run_decode_configurations(
            model,
            ranged_audio,
            acoustics["duration_seconds"],
            settings,
            vad_audio,
            vad_sample_rate,
            unpadded_timestamps,
            item,
            voiced_materialiser,
            clock,
        )
    return {
        "work_item": work_item_metadata(item),
        "model_configuration": model_configuration(settings),
        "acoustic_measurements": acoustics,
        "vad_analyses": vad,
        "decode_results": decodes,
    }


def run(
    args,
    model_factory=default_model_factory,
    range_extractor=production_worker.extract_audio_range,
    audio_reader=read_ranged_audio,
    vad_analyser=default_vad_analyser,
    voiced_materialiser=materialise_voiced_audio,
    clock=time.perf_counter,
):
    session_directory = args.session.resolve()
    items = load_session_manifest(session_directory)
    selected = select_work_items(items, args.sequence)
    settings = load_diagnostic_config(args.config)
    if settings["vocabulary_warning"]:
        print(settings["vocabulary_warning"], file=sys.stderr)

    sources = [resolve_source(session_directory, item) for item in selected]
    model = model_factory(settings)

    first_item = selected[0]
    with range_extractor(
        sources[0],
        first_item["source_start_ms"],
        first_item["source_end_ms"],
    ) as warmup_audio:
        warm_up_model(model, warmup_audio, settings)

    evidence = [
        diagnose_item(
            item,
            source_path,
            model,
            settings,
            range_extractor,
            audio_reader,
            vad_analyser,
            voiced_materialiser,
            clock,
        )
        for item, source_path in zip(selected, sources, strict=True)
    ]
    print_readable(evidence)
    if args.output is not None:
        write_jsonl(args.output, evidence)
    return evidence


def print_readable(evidence):
    for item_evidence in evidence:
        item = item_evidence["work_item"]
        acoustics = item_evidence["acoustic_measurements"]
        print(
            f"Sequence {item['sequence']} ({item['work_item_id']}) — "
            f"{item['speaker']} — "
            f"{item['source_path']} "
            f"[{item['source_start_ms']}..{item['source_end_ms']} ms]"
        )
        print(
            "  Acoustic: "
            f"duration={acoustics['duration_seconds']:.3f}s "
            f"peak={acoustics['peak_amplitude']:.6f} "
            f"({format_dbfs(acoustics['peak_dbfs'])}) "
            f"RMS={acoustics['rms_amplitude']:.6f} "
            f"({format_dbfs(acoustics['rms_dbfs'])})"
        )
        frame_rms = acoustics["frame_rms_20ms"]
        print(
            "    20 ms frame RMS: "
            f"mean={frame_rms['mean']:.6f} "
            f"std={frame_rms['standard_deviation']:.6f} "
            f"max={frame_rms['maximum']:.6f}"
        )
        for label, analysis in item_evidence["vad_analyses"].items():
            print(
                f"  VAD {label}: spans={len(analysis['speech_timestamps'])} "
                f"voiced={analysis['total_voiced_duration_seconds']:.3f}s "
                f"occupancy={analysis['voiced_occupancy_fraction']:.3f}"
            )
            for timestamp in analysis["speech_timestamps"]:
                print(
                    "    speech: "
                    f"samples {timestamp['start_sample']}.."
                    f"{timestamp['end_sample']} "
                    f"({timestamp['start_seconds']:.3f}.."
                    f"{timestamp['end_seconds']:.3f}s)"
                )
        for label, result in item_evidence["decode_results"].items():
            invoked = "decoded" if result["whisper_invoked"] else "not invoked"
            hotwords = result["configured_hotwords"]
            overrun = result["maximum_segment_end_overrun_seconds"]
            print(
                f"  {label}: {result['elapsed_seconds']:.3f}s, {invoked}, "
                f"segments={result['segment_count']}, "
                f"input={result['input_duration_seconds']:.3f}s, "
                f"overrun={overrun:.3f}s, "
                f"hotwords={hotwords['enabled']}, "
                f"internal_vad={result['internal_vad_enabled']}"
            )
            print(f"    text: {result['text']!r}")
            for segment in result["segments"]:
                print(
                    "    segment: "
                    f"{format_seconds(segment['start_seconds'])}.."
                    f"{format_seconds(segment['end_seconds'])} "
                    f"temp={segment['temperature']} "
                    f"avg_logprob={segment['average_log_probability']} "
                    f"compression={segment['compression_ratio']} "
                    f"no_speech={segment['no_speech_probability']} "
                    f"text={segment['text']!r}"
                )
                for word in segment["words"] or []:
                    print(
                        "      word: "
                        f"{format_seconds(word['start_seconds'])}.."
                        f"{format_seconds(word['end_seconds'])} "
                        f"probability={word['probability']} "
                        f"text={word['text']!r}"
                    )


def format_dbfs(value):
    return "-inf dBFS" if value is None else f"{value:.2f} dBFS"


def format_seconds(value):
    return "unknown" if value is None else f"{value:.3f}s"


def write_jsonl(path, evidence):
    with path.open("w", encoding="utf-8", newline="\n") as output:
        for item_evidence in evidence:
            output.write(
                json.dumps(
                    item_evidence,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            )
            output.write("\n")


def main(argv=None):
    args = parse_args(argv)
    try:
        run(args)
    except Exception as error:
        print(f"EchoScribe diagnostic replay failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
