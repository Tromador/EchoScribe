import argparse
import importlib.util
import io
import json
import math
import sys
import tempfile
import unittest
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest import mock


WORKER_DIRECTORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKER_DIRECTORY))
MODULE_PATH = WORKER_DIRECTORY / "diagnose_ranges.py"
SPEC = importlib.util.spec_from_file_location("diagnose_ranges", MODULE_PATH)
diagnose = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(diagnose)


class FakeClock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        self.value += 0.25
        return self.value


class FakeModel:
    def __init__(self):
        self.calls = []

    def transcribe(self, path, **options):
        self.calls.append((Path(path), options))
        word = SimpleNamespace(
            start=0.1,
            end=0.4,
            word=" evidence",
            probability=0.91,
        )
        segment = SimpleNamespace(
            start=0.0,
            end=1.25,
            text=" diagnostic   evidence ",
            temperature=0.2,
            avg_logprob=-0.3,
            compression_ratio=1.1,
            no_speech_prob=0.07,
            words=[word] if options.get("word_timestamps") else None,
        )
        return iter([segment]), object()


class DiagnoseRangesTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.session = self.root / "session-test"
        self.session.mkdir()
        (self.session / "tracks").mkdir()
        (self.session / "transcription").mkdir()
        for user_id in (11, 22):
            (self.session / "tracks" / f"user-{user_id}.flac").write_bytes(
                b"substituted by the range extractor"
            )
        self.items = [
            make_item(1, 11, "Alice", 0, 1_000),
            make_item(2, 22, "Bob", 1_500, 2_000),
        ]
        self.manifest = self.session / "transcription" / "work-items.jsonl"
        write_manifest(self.manifest, self.items)
        write_session(self.session)
        self.config = self.root / "echoscribe.toml"
        self.config.write_text(
            """
version = 1

[transcription]
model = "test-model"
language = "en"
device = "cpu"
compute_type = "int8"
beam_size = 3
vocabulary_file = "vocabulary.txt"
""".lstrip(),
            encoding="utf-8",
        )
        (self.root / "vocabulary.txt").write_text(
            " Emperor Coaltongue \n# comment\nAgent #7\n",
            encoding="utf-8",
        )

    def tearDown(self):
        self.temporary.cleanup()

    def test_repeatable_sequence_selection_preserves_requested_order(self):
        selected = diagnose.select_work_items(self.items, [2, 1, 2])

        self.assertEqual([item["sequence"] for item in selected], [2, 1, 2])

    def test_unknown_sequence_is_rejected(self):
        with self.assertRaisesRegex(
            ValueError, "unknown work-item sequence 3"
        ):
            diagnose.select_work_items(self.items, [1, 3])

    def test_source_path_must_remain_within_session(self):
        escaped = dict(self.items[0], source="../outside.flac")
        (self.root / "outside.flac").write_bytes(b"outside")

        with self.assertRaisesRegex(ValueError, "source escapes"):
            diagnose.resolve_source(self.session, escaped)

    def test_acoustic_metrics_use_twenty_millisecond_frames(self):
        samples = [0.0] * 20 + [1.0] * 20

        result = diagnose.acoustic_measurements(samples, sample_rate=1_000)

        self.assertEqual(result["sample_count"], 40)
        self.assertEqual(result["duration_seconds"], 0.04)
        self.assertEqual(result["peak_amplitude"], 1.0)
        self.assertEqual(result["peak_dbfs"], 0.0)
        self.assertAlmostEqual(result["rms_amplitude"], math.sqrt(0.5))
        self.assertAlmostEqual(result["rms_dbfs"], -3.0102999566)
        frames = result["frame_rms_20ms"]
        self.assertEqual(frames["frame_count"], 2)
        self.assertEqual(frames["mean"], 0.5)
        self.assertEqual(frames["standard_deviation"], 0.5)
        self.assertEqual(frames["maximum"], 1.0)

    def test_zero_amplitude_has_no_finite_dbfs_value(self):
        result = diagnose.acoustic_measurements([0.0] * 20, sample_rate=1_000)

        self.assertIsNone(result["peak_dbfs"])
        self.assertIsNone(result["rms_dbfs"])

    def test_voiced_occupancy_uses_complete_range_duration(self):
        result = diagnose.vad_measurements(
            [{"start": 0, "end": 20}, {"start": 40, "end": 60}],
            audio_sample_count=100,
            sample_rate=1_000,
        )

        self.assertEqual(result["total_voiced_duration_seconds"], 0.04)
        self.assertEqual(result["voiced_occupancy_fraction"], 0.4)
        self.assertEqual(
            result["speech_timestamps"][1],
            {
                "start_sample": 40,
                "end_sample": 60,
                "start_seconds": 0.04,
                "end_seconds": 0.06,
            },
        )

    def test_default_vad_runs_production_and_unpadded_options(self):
        observed = []
        audio = [0.0] * 160
        audio_module = ModuleType("faster_whisper.audio")
        audio_module.decode_audio = lambda path, sampling_rate: audio
        vad_module = ModuleType("faster_whisper.vad")

        class FakeVadOptions:
            def __init__(self, speech_pad_ms=400):
                self.speech_pad_ms = speech_pad_ms

        def get_speech_timestamps(samples, options, sampling_rate):
            observed.append((samples, options.speech_pad_ms, sampling_rate))
            return [{"start": 0, "end": 80}]

        vad_module.VadOptions = FakeVadOptions
        vad_module.get_speech_timestamps = get_speech_timestamps
        package = ModuleType("faster_whisper")
        package.__path__ = []
        with mock.patch.dict(
            sys.modules,
            {
                "faster_whisper": package,
                "faster_whisper.audio": audio_module,
                "faster_whisper.vad": vad_module,
            },
        ):
            result = diagnose.default_vad_analyser(self.root / "range.wav")

        self.assertIs(result[0], audio)
        self.assertEqual(result[1], 16_000)
        self.assertEqual([entry[1] for entry in observed], [400, 0])
        self.assertTrue(all(entry[0] is audio for entry in observed))

    def test_empty_unpadded_vad_result_skips_only_explicit_trim_decode(self):
        model = FakeModel()

        results = diagnose.run_decode_configurations(
            model,
            self.root / "range.wav",
            1.0,
            settings(),
            [0.0] * 160,
            16_000,
            [],
            self.items[0],
            voiced_materialiser=lambda *unused: self.fail(
                "empty VAD result must not materialise trimmed audio"
            ),
            clock=FakeClock(),
        )

        self.assertEqual(len(model.calls), 4)
        self.assertEqual(
            model.calls[3][0],
            self.root / "range.wav",
        )
        self.assertEqual(
            list(results),
            [
                diagnose.CURRENT_DECODE,
                diagnose.NO_HOTWORD_DECODE,
                diagnose.CRAIG_LIKE_DECODE,
                diagnose.INTERNAL_VAD_NO_HOTWORD_DECODE,
                diagnose.EXPLICIT_TRIM_DECODE,
            ],
        )
        internal_vad_no_hotwords = results[
            diagnose.INTERNAL_VAD_NO_HOTWORD_DECODE
        ]
        self.assertTrue(internal_vad_no_hotwords["whisper_invoked"])
        self.assertEqual(
            internal_vad_no_hotwords["configured_hotwords"],
            {"enabled": False, "phrases": []},
        )
        self.assertTrue(internal_vad_no_hotwords["internal_vad_enabled"])
        self.assertTrue(
            internal_vad_no_hotwords["word_timestamps_enabled"]
        )
        trimmed = results[diagnose.EXPLICIT_TRIM_DECODE]
        self.assertFalse(trimmed["whisper_invoked"])
        self.assertEqual(trimmed["text"], "")
        self.assertEqual(trimmed["segments"], [])
        self.assertEqual(trimmed["input_duration_seconds"], 0.0)

    def test_decode_configurations_have_exact_option_differences(self):
        model = FakeModel()
        materialised = []

        @contextmanager
        def voiced_materialiser(audio, timestamps, sample_rate):
            materialised.append((audio, timestamps, sample_rate))
            yield self.root / "trimmed.wav", 0.5

        results = diagnose.run_decode_configurations(
            model,
            self.root / "range.wav",
            1.0,
            settings(),
            [0.0] * 160,
            160,
            [{"start": 16, "end": 96}],
            self.items[0],
            voiced_materialiser=voiced_materialiser,
            clock=FakeClock(),
        )

        expected_base = {
            "beam_size": 3,
            "language": "en",
            "condition_on_previous_text": False,
        }
        self.assertEqual(
            model.calls[0][1],
            {
                **expected_base,
                "hotwords": "Emperor Coaltongue, Agent #7",
                "vad_filter": False,
            },
        )
        self.assertEqual(
            model.calls[1][1],
            {
                **expected_base,
                "hotwords": None,
                "vad_filter": False,
            },
        )
        self.assertEqual(
            model.calls[2][1],
            {
                **expected_base,
                "hotwords": "Emperor Coaltongue, Agent #7",
                "vad_filter": True,
                "word_timestamps": True,
            },
        )
        self.assertEqual(
            model.calls[3][1],
            {
                **expected_base,
                "hotwords": None,
                "vad_filter": True,
                "word_timestamps": True,
            },
        )
        self.assertEqual(
            model.calls[4][1],
            {
                **expected_base,
                "hotwords": "Emperor Coaltongue, Agent #7",
                "vad_filter": False,
            },
        )
        self.assertEqual(len(model.calls), 5)
        self.assertTrue(
            all(
                call[0] == self.root / "range.wav"
                for call in model.calls[:4]
            )
        )
        self.assertEqual(model.calls[4][0], self.root / "trimmed.wav")
        self.assertEqual(len(materialised), 1)
        self.assertTrue(
            results[diagnose.CRAIG_LIKE_DECODE]["word_timestamps_enabled"]
        )
        internal_vad_no_hotwords = results[
            diagnose.INTERNAL_VAD_NO_HOTWORD_DECODE
        ]
        self.assertEqual(
            internal_vad_no_hotwords["configured_hotwords"],
            {"enabled": False, "phrases": []},
        )
        self.assertTrue(internal_vad_no_hotwords["internal_vad_enabled"])
        self.assertTrue(
            internal_vad_no_hotwords["word_timestamps_enabled"]
        )
        self.assertTrue(internal_vad_no_hotwords["whisper_invoked"])
        self.assertEqual(
            results[diagnose.EXPLICIT_TRIM_DECODE]["retained_source_spans"][0][
                "source_start_ms"
            ],
            100.0,
        )

    def test_segment_serialisation_includes_diagnostic_fields_and_words(self):
        segment = SimpleNamespace(
            start=0.1,
            end=1.2,
            text=" text ",
            temperature=0.4,
            avg_logprob=-0.8,
            compression_ratio=1.3,
            no_speech_prob=0.2,
            words=[
                SimpleNamespace(
                    start=0.1,
                    end=0.4,
                    word=" text",
                    probability=0.88,
                )
            ],
        )

        result = diagnose.serialise_segment(segment, include_words=True)

        self.assertEqual(result["start_seconds"], 0.1)
        self.assertEqual(result["end_seconds"], 1.2)
        self.assertEqual(result["temperature"], 0.4)
        self.assertEqual(result["average_log_probability"], -0.8)
        self.assertEqual(result["compression_ratio"], 1.3)
        self.assertEqual(result["no_speech_probability"], 0.2)
        self.assertEqual(result["words"][0]["text"], " text")
        self.assertEqual(result["words"][0]["probability"], 0.88)

    def test_run_loads_model_once_warms_up_and_writes_selected_jsonl(self):
        output = self.root / "diagnostics.jsonl"
        model = FakeModel()
        model_loads = []
        context_paths = []
        cleaned_paths = []
        call_number = 0

        @contextmanager
        def range_extractor(source, start_ms, end_ms):
            nonlocal call_number
            call_number += 1
            path = self.root / f"range-{call_number}.wav"
            path.write_bytes(b"temporary")
            context_paths.append((source, start_ms, end_ms, path))
            try:
                yield path
            finally:
                path.unlink()
                cleaned_paths.append(path)

        def model_factory(config):
            model_loads.append(config["model"])
            return model

        args = argparse.Namespace(
            config=self.config,
            session=self.session,
            sequence=[2, 1],
            output=output,
        )
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            evidence = diagnose.run(
                args,
                model_factory=model_factory,
                range_extractor=range_extractor,
                audio_reader=lambda unused: ([0.0] * 480, 48_000),
                vad_analyser=lambda unused: (
                    [0.0] * 160,
                    16_000,
                    [],
                    [],
                ),
                voiced_materialiser=lambda *unused: self.fail(
                    "empty VAD must not materialise trimmed audio"
                ),
                clock=FakeClock(),
            )

        self.assertEqual(model_loads, ["test-model"])
        self.assertEqual(
            [entry["work_item"]["sequence"] for entry in evidence],
            [2, 1],
        )
        self.assertEqual(len(model.calls), 9)
        self.assertEqual(len(context_paths), 3)
        self.assertTrue(all(not path.exists() for path in cleaned_paths))
        records = [
            json.loads(line)
            for line in output.read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(
            [record["work_item"]["sequence"] for record in records],
            [2, 1],
        )
        expected_decodes = [
            diagnose.CURRENT_DECODE,
            diagnose.NO_HOTWORD_DECODE,
            diagnose.CRAIG_LIKE_DECODE,
            diagnose.INTERNAL_VAD_NO_HOTWORD_DECODE,
            diagnose.EXPLICIT_TRIM_DECODE,
        ]
        for record in records:
            self.assertEqual(list(record["decode_results"]), expected_decodes)

    def test_jsonl_contains_one_object_per_work_item(self):
        output = self.root / "evidence.jsonl"
        evidence = [
            {"work_item": {"sequence": 4}},
            {"work_item": {"sequence": 9}},
        ]

        diagnose.write_jsonl(output, evidence)

        self.assertEqual(
            [json.loads(line) for line in output.read_text().splitlines()],
            evidence,
        )

    def test_voiced_audio_temporary_file_is_cleaned(self):
        import numpy

        audio = numpy.array([0.0, 0.25, -0.25, 0.0], dtype="float32")
        with diagnose.materialise_voiced_audio(
            audio,
            [{"start": 1, "end": 3}],
            sample_rate=16_000,
        ) as (path, duration):
            self.assertTrue(path.is_file())
            retained_path = path
            self.assertEqual(duration, 2 / 16_000)

        self.assertFalse(retained_path.exists())

    def test_manifest_failures_are_clear(self):
        self.manifest.unlink()
        with self.assertRaisesRegex(ValueError, "work manifest is missing"):
            diagnose.load_session_manifest(self.session)

        self.manifest.write_text('{"format":1}\n', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "unexpected schema"):
            diagnose.load_session_manifest(self.session)

    def test_format_two_session_and_work_item_are_supported(self):
        item = make_current_item(
            1,
            11,
            "Tromador",
            "Stefan",
            "Stefan",
            "speaker",
            0,
            1_000,
        )
        write_manifest(self.manifest, [item])
        write_session(self.session, manifest_format=2)

        items = diagnose.load_session_manifest(self.session)

        self.assertEqual(items, [item])

        write_manifest(self.manifest, [self.items[0]])
        with self.assertRaisesRegex(ValueError, "session-declared format"):
            diagnose.load_session_manifest(self.session)

    def test_vocabulary_matches_production_missing_and_empty_behaviour(self):
        missing_hotwords, missing_warning = diagnose.load_vocabulary(
            self.root / "missing.txt"
        )
        empty_path = self.root / "empty.txt"
        empty_path.write_text(" \n# comment only\n", encoding="utf-8")
        empty_hotwords, empty_warning = diagnose.load_vocabulary(empty_path)

        self.assertEqual(missing_hotwords, [])
        self.assertIn("is missing", missing_warning)
        self.assertEqual(empty_hotwords, [])
        self.assertIn("contains no vocabulary phrases", empty_warning)


def settings():
    return {
        "model": "test-model",
        "language": "en",
        "device": "cpu",
        "compute_type": "int8",
        "beam_size": 3,
        "hotwords": ["Emperor Coaltongue", "Agent #7"],
        "vocabulary_warning": None,
    }


def make_item(sequence, user_id, speaker, start_ms, end_ms):
    return {
        "format": 1,
        "id": f"session-test:{sequence:06d}",
        "session_id": "session-test",
        "sequence": sequence,
        "discord_user_id": str(user_id),
        "speaker": speaker,
        "role": "player",
        "character": None,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "source": f"tracks/user-{user_id}.flac",
        "source_start_ms": start_ms,
        "source_end_ms": end_ms,
    }


def make_current_item(
    sequence, user_id, discord_name, name, speaker, role, start_ms, end_ms
):
    return {
        "format": 2,
        "id": f"session-test:{sequence:06d}",
        "session_id": "session-test",
        "sequence": sequence,
        "discord_user_id": str(user_id),
        "discord_name": discord_name,
        "name": name,
        "speaker": speaker,
        "role": role,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "source": f"tracks/user-{user_id}.flac",
        "source_start_ms": start_ms,
        "source_end_ms": end_ms,
    }


def write_manifest(path, items):
    path.write_text(
        "".join(json.dumps(item) + "\n" for item in items),
        encoding="utf-8",
    )


def write_session(path, manifest_format=1):
    (path / "session.json").write_text(
        json.dumps(
            {
                "files": {
                    "work_items": {
                        "path": "transcription/work-items.jsonl",
                        "format": manifest_format,
                    }
                }
            }
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    unittest.main()
