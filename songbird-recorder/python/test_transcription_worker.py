import argparse
import importlib.util
import json
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


MODULE_PATH = Path(__file__).with_name("transcription_worker.py")
SPEC = importlib.util.spec_from_file_location("transcription_worker", MODULE_PATH)
worker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(worker)


class FakeModel:
    def __init__(self, texts, fail_on_call=None):
        self.texts = texts
        self.fail_on_call = fail_on_call
        self.calls = []

    def transcribe(self, path, **options):
        call = len(self.calls) + 1
        self.calls.append((Path(path), options))
        if self.fail_on_call == call:
            raise RuntimeError("injected item failure")
        segments = [SimpleNamespace(text=text) for text in self.texts[call - 1]]
        return iter(segments), object()


class TranscriptionWorkerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        (self.directory / "tracks").mkdir()
        (self.directory / "tracks" / "user-11.flac").write_bytes(b"not decoded")
        self.manifest = self.directory / "work-items.jsonl"
        self.results = self.directory / "results.jsonl"
        self.transcript = self.directory / "transcript.partial.txt"
        self.results.write_bytes(b"")
        self.transcript.write_bytes(b"")

    def tearDown(self):
        self.temporary.cleanup()

    def test_one_model_load_chronological_ranges_and_matching_outputs(self):
        items = [
            make_item(1, 11, "Alice", 1_500, 2_000, None),
            make_item(2, 11, "Alice", 3_000, 3_600, "Emperor Coaltongue"),
        ]
        write_manifest(self.manifest, items)
        model = FakeModel(
            [
                [" rules discussion ", " and jokes "],
                ["in character", "out of character"],
            ]
        )
        model_loads = []
        extracted = []

        def model_factory(args):
            model_loads.append(args.model)
            return model

        @contextmanager
        def range_extractor(path, start_ms, end_ms):
            extracted.append((path, start_ms, end_ms))
            yield self.directory / "range.wav"

        worker.process(
            make_args(self),
            model_factory=model_factory,
            range_extractor=range_extractor,
        )

        self.assertEqual(model_loads, ["test-model"])
        self.assertEqual(
            extracted,
            [
                (
                    (self.directory / "tracks" / "user-11.flac").resolve(),
                    1_500,
                    2_000,
                ),
                (
                    (self.directory / "tracks" / "user-11.flac").resolve(),
                    3_000,
                    3_600,
                ),
            ],
        )
        self.assertEqual(len(model.calls), 2)
        for _, options in model.calls:
            self.assertFalse(options["condition_on_previous_text"])
            self.assertEqual(options["hotwords"], "Emperor Coaltongue, Dragon Lance")
        results = [
            json.loads(line)
            for line in self.results.read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual([result["sequence"] for result in results], [1, 2])
        self.assertEqual(results[0]["text"], "rules discussion and jokes")
        self.assertEqual(
            results[1]["text"], "in character out of character"
        )
        self.assertIsNone(results[0]["character"])
        self.assertEqual(results[1]["character"], "Emperor Coaltongue")
        self.assertEqual(
            self.transcript.read_text(encoding="utf-8"),
            "[00:00:01] Alice: rules discussion and jokes\n"
            "[00:00:03] Alice: in character out of character\n",
        )

    def test_item_failure_retains_prior_committed_result_and_text(self):
        items = [
            make_item(1, 11, "Alice", 0, 500, None),
            make_item(2, 11, "Alice", 500, 1_000, None),
        ]
        write_manifest(self.manifest, items)
        model = FakeModel([["first"], ["never committed"]], fail_on_call=2)

        @contextmanager
        def range_extractor(path, start_ms, end_ms):
            yield self.directory / "range.wav"

        with self.assertRaisesRegex(RuntimeError, "injected item failure"):
            worker.process(
                make_args(self),
                model_factory=lambda args: model,
                range_extractor=range_extractor,
            )

        results = self.results.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(results), 1)
        self.assertEqual(json.loads(results[0])["sequence"], 1)
        self.assertEqual(
            self.transcript.read_text(encoding="utf-8"),
            "[00:00:00] Alice: first\n",
        )

    def test_range_extraction_uses_requested_time_and_clamps_rounded_final_frame(self):
        observed = {}

        class FakeSource:
            samplerate = 48_000
            channels = 1

            def __enter__(self):
                return self

            def __exit__(self, *unused):
                return None

            def __len__(self):
                return 961

            def seek(self, frame):
                observed["seek"] = frame

            def read(self, frames, **options):
                observed["read"] = (frames, options)
                return "audio samples"

        def write(path, audio, sample_rate, **options):
            observed["write"] = (audio, sample_rate, options)
            Path(path).write_bytes(b"wave")

        fake_soundfile = SimpleNamespace(
            SoundFile=lambda unused: FakeSource(),
            write=write,
        )
        with mock.patch.dict(sys.modules, {"soundfile": fake_soundfile}):
            with worker.extract_audio_range(
                self.directory / "tracks" / "user-11.flac", 0, 21
            ) as ranged:
                self.assertTrue(ranged.is_file())
                ranged_path = ranged

        self.assertEqual(observed["seek"], 0)
        self.assertEqual(observed["read"][0], 961)
        self.assertEqual(
            observed["read"][1],
            {"dtype": "float32", "always_2d": True},
        )
        self.assertEqual(observed["write"][1], 48_000)
        self.assertFalse(ranged_path.exists())

    def test_start_sequence_prevents_duplicate_output(self):
        items = [
            make_item(1, 11, "Alice", 0, 500, None),
            make_item(2, 11, "Alice", 500, 1_000, None),
        ]
        write_manifest(self.manifest, items)
        prior = worker.result_from_item(items[0], "already committed")
        self.results.write_text(
            json.dumps(prior, separators=(",", ":")) + "\n", encoding="utf-8"
        )
        self.transcript.write_text(
            worker.transcript_line(prior), encoding="utf-8"
        )
        model = FakeModel([["second only"]])

        @contextmanager
        def range_extractor(path, start_ms, end_ms):
            yield self.directory / "range.wav"

        args = make_args(self)
        args.start_sequence = 2
        worker.process(
            args,
            model_factory=lambda unused: model,
            range_extractor=range_extractor,
        )

        results = [
            json.loads(line)
            for line in self.results.read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual([result["sequence"] for result in results], [1, 2])
        self.assertEqual(len(model.calls), 1)
        self.assertEqual(
            self.transcript.read_text(encoding="utf-8").splitlines(),
            [
                "[00:00:00] Alice: already committed",
                "[00:00:00] Alice: second only",
            ],
        )

    def test_malformed_manifest_and_worker_failure_return_nonzero(self):
        self.manifest.write_text('{"format":1}\n', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "unexpected schema"):
            worker.load_manifest(self.manifest)

        with mock.patch.object(worker, "process", side_effect=RuntimeError("boom")):
            self.assertEqual(worker.main(command_line(self)), 1)


def make_item(sequence, user_id, speaker, start_ms, end_ms, character):
    return {
        "format": 1,
        "id": f"session-test:{sequence:06d}",
        "session_id": "session-test",
        "sequence": sequence,
        "discord_user_id": str(user_id),
        "speaker": speaker,
        "role": "player",
        "character": character,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "source": f"tracks/user-{user_id}.flac",
        "source_start_ms": start_ms,
        "source_end_ms": end_ms,
    }


def write_manifest(path, items):
    path.write_text(
        "".join(json.dumps(item) + "\n" for item in items), encoding="utf-8"
    )


def make_args(test):
    return argparse.Namespace(
        config=test.directory / "echoscribe.toml",
        session=test.directory,
        manifest=test.manifest,
        results=test.results,
        transcript=test.transcript,
        start_sequence=1,
        model="test-model",
        language="en",
        device="cpu",
        compute_type="int8",
        beam_size=1,
        hotword=["Emperor Coaltongue", "Dragon Lance"],
    )


def command_line(test):
    args = make_args(test)
    return [
        "--config",
        str(args.config),
        "--session",
        str(args.session),
        "--manifest",
        str(args.manifest),
        "--results",
        str(args.results),
        "--transcript",
        str(args.transcript),
        "--start-sequence",
        str(args.start_sequence),
        "--model",
        args.model,
        "--language",
        args.language,
        "--device",
        args.device,
        "--compute-type",
        args.compute_type,
        "--beam-size",
        str(args.beam_size),
    ]


if __name__ == "__main__":
    unittest.main()
