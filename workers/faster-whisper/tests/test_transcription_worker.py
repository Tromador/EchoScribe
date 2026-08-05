import argparse
import importlib.util
import json
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest import mock


MODULE_PATH = Path(__file__).resolve().parents[1] / "transcription_worker.py"
SPEC = importlib.util.spec_from_file_location(
    "transcription_worker", MODULE_PATH
)
worker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(worker)


class FakeModel:
    def __init__(self, responses, fail_on_call=None):
        self.responses = responses
        self.fail_on_call = fail_on_call
        self.calls = []

    def transcribe(self, path, **options):
        call = len(self.calls) + 1
        self.calls.append((Path(path), options))
        if self.fail_on_call == call:
            raise RuntimeError("injected item failure")
        segments = []
        for response in self.responses[call - 1]:
            if isinstance(response, tuple):
                text, no_speech_prob = response
            else:
                text, no_speech_prob = response, 0.0
            segments.append(
                SimpleNamespace(
                    text=text,
                    no_speech_prob=no_speech_prob,
                )
            )
        return iter(segments), object()


class TranscriptionWorkerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        (self.directory / "tracks").mkdir()
        source = self.directory / "tracks" / "user-11.flac"
        source.write_bytes(b"not decoded")
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
            vad_analyser=lambda unused: self.fail(
                "disabled VAD must not analyse a range"
            ),
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
            self.assertIsNone(options["hotwords"])
            self.assertNotIn("vad_filter", options)
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

    def test_range_extraction_clamps_rounded_final_frame(self):
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

    def test_materially_truncated_source_commits_no_output(self):
        write_manifest(
            self.manifest,
            [make_item(1, 11, "Alice", 0, 21, None)],
        )
        model = FakeModel([["must not be committed"]])

        class TruncatedSource:
            samplerate = 48_000
            channels = 1

            def __enter__(self):
                return self

            def __exit__(self, *unused):
                return None

            def __len__(self):
                return 900

            def seek(self, unused):
                raise AssertionError(
                    "truncated source must fail before seeking"
                )

            def read(self, unused, **options):
                raise AssertionError(
                    "truncated source must fail before reading"
                )

        fake_soundfile = SimpleNamespace(
            SoundFile=lambda unused: TruncatedSource(),
            write=lambda *unused, **options: self.fail(
                "truncated source must not be materialised"
            ),
        )
        with mock.patch.dict(sys.modules, {"soundfile": fake_soundfile}):
            with self.assertRaisesRegex(ValueError, "108 frames shorter"):
                worker.process(
                    make_args(self),
                    model_factory=lambda unused: model,
                )

        self.assertEqual(self.results.read_bytes(), b"")
        self.assertEqual(self.transcript.read_bytes(), b"")
        self.assertEqual(model.calls, [])

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

    def test_start_sequence_advances_past_committed_empty_result(self):
        items = [
            make_item(1, 11, "Alice", 0, 500, None),
            make_item(2, 11, "Alice", 500, 1_000, None),
        ]
        write_manifest(self.manifest, items)
        prior = worker.result_from_item(items[0], "")
        self.results.write_text(
            json.dumps(prior, separators=(",", ":")) + "\n", encoding="utf-8"
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
            self.transcript.read_text(encoding="utf-8"),
            "[00:00:00] Alice: second only\n",
        )

    def test_silero_positive_transcribes_complete_quiet_range(self):
        model, results = self.run_vad_case(
            speech_detected=True,
            end_ms=1_000,
        )

        self.assertEqual(len(model.calls), 1)
        called_path, options = model.calls[0]
        self.assertEqual(called_path, self.directory / "complete-range.wav")
        self.assertNotIn("vad_filter", options)
        self.assertEqual(results[0]["text"], "accepted speech")

    def test_default_vad_analyser_uses_pinned_faster_whisper_api(self):
        observed = {}
        expected_audio = [0.1, -0.1]

        audio_module = ModuleType("faster_whisper.audio")

        def decode_audio(path, sampling_rate):
            observed["decode"] = (path, sampling_rate)
            return expected_audio

        audio_module.decode_audio = decode_audio
        vad_module = ModuleType("faster_whisper.vad")

        class FakeVadOptions:
            pass

        def get_speech_timestamps(audio, options, sampling_rate):
            observed["speech"] = (audio, options, sampling_rate)
            return [{"start": 0, "end": 2}]

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
            speech = worker.default_vad_analyser(
                self.directory / "complete-range.wav"
            )

        self.assertTrue(speech)
        self.assertEqual(
            observed["decode"],
            (str(self.directory / "complete-range.wav"), 16_000),
        )
        self.assertIs(observed["speech"][0], expected_audio)
        self.assertIsInstance(observed["speech"][1], FakeVadOptions)
        self.assertEqual(observed["speech"][2], 16_000)

    def test_silero_rejection_skips_whisper_and_commits_empty(self):
        model, results = self.run_vad_case(
            speech_detected=False,
            end_ms=1_000,
        )

        self.assertEqual(model.calls, [])
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "complete")
        self.assertEqual(results[0]["text"], "")
        self.assertEqual(self.transcript.read_bytes(), b"")

    def test_silero_negative_short_loud_burst_is_rejected(self):
        model, results = self.run_vad_case(
            speech_detected=False,
            end_ms=1_000,
        )

        self.assertEqual(model.calls, [])
        self.assertEqual(results[0]["status"], "complete")
        self.assertEqual(results[0]["text"], "")
        self.assertEqual(self.transcript.read_bytes(), b"")

    def test_vad_inference_failure_commits_no_output(self):
        write_manifest(
            self.manifest,
            [make_item(1, 11, "Alice", 0, 1_000, None)],
        )
        model = FakeModel([["must not be committed"]])

        @contextmanager
        def range_extractor(path, start_ms, end_ms):
            yield self.directory / "complete-range.wav"

        args = make_args(self)
        args.vad_enabled = True

        def fail_vad(unused):
            raise RuntimeError("injected VAD failure")

        with self.assertRaisesRegex(RuntimeError, "injected VAD failure"):
            worker.process(
                args,
                model_factory=lambda unused: model,
                range_extractor=range_extractor,
                vad_analyser=fail_vad,
            )

        self.assertEqual(model.calls, [])
        self.assertEqual(self.results.read_bytes(), b"")
        self.assertEqual(self.transcript.read_bytes(), b"")

    def test_empty_unprompted_result_is_rejected(self):
        model, results, _ = self.run_lexical_case([[]])

        self.assertEqual(len(model.calls), 1)
        self.assertEqual(results[0]["text"], "")
        self.assertEqual(self.transcript.read_bytes(), b"")

    def test_plausible_high_no_speech_phrase_is_rejected(self):
        model, results, _ = self.run_lexical_case(
            [[("The guards are waiting outside.", 0.9)]]
        )

        self.assertEqual(len(model.calls), 1)
        self.assertEqual(results[0]["status"], "complete")
        self.assertEqual(results[0]["text"], "")

    def test_all_nonempty_segments_at_or_above_threshold_are_rejected(self):
        segments = [
            SimpleNamespace(text="at threshold", no_speech_prob=0.75),
            SimpleNamespace(text="above threshold", no_speech_prob=0.92),
        ]

        decision, text = worker.qualify_lexical_speech(segments, 0.75)

        self.assertEqual(decision, worker.LEXICAL_REJECTED_HIGH_NO_SPEECH)
        self.assertEqual(text, "at threshold above threshold")

    def test_one_nonempty_segment_below_threshold_accepts_range(self):
        segments = [
            SimpleNamespace(text="doubtful", no_speech_prob=0.8),
            SimpleNamespace(text="clear speech", no_speech_prob=0.1),
        ]

        decision, text = worker.qualify_lexical_speech(segments, 0.75)

        self.assertEqual(decision, worker.LEXICAL_ACCEPTED)
        self.assertEqual(text, "doubtful clear speech")

    def test_empty_text_segments_do_not_qualify_range(self):
        segments = [
            SimpleNamespace(text="  \n ", no_speech_prob=0.0),
            SimpleNamespace(text="", no_speech_prob=0.1),
        ]

        decision, text = worker.qualify_lexical_speech(segments, 0.75)

        self.assertEqual(decision, worker.LEXICAL_REJECTED_EMPTY)
        self.assertEqual(text, "")

    def test_failed_qualification_does_not_run_hotword_decode(self):
        model, results, _ = self.run_lexical_case(
            [[("plausible but doubtful", 0.8)]],
            hotwords=["Emperor Coaltongue"],
        )

        self.assertEqual(len(model.calls), 1)
        self.assertIsNone(model.calls[0][1]["hotwords"])
        self.assertEqual(results[0]["text"], "")

    def test_qualified_hotword_pass_uses_same_complete_range(self):
        model, results, _ = self.run_lexical_case(
            [
                [("unprompted words", 0.1)],
                ["Emperor Coaltongue speaks"],
            ],
            hotwords=["Emperor Coaltongue", "Dragon Lance"],
        )

        self.assertEqual(
            [path for path, unused in model.calls],
            [
                self.directory / "complete-range.wav",
                self.directory / "complete-range.wav",
            ],
        )
        common_options = {
            "beam_size": 1,
            "language": "en",
            "condition_on_previous_text": False,
        }
        self.assertEqual(
            model.calls[0][1],
            {**common_options, "hotwords": None},
        )
        self.assertEqual(
            model.calls[1][1],
            {
                **common_options,
                "hotwords": "Emperor Coaltongue, Dragon Lance",
            },
        )
        self.assertEqual(results[0]["text"], "Emperor Coaltongue speaks")

    def test_qualified_unprompted_text_is_reused_without_hotwords(self):
        model, results, _ = self.run_lexical_case(
            [[("  genuine   speech ", 0.01)]]
        )

        self.assertEqual(len(model.calls), 1)
        self.assertEqual(results[0]["text"], "genuine speech")

    def test_lexical_summary_distinguishes_rejection_reasons(self):
        model, results, emitted = self.run_lexical_case(
            [[], [("plausible phrase", 0.8)]],
            item_count=2,
        )

        self.assertEqual(len(model.calls), 2)
        self.assertEqual([result["text"] for result in results], ["", ""])
        self.assertEqual(
            emitted,
            [
                mock.call(
                    "Lexical accepted: 0; empty rejected: 1; "
                    "high-no-speech rejected: 1"
                )
            ],
        )

    def test_invalid_lexical_threshold_arguments_are_rejected(self):
        for value in ["-0.01", "1.01", "nan", "inf", "-inf"]:
            with self.subTest(value=value), mock.patch("sys.stderr"):
                arguments = command_line(self)
                arguments[-1] = value

                with self.assertRaises(SystemExit):
                    worker.parse_args(arguments)

    def run_lexical_case(self, responses, hotwords=None, item_count=1):
        items = [
            make_item(
                sequence,
                11,
                "Alice",
                (sequence - 1) * 1_000,
                sequence * 1_000,
                None,
            )
            for sequence in range(1, item_count + 1)
        ]
        write_manifest(self.manifest, items)
        model = FakeModel(responses)

        @contextmanager
        def range_extractor(path, start_ms, end_ms):
            yield self.directory / "complete-range.wav"

        args = make_args(self)
        args.hotword = hotwords or []
        with mock.patch("builtins.print") as emit:
            worker.process(
                args,
                model_factory=lambda unused: model,
                range_extractor=range_extractor,
            )
        results = [
            json.loads(line)
            for line in self.results.read_text(encoding="utf-8").splitlines()
        ]
        return model, results, emit.call_args_list

    def run_vad_case(self, speech_detected, end_ms):
        write_manifest(
            self.manifest,
            [make_item(1, 11, "Alice", 0, end_ms, None)],
        )
        model = FakeModel([["accepted speech"]])

        @contextmanager
        def range_extractor(path, start_ms, extracted_end_ms):
            self.assertEqual((start_ms, extracted_end_ms), (0, end_ms))
            yield self.directory / "complete-range.wav"

        args = make_args(self)
        args.vad_enabled = True
        model_loads = []

        def model_factory(unused):
            model_loads.append(True)
            return model

        with mock.patch("builtins.print") as emit:
            worker.process(
                args,
                model_factory=model_factory,
                range_extractor=range_extractor,
                vad_analyser=lambda unused: speech_detected,
            )
        self.assertEqual(len(model_loads), 1)
        if speech_detected:
            expected = "VAD accepted: 1; non-speech rejected: 0"
        else:
            expected = "VAD accepted: 0; non-speech rejected: 1"
        if speech_detected:
            lexical_expected = (
                "Lexical accepted: 1; empty rejected: 0; "
                "high-no-speech rejected: 0"
            )
        else:
            lexical_expected = (
                "Lexical accepted: 0; empty rejected: 0; "
                "high-no-speech rejected: 0"
            )
        self.assertEqual(
            emit.call_args_list,
            [mock.call(expected), mock.call(lexical_expected)],
        )
        results = [
            json.loads(line)
            for line in self.results.read_text(encoding="utf-8").splitlines()
        ]
        return model, results

    def test_malformed_manifest_and_worker_failure_return_nonzero(self):
        self.manifest.write_text('{"format":1}\n', encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "unexpected schema"):
            worker.load_manifest(self.manifest)

        with mock.patch.object(
            worker, "process", side_effect=RuntimeError("boom")
        ):
            self.assertEqual(worker.main(command_line(self)), 1)

    def test_current_participant_metadata_is_preserved_in_results(self):
        item = make_current_item(
            1, 11, "Tromador", "Stefan", "Tromador", "chair", 0, 500
        )
        write_manifest(self.manifest, [item])

        @contextmanager
        def range_extractor(path, start_ms, end_ms):
            yield self.directory / "range.wav"

        worker.process(
            make_args(self),
            model_factory=lambda args: FakeModel([["Welcome"]]),
            range_extractor=range_extractor,
            vad_analyser=lambda unused: self.fail(
                "disabled VAD must not analyse a range"
            ),
        )

        result = json.loads(self.results.read_text(encoding="utf-8"))
        self.assertEqual(result["format"], 2)
        self.assertEqual(result["discord_name"], "Tromador")
        self.assertEqual(result["name"], "Stefan")
        self.assertEqual(result["speaker"], "Tromador")
        self.assertEqual(result["role"], "chair")
        self.assertNotIn("character", result)

    def test_current_speaker_must_match_retained_name_provenance(self):
        item = make_current_item(
            1,
            11,
            "Tromador",
            "Stefan",
            "Entirely Different Person",
            "chair",
            0,
            500,
        )
        write_manifest(self.manifest, [item])

        with self.assertRaisesRegex(
            ValueError, "invalid work item sequence 1"
        ):
            worker.load_manifest(self.manifest)


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
        vad_enabled=False,
        lexical_no_speech_threshold=0.60,
        hotword=[],
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
        "--vad-enabled",
        "true" if args.vad_enabled else "false",
        "--lexical-no-speech-threshold",
        str(args.lexical_no_speech_threshold),
    ]


if __name__ == "__main__":
    unittest.main()
