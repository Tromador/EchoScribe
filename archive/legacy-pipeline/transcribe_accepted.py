import argparse
import json
from pathlib import Path

from faster_whisper import WhisperModel
from tqdm import tqdm


def load_entries(jsonl_path):
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        return [json.loads(line) for line in f if line.strip()]


def save_jsonl(entries, output_path):
    with open(output_path, 'w', encoding='utf-8') as f:
        for entry in entries:
            json.dump(entry, f)
            f.write('\n')


def main():
    parser = argparse.ArgumentParser(
        description="Transcribe accepted entries and write an updated JSONL."
    )
    parser.add_argument('--input-log', type=Path, required=True, help="Path to deduped .jsonl log")
    parser.add_argument('--audio-dir', type=Path, required=True, help="Directory containing audio files")
    parser.add_argument('--model-dir', type=Path, required=True, help="Path to Whisper CTranslate2 model")
    parser.add_argument('--output-jsonl', type=Path, required=True, help="Path to save updated JSONL with transcriptions")
    parser.add_argument(
        '--device',
        default='auto',
        choices=['auto', 'cpu', 'cuda'],
        help="Inference device. Use 'cpu' to run without GPU/CUDA (default: auto).",
    )
    parser.add_argument(
        '--compute-type',
        default='auto',
        help="faster-whisper compute type, e.g. auto/int8/int8_float16/float16.",
    )
    parser.add_argument(
        '--cpu-threads',
        type=int,
        default=4,
        help="CPU thread count used by CTranslate2 when CPU is selected or auto falls back.",
    )
    parser.add_argument('--beam-size', type=int, default=5, help="Beam size for Whisper decoding (default: 5)")
    args = parser.parse_args()

    print(
        f"🚀 Loading Whisper model from {args.model_dir} "
        f"(device={args.device}, compute_type={args.compute_type}, cpu_threads={args.cpu_threads})"
    )

    model = WhisperModel(
        str(args.model_dir.resolve()),
        device=args.device,
        compute_type=args.compute_type,
        cpu_threads=args.cpu_threads,
    )

    entries = load_entries(args.input_log)
    accepted = [e for e in entries if e.get('status') == 'accepted']

    print(f"📝 Transcribing {len(accepted)} accepted entries out of {len(entries)} total...")
    for entry in tqdm(accepted, desc='Transcribing'):
        audio_path = args.audio_dir / entry['filename']
        if not audio_path.exists():
            entry['transcription_error'] = f"missing_audio:{entry['filename']}"
            print(f"⚠️ Missing: {entry['filename']}")
            continue

        try:
            segments, _ = model.transcribe(str(audio_path), beam_size=args.beam_size)
            entry['text'] = ' '.join([seg.text.strip() for seg in segments]).strip()
        except Exception as e:
            entry['transcription_error'] = str(e)
            print(f"❌ Failed to transcribe {entry['filename']}: {e}")

    save_jsonl(entries, args.output_jsonl)
    print(f"✅ Saved updated JSONL to: {args.output_jsonl}")


if __name__ == '__main__':
    main()
