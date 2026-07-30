import argparse
import json
import os
import hashlib
from pathlib import Path
from tqdm import tqdm
import soundfile as sf
import torch
from burst_scope import is_bursty_candidate  # make sure this file is present

# 🔄 Load Silero VAD from TorchHub (legacy tuple-based loader)
vad_model, utils = torch.hub.load(
    repo_or_dir='snakers4/silero-vad',
    model='silero_vad',
    force_reload=True,
    trust_repo=True
)

# Silero utils: ordered tuple expected — upstream may break this
try:
    get_speech_timestamps, _, read_audio, *_ = utils
except Exception as e:
    raise RuntimeError("❌ Silero VAD unpacking failed — upstream format may have changed") from e

def sha256sum(file_path):
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()

def run_vad(filepath):
    wav = read_audio(filepath, sampling_rate=16000)
    speech_ts = get_speech_timestamps(wav, vad_model, sampling_rate=16000)
    return len(speech_ts) > 0

def audit_session(log_file, audio_dir, model_dir, mode="dry-run", output="dedupe_audit.jsonl", vad_min_duration=2.0):
    with open(log_file, "r") as f:
        entries = [json.loads(line) for line in f]

    seen_hashes = set()
    seen_filenames = set()
    audit_log = []

    dupes = 0
    vad_short_rejects = 0
    vad_rejects = 0
    text_sim_rejects = 0
    rms_rejects = 0
    burst_rescues = 0
    accepted = 0

    for entry in tqdm(entries, desc="Auditing entries"):
        fname = entry.get("filename")
        if not fname:
            continue

        fpath = os.path.join(audio_dir, fname)
        if not os.path.exists(fpath):
            continue

        result = dict(entry)  # start with base entry

        # Hash duplicate check
        h = sha256sum(fpath)
        if h in seen_hashes:
            result["status"] = "sha256_duplicate"
            dupes += 1
            audit_log.append(result)
            continue
        seen_hashes.add(h)

        # Filename duplicate check
        if fname in seen_filenames:
            result["status"] = "filename_duplicate"
            dupes += 1
            audit_log.append(result)
            continue
        seen_filenames.add(fname)

        # Duration
        try:
            f = sf.SoundFile(fpath)
            duration = len(f) / f.samplerate
        except:
            result["status"] = "corrupt"
            audit_log.append(result)
            continue

        # Silence pre-filter (RMS)
        y, sr = sf.read(fpath)
        rms_total = (y ** 2).mean() ** 0.5
        if rms_total < 0.003:
            result["status"] = "rms_silent"
            rms_rejects += 1
            audit_log.append(result)
            continue

        # VAD logic
        if not run_vad(fpath):
            if duration < vad_min_duration:
                if is_bursty_candidate(fpath):
                    result["override"] = "burst_scope"
                    result["status"] = "accepted"
                    burst_rescues += 1
                    accepted += 1
                    audit_log.append(result)
                    continue
                else:
                    result["status"] = "vad_reject_short"
                    vad_short_rejects += 1
                    audit_log.append(result)
                    continue
            else:
                result["status"] = "vad_reject"
                vad_rejects += 1
                audit_log.append(result)
                continue

        # Text dedupe - Legacy code, requires transcription.
        # if "text" in entry:
        #    text = entry["text"].strip().lower()
        #    if len(text) < 3 or text in {"thank you", "thanks", "no", "yes", "uh"}:
        #        result["status"] = "text_similar"
        #        text_sim_rejects += 1
        #        audit_log.append(result)
        #        continue

        result["status"] = "accepted"
        accepted += 1
        audit_log.append(result)

    if mode != "dry-run":
        with open(output, "w") as out:
            for r in audit_log:
                out.write(json.dumps(r) + "\n")

    print("\n🔍 Deduplication Summary:")
    print(f"  Total entries:           {len(entries)}")
    print(f"  Accepted:                {accepted}")
    print(f"  SHA-256 duplicates:      {dupes}")
    print(f"  RMS silent rejects:      {rms_rejects}")
    print(f"  VAD rejected (short):    {vad_short_rejects}")
    print(f"  VAD rejected (long):     {vad_rejects}")
    print(f"  Burst_scope rescues:     {burst_rescues}")
#    print(f"  Text similarity rejects: {text_sim_rejects}") # Legacy as above 

    print(f"  Audit log written:       {output}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-file", required=True)
    parser.add_argument("--audio-dir", required=True)
    parser.add_argument("--model-dir", required=True)
    parser.add_argument("--mode", default="dry-run")
    parser.add_argument("--output", default="dedupe_audit.jsonl")
    parser.add_argument("--vad-min-duration", type=float, default=2.0)
    args = parser.parse_args()

    audit_session(
        log_file=args.log_file,
        audio_dir=args.audio_dir,
        model_dir=args.model_dir,
        mode=args.mode,
        output=args.output,
        vad_min_duration=args.vad_min_duration
    )
