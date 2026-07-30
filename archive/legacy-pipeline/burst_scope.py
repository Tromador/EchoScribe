import numpy as np
import soundfile as sf
import matplotlib.pyplot as plt
import sys
import os

# Import-safe burstiness checker
def is_bursty_candidate(filepath, rms_threshold=0.003, std_threshold=0.03, frame_ms=20):
    try:
        y, sr = sf.read(filepath)
    except Exception as e:
        print(f"❌ Could not read {filepath}: {e}")
        return False

    # Basic silence filter
    rms_total = (y ** 2).mean() ** 0.5
    if rms_total < rms_threshold:
        return False

    # Frame-by-frame RMS to detect variation
    frame_size = int(sr * (frame_ms / 1000.0))
    rms_vals = [((y[i:i+frame_size] ** 2).mean() ** 0.5)
                for i in range(0, len(y), frame_size)]
    std_rms = np.std(rms_vals)

    return std_rms > std_threshold

# Optional CLI usage
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 burst_scope.py <audio_file.wav>")
        sys.exit(1)

    path = sys.argv[1]
    if not os.path.exists(path):
        print(f"❌ File not found: {path}")
        sys.exit(1)

    y, sr = sf.read(path)
    duration = len(y) / sr
    rms_total = (y ** 2).mean() ** 0.5
    frame_size = int(sr * 0.02)
    rms_vals = [((y[i:i+frame_size] ** 2).mean() ** 0.5)
                for i in range(0, len(y), frame_size)]
    std_rms = np.std(rms_vals)

    print(f"\n📄 File: {path}")
    print(f"⏱ Duration: {duration:.2f} seconds")
    print(f"🔋 Total RMS: {rms_total:.6f}")
    print(f"📈 Frame RMS std: {std_rms:.6f}")
    print(f"   Max Frame RMS: {max(rms_vals):.6f}")
    print(f"   Min Frame RMS: {min(rms_vals):.6f}")

    if rms_total < 0.003:
        print("❌ Rejected: Below RMS threshold (silence)")
    elif std_rms > 0.03:
        print("💥 Bursty Signal? YES")
        print("✅ Candidate for Whisper rescue")
    else:
        print("💥 Bursty Signal? NO")
        print("❌ Likely background noise / hum / click")

    try:
        plt.plot(rms_vals)
        plt.title("RMS Over Time")
        plt.xlabel("Frame index")
        plt.ylabel("RMS")
        plt.grid(True)
        plt.show()
    except:
        pass
