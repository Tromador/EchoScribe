# Discord Audio Transcript Deduplication Pipeline

This repository contains a multi-stage pipeline for processing, transcribing, and deduplicating Discord voice session recordings into clean text transcripts. It includes tools for audio capture, filtering, transcription, clustering-based deduplication, and final text output.

## 📚 Overview

The pipeline operates in the following phases:
1. **Phase 0** – **Discord Audio Capture**  
   Captures user audio streams as individual `.wav` files and generates session logs.

2. **Phase 1** – **Audio Validation and Filtering**  
   Filters audio for silence, duration constraints, and rescues bursty utterances with VAD.

3. **Phase 2** – **Whisper Transcription**  
   Transcribes accepted audio files to text using a CTranslate2-based Whisper model.

4. **Phase 3** – **Deduplication by Clustering**  
   Clusters transcriptions and deduplicates based on similarity, canonical form, and scoring.

5. **Output** – A cleaned `.txt` transcript preserving character, flow, and session integrity.

## 🛠 Scripts

| Script                  | Purpose                                                   |
|--------------------------|-----------------------------------------------------------|
| `index.ts`               | Captures Discord voice as per-user `.wav` files          |
| `dedupe_audit.py`        | Filters raw audio: silence, noise, duplicates, duration   |
| `burst_scope.py`         | Rescues short sharp utterances from false VAD rejection   |
| `transcribe_accepted.py` | Transcribes accepted `.wav` files into enriched JSONL     |
| `dedupe_transcript.py`   | Deduplicates transcribed JSONL using clustering           |

## 🚀 Quick Start

1. Clone the repo and install required Python and Node.js dependencies.
2. Configure `.env` with your Discord bot credentials.
3. Run each phase in sequence:
   - `index.ts` to capture audio.
   - `dedupe_audit.py` to filter audio.
   - `transcribe_accepted.py` to transcribe.
   - `dedupe_transcript.py` to deduplicate.
4. Review the final transcript output.

---

## ⚡ Key Notes
- Built around **faster-whisper** with standard **CTranslate2** binary releases from PyPI.
- Supports both **GPU (CUDA)** and **CPU** transcription paths via runtime flags in `transcribe_accepted.py`.
- Still tested heavily on RTX-class GPUs, but no longer documented as requiring a custom local CTranslate2 build.

---

## 📦 Installation

### Python Dependencies
    pip install -r requirements.txt
(further dependencies may be required)

CTranslate2 is available on PyPI (for example, `pip index versions ctranslate2` currently reports 4.7.1 and historical releases).

### Node.js Dependencies
    npm install 
(further dependencies may be required)

---

**AI Transparency Statement** - Mostly built with the aid of ChatGPT. Author is a sysadmin and project manager with some decades of experience. Author believes he can appropriately supervise the "dev team", nevertheless wishes to be honest and upfront for anyone who worries about such things.

---

See also [Pipeline Document](https://github.com/Tromador/EchoScribe/blob/main/discord_transcript_pipeline.md)

---

![GPU Accelerated](https://img.shields.io/badge/GPU-accelerated-blueviolet?logo=nvidia&style=flat-square)
![Backend: CTranslate2](https://img.shields.io/badge/Backend-CTranslate2-green?style=flat-square)
![Transcription: Whisper](https://img.shields.io/badge/Transcription-Whisper_Large_v3-informational?style=flat-square)
![Voice Activity Detection](https://img.shields.io/badge/VAD-Silero-orange?style=flat-square)
![Clustering](https://img.shields.io/badge/Deduplication-Clustering_based-critical?style=flat-square)
![Burst Rescue](https://img.shields.io/badge/Burst_Rescue-Enabled-ff69b4?style=flat-square)
![Audio Format](https://img.shields.io/badge/Audio-WAV_48kHz_16bit_mono-lightgrey?style=flat-square)
![Session Logs](https://img.shields.io/badge/Logging-JSONL_per_user-yellow?style=flat-square)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&style=flat-square)
![Node.js](https://img.shields.io/badge/Node.js-Required-yellowgreen?logo=node.js&style=flat-square)
![BSD 3-Clause](https://img.shields.io/badge/License-BSD_3--Clause-lightgrey?style=flat-square)

---
