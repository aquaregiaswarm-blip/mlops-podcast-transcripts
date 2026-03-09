#!/usr/bin/env python3
"""Retry failed episodes with longer upload timeout (300s)."""

import os
import json
import time
import subprocess
from pathlib import Path
from datetime import datetime

from google.cloud import speech_v1p1beta1 as speech
from google.cloud import storage
from google.cloud.storage import transfer_manager
import google.generativeai as genai

# Paths
EPISODES_DIR = Path("episodes")
TRANSCRIPTS_DIR = Path("transcripts")
TAGS_DIR = Path("tags")
ANALYSIS_DIR = Path("analysis")
METADATA_FILE = Path("episodes_metadata.json")

# Config
GCP_PROJECT = "prj-cts-lab-vertex-sandbox"
GCS_BUCKET = "mlops-podcast-audio"
GCP_CREDENTIALS = "/home/jdgough/.openclaw/media/inbound/file_4---961897d5-1212-4747-bf78-cdf8829e5295.json"
GEMINI_API_KEY = "AIzaSyCGzlZd01l0Gy7cfn66HKxvxbiPv11VO_s"
UPLOAD_TIMEOUT = 300  # 5 minutes

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_CREDENTIALS


def convert_to_flac(mp3_path: Path) -> Path:
    """Convert MP3 to FLAC for Speech-to-Text (mono, 16kHz)."""
    flac_path = mp3_path.with_suffix(".flac")
    if flac_path.exists():
        return flac_path
    
    print(f"    Converting to FLAC...")
    cmd = [
        "ffmpeg", "-y", "-i", str(mp3_path),
        "-ac", "1", "-ar", "16000", "-c:a", "flac",
        str(flac_path)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"    FFmpeg error: {result.stderr[:200]}")
        return None
    return flac_path


def upload_with_timeout(local_path: Path, bucket) -> str:
    """Upload file to GCS with extended timeout."""
    blob_name = f"audio/{local_path.name}"
    blob = bucket.blob(blob_name)
    
    if blob.exists():
        print(f"    Already in GCS")
        return f"gs://{GCS_BUCKET}/{blob_name}"
    
    file_size_mb = local_path.stat().st_size / (1024 * 1024)
    print(f"    Uploading {file_size_mb:.1f}MB (timeout: {UPLOAD_TIMEOUT}s)...")
    
    # Use resumable upload for large files
    blob.upload_from_filename(
        str(local_path),
        timeout=UPLOAD_TIMEOUT,
        retry=None  # Disable retry to see actual errors
    )
    return f"gs://{GCS_BUCKET}/{blob_name}"


def transcribe_from_gcs(gcs_uri: str) -> str:
    """Transcribe audio from GCS."""
    client = speech.SpeechClient()
    
    audio = speech.RecognitionAudio(uri=gcs_uri)
    config = speech.RecognitionConfig(
        encoding=speech.RecognitionConfig.AudioEncoding.FLAC,
        sample_rate_hertz=16000,
        language_code="en-US",
        enable_automatic_punctuation=True,
        model="latest_long",
        use_enhanced=True,
    )
    
    print(f"    Transcribing...")
    operation = client.long_running_recognize(config=config, audio=audio)
    
    start_time = time.time()
    while not operation.done():
        elapsed = int(time.time() - start_time)
        print(f"    Transcribing... ({elapsed}s)", end="\r")
        time.sleep(10)
    
    response = operation.result(timeout=1800)
    
    transcript = ""
    for result in response.results:
        transcript += result.alternatives[0].transcript + "\n"
    
    return transcript.strip()


def tag_episode(transcript: str, title: str) -> dict:
    """Use Gemini to extract tags."""
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-2.0-flash")
    
    prompt = f"""Analyze this podcast transcript and extract:
1. Technology Tags (5-10 specific technologies mentioned)
2. Business Tags (3-5 business concepts/themes)
3. Key Topics (3-5 main discussion topics)
4. Guest Info (name, role, company)
5. One-line Summary (25 words max)

Episode: {title}
Transcript (first 10000 chars):
{transcript[:10000]}

Respond in valid JSON only:
{{"tech_tags": [], "business_tags": [], "key_topics": [], "guest": {{"name": "", "role": "", "company": ""}}, "summary": ""}}"""
    
    response = model.generate_content(prompt)
    text = response.text.strip()
    if text.startswith("```"):
        text = "\n".join(text.split("\n")[1:-1])
    return json.loads(text)


def get_failed_episodes():
    """Find episodes that don't have transcripts yet."""
    with open(METADATA_FILE) as f:
        episodes = json.load(f)
    
    failed = []
    for ep in episodes:
        # Find the MP3 file
        for mp3 in EPISODES_DIR.glob("*.mp3"):
            title_words = ep.get("title", "").lower().split()[:2]
            if all(w in mp3.name.lower() for w in title_words if len(w) > 3):
                transcript_file = TRANSCRIPTS_DIR / f"{mp3.stem}.txt"
                if not transcript_file.exists():
                    ep["local_file"] = str(mp3)
                    failed.append(ep)
                break
    
    return failed


def main():
    print("🔄 Retrying failed episodes with 300s upload timeout\n")
    
    # Get GCS bucket
    client = storage.Client(project=GCP_PROJECT)
    bucket = client.bucket(GCS_BUCKET)
    
    # Find failed episodes
    failed = get_failed_episodes()
    print(f"Found {len(failed)} episodes to retry\n")
    
    success = 0
    for i, ep in enumerate(failed):
        title = ep.get("title", "Unknown")[:55]
        mp3_path = Path(ep.get("local_file", ""))
        
        print(f"[{i+1}/{len(failed)}] {title}...")
        
        if not mp3_path.exists():
            print(f"  ⚠️ MP3 not found")
            continue
        
        try:
            # Convert to FLAC
            flac_path = convert_to_flac(mp3_path)
            if not flac_path:
                continue
            
            # Upload with extended timeout
            gcs_uri = upload_with_timeout(flac_path, bucket)
            
            # Transcribe
            transcript = transcribe_from_gcs(gcs_uri)
            if not transcript:
                print(f"\n  ⚠️ Empty transcript")
                continue
            
            # Save transcript
            transcript_file = TRANSCRIPTS_DIR / f"{mp3_path.stem}.txt"
            transcript_file.write_text(transcript)
            print(f"\n  ✓ Transcript saved ({len(transcript)} chars)")
            
            # Tag
            print(f"  🏷️ Tagging...")
            tags = tag_episode(transcript, ep.get("title", ""))
            tags_file = TAGS_DIR / f"{mp3_path.stem}.json"
            with open(tags_file, "w") as f:
                json.dump(tags, f, indent=2)
            print(f"  ✓ Tags saved")
            
            # Cleanup FLAC
            if flac_path.exists():
                flac_path.unlink()
            
            success += 1
            time.sleep(2)
            
        except Exception as e:
            print(f"\n  ❌ Error: {e}")
            continue
    
    print(f"\n✅ Retry complete: {success}/{len(failed)} succeeded")
    print(f"   Total transcripts: {len(list(TRANSCRIPTS_DIR.glob('*.txt')))}")


if __name__ == "__main__":
    main()
