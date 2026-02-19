#!/usr/bin/env python3
"""
MLOps Podcast Pipeline: Transcribe → Tag → Analyze

Processes episodes sequentially for reliability.
Uses Google Speech-to-Text (via GCS) and Gemini for analysis.
"""

import os
import json
import time
import subprocess
from pathlib import Path
from datetime import datetime

# Google Cloud clients
from google.cloud import speech_v1p1beta1 as speech
from google.cloud import storage

# Vertex AI Gemini for tagging
import vertexai
from vertexai.generative_models import GenerativeModel

# Paths
EPISODES_DIR = Path("episodes")
TRANSCRIPTS_DIR = Path("transcripts")
TAGS_DIR = Path("tags")
ANALYSIS_DIR = Path("analysis")
METADATA_FILE = Path("episodes_metadata.json")
PROGRESS_FILE = Path("pipeline_progress.json")

# Config
GCP_PROJECT = "prj-cts-lab-vertex-sandbox"
GCS_BUCKET = "mlops-podcast-audio"
GCP_CREDENTIALS = "/home/jdgough/.openclaw/media/inbound/file_4---961897d5-1212-4747-bf78-cdf8829e5295.json"

# Set credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_CREDENTIALS

# Initialize Vertex AI
vertexai.init(project=GCP_PROJECT, location="us-central1")


def load_progress():
    """Load pipeline progress."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"transcribed": [], "tagged": [], "started_at": datetime.now().isoformat()}


def save_progress(progress):
    """Save pipeline progress."""
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def ensure_bucket_exists():
    """Create GCS bucket if it doesn't exist."""
    client = storage.Client(project=GCP_PROJECT)
    try:
        bucket = client.get_bucket(GCS_BUCKET)
        print(f"  Using existing bucket: gs://{GCS_BUCKET}")
    except Exception:
        print(f"  Creating bucket: gs://{GCS_BUCKET}")
        bucket = client.create_bucket(GCS_BUCKET, location="us-east1")
    return bucket


def convert_to_flac(mp3_path: Path) -> Path:
    """Convert MP3 to FLAC for Speech-to-Text (mono, 16kHz)."""
    flac_path = mp3_path.with_suffix(".flac")
    if flac_path.exists():
        return flac_path
    
    print(f"    Converting to FLAC (mono 16kHz)...")
    cmd = [
        "ffmpeg", "-y", "-i", str(mp3_path),
        "-ac", "1",  # mono
        "-ar", "16000",  # 16kHz
        "-c:a", "flac",
        str(flac_path)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"    FFmpeg error: {result.stderr[:200]}")
        return None
    return flac_path


def upload_to_gcs(local_path: Path, bucket) -> str:
    """Upload file to GCS and return the gs:// URI."""
    blob_name = f"audio/{local_path.name}"
    blob = bucket.blob(blob_name)
    
    if blob.exists():
        print(f"    Already in GCS: gs://{GCS_BUCKET}/{blob_name}")
        return f"gs://{GCS_BUCKET}/{blob_name}"
    
    print(f"    Uploading to GCS...")
    # Use resumable upload with longer timeout for large files
    from google.cloud.storage import retry
    blob.upload_from_filename(
        str(local_path),
        timeout=600,  # 10 min timeout
        retry=retry.DEFAULT_RETRY.with_deadline(600)
    )
    return f"gs://{GCS_BUCKET}/{blob_name}"


def transcribe_from_gcs(gcs_uri: str) -> str:
    """Transcribe audio from GCS using Speech-to-Text long-running API."""
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
    
    print(f"    Starting transcription (this takes a while)...")
    operation = client.long_running_recognize(config=config, audio=audio)
    
    # Poll for completion with progress updates
    start_time = time.time()
    while not operation.done():
        elapsed = int(time.time() - start_time)
        print(f"    Transcribing... ({elapsed}s elapsed)", end="\r")
        time.sleep(10)
    
    response = operation.result(timeout=1800)  # 30 min timeout
    
    transcript = ""
    for result in response.results:
        transcript += result.alternatives[0].transcript + "\n"
    
    return transcript.strip()


def tag_episode(transcript: str, title: str) -> dict:
    """Use Vertex AI Gemini to extract tags and themes from transcript."""
    model = GenerativeModel("gemini-2.0-flash-001")
    
    prompt = f"""Analyze this podcast episode transcript and extract:

1. **Technology Tags** (5-10 specific technologies, frameworks, or tools mentioned)
2. **Business Tags** (3-5 business concepts, strategies, or themes)
3. **Key Topics** (3-5 main discussion topics)
4. **Guest Info** (name, role, company if mentioned)
5. **One-line Summary** (25 words max)

Episode Title: {title}

Transcript (first 10000 chars):
{transcript[:10000]}

Respond in valid JSON only (no markdown):
{{
    "tech_tags": ["tag1", "tag2"],
    "business_tags": ["tag1", "tag2"],
    "key_topics": ["topic1", "topic2"],
    "guest": {{"name": "...", "role": "...", "company": "..."}},
    "summary": "..."
}}
"""
    
    response = model.generate_content(prompt)
    
    # Parse JSON from response
    text = response.text.strip()
    # Remove markdown code blocks if present
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1])
    
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"error": "Failed to parse response", "raw": text[:500]}


def process_episode(ep: dict, progress: dict, bucket) -> bool:
    """Process a single episode through the pipeline."""
    title = ep.get("title", "Unknown")
    local_file = ep.get("local_file")
    
    if not local_file:
        # Try to find the file
        for f in EPISODES_DIR.glob("*.mp3"):
            if any(word.lower() in f.name.lower() for word in title.split()[:3]):
                local_file = str(f)
                break
    
    if not local_file or not Path(local_file).exists():
        print(f"  ⚠️  No audio file found")
        return False
    
    mp3_path = Path(local_file)
    base_name = mp3_path.stem
    
    # Step 1: Transcribe
    transcript_file = TRANSCRIPTS_DIR / f"{base_name}.txt"
    if transcript_file.exists():
        print(f"  ✓ Transcript exists ({transcript_file.stat().st_size} bytes)")
        transcript = transcript_file.read_text()
    else:
        print(f"  📝 Transcribing...")
        
        # Convert to FLAC
        flac_path = convert_to_flac(mp3_path)
        if not flac_path:
            print(f"  ⚠️  Failed to convert to FLAC")
            return False
        
        try:
            # Upload to GCS
            gcs_uri = upload_to_gcs(flac_path, bucket)
            
            # Transcribe
            transcript = transcribe_from_gcs(gcs_uri)
            
            if not transcript:
                print(f"  ⚠️  Empty transcript")
                return False
            
            transcript_file.write_text(transcript)
            print(f"\n  ✓ Saved transcript ({len(transcript)} chars)")
            
            # Clean up FLAC to save space
            if flac_path.exists():
                flac_path.unlink()
            
            progress["transcribed"].append(base_name)
            save_progress(progress)
            
        except Exception as e:
            print(f"\n  ⚠️  Transcription failed: {e}")
            # Clean up FLAC
            if flac_path and flac_path.exists():
                flac_path.unlink()
            return False
    
    # Step 2: Tag
    tags_file = TAGS_DIR / f"{base_name}.json"
    if tags_file.exists():
        print(f"  ✓ Tags exist")
    else:
        print(f"  🏷️  Extracting tags...")
        try:
            tags = tag_episode(transcript, title)
            with open(tags_file, "w") as f:
                json.dump(tags, f, indent=2)
            print(f"  ✓ Saved tags")
            
            progress["tagged"].append(base_name)
            save_progress(progress)
            
            # Rate limit for Gemini
            time.sleep(2)
        except Exception as e:
            print(f"  ⚠️  Tagging failed: {e}")
    
    return True


def build_analysis_index():
    """Build aggregated analysis from all tagged episodes."""
    print("\n📊 Building analysis index...")
    
    all_tech_tags = {}
    all_business_tags = {}
    all_topics = {}
    episodes_summary = []
    
    for tags_file in TAGS_DIR.glob("*.json"):
        with open(tags_file) as f:
            data = json.load(f)
        
        if "error" in data:
            continue
        
        # Count tech tags
        for tag in data.get("tech_tags", []):
            tag_lower = tag.lower()
            all_tech_tags[tag_lower] = all_tech_tags.get(tag_lower, 0) + 1
        
        # Count business tags
        for tag in data.get("business_tags", []):
            tag_lower = tag.lower()
            all_business_tags[tag_lower] = all_business_tags.get(tag_lower, 0) + 1
        
        # Count topics
        for topic in data.get("key_topics", []):
            topic_lower = topic.lower()
            all_topics[topic_lower] = all_topics.get(topic_lower, 0) + 1
        
        episodes_summary.append({
            "file": tags_file.stem,
            "summary": data.get("summary", ""),
            "guest": data.get("guest", {}),
            "tech_tags": data.get("tech_tags", []),
        })
    
    # Sort by frequency
    tech_sorted = sorted(all_tech_tags.items(), key=lambda x: -x[1])
    business_sorted = sorted(all_business_tags.items(), key=lambda x: -x[1])
    topics_sorted = sorted(all_topics.items(), key=lambda x: -x[1])
    
    analysis = {
        "generated_at": datetime.now().isoformat(),
        "episodes_analyzed": len(episodes_summary),
        "top_tech_themes": tech_sorted[:25],
        "top_business_themes": business_sorted[:20],
        "top_topics": topics_sorted[:20],
        "episodes": episodes_summary,
    }
    
    with open(ANALYSIS_DIR / "index.json", "w") as f:
        json.dump(analysis, f, indent=2)
    
    print(f"  ✓ Analyzed {len(episodes_summary)} episodes")
    if tech_sorted:
        print(f"  Top tech themes: {[t[0] for t in tech_sorted[:5]]}")
    if business_sorted:
        print(f"  Top business themes: {[t[0] for t in business_sorted[:5]]}")
    
    return analysis


def main():
    # Ensure directories exist
    for d in [TRANSCRIPTS_DIR, TAGS_DIR, ANALYSIS_DIR]:
        d.mkdir(exist_ok=True)
    
    # Load metadata
    if not METADATA_FILE.exists():
        print("❌ No episodes metadata found. Run download_episodes.py first.")
        return
    
    with open(METADATA_FILE) as f:
        episodes = json.load(f)
    
    print(f"🎙️  MLOps Podcast Pipeline")
    print(f"   Episodes to process: {len(episodes)}")
    print(f"   GCS Bucket: gs://{GCS_BUCKET}")
    print(f"   Output: transcripts/, tags/, analysis/\n")
    
    # Ensure GCS bucket exists
    bucket = ensure_bucket_exists()
    
    # Load progress
    progress = load_progress()
    
    # Process each episode sequentially
    for i, ep in enumerate(episodes):
        title = ep.get("title", "Unknown")[:60]
        print(f"\n[{i+1}/{len(episodes)}] {title}...")
        
        try:
            process_episode(ep, progress, bucket)
        except Exception as e:
            print(f"  ❌ Error: {e}")
            import traceback
            traceback.print_exc()
            continue
        
        # Small delay between episodes
        time.sleep(1)
    
    # Build analysis index
    build_analysis_index()
    
    print("\n✅ Pipeline complete!")
    print(f"   Transcripts: {len(list(TRANSCRIPTS_DIR.glob('*.txt')))}")
    print(f"   Tagged: {len(list(TAGS_DIR.glob('*.json')))}")


if __name__ == "__main__":
    main()
