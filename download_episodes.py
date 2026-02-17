#!/usr/bin/env python3
"""Download MLOps Community podcast episodes from RSS feed."""

import os
import re
import json
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

RSS_URL = "https://anchor.fm/s/174cb1b8/podcast/rss"
EPISODES_DIR = Path("episodes")
METADATA_FILE = Path("episodes_metadata.json")

def clean_filename(title: str) -> str:
    """Create a clean filename from episode title."""
    # Remove special chars, keep alphanumeric and spaces
    clean = re.sub(r'[^\w\s-]', '', title)
    clean = re.sub(r'\s+', '-', clean.strip())
    return clean[:80].lower()

def parse_rss(xml_content: str) -> list:
    """Parse RSS feed and extract episode info."""
    root = ET.fromstring(xml_content)
    episodes = []
    
    ns = {
        'itunes': 'http://www.itunes.com/dtds/podcast-1.0.dtd',
        'content': 'http://purl.org/rss/1.0/modules/content/'
    }
    
    for item in root.findall('.//item'):
        title = item.find('title').text if item.find('title') is not None else ''
        
        # Get episode number from title if present
        ep_match = re.search(r'#(\d+)', title)
        ep_num = ep_match.group(1) if ep_match else None
        
        enclosure = item.find('enclosure')
        audio_url = enclosure.get('url') if enclosure is not None else None
        
        pub_date = item.find('pubDate')
        pub_date = pub_date.text if pub_date is not None else ''
        
        duration = item.find('itunes:duration', ns)
        duration = duration.text if duration is not None else ''
        
        description = item.find('description')
        description = description.text if description is not None else ''
        
        episodes.append({
            'title': title,
            'episode_number': ep_num,
            'audio_url': audio_url,
            'pub_date': pub_date,
            'duration': duration,
            'description': description[:500] + '...' if len(description or '') > 500 else description,
        })
    
    return episodes

def download_episode(ep: dict, index: int) -> str:
    """Download a single episode MP3."""
    if not ep['audio_url']:
        return None
    
    ep_num = ep['episode_number'] or f"ep{index:03d}"
    filename = f"ep{ep_num}-{clean_filename(ep['title'])}.mp3"
    filepath = EPISODES_DIR / filename
    
    if filepath.exists():
        print(f"  Skipping (exists): {filename}")
        return str(filepath)
    
    print(f"  Downloading: {filename}")
    try:
        urllib.request.urlretrieve(ep['audio_url'], filepath)
        return str(filepath)
    except Exception as e:
        print(f"  Error downloading: {e}")
        return None

def main():
    EPISODES_DIR.mkdir(exist_ok=True)
    
    print("Fetching RSS feed...")
    with urllib.request.urlopen(RSS_URL) as response:
        xml_content = response.read().decode('utf-8')
    
    print("Parsing episodes...")
    episodes = parse_rss(xml_content)
    print(f"Found {len(episodes)} episodes")
    
    # Download latest 20 episodes
    latest = episodes[:20]
    print(f"\nDownloading latest {len(latest)} episodes...")
    
    for i, ep in enumerate(latest):
        print(f"\n[{i+1}/{len(latest)}] {ep['title'][:60]}...")
        filepath = download_episode(ep, i)
        if filepath:
            ep['local_file'] = filepath
    
    # Save metadata
    print(f"\nSaving metadata to {METADATA_FILE}...")
    with open(METADATA_FILE, 'w') as f:
        json.dump(latest, f, indent=2)
    
    print("\nDone!")

if __name__ == "__main__":
    main()
