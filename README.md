# MLOps Community Podcast Transcripts

Transcripts, tags, and analysis of the [MLOps Community Podcast](https://mlops.community/) hosted by Demetrios Brinkmann.

## 📊 Status

| Metric | Count |
|--------|-------|
| Total Episodes Available | 499 |
| Episodes Downloaded | 20 |
| Episodes Transcribed | 0 |
| Episodes Tagged | 0 |

## 🎯 Project Goals

1. **Transcribe** all episodes using Whisper
2. **Tag** each episode with technology themes
3. **Identify** business and technical trends
4. **Build** a searchable analysis index

## 📁 Structure

```
├── episodes/           # MP3 files (not tracked in git)
├── transcripts/        # Text transcriptions
├── tags/               # Episode tags and themes
├── analysis/           # Aggregated analysis and trends
├── episodes_metadata.json  # Episode metadata from RSS
└── download_episodes.py    # Download script
```

## 🔗 Data Source

- **RSS Feed:** `https://anchor.fm/s/174cb1b8/podcast/rss`
- **Apple Podcasts:** [MLOps.community](https://podcasts.apple.com/us/podcast/mlops-community/id1505372978)

## 🛠️ Usage

```bash
# Download episodes
python download_episodes.py

# Transcribe (coming soon)
python transcribe_episodes.py
```

## 📜 License

Transcripts are for research purposes. Original content © MLOps Community.
