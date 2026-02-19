#!/bin/bash
cd ~/.openclaw/workspace/mlops-podcast-transcripts
if ! pgrep -f "python.*pipeline.py" > /dev/null; then
    echo "$(date): Pipeline not running, restarting..." >> restart.log
    nohup ./run_pipeline.sh > /dev/null 2>&1 &
else
    echo "$(date): Pipeline running" >> restart.log
fi
