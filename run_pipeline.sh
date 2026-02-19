#!/bin/bash
# Run pipeline with logging, auto-restart on failure
LOG_FILE="pipeline_$(date +%Y%m%d_%H%M%S).log"
echo "Starting pipeline at $(date)" | tee -a "$LOG_FILE"
while true; do
    python3 -u pipeline.py 2>&1 | tee -a "$LOG_FILE"
    EXIT_CODE=$?
    if [ $EXIT_CODE -eq 0 ]; then
        echo "Pipeline completed successfully at $(date)" | tee -a "$LOG_FILE"
        break
    else
        echo "Pipeline exited with code $EXIT_CODE at $(date), restarting in 10s..." | tee -a "$LOG_FILE"
        sleep 10
    fi
done
