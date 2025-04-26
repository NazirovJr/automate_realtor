#!/bin/bash

# Find all running instances of tg.py and stop them
echo "Stopping all running Telegram bot instances..."
pkill -f "python.*tg.py" || echo "No running bot instances found"

# Remove the lock file if it exists
LOCK_FILE="$(dirname "$0")/bot.lock"
if [ -f "$LOCK_FILE" ]; then
    echo "Removing lock file: $LOCK_FILE"
    rm -f "$LOCK_FILE"
else
    echo "No lock file found"
fi

echo "All bot instances have been stopped and lock files cleaned up."
echo "You can now start the bot again." 