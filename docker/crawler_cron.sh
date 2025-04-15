#!/bin/sh

# Configure cron job to run at 1:00 PM every day
echo "0 13 * * * cd /app && python -m src.krisha.main >> /app/cron.log 2>&1" > /etc/cron.d/krisha-cron
chmod 0644 /etc/cron.d/krisha-cron

# Apply cron job
crontab /etc/cron.d/krisha-cron

# Start cron daemon
echo "Starting cron..."
cron -f 