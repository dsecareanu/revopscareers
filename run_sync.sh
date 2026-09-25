#!/bin/bash
# =============================================================================
# RevOpsCareers — server cron entry point (replaces the GitHub Action)
#   bash run_sync.sh full      # imports + cleanup (once a day)
#   bash run_sync.sh imports   # imports only
#
# - Skips quietly if the previous run is still going (flock)
# - Pulls code updates from GitHub before each run
# - Per-run logs in ~/logs/revopscareers-sync/ (kept 14 days)
# - Emails ALERT_EMAIL (from .env) via WordPress wp_mail if the run fails
# =============================================================================

MODE="${1:-full}"
DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$HOME/logs/revopscareers-sync"
WP_PATH="$HOME/revopscareers.com/public"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/$(date -u +%Y%m%d-%H%M)-$MODE.log"

exec 9>"$LOG_DIR/.lock"
if ! flock -n 9; then
    echo "$(date -u '+%F %T') $MODE run skipped — previous run still active" >> "$LOG_DIR/skipped.log"
    exit 0
fi

cd "$DIR"
git pull -q --ff-only >> "$LOG" 2>&1 || echo "WARNING: git pull failed, running current code" >> "$LOG"

SYNC_LOG_FILE=/dev/null nice -n 10 ionice -c2 -n7 bash daily_sync.sh "$MODE" >> "$LOG" 2>&1
STATUS=$?

if [ $STATUS -ne 0 ]; then
    ALERT_EMAIL="$(grep -E '^ALERT_EMAIL=' .env | cut -d= -f2-)"
    ALERT_EMAIL="$ALERT_EMAIL" SYNC_MODE="$MODE" SYNC_LOG="$LOG" \
        wp --path="$WP_PATH" eval-file "$DIR/notify_failure.php" >> "$LOG" 2>&1
fi

find "$LOG_DIR" -name "*.log" -mtime +14 -delete
exit $STATUS
