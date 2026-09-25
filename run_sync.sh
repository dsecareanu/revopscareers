#!/bin/bash
# =============================================================================
# RevOpsCareers — server cron entry point (replaces the GitHub Action)
#   bash run_sync.sh full      # imports + cleanup (once a day)
#   bash run_sync.sh imports   # imports only
#
# - Skips quietly if the previous run is still going (flock)
# - Pulls code updates from GitHub before each run
# - Exports .env so every script gets the credentials (as the Action did)
# - Per-run logs in ~/logs/revopscareers-sync/ (kept 14 days)
# - Emails ALERT_EMAIL (from .env) via WordPress wp_mail if the run fails
#
# Everything runs inside main() so bash parses the whole file before executing:
# `git pull` may replace this file while it is running.
# =============================================================================

main() {
    local MODE="${1:-full}"
    local DIR LOG_DIR WP_PATH LOG STATUS
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

    # Export .env line by line (values may contain spaces, so don't `source` it)
    while IFS= read -r line || [ -n "$line" ]; do
        [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] && export "${line%%=*}=${line#*=}"
    done < .env

    SYNC_LOG_FILE=/dev/null nice -n 10 ionice -c2 -n7 bash daily_sync.sh "$MODE" >> "$LOG" 2>&1
    STATUS=$?

    if [ $STATUS -ne 0 ]; then
        SYNC_MODE="$MODE" SYNC_LOG="$LOG" \
            wp --path="$WP_PATH" eval-file "$DIR/notify_failure.php" >> "$LOG" 2>&1
    fi

    find "$LOG_DIR" -name "*.log" -mtime +14 -delete
    exit $STATUS
}

main "$@"
exit
