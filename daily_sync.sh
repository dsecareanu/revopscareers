#!/bin/bash
# =============================================================================
# RevOpsCareers — Daily Job Sync
# Runs each morning to import new jobs posted in the past 1 day.
# Logs to ~/Library/Logs/revopscareers_sync.log (rotated weekly by macOS)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$HOME/Library/Logs/revopscareers_sync.log"
PYTHON="$(which python3)"
LOCK_FILE="/tmp/revopscareers_sync.lock"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

# Prevent concurrent runs
if [ -f "$LOCK_FILE" ]; then
    LOCK_PID=$(cat "$LOCK_FILE" 2>/dev/null)
    if kill -0 "$LOCK_PID" 2>/dev/null; then
        log "ERROR: Sync already running (PID $LOCK_PID). Exiting."
        exit 1
    else
        log "WARNING: Stale lock file found (PID $LOCK_PID no longer running). Removing."
        rm -f "$LOCK_FILE"
    fi
fi
echo $$ > "$LOCK_FILE"
trap "rm -f '$LOCK_FILE'" EXIT

log "===== Daily sync started ====="

# 1. Import new jobs from Hirebase (past 1 day)
#    - Skips dupes via imported_jobs.json + WP application URL check
#    - Sets ALT text on newly uploaded logos inline
#    - Notifies n8n data tables (jobs + logos)
log "Step 1/4 — Hirebase sync (--since 1)..."
cd "$SCRIPT_DIR" && "$PYTHON" -u sync_hirebase_jobs.py --since 1 2>&1 | tee -a "$LOG_FILE"
SYNC_EXIT=${PIPESTATUS[0]}
if [ $SYNC_EXIT -ne 0 ]; then
    log "ERROR: sync_hirebase_jobs.py exited with code $SYNC_EXIT"
fi

# 2. Fix ALT text on any logos missing it (catches logos from previous runs)
log "Step 2/4 — ALT text fix (last 2 days)..."
cd "$SCRIPT_DIR" && "$PYTHON" -u fix_logo_alt_text.py --since 2 2>&1 | tee -a "$LOG_FILE"

# 3. Import new jobs from WhatJobs US (past 2 days)
log "Step 3/5 — WhatJobs US sync (--region us --max-age 5)..."
cd "$SCRIPT_DIR" && "$PYTHON" -u sync_whatjobs_jobs.py --region us --max-age 5 2>&1 | tee -a "$LOG_FILE"
SYNC_EXIT=${PIPESTATUS[0]}
if [ $SYNC_EXIT -ne 0 ]; then
    log "ERROR: sync_whatjobs_jobs.py (US) exited with code $SYNC_EXIT"
fi

# 4. Import new jobs from WhatJobs Singapore (past 2 days)
log "Step 4/5 — WhatJobs SG sync (--region sg --max-age 5)..."
cd "$SCRIPT_DIR" && "$PYTHON" -u sync_whatjobs_jobs.py --region sg --max-age 5 2>&1 | tee -a "$LOG_FILE"
SYNC_EXIT=${PIPESTATUS[0]}
if [ $SYNC_EXIT -ne 0 ]; then
    log "ERROR: sync_whatjobs_jobs.py (SG) exited with code $SYNC_EXIT"
fi

# 5. Import new jobs from Lensa
log "Step 5/5 — Lensa sync..."
cd "$SCRIPT_DIR" && "$PYTHON" -u sync_lensa_jobs.py 2>&1 | tee -a "$LOG_FILE"
SYNC_EXIT=${PIPESTATUS[0]}
if [ $SYNC_EXIT -ne 0 ]; then
    log "ERROR: sync_lensa_jobs.py exited with code $SYNC_EXIT"
fi

# 6. Unfeature webadmin jobs older than 1 day
log "Step 6/7 — Unfeature old jobs (>1 day)..."
cd "$SCRIPT_DIR" && "$PYTHON" -u unfeature_old_jobs.py 2>&1 | tee -a "$LOG_FILE"
SYNC_EXIT=${PIPESTATUS[0]}
if [ $SYNC_EXIT -ne 0 ]; then
    log "ERROR: unfeature_old_jobs.py exited with code $SYNC_EXIT"
fi

# 7. Add missing logos for jobs posted in the last 1 day
log "Step 7/7 — Add missing logos (last 1 day)..."
cd "$SCRIPT_DIR" && "$PYTHON" -u add_missing_logos.py --since 1 2>&1 | tee -a "$LOG_FILE"
SYNC_EXIT=${PIPESTATUS[0]}
if [ $SYNC_EXIT -ne 0 ]; then
    log "ERROR: add_missing_logos.py exited with code $SYNC_EXIT"
fi

log "===== Daily sync complete ====="
