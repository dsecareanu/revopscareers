#!/bin/bash
# =============================================================================
# RevOpsCareers — Daily Job Sync
# Runs each morning to import new jobs posted in the past 1 day.
# Logs to ~/Library/Logs/revopscareers_sync.log (rotated weekly by macOS)
#
# Phase 1: All 4 import scripts run in parallel to cut wall-clock time.
#          Each writes to a temp log; output is printed in order after all finish.
# Phase 2: Cleanup steps run sequentially (require imports to be done).
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$HOME/Library/Logs/revopscareers_sync.log"
mkdir -p "$(dirname "$LOG_FILE")"
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

SYNC_TMPDIR=$(mktemp -d)
trap "rm -f '$LOCK_FILE'; rm -rf '$SYNC_TMPDIR'" EXIT

cd "$SCRIPT_DIR"

log "===== Daily sync started ====="

# =============================================================================
# Phase 1 — Parallel imports
# Each script has its own state file (no conflicts) and its own timeout.
# State is saved per page, so a timeout loses at most one page of imports.
# =============================================================================
log "Phase 1 — Starting parallel imports (Hirebase 90m / WhatJobs 100m / Lensa 100m)..."

timeout 90m "$PYTHON" -u sync_hirebase_jobs.py --since 1 --max-new 600 \
    > "$SYNC_TMPDIR/hirebase.log" 2>&1 &
PID_HB=$!

timeout 100m "$PYTHON" -u sync_whatjobs_jobs.py --region us --max-age 5 \
    > "$SYNC_TMPDIR/whatjobs_us.log" 2>&1 &
PID_WJ_US=$!

timeout 100m "$PYTHON" -u sync_whatjobs_jobs.py --region sg --max-age 5 \
    > "$SYNC_TMPDIR/whatjobs_sg.log" 2>&1 &
PID_WJ_SG=$!

timeout 100m "$PYTHON" -u sync_lensa_jobs.py \
    > "$SYNC_TMPDIR/lensa.log" 2>&1 &
PID_LENSA=$!

# Wait for all four and collect exit codes
wait $PID_HB;    EXIT_HB=$?
wait $PID_WJ_US; EXIT_WJ_US=$?
wait $PID_WJ_SG; EXIT_WJ_SG=$?
wait $PID_LENSA; EXIT_LENSA=$?

# Print logs in order (stdout + append to log file)
log "--- Hirebase output ---"
cat "$SYNC_TMPDIR/hirebase.log" | tee -a "$LOG_FILE"
if   [ $EXIT_HB -eq 124 ]; then log "WARNING: Hirebase timed out after 90 min"
elif [ $EXIT_HB -ne 0 ];   then log "ERROR: Hirebase exited with code $EXIT_HB"; fi

log "--- WhatJobs US output ---"
cat "$SYNC_TMPDIR/whatjobs_us.log" | tee -a "$LOG_FILE"
if   [ $EXIT_WJ_US -eq 124 ]; then log "WARNING: WhatJobs US timed out after 100 min"
elif [ $EXIT_WJ_US -ne 0 ];   then log "ERROR: WhatJobs US exited with code $EXIT_WJ_US"; fi

log "--- WhatJobs SG output ---"
cat "$SYNC_TMPDIR/whatjobs_sg.log" | tee -a "$LOG_FILE"
if   [ $EXIT_WJ_SG -eq 124 ]; then log "WARNING: WhatJobs SG timed out after 100 min"
elif [ $EXIT_WJ_SG -ne 0 ];   then log "ERROR: WhatJobs SG exited with code $EXIT_WJ_SG"; fi

log "--- Lensa output ---"
cat "$SYNC_TMPDIR/lensa.log" | tee -a "$LOG_FILE"
if   [ $EXIT_LENSA -eq 124 ]; then log "WARNING: Lensa timed out after 100 min"
elif [ $EXIT_LENSA -ne 0 ];   then log "ERROR: Lensa exited with code $EXIT_LENSA"; fi

log "Phase 1 complete."

# =============================================================================
# Phase 2 — Cleanup (sequential, requires imports to be done)
# =============================================================================

log "Step 1/5 — ALT text fix (last 2 days)..."
"$PYTHON" -u fix_logo_alt_text.py --since 2 2>&1 | tee -a "$LOG_FILE"

log "Step 2/5 — Unfeature old jobs (>1 day)..."
timeout 60m "$PYTHON" -u unfeature_old_jobs.py 2>&1 | tee -a "$LOG_FILE"
SYNC_EXIT=${PIPESTATUS[0]}
if   [ $SYNC_EXIT -eq 124 ]; then log "WARNING: Unfeature timed out after 60 min"
elif [ $SYNC_EXIT -ne 0 ];   then log "ERROR: unfeature_old_jobs.py exited with code $SYNC_EXIT"; fi

log "Step 3/5 — Add missing logos (last 1 day)..."
"$PYTHON" -u add_missing_logos.py --since 1 2>&1 | tee -a "$LOG_FILE"

log "Step 4/5 — Fallback logo patch (last 1 day)..."
"$PYTHON" -u patch_fallback_logos.py --days 1 --live 2>&1 | tee -a "$LOG_FILE"

log "Step 5/5 — Missing tags patch (last 1 day)..."
"$PYTHON" -u patch_missing_tags.py --days 1 --live 2>&1 | tee -a "$LOG_FILE"

log "===== Daily sync complete ====="
