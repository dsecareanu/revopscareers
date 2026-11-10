#!/usr/bin/env python3
"""
expire_closed_jobs.py — set _job_expires = today on posts listed in would_expire.csv
(from check_expired_hirebase.py). WPJM's hourly cron job_manager_check_for_expired_jobs
then moves them to status 'expired' (auto-delete is off, so this is reversible).

DRY RUN by default: reads each post's live status and _job_expires, writes the backup
CSV it would use, changes nothing. --apply writes the backup first, then each post.

Usage:
  PYTHONUNBUFFERED=1 /opt/homebrew/bin/python3.14 -u expire_closed_jobs.py --limit 20
  ... --limit 20 --apply
  ... --csv expired_check/daily/would_expire.csv --max 1000 --apply   # daily_sync.sh
  ... --restore expired_check/<date>/expire_backup_<ts>.csv    # put old values back
"""
import argparse, csv, os
from datetime import datetime, timezone
from pathlib import Path

import requests

WP = "https://revopscareers.com/wp-json/wp/v2/job-listings"
wp = requests.Session()


def env(name: str) -> str:
    if os.environ.get(name):
        return os.environ[name]
    import envfile   # local Mac helper; tig exports .env instead
    return envfile.env(name)


wp.auth = (os.environ.get("WP_USERNAME", "webadmin"), env("WP_APP_PASSWORD"))
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")


def get_post(pid: int) -> dict | None:
    r = wp.get(f"{WP}/{pid}", params={"context": "edit", "_fields": "id,status,meta"}, timeout=60)
    return r.json() if r.status_code == 200 else None


def update(pid: int, body: dict) -> bool:
    r = wp.post(f"{WP}/{pid}", json=body, timeout=60)
    if r.status_code != 200:
        print(f"  {pid}: update failed {r.status_code} {r.text[:150]}")
    return r.status_code == 200


def restore(path: Path):
    rows = list(csv.DictReader(open(path)))
    ok = 0
    for row in rows:
        if row["action"] != "expire":
            continue
        pid = int(row["post_id"])
        if update(pid, {"status": row["old_status"], "meta": {"_job_expires": row["old_job_expires"]}}):
            ok += 1
    print(f"Restored {ok} posts from {path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=f"expired_check/{datetime.now():%Y-%m-%d}/would_expire.csv")
    ap.add_argument("--limit", type=int, help="first N rows only")
    ap.add_argument("--apply", action="store_true", help="write (default: dry run)")
    ap.add_argument("--restore", help="backup CSV to restore")
    ap.add_argument("--max", type=int, help="abort without changes if the CSV has more rows than this")
    args = ap.parse_args()

    base = Path(__file__).parent
    if args.restore:
        return restore(base / args.restore)

    src = base / args.csv
    rows = list(csv.DictReader(open(src)))
    if args.max is not None and len(rows) > args.max:
        raise SystemExit(f"ABORT: {len(rows)} rows in {src} > --max {args.max}; nothing changed")
    rows = rows[: args.limit] if args.limit else rows
    print(f"{'APPLY' if args.apply else 'DRY RUN'}: {len(rows)} rows from {src}, new _job_expires = {TODAY}")

    plan = []
    for row in rows:
        pid = int(row["post_id"])
        p = get_post(pid)
        if not p:
            plan.append(dict(post_id=pid, old_status="?", old_job_expires="", action="skip: not found"))
            continue
        old = (p.get("meta") or {}).get("_job_expires", "")
        if p["status"] != "publish":
            action = f"skip: status {p['status']}"
        elif old and old <= TODAY:
            action = "skip: already expires today or earlier"
        else:
            action = "expire"
        plan.append(dict(post_id=pid, old_status=p["status"], old_job_expires=old, action=action,
                         company=row["company"], title=row["title"]))

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = src.parent / f"expire_backup_{ts}{'' if args.apply else '_dryrun'}.csv"
    with open(backup, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["post_id", "old_status", "old_job_expires", "action", "company", "title"])
        w.writeheader()
        w.writerows(plan)

    for r in plan:
        print(f"  {r['post_id']}  {r['old_status']:8} {r['old_job_expires'] or '-':10} → "
              f"{TODAY if r['action'] == 'expire' else '(no change)'}  {r['action']}  "
              f"{r.get('company', '')} — {r.get('title', '')}")

    todo = [r for r in plan if r["action"] == "expire"]
    if not args.apply:
        print(f"\nDRY RUN: {len(todo)} would change, {len(plan) - len(todo)} skipped. Plan saved: {backup}")
        return

    done = 0
    for r in todo:
        if update(r["post_id"], {"meta": {"_job_expires": TODAY}}):
            new = (get_post(r["post_id"]) or {}).get("meta", {}).get("_job_expires")
            if new == TODAY:
                done += 1
            else:
                print(f"  {r['post_id']}: verify failed, _job_expires = {new}")
    print(f"\nAPPLIED: {done}/{len(todo)} set to {TODAY}. Backup: {backup}")


if __name__ == "__main__":
    main()
