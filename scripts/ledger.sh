#!/usr/bin/env bash
# Every Databricks run goes through this wrapper.
#   scripts/ledger.sh "<what ran>" "<data size>" <command> [args...]
# Quota guard (opt-in): when TAXI_QUOTA_LOG names a file, refuses to start while that file ends in a QUOTA-STOP.
# Appends one row to the run ledger.
# Every command gets a wall-clock limit (LEDGER_LIMIT_S, default 1800 s), then TERM, then KILL 30 s later.
set -uo pipefail
export PYTHONUNBUFFERED=1
what=$1; size=$2; shift 2
here=$(cd "$(dirname "$0")/.." && pwd)
quota=${TAXI_QUOTA_LOG:-}   # unset or empty: no quota guard
ledger=${TAXI_RUN_LEDGER:-$here/docs/run-ledger.md}

last=""
[[ -n $quota ]] && last=$(grep -E '^QUOTA-(STOP|RESUME)' "$quota" 2>/dev/null | tail -1)
if [[ $last == QUOTA-STOP* ]]; then
  echo "LEDGER: refused, quota stop in force: $last" >&2
  exit 75
fi

limit=${LEDGER_LIMIT_S:-1800}
start=$(date '+%Y-%m-%d %H:%M:%S'); t0=$(date +%s)
echo "LEDGER: start $start, limit ${limit} s: $what" >&2
timeout --kill-after=30 "$limit" "$@"; rc=$?
dur=$(( $(date +%s) - t0 ))
[[ -f $ledger ]] || printf '# Databricks run ledger\n\n| start (IST) | what | data size | duration | result |\n|---|---|---|---|---|\n' > "$ledger"
case $rc in
  0) result=OK ;;
  99) result="SKIP rc=99 (month not published)" ;;
  124|137) result="TIMEOUT after ${limit} s (rc=$rc)" ;;
  *) result="FAIL rc=$rc" ;;
esac
echo "| $start | $what | $size | ${dur} s | $result |" >> "$ledger"
exit $rc
