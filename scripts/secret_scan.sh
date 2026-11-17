#!/usr/bin/env bash
# Secret scan over the staged diff (default) or over all tracked + untracked files (--all).
# Prints only file name and pattern name, never the matched text. Exit 1 on any hit.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

patterns=(
  'databricks-pat:dapi[0-9a-f]{32}'
  'databricks-oauth-secret:dose[0-9a-f]{32}'
  'aws-access-key-id:(AKIA|ASIA)[0-9A-Z]{16}'
  'aws-secret-assignment:aws_secret_access_key[[:space:]]*[=:][[:space:]]*[^[:space:]]+'
  'databricks-token-assignment:DATABRICKS_TOKEN[[:space:]]*[=:][[:space:]]*["'"'"']?[A-Za-z0-9]{8,}'
  'private-key:-----BEGIN [A-Z ]*PRIVATE KEY-----'
  'workspace-host:dbc-[0-9a-f]{8}-[0-9a-f]{4}\.cloud\.databricks\.com'
)

if [[ "${1:-}" == "--all" ]]; then
  files=()
  while IFS= read -r -d '' f; do files+=("$f"); done < <(git ls-files -z --cached --others --exclude-standard)
  scan() { [[ ${#files[@]} -eq 0 ]] && return 1; grep -lE -- "$1" "${files[@]}" 2>/dev/null; }
  mode="all files"
else
  scan() { git diff --cached -U0 --no-color | awk '/^\+\+\+ /{f=substr($0,7)} /^\+[^+]/{print f": "substr($0,2)}' | grep -E -- "$1" | cut -d: -f1 | sort -u; }
  mode="staged diff"
fi

hits=0
for entry in "${patterns[@]}"; do
  name="${entry%%:*}"; rx="${entry#*:}"
  while IFS= read -r f; do
    [[ -n "$f" ]] || continue
    echo "SECRET-SCAN HIT: $name in $f"
    hits=$((hits + 1))
  done < <(scan "$rx" || true)
done

if [[ $hits -gt 0 ]]; then
  echo "SECRET-SCAN: FAIL ($hits hit(s), $mode)"; exit 1
fi
echo "SECRET-SCAN: CLEAN ($mode)"
