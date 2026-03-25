#!/usr/bin/env bash

set -euo pipefail

SOURCE_CRON="/app/scripts/cron/traders_family_etl.cron"
TARGET_CRON="/etc/cron.d/traders-family-etl"

printenv | while IFS='=' read -r name value; do
  if [[ -n "${name}" ]]; then
    printf '%s="%s"\n' "${name}" "${value//\"/\\\"}"
  fi
done > /etc/environment

awk '
  BEGIN { OFS = " " }
  /^[[:space:]]*#/ || /^[[:space:]]*$/ || /^[A-Za-z_][A-Za-z0-9_]*=/ {
    print
    next
  }
  {
    command = substr($0, index($0, $6))
    gsub("/Users/macbook/Documents/TF", "/app", command)
    print $1, $2, $3, $4, $5, "root", ". /etc/environment; " command
  }
' "${SOURCE_CRON}" > "${TARGET_CRON}"

chmod 0644 "${TARGET_CRON}"
touch /var/log/cron.log

exec cron -f
