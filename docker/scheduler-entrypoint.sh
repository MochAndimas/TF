#!/usr/bin/env bash

set -euo pipefail

SOURCE_CRON="/app/scripts/cron/traders_family_etl.cron"
TARGET_CRON="/etc/cron.d/traders-family-etl"
LOGROTATE_SOURCE="/app/docker/logrotate-app.conf"
LOGROTATE_TARGET="/etc/logrotate.d/traders-family"
LOGROTATE_CRON="/etc/cron.d/traders-family-logrotate"
PROJECT_ROOT="${PROJECT_ROOT:-/app}"

printenv | while IFS='=' read -r name value; do
  if [[ -n "${name}" ]]; then
    printf '%s="%s"\n' "${name}" "${value//\"/\\\"}"
  fi
done > /etc/environment
printf 'PROJECT_ROOT="%s"\n' "${PROJECT_ROOT}" >> /etc/environment

awk -v project_root="${PROJECT_ROOT}" '
  BEGIN { OFS = " " }
  /^[[:space:]]*#/ || /^[[:space:]]*$/ || /^[A-Za-z_][A-Za-z0-9_]*=/ {
    print
    next
  }
  {
    command = substr($0, index($0, $6))
    gsub("\\{\\{PROJECT_ROOT\\}\\}", project_root, command)
    print $1, $2, $3, $4, $5, "root", ". /etc/environment; " command " >> /var/log/cron.log 2>&1"
  }
' "${SOURCE_CRON}" > "${TARGET_CRON}"

chmod 0644 "${TARGET_CRON}"
cp "${LOGROTATE_SOURCE}" "${LOGROTATE_TARGET}"
printf '%s\n' '17 0 * * * root /usr/sbin/logrotate /etc/logrotate.d/traders-family' > "${LOGROTATE_CRON}"
chmod 0644 "${LOGROTATE_CRON}"
touch /var/log/cron.log
mkdir -p /app/logs

exec cron -f
