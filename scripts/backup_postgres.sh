#!/usr/bin/env bash
# Make a complete PostgreSQL snapshot outside Docker volumes.
set -euo pipefail

mkdir -p backups
timestamp=$(date -u +%Y%m%dT%H%M%SZ)
destination="backups/portpulse-${timestamp}.dump"
temporary=$(mktemp "backups/.portpulse-${timestamp}.XXXXXX")
trap 'rm -f "$temporary"' EXIT

docker compose exec -T db pg_dump -U portpulse -d portpulse -Fc > "$temporary"
# Verify the archive format is readable before making it available.
docker compose exec -T db pg_restore --list < "$temporary" > /dev/null
mv "$temporary" "$destination"
trap - EXIT
printf 'Backup verified: %s\n' "$destination"
