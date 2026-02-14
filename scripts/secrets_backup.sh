#!/usr/bin/env bash
set -euo pipefail

# Creates encrypted backups for local secret files and optional config_service DB schema.
# Required for encryption: SECRETS_ARCHIVE_KEY environment variable.

ROOT_DIR="${ROOT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
BACKUP_DIR="${BACKUP_DIR:-$ROOT_DIR/backups/secrets}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RETENTION_DAYS="${RETENTION_DAYS:-30}"
CONFIG_SCHEMA="${CONFIG_SCHEMA:-config_service}"

mkdir -p "$BACKUP_DIR"
STAGE_DIR="$(mktemp -d)"
trap 'rm -rf "$STAGE_DIR"' EXIT

echo "timestamp=$TIMESTAMP" > "$STAGE_DIR/metadata.txt"
echo "host=$(hostname)" >> "$STAGE_DIR/metadata.txt"
echo "cwd=$ROOT_DIR" >> "$STAGE_DIR/metadata.txt"
echo "git_commit=$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || echo n/a)" >> "$STAGE_DIR/metadata.txt"

copy_secret_file() {
  local file="$1"
  if [[ -f "$ROOT_DIR/$file" ]]; then
    cp "$ROOT_DIR/$file" "$STAGE_DIR/"
    chmod 600 "$STAGE_DIR/$(basename "$file")"
    echo "included_file=$file" >> "$STAGE_DIR/metadata.txt"
  fi
}

copy_secret_file "secrets.dev.enc"
copy_secret_file "secrets.staging.enc"
copy_secret_file "secrets.test.enc"
copy_secret_file ".config_service_backup.env"

if command -v pg_dump >/dev/null 2>&1; then
  if [[ -n "${DATABASE_URL:-}" ]]; then
    pg_dump --format=custom --schema="$CONFIG_SCHEMA" --file="$STAGE_DIR/config_service_${CONFIG_SCHEMA}.dump" "$DATABASE_URL"
    echo "included_db_dump=true" >> "$STAGE_DIR/metadata.txt"
  elif [[ -n "${PGDATABASE:-}" ]]; then
    pg_dump --format=custom --schema="$CONFIG_SCHEMA" --file="$STAGE_DIR/config_service_${CONFIG_SCHEMA}.dump" "${PGDATABASE}"
    echo "included_db_dump=true" >> "$STAGE_DIR/metadata.txt"
  else
    echo "included_db_dump=false" >> "$STAGE_DIR/metadata.txt"
  fi
else
  echo "included_db_dump=false" >> "$STAGE_DIR/metadata.txt"
fi

if [[ -z "$(find "$STAGE_DIR" -maxdepth 1 -type f ! -name metadata.txt -print -quit)" ]]; then
  echo "No secrets or config dumps found to archive."
  exit 1
fi

ARCHIVE_TAR="$BACKUP_DIR/secrets_bundle_${TIMESTAMP}.tar.gz"
tar -czf "$ARCHIVE_TAR" -C "$STAGE_DIR" .
chmod 600 "$ARCHIVE_TAR"

FINAL_ARCHIVE="$ARCHIVE_TAR"
if [[ -n "${SECRETS_ARCHIVE_KEY:-}" ]]; then
  FINAL_ARCHIVE="${ARCHIVE_TAR}.enc"
  openssl enc -aes-256-cbc -salt -pbkdf2 -in "$ARCHIVE_TAR" -out "$FINAL_ARCHIVE" -pass "env:SECRETS_ARCHIVE_KEY"
  rm -f "$ARCHIVE_TAR"
  chmod 600 "$FINAL_ARCHIVE"
fi

sha256sum "$FINAL_ARCHIVE" > "${FINAL_ARCHIVE}.sha256"
chmod 600 "${FINAL_ARCHIVE}.sha256"

find "$BACKUP_DIR" -type f \( -name 'secrets_bundle_*.tar.gz' -o -name 'secrets_bundle_*.tar.gz.enc' -o -name 'secrets_bundle_*.sha256' \) -mtime +"$RETENTION_DAYS" -delete || true

echo "Backup created: $FINAL_ARCHIVE"
echo "Checksum: ${FINAL_ARCHIVE}.sha256"
