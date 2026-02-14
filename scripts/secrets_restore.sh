#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 <archive_file> [destination_dir]"
  echo "Example: $0 backups/secrets/secrets_bundle_20260214T150000Z.tar.gz.enc ./restored_secrets"
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

ARCHIVE_FILE="$1"
DEST_DIR="${2:-./restored_secrets_$(date -u +%Y%m%dT%H%M%SZ)}"

if [[ ! -f "$ARCHIVE_FILE" ]]; then
  echo "Archive file not found: $ARCHIVE_FILE"
  exit 1
fi

CHECKSUM_FILE="${ARCHIVE_FILE}.sha256"
if [[ -f "$CHECKSUM_FILE" ]]; then
  sha256sum -c "$CHECKSUM_FILE"
fi

mkdir -p "$DEST_DIR"
TMP_TAR=""

if [[ "$ARCHIVE_FILE" == *.enc ]]; then
  if [[ -z "${SECRETS_ARCHIVE_KEY:-}" ]]; then
    echo "SECRETS_ARCHIVE_KEY is required for encrypted archives."
    exit 1
  fi
  TMP_TAR="$(mktemp)"
  openssl enc -d -aes-256-cbc -pbkdf2 -in "$ARCHIVE_FILE" -out "$TMP_TAR" -pass "env:SECRETS_ARCHIVE_KEY"
  tar -xzf "$TMP_TAR" -C "$DEST_DIR"
  rm -f "$TMP_TAR"
else
  tar -xzf "$ARCHIVE_FILE" -C "$DEST_DIR"
fi

find "$DEST_DIR" -type f \( -name '*.enc' -o -name '*.env' -o -name '*.dump' \) -exec chmod 600 {} \;

echo "Restore completed in: $DEST_DIR"
echo "Review metadata in: $DEST_DIR/metadata.txt"
