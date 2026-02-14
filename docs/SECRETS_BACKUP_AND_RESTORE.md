# Secrets Backup And Restore

This runbook prevents loss of local secret files and config_service secret state.

## What is protected

- `secrets.dev.enc`
- `secrets.staging.enc`
- `secrets.test.enc`
- `.config_service_backup.env`
- Optional `config_service` PostgreSQL schema dump (if `pg_dump` is available and DB variables are set)

## Backup

Run:

```bash
chmod +x scripts/secrets_backup.sh scripts/secrets_restore.sh
export SECRETS_ARCHIVE_KEY='<strong-passphrase>'
export DATABASE_URL='postgresql://user:pass@127.0.0.1:5432/stocksblitz_unified_prod'   # optional
./scripts/secrets_backup.sh
```

Output:

- `backups/secrets/secrets_bundle_<timestamp>.tar.gz.enc` (encrypted if key provided)
- `backups/secrets/secrets_bundle_<timestamp>.tar.gz.enc.sha256`

Notes:

- If `SECRETS_ARCHIVE_KEY` is not set, a plain `.tar.gz` archive is created. Use encryption for production.
- Retention defaults to 30 days. Override with `RETENTION_DAYS=<n>`.

## Restore

Run:

```bash
export SECRETS_ARCHIVE_KEY='<strong-passphrase>'   # required for .enc archives
./scripts/secrets_restore.sh backups/secrets/secrets_bundle_<timestamp>.tar.gz.enc ./restored_secrets
```

The script verifies checksum when `.sha256` is present, then extracts files.

## Automation (recommended)

Install cron on prod host:

```bash
crontab -e
```

Add:

```cron
0 */6 * * * cd /home/stocksadmin && SECRETS_ARCHIVE_KEY='<strong-passphrase>' DATABASE_URL='postgresql://user:pass@127.0.0.1:5432/stocksblitz_unified_prod' ./scripts/secrets_backup.sh >> /home/stocksadmin/logs/secrets_backup.log 2>&1
```

## Operational policy

- Keep `SECRETS_ARCHIVE_KEY` outside git and outside service logs.
- Replicate encrypted `backups/secrets/*.enc` to off-host storage.
- Test restore at least once per month.
