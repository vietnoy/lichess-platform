# Backup & Disaster Recovery

This document captures what's at risk on the cluster, what we currently back up (nothing automated), and the minimum viable backup story for production.

## What's at risk

| Asset | Storage | Loss impact |
|-------|---------|-------------|
| **Iceberg table data** (chess_move_events, move_evaluations) | MinIO PVC, ~30GB | Months of ingested + processed game data. Re-derivable from Lichess API but slow (weeks of API calls). |
| **Polaris catalog metadata** | Postgres PVC, ~50MB | Catalog mappings: namespaces, tables, principals. Without it the Iceberg files in MinIO are unreadable until re-registered. |
| **StarRocks shared-data state** | MinIO `chess-prod/starrocks/` prefix | Persisted query metadata. Recoverable from FE re-init, but indexes rebuild slowly. |
| **chess-secrets** | k8s Secret | Lichess token, Groq API key, Polaris client credentials, GCP service account, Postgres + MinIO + StarRocks passwords. Loss = re-issue every credential. |
| **Tracked-players SQLite** (ingestor) | Local file on VPS, ~5MB | List of usernames the ingestor follows. Re-derivable by re-scraping but loses history. |
| **k8s cluster state** | k3s SQLite on VPS | Deployments, services, configmaps. Recoverable by re-applying `infra/k8s/*.yaml`. |

## Current state

- **Nothing automated.** No backup jobs, no off-host copies, no PVC snapshots.
- The single VPS hosts everything: k3s control plane, MinIO storage, Postgres, ingestor, secrets. **Disk failure = full data loss** of months of work.
- Source code is on GitHub — that's the only thing that's safe.

## Minimum viable plan

Three backup classes, ordered by ease + impact:

### Tier 1 — secrets + small databases (daily, off-host, ~5 min/day)

Cron job on the VPS:

```bash
#!/bin/bash
# /opt/chess/backup.sh — runs nightly via cron
set -euo pipefail
DATE=$(date +%Y-%m-%d)
DEST=/var/backups/chess/$DATE
mkdir -p "$DEST"

# 1. k8s secrets
kubectl get secret chess-secrets -n chess -o yaml > "$DEST/chess-secrets.yaml"

# 2. Polaris postgres
kubectl exec -n chess statefulset/postgres -- pg_dump -U postgres polaris_db | gzip > "$DEST/polaris_db.sql.gz"

# 3. Tracked players SQLite (from ingestor)
cp /opt/chess-ingestor/players.db "$DEST/players.db"

# 4. k8s manifests (in case YAMLs drift from git)
cp -r /root/k8s "$DEST/k8s-applied"

# 5. Off-host: ship to a Cloudflare R2 / Backblaze B2 bucket via rclone
rclone sync "$DEST" "remote:chess-backups/$DATE" --transfers 4

# 6. Retention: keep 30 days local, R2 retains as configured
find /var/backups/chess -mindepth 1 -maxdepth 1 -mtime +30 -exec rm -rf {} +
```

Cost: ~$1/month on Backblaze B2 for everything except MinIO bulk data.

### Tier 2 — MinIO bulk data (weekly, off-host)

MinIO holds the Iceberg parquet files (~30GB and growing). Two viable options:

**Option A — `mc mirror` to a second bucket (off-host)**
```bash
# Run weekly. Uses MinIO's own mirror to a different region/provider.
mc mirror --remove --watch=false alias-prod/chess-prod alias-r2/chess-prod-mirror
```
Cost: ~$0.40/month on R2 (free egress) for 30GB. Doubles storage cost but gives full recoverability.

**Option B — daily Iceberg snapshot via S3 versioning + lifecycle policy**
- Enable bucket versioning on MinIO `chess-prod` (in-place, doesn't double the data)
- Add lifecycle: keep 14 days of object versions, expire older
- Recovery = restore to a previous version

Option B is cheaper but only protects against accidental deletion / corruption — not against hardware failure of the VPS itself. **Use Option A for production.**

### Tier 3 — k3s cluster snapshot (weekly)

```bash
# k3s ships with built-in etcd snapshot, but on this single-node SQLite setup just back up the state file.
systemctl stop k3s
cp /var/lib/rancher/k3s/server/db/state.db /var/backups/chess/k3s-state-$(date +%Y-%m-%d).db
systemctl start k3s
gzip /var/backups/chess/k3s-state-*.db
```
Recovery: stop k3s, replace state.db with backup, restart. Worth doing weekly given how cheap it is.

## Recovery drill

A backup that's never been tested is not a backup. Schedule one quarterly:

1. Spin up a second VPS.
2. Install k3s, restore MinIO + Postgres + secrets from the latest backup.
3. Apply `infra/k8s/*.yaml` from git.
4. Confirm a known game ID returns data via the API.
5. Document any gaps that prevented full recovery.

## What this doesn't cover

- **Real-time failover.** Single-VPS architecture means downtime during recovery (hours).
- **Geographic redundancy.** All backups go to one cloud provider. Add a second target if regulatory.
- **Lichess API rate limits.** If we lose months of game data, re-scraping from Lichess will hit rate limits — recovery from API alone takes weeks. This is why MinIO bulk backup matters more than just metadata.

## Action items

1. [ ] Create `/opt/chess/backup.sh` and add to root crontab (`0 3 * * *`).
2. [ ] Provision Backblaze B2 / Cloudflare R2 bucket + rclone config.
3. [ ] Set up `mc mirror` weekly cron.
4. [ ] Schedule first quarterly recovery drill.

Until items 1–3 are done, treat every deploy as if there is no backup — because there isn't.
