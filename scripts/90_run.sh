#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[run] Iniciando pipeline completo de integridad HDFS"

bash scripts/00_bootstrap.sh
bash scripts/10_generate_data.sh
bash scripts/20_ingest_hdfs.sh
bash scripts/30_fsck_audit.sh
bash scripts/40_backup_copy.sh
bash scripts/50_inventory_compare.sh
bash scripts/70_incident_simulation.sh
bash scripts/80_recovery_restore.sh

echo "[run] Pipeline finalizado correctamente"
