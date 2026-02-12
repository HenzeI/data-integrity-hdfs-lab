#!/usr/bin/env bash
set -euo pipefail

NN_CONTAINER=${NN_CONTAINER:-namenode}
DT=${DT:-$(date +%F)}

echo "[backup] DT=$DT"

# Crear directorios backup
docker exec -i "$NN_CONTAINER" bash -lc "\
  hdfs dfs -mkdir -p /backup/logs/raw/dt=$DT /backup/iot/raw/dt=$DT; \
  hdfs dfs -rm -r -f /backup/logs/raw/dt=$DT/* /backup/iot/raw/dt=$DT/* 2>/dev/null || true; \
  hdfs dfs -cp -p /data/logs/raw/dt=$DT/* /backup/logs/raw/dt=$DT/; \
  hdfs dfs -cp -p /data/iot/raw/dt=$DT/* /backup/iot/raw/dt=$DT/"

# Copiar datos desde /data hacia /backup
docker exec -i "$NN_CONTAINER" bash -lc "\
  hdfs dfs -ls -R /backup/logs/raw/dt=$DT /backup/iot/raw/dt=$DT | tee /tmp/backup_copy_$DT.log; \
  hdfs dfs -du -h /backup/logs/raw/dt=$DT /backup/iot/raw/dt=$DT >> /tmp/backup_copy_$DT.log"

# Guardar log de backup
docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -mkdir -p /audit/inventory/$DT"
# Subir log a auditoría
docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -put -f /tmp/backup_copy_$DT.log /audit/inventory/$DT/backup_copy.log"

echo "[backup] OK"
