#!/usr/bin/env bash
set -euo pipefail

NN_CONTAINER=${NN_CONTAINER:-namenode}
DT=${DT:-$(date +%F)}

echo "[fsck] DT=$DT"

# Crear directorio de auditoría
docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -mkdir -p /audit/fsck/$DT"

# Ejecutar fsck en /data para verificar integridad
docker exec -i "$NN_CONTAINER" bash -lc "hdfs fsck /data -files -blocks -locations | tee /tmp/fsck_data_$DT.txt"
# Guardar resultado en HDFS
docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -put -f /tmp/fsck_data_$DT.txt /audit/fsck/$DT/fsck_data.txt"

# Generar resumen CSV
docker exec -i "$NN_CONTAINER" bash -lc "\
  corrupt=\$(grep -c 'CORRUPT' /tmp/fsck_data_$DT.txt || true); \
  missing=\$(grep -c 'MISSING' /tmp/fsck_data_$DT.txt || true); \
  under=\$(grep -c 'Under replicated' /tmp/fsck_data_$DT.txt || true); \
  echo 'path,corrupt,missing,under_replicated' > /tmp/fsck_summary_$DT.csv; \
  echo \"/data,\$corrupt,\$missing,\$under\" >> /tmp/fsck_summary_$DT.csv"

if docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -test -d /backup"; then
  docker exec -i "$NN_CONTAINER" bash -lc "hdfs fsck /backup -files -blocks -locations | tee /tmp/fsck_backup_$DT.txt"
  docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -put -f /tmp/fsck_backup_$DT.txt /audit/fsck/$DT/fsck_backup.txt"
  docker exec -i "$NN_CONTAINER" bash -lc "\
    corrupt=\$(grep -c 'CORRUPT' /tmp/fsck_backup_$DT.txt || true); \
    missing=\$(grep -c 'MISSING' /tmp/fsck_backup_$DT.txt || true); \
    under=\$(grep -c 'Under replicated' /tmp/fsck_backup_$DT.txt || true); \
    echo \"/backup,\$corrupt,\$missing,\$under\" >> /tmp/fsck_summary_$DT.csv"
fi

# Subir resumen a HDFS
docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -put -f /tmp/fsck_summary_$DT.csv /audit/fsck/$DT/fsck_summary.csv"

echo "[fsck] OK"
