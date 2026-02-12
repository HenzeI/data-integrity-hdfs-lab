#!/usr/bin/env bash
set -euo pipefail

NN_CONTAINER=${NN_CONTAINER:-namenode}
DT=${DT:-$(date +%F)}

echo "[recovery] DT=$DT"
echo "[recovery] NN_CONTAINER=$NN_CONTAINER"

docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -mkdir -p /audit/recovery/$DT"

# Leer qué DataNode se paró (si existe)
DN_CONTAINER="$(docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -cat /audit/incidents/$DT/datanode_stopped.txt 2>/dev/null || true" | tr -d '\r' | tail -n 1)"
if [[ -n "${DN_CONTAINER}" ]]; then
  echo "[recovery] Starting DataNode: $DN_CONTAINER"
  docker start "$DN_CONTAINER" >/dev/null || true
else
  echo "[recovery] WARN: No encuentro datanode_stopped.txt en /audit/incidents/$DT. Continuo sin arrancar DN."
fi

# Esperar a que el clúster “vea” los DataNodes
echo "[recovery] Waiting for DataNodes to be reported..."
for i in {1..18}; do
  if docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfsadmin -report | grep -q 'Datanodes available:'"; then
    break
  fi
  sleep 10
done

# Intentar que salga de safemode (si aplica)
docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfsadmin -safemode leave >/dev/null 2>&1 || true"

# Evidencia inicial post-arranque
docker exec -i "$NN_CONTAINER" bash -lc "\
  echo '=== DFSADMIN REPORT (AFTER START) ===' > /tmp/recovery_report_$DT.txt; \
  hdfs dfsadmin -report >> /tmp/recovery_report_$DT.txt; \
  echo '\n=== FSCK /data (AFTER START) ===' >> /tmp/recovery_report_$DT.txt; \
  hdfs fsck /data -files -blocks -locations >> /tmp/recovery_report_$DT.txt"

docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -put -f /tmp/recovery_report_$DT.txt /audit/recovery/$DT/recovery_after_start.txt"

# Restauración desde /backup -> /data si faltan ficheros del día
echo "[recovery] Checking missing files in /data for dt=$DT (restore from /backup if needed)..."

docker exec -i "$NN_CONTAINER" bash -lc "\
  restore_log=/tmp/restore_actions_$DT.log; \
  : > \$restore_log; \
  for p in logs/raw iot/raw; do \
    src=\"/backup/\$p/dt=$DT\"; dst=\"/data/\$p/dt=$DT\"; \
    if hdfs dfs -test -d \"\$src\"; then \
      hdfs dfs -mkdir -p \"\$dst\"; \
      while read -r f; do \
        base=\$(basename \"\$f\"); \
        if ! hdfs dfs -test -e \"\$dst/\$base\"; then \
          echo \"RESTORE missing \$dst/\$base from \$src/\$base\" | tee -a \$restore_log; \
          hdfs dfs -cp -p \"\$src/\$base\" \"\$dst/\"; \
        fi; \
      done < <(hdfs dfs -ls \"\$src\" 2>/dev/null | awk '{print \$8}'); \
    else \
      echo \"WARN: backup path not found: \$src\" | tee -a \$restore_log; \
    fi; \
  done"

docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -put -f /tmp/restore_actions_$DT.log /audit/recovery/$DT/restore_actions.log"

# Evidencia final (fsck + du)
docker exec -i "$NN_CONTAINER" bash -lc "\
  echo '=== FSCK /data (FINAL) ===' > /tmp/recovery_final_$DT.txt; \
  hdfs fsck /data -files -blocks -locations >> /tmp/recovery_final_$DT.txt; \
  echo '\n=== DU /data (FINAL) ===' >> /tmp/recovery_final_$DT.txt; \
  hdfs dfs -du -h /data >> /tmp/recovery_final_$DT.txt"

docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -put -f /tmp/recovery_final_$DT.txt /audit/recovery/$DT/recovery_final.txt"

echo "[recovery] OK"
echo "[recovery] Evidence in HDFS: /audit/recovery/$DT/"
