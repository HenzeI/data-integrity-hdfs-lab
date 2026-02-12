#!/usr/bin/env bash
set -euo pipefail

NN_CONTAINER=${NN_CONTAINER:-namenode}
DT=${DT:-$(date +%F)}

# Patron para elegir un datanode automáticamente
DN_NAME_FILTER=${DN_NAME_FILTER:-dnnm-3}

# Tiempo máximo de espera para que el NameNode marque el DN como muerto (segundos)
WAIT_DEAD_TIMEOUT=${WAIT_DEAD_TIMEOUT:-600}   # 10 min
# Cada cuánto comprobar (segundos)
WAIT_DEAD_INTERVAL=${WAIT_DEAD_INTERVAL:-10}

echo "[incident] DT=$DT"
echo "[incident] NN_CONTAINER=$NN_CONTAINER"
echo "[incident] DN_NAME_FILTER=$DN_NAME_FILTER"
echo "[incident] WAIT_DEAD_TIMEOUT=${WAIT_DEAD_TIMEOUT}s (interval=${WAIT_DEAD_INTERVAL}s)"

# Elegir un DataNode "real" por nombre de contenedor
DN_CONTAINER="$(docker ps --format '{{.Names}}' | grep -E "$DN_NAME_FILTER" | head -n 1 || true)"
if [[ -z "${DN_CONTAINER}" ]]; then
  echo "[incident] ERROR: No encuentro contenedor DataNode con filtro '$DN_NAME_FILTER'."
  echo "[incident] Pista: export DN_NAME_FILTER='datanode|dn|dnnm' o similar."
  exit 1
fi
echo "[incident] Contenedor DataNode seleccionado: $DN_CONTAINER"

# Crear carpeta de auditoría del incidente en HDFS
docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -mkdir -p /audit/incidents/$DT"

# Capturar estado previo (dfsadmin report + fsck)
docker exec -i "$NN_CONTAINER" bash -lc "\
  echo '=== DFSADMIN REPORT (BEFORE) ===' > /tmp/incident_before_$DT.txt; \
  hdfs dfsadmin -report >> /tmp/incident_before_$DT.txt; \
  echo '\n=== FSCK /data (BEFORE) ===' >> /tmp/incident_before_$DT.txt; \
  hdfs fsck /data -files -blocks -locations >> /tmp/incident_before_$DT.txt"

docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -put -f /tmp/incident_before_$DT.txt /audit/incidents/$DT/incident_before.txt"

# Parar 1 DataNode (incidente)
echo "[incident] Deteniendo DataNode: $DN_CONTAINER"
docker stop "$DN_CONTAINER" >/dev/null

# Obtener el "Name:" (host:puerto) del DN parado desde el dfsadmin report BEFORE
#    Esto nos permite buscarlo luego dentro de "Live datanodes" / "Dead datanodes".
STOPPED_DN_NAME="$(docker exec -i "$NN_CONTAINER" bash -lc "\
  hdfs dfsadmin -report | \
  awk '/^Name: /{name=\$2} /^Hostname: /{host=\$2} { \
    if (host ~ /'"$DN_NAME_FILTER"'/) print name \
  }' | head -n 1" | tr -d '\r' || true)"

# Si no se pudo extraer, no pasa nada: seguiremos con un criterio más general
if [[ -n "${STOPPED_DN_NAME}" ]]; then
  echo "[incident] Nombre de DN detenido detectado (del informe): $STOPPED_DN_NAME"
else
  echo "[incident] WARN: No pude detectar el 'Name:' del DN a partir del report. Usaré comprobación general."
fi

# Esperar activamente a que el NameNode lo detecte (dead o no-live)
echo "[incident] Esperando a que HDFS detecte que DataNode está inactivo..."
start_ts=$(date +%s)
marked_dead="no"

while true; do
  now_ts=$(date +%s)
  elapsed=$((now_ts - start_ts))

  # Sacar un snapshot corto del report para evidenciar cambios
  docker exec -i "$NN_CONTAINER" bash -lc "\
    echo '--- DFSADMIN REPORT SNAPSHOT ---'; \
    hdfs dfsadmin -report | egrep -i 'Live datanodes|Dead datanodes|Datanodes available|Name:|Hostname:|Last contact' | head -n 120" \
    > "/tmp/dfsadmin_snapshot_${DT}.txt" || true

  # Lógica de detección:
  #  - Si tenemos STOPPED_DN_NAME: comprobar si aparece bajo "Dead datanodes"
  #  - Si no: comprobar si el número de Live datanodes baja (cuando el NN lo marca dead)
  if [[ -n "${STOPPED_DN_NAME}" ]]; then
    if docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfsadmin -report | awk '/^Dead datanodes/{f=1} f{print}' | grep -q \"$STOPPED_DN_NAME\""; then
      marked_dead="yes"
      echo "[incident] OK: NameNode muestra el DataNode como DEAD (muerto).: $STOPPED_DN_NAME"
      break
    fi
  else
    # método alternativo: esperar a que "Live datanodes" refleje caída
    live_count="$(docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfsadmin -report | awk '/^Live datanodes/{print \$3}' | head -n 1" 2>/dev/null | tr -d '\r' || true)"
    # si puede parsear y es un número, y es < 3 (o < total), lo damos por detectado
    if [[ -n "$live_count" ]] && [[ "$live_count" =~ ^[0-9]+$ ]] && (( live_count < 3 )); then
      marked_dead="yes"
      echo "[incident] OK: Disminución de los nodos de datos en vivo (now=$live_count)."
      break
    fi
  fi

  # Timeout
  if (( elapsed >= WAIT_DEAD_TIMEOUT )); then
    echo "[incident] WARN: Timeout esperando a que el NameNode marque el DN como DEAD (${WAIT_DEAD_TIMEOUT}s)."
    echo "[incident] Continuo igualmente (puede salir aún 'HEALTHY' en fsck si no ha expirado el heartbeat)."
    break
  fi

  sleep "$WAIT_DEAD_INTERVAL"
done

# Capturar estado durante el incidente (dfsadmin report + fsck)
docker exec -i "$NN_CONTAINER" bash -lc "\
  echo '=== DFSADMIN REPORT (DURING) ===' > /tmp/incident_during_$DT.txt; \
  hdfs dfsadmin -report >> /tmp/incident_during_$DT.txt; \
  echo '\n=== FSCK /data (DURING) ===' >> /tmp/incident_during_$DT.txt; \
  hdfs fsck /data -files -blocks -locations >> /tmp/incident_during_$DT.txt; \
  echo '\n=== SUMMARY COUNTS (DURING) ===' >> /tmp/incident_during_$DT.txt; \
  echo -n 'CORRUPT=' >> /tmp/incident_during_$DT.txt; grep -c 'CORRUPT' /tmp/incident_during_$DT.txt || true; \
  echo -n 'MISSING=' >> /tmp/incident_during_$DT.txt; grep -c 'MISSING' /tmp/incident_during_$DT.txt || true; \
  echo -n 'Under replicated=' >> /tmp/incident_during_$DT.txt; grep -c 'Under replicated' /tmp/incident_during_$DT.txt || true; \
  echo -n 'Marked dead=' >> /tmp/incident_during_$DT.txt; echo '$marked_dead' >> /tmp/incident_during_$DT.txt"

docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -put -f /tmp/incident_during_$DT.txt /audit/incidents/$DT/incident_during.txt"

# Guardar qué DataNode paré (para el script 80)
docker exec -i "$NN_CONTAINER" bash -lc "echo '$DN_CONTAINER' > /tmp/incident_datanode_$DT.txt"
docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -put -f /tmp/incident_datanode_$DT.txt /audit/incidents/$DT/datanode_stopped.txt"

echo "[incident] OK (DataNode detenido). A continuación: ejecute scripts/80_recovery_restore.sh"
