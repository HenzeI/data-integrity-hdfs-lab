#!/usr/bin/env bash
set -euo pipefail

NN_CONTAINER=${NN_CONTAINER:-namenode}
DT=${DT:-$(date +%F)}

# Patron para elegir un datanode automaticamente
DN_NAME_FILTER=${DN_NAME_FILTER:-dnnm-2}

# Cada cuanto comprobar (segundos)
WAIT_DEAD_INTERVAL=${WAIT_DEAD_INTERVAL:-10}

# Tiempo maximo de espera para que el NameNode marque el DN como muerto (segundos).
# Se calcula desde heartbeat/recheck del NameNode:
# - deteccion rapida (~1-2 min)  -> 100s
# - deteccion normal/lenta (>=~10 min) -> 900s
RECHECK_INTERVAL_MS_RAW="$(docker exec -i "$NN_CONTAINER" bash -lc "hdfs getconf -confKey dfs.namenode.heartbeat.recheck-interval 2>/dev/null || true" | tr -d '\r')"
HEARTBEAT_INTERVAL_S_RAW="$(docker exec -i "$NN_CONTAINER" bash -lc "hdfs getconf -confKey dfs.heartbeat.interval 2>/dev/null || true" | tr -d '\r')"
RECHECK_INTERVAL_MS="${RECHECK_INTERVAL_MS_RAW:-0}"
HEARTBEAT_INTERVAL_S="${HEARTBEAT_INTERVAL_S_RAW:-0}"
[[ "$RECHECK_INTERVAL_MS" =~ ^[0-9]+$ ]] || RECHECK_INTERVAL_MS=0
[[ "$HEARTBEAT_INTERVAL_S" =~ ^[0-9]+$ ]] || HEARTBEAT_INTERVAL_S=0

RECHECK_INTERVAL_S=$(( (RECHECK_INTERVAL_MS + 999) / 1000 ))
# Estimacion simple del tiempo de deteccion: recheck domina, con respaldo por heartbeat.
DETECTION_EST_S=$RECHECK_INTERVAL_S
if (( HEARTBEAT_INTERVAL_S > DETECTION_EST_S )); then
  DETECTION_EST_S=$HEARTBEAT_INTERVAL_S
fi

if (( DETECTION_EST_S <= 120 )); then
  AUTO_WAIT_DEAD_TIMEOUT=100
else
  AUTO_WAIT_DEAD_TIMEOUT=900
fi

# Permitir override manual por variable de entorno.
WAIT_DEAD_TIMEOUT=${WAIT_DEAD_TIMEOUT:-$AUTO_WAIT_DEAD_TIMEOUT}

echo "[incident] DT=$DT"
echo "[incident] NN_CONTAINER=$NN_CONTAINER"
echo "[incident] DN_NAME_FILTER=$DN_NAME_FILTER"
echo "[incident] heartbeat.interval=${HEARTBEAT_INTERVAL_S}s recheck.interval=${RECHECK_INTERVAL_MS}ms (~${RECHECK_INTERVAL_S}s)"
echo "[incident] DETECTION_EST_S=${DETECTION_EST_S}s AUTO_WAIT_DEAD_TIMEOUT=${AUTO_WAIT_DEAD_TIMEOUT}s"
echo "[incident] WAIT_DEAD_TIMEOUT=${WAIT_DEAD_TIMEOUT}s (interval=${WAIT_DEAD_INTERVAL}s)"

# Elegir un DataNode "real" por nombre de contenedor
DN_CONTAINER="$(docker ps --format '{{.Names}}' | grep -E "$DN_NAME_FILTER" | head -n 1 || true)"
if [[ -z "${DN_CONTAINER}" ]]; then
  echo "[incident] ERROR: No encuentro contenedor DataNode con filtro '$DN_NAME_FILTER'."
  echo "[incident] Pista: export DN_NAME_FILTER='datanode|dn|dnnm' o similar."
  exit 1
fi
echo "[incident] Contenedor DataNode seleccionado: $DN_CONTAINER"

# Crear carpeta de auditoria del incidente en HDFS
docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -mkdir -p /audit/incidents/$DT"

# Capturar estado previo (dfsadmin report + fsck)
docker exec -i "$NN_CONTAINER" bash -lc "\
  echo '=== DFSADMIN REPORT (BEFORE) ===' > /tmp/incident_before_$DT.txt; \
  hdfs dfsadmin -report >> /tmp/incident_before_$DT.txt; \
  echo '\n=== FSCK /data (BEFORE) ===' >> /tmp/incident_before_$DT.txt; \
  hdfs fsck /data -files -blocks -locations >> /tmp/incident_before_$DT.txt"

docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -put -f /tmp/incident_before_$DT.txt /audit/incidents/$DT/incident_before.txt"

# Contadores previos para comparacion robusta
initial_live_count="$(docker exec -i "$NN_CONTAINER" bash -lc "sed -n 's/^Live datanodes (\\([0-9][0-9]*\\)).*/\\1/p' /tmp/incident_before_$DT.txt | head -n 1" | tr -d '\r' || true)"
if [[ -z "${initial_live_count}" ]] || ! [[ "${initial_live_count}" =~ ^[0-9]+$ ]]; then
  initial_live_count=0
fi
echo "[incident] Live DataNodes antes del incidente: $initial_live_count"

# Identidad del DataNode para localizarlo en el report
DN_HOSTNAME="$(docker inspect -f '{{.Config.Hostname}}' "$DN_CONTAINER" 2>/dev/null || true)"
DN_IP="$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$DN_CONTAINER" 2>/dev/null || true)"
echo "[incident] DN_HOSTNAME=$DN_HOSTNAME"
echo "[incident] DN_IP=$DN_IP"

# Obtener el Name: host:puerto del DataNode parado usando el report BEFORE
STOPPED_DN_NAME="$(docker exec -i "$NN_CONTAINER" bash -lc "\
  awk '/^Name: /{name=\$2} /^Hostname: /{host=\$2; if (host==\"$DN_HOSTNAME\" || name ~ /^$DN_IP:/) print name}' /tmp/incident_before_$DT.txt | head -n 1" | tr -d '\r' || true)"

if [[ -n "${STOPPED_DN_NAME}" ]]; then
  echo "[incident] Name detectado para DN detenido: $STOPPED_DN_NAME"
else
  echo "[incident] WARN: No pude detectar el Name exacto del DN. Usare comprobacion general (live/dead)."
fi

# Parar 1 DataNode (incidente)
echo "[incident] Deteniendo DataNode: $DN_CONTAINER"
docker stop "$DN_CONTAINER" >/dev/null

# Esperar activamente a que el NameNode lo detecte como dead
echo "[incident] Esperando a que HDFS detecte que DataNode esta inactivo..."
start_ts=$(date +%s)
marked_dead="no"

while true; do
  now_ts=$(date +%s)
  elapsed=$((now_ts - start_ts))

  live_count="$(docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfsadmin -report | sed -n 's/^Live datanodes (\\([0-9][0-9]*\\)).*/\\1/p' | head -n 1" 2>/dev/null | tr -d '\r' || true)"
  dead_count="$(docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfsadmin -report | sed -n 's/^Dead datanodes (\\([0-9][0-9]*\\)).*/\\1/p' | head -n 1" 2>/dev/null | tr -d '\r' || true)"
  [[ "$live_count" =~ ^[0-9]+$ ]] || live_count=-1
  [[ "$dead_count" =~ ^[0-9]+$ ]] || dead_count=-1

  echo "[incident] Espera... elapsed=${elapsed}s live=${live_count} dead=${dead_count}"

  # Snapshot corto para evidencia
docker exec -i "$NN_CONTAINER" bash -lc "\
    echo '--- DFSADMIN REPORT SNAPSHOT ---'; \
    hdfs dfsadmin -report | egrep -i 'Live datanodes|Dead datanodes|Datanodes available|Name:|Hostname:|Last contact' | head -n 120" \
    > "/tmp/dfsadmin_snapshot_${DT}.txt" || true

  if [[ -n "${STOPPED_DN_NAME}" ]]; then
    if docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfsadmin -report | awk '/^Dead datanodes/{f=1} f{print}' | grep -q \"$STOPPED_DN_NAME\""; then
      marked_dead="yes"
      echo "[incident] OK: NameNode muestra el DataNode como DEAD: $STOPPED_DN_NAME"
      break
    fi
  else
    if (( initial_live_count > 0 )) && (( live_count >= 0 )) && (( dead_count >= 0 )) && \
       (( live_count < initial_live_count )) && (( dead_count >= 1 )); then
      marked_dead="yes"
      echo "[incident] OK: NameNode refleja incidente (live=$live_count dead=$dead_count)."
      break
    fi
  fi

  if (( elapsed >= WAIT_DEAD_TIMEOUT )); then
    echo "[incident] WARN: Timeout esperando que NameNode marque DEAD (${WAIT_DEAD_TIMEOUT}s)."
    echo "[incident] Continuo igualmente; revisa heartbeat/recheck o aumenta WAIT_DEAD_TIMEOUT."
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

# Guardar que DataNode pare (para el script 80)
docker exec -i "$NN_CONTAINER" bash -lc "echo '$DN_CONTAINER' > /tmp/incident_datanode_$DT.txt"
docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -put -f /tmp/incident_datanode_$DT.txt /audit/incidents/$DT/datanode_stopped.txt"

echo "[incident] OK (DataNode detenido). A continuacion: ejecute scripts/80_recovery_restore.sh"
