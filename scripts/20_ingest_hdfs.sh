#!/usr/bin/env bash
set -euo pipefail

NN_CONTAINER=${NN_CONTAINER:-namenode} # Contenedor NameNode
DT=${DT:-$(date +%F)} # Fecha de trabajo
LOCAL_DIR=${LOCAL_DIR:-./data_local/$DT} # Directorio local donde están los archivos generados

# Nombre de archivos locales
DATE_NODASH=${DT//-/}
LOG_FILE="$LOCAL_DIR/logs_${DATE_NODASH}.log"
IOT_FILE="$LOCAL_DIR/iot_${DATE_NODASH}.jsonl"

echo "[ingest] DT=$DT"
echo "[ingest] Directorio local=$LOCAL_DIR"

# Verificar que los archivos existen
if [ ! -f "$LOG_FILE" ] || [ ! -f "$IOT_FILE" ]; then
  echo "[ingest] Faltan archivos de entrada. Se esperaba:"
  echo "  - $LOG_FILE"
  echo "  - $IOT_FILE"
  exit 1
fi

# Crear directorios destino en HDFS
docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -mkdir -p /data/logs/raw/dt=$DT /data/iot/raw/dt=$DT"

# Subir archivo logs a HDFS
cat "$LOG_FILE" | docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -put -f - /data/logs/raw/dt=$DT/$(basename "$LOG_FILE")"
# Subir archivo iot a HDFS
cat "$IOT_FILE" | docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -put -f - /data/iot/raw/dt=$DT/$(basename "$IOT_FILE")"

# Mostrar contenido en HDFS
docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -ls -R /data | head -n 200"
docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -du -h /data"

echo "[ingest] OK"
