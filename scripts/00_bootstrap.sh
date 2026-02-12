#!/usr/bin/env bash
set -euo pipefail

# Nombre del contenedor NameNode (cliente HDFS)
NN_CONTAINER=${NN_CONTAINER:-namenode}

# Fecha de trabajo (por defecto hoy)
DT=${DT:-$(date +%F)}

echo "[bootstrap] DT=$DT"

# Crear estructura de directorios base en HDFS
# /data   -> datos principales
# /backup -> copia de seguridad
# /audit  -> resultados de auditoría
docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -mkdir -p \
  /data/logs/raw/dt=$DT \
  /data/iot/raw/dt=$DT \
  /backup/logs/raw/dt=$DT \
  /backup/iot/raw/dt=$DT \
  /audit/fsck/$DT \
  /audit/inventory/$DT"

# Mostrar estructura creada (verificación)
docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -ls -R /data /backup /audit | head -n 50"

echo "[bootstrap] OK"
