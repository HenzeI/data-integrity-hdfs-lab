#!/usr/bin/env bash
set -euo pipefail

OUT_DIR=${OUT_DIR:-./data_local} # Directorio local donde se generarán los archivos
DT=${DT:-$(date +%F)} # Fecha de trabajo
mkdir -p "$OUT_DIR/$DT" # Crear carpeta local para esta fecha

# Tamaño objetivo en MB para cada archivo
LOGS_MB=${LOGS_MB:-1024}
IOT_MB=${IOT_MB:-1024}

# Nombre de archivos generados
LOG_FILE="$OUT_DIR/$DT/logs_${DT//-/}.log"
IOT_FILE="$OUT_DIR/$DT/iot_${DT//-/}.jsonl"

echo "[generate] Tamaño objetivo (each): logs=${LOGS_MB}MB, iot=${IOT_MB}MB"
echo "[generate] Directorio de salida: $OUT_DIR/$DT"
echo "[generate] Archivo Logs: $LOG_FILE"
echo "[generate] Archivo IoT:  $IOT_FILE"

# Función que genera logs hasta alcanzar el tamaño objetivo
gen_logs() {
  local target_mb=$1
  local target_bytes=$((target_mb * 1024 * 1024))
  : > "$LOG_FILE" # Crear archivo vacío
  echo "[generate] Generando logs ~${target_mb}MB..."

  # Generar datos hasta alcanzar tamaño objetivo
  while [ "$(wc -c < "$LOG_FILE")" -lt "$target_bytes" ]; do

    # Obtener timestamp una sola vez por lote
    TS=$(date +%Y-%m-%dT%H:%M:%S)

    awk -v n=10000 -v ts="$TS" '
      BEGIN {
        srand();
        actions[1]="login"; actions[2]="logout"; actions[3]="purchase"; actions[4]="view";
        actions[5]="download"; actions[6]="upload"; actions[7]="search";

        for (i=1; i<=n; i++) {
          uid=int(100000*rand());
          action=actions[int(1+7*rand())];
          status=(rand()<0.98)?"OK":"ERR";
          printf "%s userId=%d action=%s status=%s\n", ts, uid, action, status;
        }
      }' >> "$LOG_FILE"
  done
}

# Función que genera datos IoT hasta alcanzar tamaño objetivo
gen_iot() {
  local target_mb=$1
  local target_bytes=$((target_mb * 1024 * 1024))
  : > "$IOT_FILE"
  echo "[generate] Generando iot ~${target_mb}MB..."

  while [ "$(wc -c < "$IOT_FILE")" -lt "$target_bytes" ]; do

    TS=$(date +%Y-%m-%dT%H:%M:%S)

    awk -v n=10000 -v ts="$TS" '
      BEGIN {
        srand();
        metrics[1]="temp";
        metrics[2]="humidity";
        metrics[3]="pressure";
        metrics[4]="vibration";

        for (i=1; i<=n; i++) {

          did=int(100000*rand());
          metric=metrics[int(1+4*rand())];

          if (metric=="temp") {
            value=15+20*rand();
          } else if (metric=="humidity") {
            value=30+60*rand();
          } else if (metric=="pressure") {
            value=900+200*rand();
          } else {
            value=0.1+5*rand();
          }

          printf "{\"deviceId\":\"dev-%05d\",\"ts\":\"%s\",\"metric\":\"%s\",\"value\":%.2f}\n",
                 did, ts, metric, value;
        }
      }' >> "$IOT_FILE"
  done
}

# Ejecutar generación
gen_logs "$LOGS_MB"
gen_iot "$IOT_MB"

echo "[generate] Tamaño final:"

# Mostrar tamaños finales
ls -lh "$LOG_FILE" "$IOT_FILE"
echo "[generate] OK"
