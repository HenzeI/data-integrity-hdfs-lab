#!/usr/bin/env bash
set -euo pipefail

NN_CONTAINER=${NN_CONTAINER:-namenode}
DT=${DT:-$(date +%F)}

echo "[inventory] DT=$DT"

# Verificar que existe backup
if ! docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -test -d /backup"; then
  echo "[inventory] /backup no existe en HDFS. Ejecute primero la copia de seguridad."
  exit 1
fi

# Crear carpeta auditoría inventario
docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -mkdir -p /audit/inventory/$DT"

# Generar inventario de /data y /backup
docker exec -i "$NN_CONTAINER" bash -lc "\
  hdfs dfs -stat '%n,%b,%y' /data/logs/raw/dt=$DT/* | sort > /tmp/inv_data_logs_$DT.csv; \
  hdfs dfs -stat '%n,%b,%y' /data/iot/raw/dt=$DT/* | sort > /tmp/inv_data_iot_$DT.csv; \
  hdfs dfs -stat '%n,%b,%y' /backup/logs/raw/dt=$DT/* | sort > /tmp/inv_bkp_logs_$DT.csv; \
  hdfs dfs -stat '%n,%b,%y' /backup/iot/raw/dt=$DT/* | sort > /tmp/inv_bkp_iot_$DT.csv"

docker exec -i "$NN_CONTAINER" bash -lc "\
  echo 'file,status,src_size,dst_size' > /tmp/inventory_compare_$DT.csv; \
  join -t, -a1 -a2 -e 'MISSING' -o 0,1.2,2.2 \
    <(awk -F, '{print \$1 \",\" \$2}' /tmp/inv_data_logs_$DT.csv /tmp/inv_data_iot_$DT.csv | sort) \
    <(awk -F, '{print \$1 \",\" \$2}' /tmp/inv_bkp_logs_$DT.csv /tmp/inv_bkp_iot_$DT.csv | sort) \
    | awk -F, '{ \
        src=\$2; dst=\$3; \
        if (dst==\"MISSING\") status=\"missing_in_backup\"; \
        else if (src==\"MISSING\") status=\"unexpected_in_backup\"; \
        else if (src!=dst) status=\"size_mismatch\"; \
        else status=\"ok\"; \
        print \$1 \",\" status \",\" src \",\" dst; \
      }' >> /tmp/inventory_compare_$DT.csv"

# Subir inventarios a HDFS
docker exec -i "$NN_CONTAINER" bash -lc "\
  hdfs dfs -put -f /tmp/inv_data_logs_$DT.csv /audit/inventory/$DT/inventory_data_logs.csv; \
  hdfs dfs -put -f /tmp/inv_data_iot_$DT.csv /audit/inventory/$DT/inventory_data_iot.csv; \
  hdfs dfs -put -f /tmp/inv_bkp_logs_$DT.csv /audit/inventory/$DT/inventory_backup_logs.csv; \
  hdfs dfs -put -f /tmp/inv_bkp_iot_$DT.csv /audit/inventory/$DT/inventory_backup_iot.csv; \
  hdfs dfs -put -f /tmp/inventory_compare_$DT.csv /audit/inventory/$DT/inventory_compare.csv"

docker exec -i "$NN_CONTAINER" bash -lc "hdfs dfs -ls -R /audit/inventory/$DT"

echo "[inventory] OK"
