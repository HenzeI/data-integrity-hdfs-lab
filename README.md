# DataSecure Lab - Integridad de Datos en HDFS

## 1. Objetivo del proyecto
Este laboratorio implementa un flujo reproducible de integridad de datos en Hadoop HDFS para una empresa ficticia (DataSecure). El objetivo es demostrar, con evidencia, todo el ciclo:

1. Generacion de datos realistas (logs e IoT).
2. Ingesta en HDFS con particionado por fecha.
3. Auditoria de integridad con `hdfs fsck`.
4. Copia a backup y validacion origen vs destino.
5. Simulacion de incidente (caida de DataNode).
6. Recuperacion del servicio y restauracion desde backup.
7. Analisis de coste/beneficio (tiempos y recursos).

## 2. Estructura del proyecto
```text
  data-integrity-hdfs-lab/
  │
  ├── docker/
  │   └── clusterA/
  │       ├── docker-compose.yml
  │       ├── docker-compose-fast.yml
  │       └── conf-fast/
  │           └── hdfs-site.xml
  │
  ├── scripts/
  │   ├── 00_bootstrap.sh
  │   ├── 10_generate_data.sh
  │   ├── 20_ingest_hdfs.sh
  │   ├── 30_fsck_audit.sh
  │   ├── 40_backup_copy.sh
  │   ├── 50_inventory_compare.sh
  │   ├── 70_incident_simulation.sh
  │   ├── 80_recovery_restore.sh
  │   └── 90_run.sh
  │
  ├── notebooks/
  │   └── 02_auditoria_integridad_ejemplo.ipynb
  │
  ├── docs/
  │   ├── enunciado_proyecto.md
  │   ├── requisitos_despliegue.md
  │   ├── evidencias.md
  │   ├── rubric.md
  │   ├── entrega.md
  │   ├── pistas.md
  │   └── capturas/
  │
  ├── .gitignore
  └── README.md
```

## 3. Prerrequisitos
- Docker Desktop (o Docker Engine + Compose v2).
- Bash disponible para ejecutar `.sh` (Linux, WSL o Git Bash).
- Contenedores con nombre por defecto del compose (especialmente `namenode`).

## 4. Quickstart

A continuación, siga los siguientes pasos copiando y pegando para el correcto funcionamiento del proyecto.

### 4.1 Clonar el repositorio

```bash
git clone https://github.com/HenzeI/data-integrity-hdfs-lab.git
```
Acceder a la carpeta del repositorio

### 4.2 Levantar cluster Hadoop
Normal (3 DataNodes, necesario para el incidente por defecto):

> *Nota: Por defecto, el namenode **tardará 10** minutos en detectar si un datanode está caído.*

```bash
cd data-integrity-hdfs-lab/docker/clusterA && docker compose up -d --scale dnnm=3
```
Recomendado:

> *Nota: Se recomienda utilizar esta opción, ya que reduce el tiempo en el que el namenode tarda en detectar si un datanode está caído a **1 minuto.***

```bash
cd data-integrity-hdfs-lab/docker/clusterA && docker compose -f docker-compose.yml -f docker-compose-fast.yml up -d --scale dnnm=3
```

### 4.3 Ejecutar pipeline completo con un solo comando
```bash
bash ../../scripts/90_run.sh
```

### 4.4 Ejecutar pipeline paso a paso (opcional)
```bash
bash scripts/00_bootstrap.sh
bash scripts/10_generate_data.sh
bash scripts/20_ingest_hdfs.sh
bash scripts/30_fsck_audit.sh
bash scripts/40_backup_copy.sh
bash scripts/50_inventory_compare.sh
bash scripts/70_incident_simulation.sh
bash scripts/80_recovery_restore.sh
```

## 5. Variables utiles
Todos los scripts usan variables de entorno para parametrizar la ejecucion:

- `DT=YYYY-MM-DD`: fecha de trabajo (por defecto, hoy).
- `NN_CONTAINER=namenode`: contenedor con cliente HDFS.

Variables adicionales:
- `OUT_DIR`, `LOGS_MB`, `IOT_MB` en `10_generate_data.sh`.
- `LOCAL_DIR` en `20_ingest_hdfs.sh`.
- `DN_NAME_FILTER`, `WAIT_DEAD_TIMEOUT`, `WAIT_DEAD_INTERVAL` en `70_incident_simulation.sh`.

Ejemplo:
```bash
export DT=2026-02-13
export NN_CONTAINER=namenode
export LOGS_MB=512
export IOT_MB=512
bash scripts/10_generate_data.sh
```

## 5.1 Parametros HDFS
### Ubicacion de XML de configuracion
- Los archivos XML de configuración en este caso concreto se encuentran en `/opt/bd/hadoop-3.3.6/etc/hadoop/`.
- Los valores `dfs.blocksize` y `dfs.replication` que estan dentro de `hdfs-site.xml`.
- En ejecucion, los XML efectivos estan dentro del contenedor `namenode` (ruta tipica de Hadoop): `$HADOOP_HOME/etc/hadoop/` o `/etc/hadoop/`.
- Para localizarlo en runtime:
```bash
docker exec -it namenode bash -lc "echo $HADOOP_CONF_DIR; ls -la $HADOOP_HOME/etc/hadoop 2>/dev/null; ls -la /etc/hadoop 2>/dev/null"
```

### Valores documentados
- `dfs.blocksize = 64m`
- `dfs.replication = 3`
- Verificacion de valor efectivo en el cluster:
```bash
docker exec -it namenode bash -lc "hdfs getconf -confKey dfs.blocksize"
docker exec -it namenode bash -lc "hdfs getconf -confKey dfs.replication"
```

### Justificacion tecnica (integridad vs coste)
Es recomendable `dfs.replication=3` porque tolera la caida de un DataNode sin perdida de disponibilidad del bloque. Con 3 replicas, la re-replicacion tras incidente es mas robusta que con 1 o 2 replicas. El coste de replicacion 3 es mayor (aprox. 3x almacenamiento logico y mas trafico de red en escritura).
Ese coste se acepta en este laboratorio porque el objetivo principal es integridad y recuperacion demostrable.
Elegí `dfs.blocksize=64m` para generar mas bloques por archivo y observar mejor auditoria (`fsck`) y distribucion.
Un bloque mas pequeno mejora granularidad de balanceo y pruebas didacticas, aunque incrementa metadatos en NameNode.
Un bloque mayor reduce metadatos y puede mejorar throughput secuencial, pero da menos visibilidad en el experimento.
HDFS usa CRC por bloque porque es muy rapido para deteccion de corrupcion en lectura/escritura de bajo nivel.
CRC detecta errores accidentales, pero no esta pensado para trazabilidad end-to-end ni para controles criptograficos.
SHA/MD5 a nivel aplicacion aporta verificacion extremo a extremo (origen vs destino), util para auditoria adicional.

## 6. Que hace cada script
### `scripts/00_bootstrap.sh`
- Crea la estructura base en HDFS para `/data`, `/backup` y `/audit`.
- Inicializa rutas particionadas por `dt=...`.

### `scripts/10_generate_data.sh`
- Genera dataset local sintetico:
  - `logs_YYYYMMDD.log`
  - `iot_YYYYMMDD.jsonl`
- Crea volumen configurable (por defecto 1 GB por archivo).

### `scripts/20_ingest_hdfs.sh`
- Valida que existan los archivos locales generados.
- Sube ambos archivos a HDFS en rutas particionadas bajo `/data`.
- Muestra inventario y tamanos en HDFS.

### `scripts/30_fsck_audit.sh`
- Ejecuta `hdfs fsck` sobre `/data` (y `/backup` si existe).
- Guarda evidencias en `/audit/fsck/<DT>/`.
- Genera resumen CSV con conteos `CORRUPT`, `MISSING`, `Under replicated`.

### `scripts/40_backup_copy.sh`
- Copia datos de `/data/.../dt=<DT>` a `/backup/.../dt=<DT>`.
- Registra listado y tamanos del backup.
- Guarda log en `/audit/inventory/<DT>/backup_copy.log`.

### `scripts/50_inventory_compare.sh`
- Genera inventarios de origen y backup.
- Compara por nombre y tamano.
- Clasifica estado por archivo (`ok`, `missing_in_backup`, `unexpected_in_backup`, `size_mismatch`).
- Guarda CSVs en `/audit/inventory/<DT>/`.

### `scripts/70_incident_simulation.sh`
- Selecciona y detiene un DataNode para simular incidente.
- Captura estado antes y durante (dfsadmin + fsck).
- Espera deteccion de nodo caido por NameNode. **Por lo general esto suele tardar 10 minutos.**
- Guarda evidencia en `/audit/incidents/<DT>/`.

> Nota: por defecto busca un DataNode cuyo nombre cumpla `DN_NAME_FILTER=dnnm-3`. Si no escalaste a 3 nodos, define un filtro valido, por ejemplo.

```bash
export DN_NAME_FILTER=dnnm
bash scripts/70_incident_simulation.sh
```

### `scripts/80_recovery_restore.sh`
- Arranca nuevamente el DataNode detenido (si existe registro del incidente).
- Recolecta evidencia post-recuperacion.
- Restaura desde `/backup` a `/data` los archivos faltantes del dia.
- Guarda evidencia final en `/audit/recovery/<DT>/`.

### `scripts/90_run.sh`
- Ejecuta automaticamente todos los scripts del pipeline en orden.
- Se detiene ante cualquier error (`set -euo pipefail`).
- Facilita la ejecucion end-to-end con un solo comando.

## 7. UIs y verificaciones rapidas
- NameNode UI: http://localhost:9870
- ResourceManager UI: http://localhost:8088
- Jupyter en NameNode: http://localhost:8889

Comandos utiles:
```bash
docker exec -it namenode bash -lc "hdfs dfsadmin -report | head -n 120"
docker exec -it namenode bash -lc "hdfs fsck /data -files -blocks -locations | head -n 200"
docker exec -it namenode bash -lc "hdfs getconf -confKey dfs.blocksize"
docker exec -it namenode bash -lc "hdfs getconf -confKey dfs.replication"
```

## 8. Flujo de salida esperado en HDFS
Al finalizar pipeline deberias tener evidencia en:

- `/data/logs/raw/dt=<DT>/...`
- `/data/iot/raw/dt=<DT>/...`
- `/backup/logs/raw/dt=<DT>/...`
- `/backup/iot/raw/dt=<DT>/...`
- `/audit/fsck/<DT>/...`
- `/audit/inventory/<DT>/...`
- `/audit/incidents/<DT>/...`
- `/audit/recovery/<DT>/...`
