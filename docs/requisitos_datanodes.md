## Requisitos de despliegue (número de DataNodes)

Este proyecto se realiza sobre el clúster Hadoop dockerizado del aula.  
Por defecto, el `docker-compose.yml` incluye **1 servicio DataNode+NodeManager** llamado `dnnm`.

### Recomendación (para el proyecto)
- ✅ **Base recomendada:** **3 DataNodes** (permite replicación 2–3 y tolerancia a fallos realista).
- ⭐ **Extra / reto:** **4 DataNodes** (permite comparar replicación 1/2/3/4).

> Nota importante: la **replicación efectiva** de HDFS no puede ser mayor que el número de DataNodes vivos.  
> Si configuras `dfs.replication=3` pero solo hay 1 DataNode, verás bloques **UNDER_REPLICATED** de forma constante.

---

## Cómo levantar el clúster

### Levantar el clúster (1 DataNode)
```bash
docker compose up -d
```

### Levantar el clúster con 3 DataNodes (recomendado)
```bash
docker compose up -d --scale dnnm=3
```

### Levantar el clúster con 4 DataNodes (reto)
```bash
docker compose up -d --scale dnnm=4
```

---

## Cómo comprobar cuántos DataNodes están activos (evidencia obligatoria)

### Opción A — UI del NameNode (más didáctico)
Abrir:
- NameNode UI: http://localhost:9870  
Ir a **Datanodes → Live Nodes** y hacer captura.

### Opción B — CLI (desde el host)
```bash
docker exec -it namenode bash -lc "hdfs dfsadmin -report | sed -n '1,120p'"
```

---

## Recomendación de replicación según DataNodes
- **1 DataNode:** usar `dfs.replication=1` (o `hdfs dfs -setrep -w 1 ...`)
- **3 DataNodes:** recomendado `dfs.replication=2` o `3`
- **4 DataNodes:** recomendado `dfs.replication=3` y comparar con `1/2/4` en el experimento

Ejemplo de experimento de replicación:
```bash
docker exec -it namenode bash -lc "hdfs dfs -setrep -w 1 /data/logs/raw/dt=YYYY-MM-DD"
docker exec -it namenode bash -lc "hdfs dfs -setrep -w 2 /data/logs/raw/dt=YYYY-MM-DD"
docker exec -it namenode bash -lc "hdfs dfs -setrep -w 3 /data/logs/raw/dt=YYYY-MM-DD"
docker exec -it namenode bash -lc "hdfs dfs -setrep -w 4 /data/logs/raw/dt=YYYY-MM-DD"
```
