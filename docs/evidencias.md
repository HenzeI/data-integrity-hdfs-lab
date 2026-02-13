# Evidencias (plantilla)

Incluye aquí (capturas o logs) con fecha:

## 1) NameNode UI (9870)
- Captura con DataNodes vivos y capacidad

![alt text](capturas/1-NameNode.png)

## 2) Auditoría fsck
- Enlace/captura de salida (bloques/locations)
- Resumen (CORRUPT/MISSING/UNDER_REPLICATED)

#### Location .jsonl 
![alt text](capturas/2.1-fsck.png)

### Location .log
![alt text](capturas/2.2-fsck.png)

### Bloques
![alt text](capturas/2.3-fsck.png)

### Directorio de auditoria de fsck
![alt text](capturas/2.4-fsck.png)

## 3) Backup + validación
- Inventario origen vs destino
- Evidencias de consistencia (tamaños/rutas)

![alt text](capturas/3-Backup+Validacion.png)

## 4) Incidente + recuperación
- Qué hiciste, cuándo y qué efecto tuvo
- Evidencia de detección y de recuperación

### Ejecucion del incidente
![alt text](capturas/4.1-Incidente+recuperacion.png)

### Muestra del datanode caido
![alt text](capturas/4.2-Incidente+recuperacion.png)
![alt text](capturas/4.3-Incidente+recuperacion.png)

### Recuperacion del datanode
![alt text](capturas/4.4-Incidente+recuperacion.png)

### Muestra de los datanodes en funcionamiento
![alt text](capturas/4.5-Incidente+recuperacion.png)

## 5) Métricas
- Capturas de docker stats durante replicación/copia
- Tabla de tiempos

### Estado normal
![alt text](capturas/5.1-Metricas.png)

### Estado en proceso de copiado
![alt text](capturas/5.2-Metricas.png)
