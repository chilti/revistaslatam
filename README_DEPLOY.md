# Guía de Despliegue de RevistasLATAM (Flujos Paralelos)

Actualmente, el proyecto soporta dos arquitecturas independientes para adaptarse al análisis regional detallado y al análisis macro-global. Debes seguir el flujo de pipeline correspondiente al entorno que tienes configurado.

---

## 🌎 Flujo 1: Arquitectura Global (ClickHouse)
**Objetivo:** Análisis bibliométrico mundial comparando macro-regiones y todos los países, manejando millones de registros sin sobrecargar la memoria local usando el motor OLAP de ClickHouse y un Snpashot de OpenAlex dinámico.

**Requisitos:** 
- Servidor ClickHouse corriendo.
- Carpeta con el snapshot actualizado de OpenAlex (`.jsonl.gz`).

### Pipeline de Ejecución (ClickHouse)
Ejecutar desde la raíz del proyecto:

1. **Ingestar el Snapshot a ClickHouse**
   *Este script escanea la carpeta del snapshot, infiere dinámicamente las entidades y crea las tablas `MergeTree` para cargarlas.*
   ```bash
   python pipeline/load_openalex_clickhouse.py /ruta/al/openalex-snapshot
   ```

2. **Cómputo Analítico (Server-Side)**
   *Delega a ClickHouse el cálculo (COUNT, AVG FWCI, % OA) y descarga solo los resúmenes estadísticos (Parquets)*
   ```bash
   python pipeline/compute_metrics_clickhouse.py
   ```

3. **Proyectar Mapas Dimensionales Globales**
   *Crea la vista UMAP para las macro regiones usando las métricas descargadas en el paso anterior.*
   ```bash
   python pipeline/calculate_umap_global.py
   ```

### Levantar el Dashboard
El entorno Mundial se sirve mediante una interfaz dedicada:
```bash
streamlit run dashboard_global.py
```

---

## 🏛️ Flujo 2: Arquitectura Regional "Legacy" (PostgreSQL)
**Objetivo:** Análisis del corpus base original (Latinoamérica). Centrado en la extracción local procesando lotes desde PostgreSQL usando Pandas.

**Requisitos:** 
- Base de datos PostgreSQL activa con las tablas `works`, `authors`, `sources`, etc.

### Pipeline de Ejecución (PostgreSQL)

Si necesitas recalcular el pipeline desde cero usando la arquitectura original, los scripts clave fueron restaurados en la carpeta `pipeline_legacy_backup/` y coexistirán con los modulares en `pipeline/`.

1. **Extracción y Cálculo Base Parcial (Opcional, según necesidad)**
   *Descarga la información en chunking localmente.*
   ```bash
   # Opción Secuencial
   python pipeline_legacy_backup/precompute_metrics.py
   
   # Opción Paralelizada
   python pipeline_legacy_backup/precompute_metrics_parallel.py
   ```

2. **Transformación de Métricas de LATAM**
   *Procesa los indicadores temporales y suavizados de la región.*
   ```bash
   python pipeline/transform_metrics.py --force
   ```

3. **Geometría de Similitud Interamericana**
   *Genera los espacios UMAP y el agrupamiento Self-Organizing Map (SOM) para los países locales.*
   ```bash
   python pipeline/calculate_umap.py
   python pipeline/process_trajectories.py
   python pipeline/calculate_som.py
   python pipeline/calculate_som_trajectories.py
   ```

### Levantar el Dashboard
El entorno originario (Latinoamérica) se despliega con su archivo principal:
```bash
streamlit run dashboard.py
```

---

*Nota de mantenimiento: Evita cruzar los archivos `.parquet` de `data/cache` generados por uno y otro flujo, ya que `dashboard_global.py` espera el prefijo `metrics_global_` mientras que el regional asume los datos calculados de forma predeterminada.*
