# Guía para Completar la Base de Datos OpenAlex

## Situación Actual

Ya ejecutaste `load2.py` que cargó:
- ✅ `works` (3,404,980 registros)
- ✅ `works_authorships` (11,201,204 registros)

Pero faltan tablas críticas:
- ❌ `sources` (0 registros)
- ❌ `institutions` (0 registros)
- ❌ `works_primary_location` (no existe)
- ❌ `works_open_access` (no existe)

## Solución: Script `load_missing_tables.py`

He creado un script complementario que carga las tablas faltantes usando la misma estrategia que tu `load2.py`.

---

## Pasos para Completar la Base de Datos

### **Paso 1: Verificar el Snapshot**

Asegúrate de tener estas carpetas en tu snapshot:

```
openalex-snapshot/data/
├── sources/           ← Necesario
├── institutions/      ← Necesario
└── works/            ← Ya lo usaste con load2.py
```

### **Paso 2: Ajustar Configuración**

Edita `load_missing_tables.py` si es necesario:

```python
DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "database": "openalex",  # ← Cambiar si usas otro nombre
    "user": "postgres",
    "password": "tu_contasena" 
}

SNAPSHOT_DIR = "./openalex-snapshot/data"  # ← Ajustar ruta si es necesario
```

### **Paso 3: Ejecutar el Script**

```bash
python load_missing_tables.py
```

**Proceso**:
1. Carga `sources` (revistas LATAM)
2. Carga `institutions` (instituciones LATAM)
3. Carga `works_primary_location` (relación work→source)
4. Carga `works_open_access` (información de OA)

**Tiempo estimado**: 30-60 minutos (dependiendo del tamaño del snapshot)

---

## Qué Hace Cada Tabla

### **1. `sources` (Revistas)**

**Datos**:
- ID de la revista
- ISSN
- Nombre
- Editorial
- País
- Si está en DOAJ
- Número de trabajos

**Importancia**: **CRÍTICA** - Permite identificar qué revistas son latinoamericanas.

---

### **2. `institutions` (Instituciones)**

**Datos**:
- ID de la institución
- ROR
- Nombre
- **País** ← Crítico
- Tipo
- Número de trabajos

**Importancia**: **CRÍTICA** - Permite identificar el país de cada trabajo por las afiliaciones de los autores.

---

### **3. `works_primary_location`**

**Datos**:
- work_id
- **source_id** ← Crítico (relaciona work con revista)
- is_oa
- landing_page_url
- pdf_url
- license
- version

**Importancia**: **CRÍTICA** - Es el puente entre `works` y `sources`.

---

### **4. `works_open_access`**

**Datos**:
- work_id
- is_oa
- oa_status (gold, green, hybrid, bronze, closed)
- oa_url
- any_repository_has_fulltext

**Importancia**: **ALTA** - Necesario para calcular métricas de acceso abierto.

---

## Después de Cargar las Tablas

### **Paso 1: Verificar**

```bash
python diagnose_postgres.py
```

**Deberías ver**:
```
sources                       :          X,XXX  ← Ya no 0
institutions                  :         XX,XXX  ← Ya no 0
works_primary_location        :      X,XXX,XXX  ← Ya no "Table not found"
works_open_access             :      X,XXX,XXX  ← Ya no "Table not found"
```

### **Paso 2: Crear Índices**

```bash
psql -U postgres -d openalex -f create_indexes.sql
```

Esto acelerará las consultas significativamente.

### **Paso 3: Extraer Datos**

Ahora puedes usar el script completo:

```bash
python data_collector_postgres.py
```

Este script:
- ✅ Identifica revistas LATAM con precisión (usando `sources.country_code`)
- ✅ Extrae works de esas revistas (usando `works_primary_location`)
- ✅ Incluye información de OA (usando `works_open_access`)
- ✅ Determina país por instituciones (usando `institutions.country_code`)

---

## Comparación: Antes vs Después

### **Antes** (solo works + authorships)

```
Estrategia: Detección de texto en afiliaciones
Precisión: ~50-80%
Velocidad: Lenta (scan completo)
Script: data_collector_postgres_simple.py
```

### **Después** (con todas las tablas)

```
Estrategia: Joins con institutions.country_code
Precisión: ~95%+
Velocidad: Rápida (con índices)
Script: data_collector_postgres.py
```

---

## Solución de Problemas

### **Error: "No se encuentra la carpeta sources"**

**Problema**: El snapshot no tiene la carpeta `sources`.

**Solución**: 
1. Verifica que descargaste el snapshot completo
2. Ajusta `SNAPSHOT_DIR` en el script

### **Error: "relation already exists"**

**Problema**: La tabla ya existe (probablemente vacía).

**Solución**: Elimina la tabla vacía:
```sql
DROP TABLE IF EXISTS openalex.sources;
DROP TABLE IF EXISTS openalex.institutions;
DROP TABLE IF EXISTS openalex.works_primary_location;
DROP TABLE IF EXISTS openalex.works_open_access;
```

Luego vuelve a ejecutar el script.

### **Error de memoria**

**Problema**: El script consume mucha memoria.

**Solución**: El script ya usa buffers y commits incrementales, pero si aún hay problemas:
1. Procesa menos archivos a la vez
2. Aumenta la memoria de PostgreSQL en `postgresql.conf`:
   ```
   shared_buffers = 2GB
   work_mem = 256MB
   ```

---

## Estimación de Espacio en Disco

### **Tablas a Cargar**

| Tabla | Registros Estimados | Espacio Estimado |
|-------|---------------------|------------------|
| `sources` | ~8,000 | ~5 MB |
| `institutions` | ~50,000 | ~30 MB |
| `works_primary_location` | ~3,400,000 | ~200 MB |
| `works_open_access` | ~3,400,000 | ~150 MB |
| **Total** | | **~385 MB** |

Más índices: ~200 MB adicionales

**Total necesario**: ~600 MB

---

## Verificación Final

Después de completar todo, ejecuta:

```bash
# 1. Verificar base de datos
python diagnose_postgres.py

# 2. Extraer datos
python data_collector_postgres.py

# 3. Verificar archivos generados
python diagnose_data.py

# 4. Calcular métricas
python precompute_metrics_parallel_optimized.py

# 5. Ver dashboard
streamlit run dashboard.py
```

---

## Resumen

1. ✅ Ya tienes: `works`, `works_authorships` (cargados con `load2.py`)
2. 🔧 Ejecuta: `python load_missing_tables.py`
3. ✅ Obtendrás: `sources`, `institutions`, `works_primary_location`, `works_open_access`
4. 🚀 Usa: `python data_collector_postgres.py` (script completo, alta precisión)

¡Esto te dará un sistema completo y preciso para analizar las revistas latinoamericanas!
