# Scripts de Diagnóstico y Verificación

Esta carpeta contiene scripts útiles para diagnosticar problemas y verificar datos en diferentes etapas del pipeline.

## 📋 Índice de Scripts

### 1. `search_in_snapshot.py`
**Propósito**: Buscar "Estudios Demográficos y Urbanos" directamente en los archivos del snapshot de OpenAlex.

**Uso**:
```bash
python tools/search_in_snapshot.py
```

**Qué hace**:
- Busca en los archivos `.gz` del snapshot
- Muestra TODOS los campos disponibles en el registro original
- Permite verificar si campos como `is_in_scielo`, `is_ojs`, etc. existen en el snapshot
- Útil para confirmar que los datos originales son correctos

**Configuración**:
Edita la variable `SNAPSHOT_BASE` en el script para apuntar a tu snapshot:
```python
SNAPSHOT_BASE = Path('/mnt/expansion/openalex-snapshot/data')
```

---

### 2. `search_journal_in_snapshot.py`
**Propósito**: Versión genérica para buscar CUALQUIER revista en el snapshot.

**Uso**:
```bash
# Buscar por nombre
python tools/search_journal_in_snapshot.py --name "Estudios Demográficos"

# Buscar por ISSN
python tools/search_journal_in_snapshot.py --issn "0186-7210"

# Buscar por ID de OpenAlex
python tools/search_journal_in_snapshot.py --id "S2737081250"

# Especificar directorio del snapshot
python tools/search_journal_in_snapshot.py --name "Salud Pública" --snapshot-dir /data/openalex
```

**Qué hace**:
- Búsqueda flexible por nombre, ISSN o ID
- Muestra información estructurada de la revista
- Exporta el registro JSON completo

---

### 3. `verify_estudios_data.py`
**Propósito**: Comparar datos entre PostgreSQL y el archivo parquet procesado.

**Uso**:
```bash
python tools/verify_estudios_data.py
```

**Qué hace**:
- Consulta PostgreSQL para obtener los datos de la revista
- Lee el archivo `latin_american_journals.parquet`
- Compara ambos para identificar discrepancias
- Útil para verificar que la extracción desde PostgreSQL funciona correctamente

**Requisitos**:
- Acceso a PostgreSQL (configurar credenciales en el script)
- Archivo `data/latin_american_journals.parquet` existente

---

### 4. `diagnose_estudios_demo.py`
**Propósito**: Diagnóstico rápido de la revista en el parquet local.

**Uso**:
```bash
python tools/diagnose_estudios_demo.py
```

**Qué hace**:
- Lee `latin_american_journals.parquet`
- Muestra todas las métricas disponibles
- Cuenta trabajos reales en `latin_american_works.parquet`
- Calcula diferencias entre conteos

---

### 5. `inspect_estudios_demograficos.py`
**Propósito**: Inspeccionar trabajos de la revista en el archivo de works.

**Uso**:
```bash
python tools/inspect_estudios_demograficos.py
```

**Qué hace**:
- Busca trabajos de la revista en `latin_american_works.parquet`
- Muestra ejemplos de trabajos
- Exporta a CSV para análisis en Excel

---

## 🔍 Flujo de Diagnóstico Recomendado

Si encuentras un problema con los datos de una revista, sigue este orden:

### Paso 1: Verificar Snapshot Original
```bash
python tools/search_in_snapshot.py
# o
python tools/search_journal_in_snapshot.py --name "Nombre Revista"
```
**Objetivo**: Confirmar que los datos están correctos en la fuente original.

### Paso 2: Verificar PostgreSQL
```bash
python tools/verify_estudios_data.py
```
**Objetivo**: Confirmar que la carga a PostgreSQL fue correcta.

### Paso 3: Verificar Parquet Procesado
```bash
python tools/diagnose_estudios_demo.py
```
**Objetivo**: Confirmar que la extracción desde PostgreSQL al parquet fue correcta.

### Paso 4: Verificar Works
```bash
python tools/inspect_estudios_demograficos.py
```
**Objetivo**: Verificar que los trabajos de la revista están presentes.

---

## 🛠️ Solución de Problemas Comunes

### Problema: "No se encontró el directorio del snapshot"
**Solución**: Edita `SNAPSHOT_BASE` o `DEFAULT_SNAPSHOT_BASE` en el script correspondiente.

Ubicaciones comunes:
- `/mnt/expansion/openalex-snapshot/data`
- `/data/openalex-snapshot/data`
- `~/openalex-snapshot/data`

### Problema: "Error conectando a PostgreSQL"
**Solución**: Verifica las credenciales en `verify_estudios_data.py`:
```python
DB_CONFIG = {
    'host': 'localhost',
    'database': 'openalex_db',
    'user': 'postgres',
    'password': 'tu_contasena',  # ← Actualiza esto
    'port': 5432
}
```

### Problema: "Campo X no existe en el snapshot"
**Solución**: Algunos campos (`is_in_scielo`, `is_ojs`, `is_core`) pueden no existir en versiones antiguas del snapshot de OpenAlex. En ese caso:
1. Actualiza el snapshot a una versión más reciente, o
2. Usa valores por defecto (el script de extracción ya tiene fallbacks)

---

## 📝 Notas

- Estos scripts están diseñados para diagnóstico, no para procesamiento masivo
- Los scripts de búsqueda en snapshot pueden tardar varios minutos (procesan archivos .gz grandes)
- Siempre verifica primero en el snapshot antes de asumir que hay un bug en el código

---

## 🆘 Ayuda

Si encuentras un problema que estos scripts no pueden diagnosticar, documenta:
1. Output completo del script
2. Versión del snapshot de OpenAlex que estás usando
3. Mensaje de error exacto (si aplica)
