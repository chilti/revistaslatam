# PostgreSQL Data Collector - Guía de Uso

## Descripción

Este conjunto de scripts permite extraer datos de revistas y trabajos latinoamericanos desde una base de datos PostgreSQL local que contiene el snapshot de OpenAlex, en lugar de usar la API de OpenAlex.

## Requisitos

### 1. Dependencias de Python

```bash
pip install psycopg2-binary pandas pyarrow
```

### 2. Base de Datos PostgreSQL

- PostgreSQL instalado y corriendo
- Base de datos `openalex` creada
- Schema `openalex` con las tablas del snapshot
- Usuario: `postgres`
- Contraseña: `[PASSWORD]`

## Archivos Creados

### 1. `diagnose_postgres.py`
Script de diagnóstico para verificar la conexión y el contenido de la base de datos.

**Uso**:
```bash
python diagnose_postgres.py
```

**Información que muestra**:
- ✓ Prueba de conexión
- ✓ Lista de tablas en el schema `openalex`
- ✓ Conteo de filas en tablas principales
- ✓ Instituciones por país latinoamericano
- ✓ Muestra de journals encontrados

### 2. `data_collector_postgres.py`
Script principal para extraer datos de journals y works.

**Uso básico**:
```bash
# Extraer journals y works
python data_collector_postgres.py

# Solo journals
python data_collector_postgres.py --journals-only

# Solo works (requiere que journals ya existan)
python data_collector_postgres.py --works-only
```

## Flujo de Trabajo Recomendado

### Paso 1: Verificar la Base de Datos

```bash
python diagnose_postgres.py
```

**Verifica**:
- ¿La conexión funciona?
- ¿Existen las tablas necesarias?
- ¿Hay datos de países latinoamericanos?

### Paso 2: Extraer Journals

```bash
python data_collector_postgres.py --journals-only
```

**Salida esperada**:
- Archivo: `data/latin_american_journals.parquet`
- Contenido: Metadata de todas las revistas latinoamericanas

**Tiempo estimado**: Depende del tamaño de la BD, puede tomar varios minutos u horas.

### Paso 3: Extraer Works

```bash
python data_collector_postgres.py --works-only
```

**Salida esperada**:
- Archivo: `data/latin_american_works.parquet`
- Contenido: Todos los trabajos de las revistas latinoamericanas

**Características**:
- ✓ Guarda incrementalmente cada 10 journals
- ✓ Puede interrumpirse con Ctrl+C y retomar
- ✓ Detecta journals ya procesados

**Tiempo estimado**: Varias horas o días, dependiendo del número de journals y works.

## Estructura de la Base de Datos Requerida

### Tablas Principales

```sql
openalex.sources              -- Journals/revistas
openalex.works                -- Trabajos/artículos
openalex.institutions         -- Instituciones (con country_code)
openalex.authors              -- Autores
```

### Tablas de Relación

```sql
openalex.works_primary_location  -- Relación work -> source
openalex.works_authorships       -- Relación work -> author -> institution
openalex.works_concepts          -- Conceptos de cada work
openalex.works_topics            -- Tópicos de cada work (opcional)
openalex.works_open_access       -- Info de acceso abierto (opcional)
```

## Estrategia de Extracción

### Identificación de Journals Latinoamericanos

El script identifica journals latinoamericanos mediante:

1. **Buscar works** que tienen al menos un autor afiliado a una institución latinoamericana
2. **Extraer source_id** de esos works
3. **Obtener metadata** de esos sources desde la tabla `sources`
4. **Determinar país principal** contando las afiliaciones más comunes

### Extracción de Works

Para cada journal:

1. **Query principal**: Obtiene works donde `works_primary_location.source_id = journal_id`
2. **Authorships**: Agrega información de autores e instituciones
3. **Concepts**: Agrega conceptos asociados
4. **Topics**: Agrega tópicos (si existe la tabla)
5. **Open Access**: Agrega información de OA (si existe la tabla)

## Optimización de Consultas

### Índices Recomendados

Para mejorar el rendimiento, crea estos índices:

```sql
-- Índices en works_primary_location
CREATE INDEX IF NOT EXISTS idx_wpl_source_id 
ON openalex.works_primary_location(source_id);

CREATE INDEX IF NOT EXISTS idx_wpl_work_id 
ON openalex.works_primary_location(work_id);

-- Índices en works_authorships
CREATE INDEX IF NOT EXISTS idx_wa_work_id 
ON openalex.works_authorships(work_id);

CREATE INDEX IF NOT EXISTS idx_wa_institution_id 
ON openalex.works_authorships(institution_id);

-- Índice en institutions
CREATE INDEX IF NOT EXISTS idx_inst_country 
ON openalex.institutions(country_code);

-- Índices en works_concepts
CREATE INDEX IF NOT EXISTS idx_wc_work_id 
ON openalex.works_concepts(work_id);

-- Índices en works_open_access (si existe)
CREATE INDEX IF NOT EXISTS idx_woa_work_id 
ON openalex.works_open_access(work_id);
```

**Ejecutar**:
```bash
psql -U postgres -d openalex -f create_indexes.sql
```

## Configuración de la Base de Datos

### Cambiar Contraseña (si es necesario)

Edita `data_collector_postgres.py` y `diagnose_postgres.py`:

```python
DB_CONFIG = {
    'host': 'localhost',
    'database': 'openalex',
    'user': 'postgres',
    'password': 'TU_CONTRASEÑA_AQUÍ',  # ← Cambiar aquí
    'port': 5432
}
```

### Conexión Remota

Si la base de datos está en otro servidor:

```python
DB_CONFIG = {
    'host': '192.168.1.100',  # IP del servidor
    'database': 'openalex',
    'user': 'postgres',
    'password': 'tu_contasena',
    'port': 5432
}
```

## Solución de Problemas

### Error: "relation does not exist"

**Problema**: Alguna tabla no existe en tu schema.

**Solución**: 
1. Ejecuta `diagnose_postgres.py` para ver qué tablas existen
2. Comenta las secciones del código que usan tablas faltantes

Ejemplo en `data_collector_postgres.py`:

```python
# Si no tienes works_topics, comenta esta sección:
# try:
#     topics_query = """..."""
#     topics_df = pd.read_sql_query(topics_query, conn, params=(work_ids,))
#     works_df = works_df.merge(topics_df, ...)
# except:
#     print("  Note: works_topics table not found, skipping topics")
```

### Error: "password authentication failed"

**Problema**: Contraseña incorrecta.

**Solución**: Verifica la contraseña en `DB_CONFIG`.

### Consultas muy lentas

**Problema**: Falta de índices.

**Solución**: 
1. Crea los índices recomendados (ver sección arriba)
2. Ejecuta `ANALYZE` en PostgreSQL:
   ```sql
   ANALYZE openalex.works;
   ANALYZE openalex.works_primary_location;
   ANALYZE openalex.works_authorships;
   ```

### Memoria insuficiente

**Problema**: El script consume mucha memoria.

**Solución**: Reduce el batch size en `data_collector_postgres.py`:

```python
# Cambiar de 10 a 5 o 1
if len(all_works) >= 5:  # ← Reducir este número
    print("\n  → Saving batch to disk...")
```

## Comparación con API de OpenAlex

| Aspecto | API OpenAlex | PostgreSQL Local |
|---------|--------------|------------------|
| **Velocidad** | Lenta (rate limits) | Rápida (sin límites) |
| **Límites** | 10 req/seg, límite diario | Sin límites |
| **Datos** | Siempre actualizados | Snapshot (puede estar desactualizado) |
| **Costo** | Gratis pero limitado | Requiere almacenamiento local |
| **Tiempo total** | ~5 días | ~Horas |

## Archivos de Salida

### `data/latin_american_journals.parquet`

Columnas:
- `id`: OpenAlex ID
- `issn_l`: ISSN-L
- `display_name`: Nombre de la revista
- `publisher`: Editorial
- `works_count`: Número de trabajos
- `cited_by_count`: Número de citas
- `is_oa`: ¿Es Open Access?
- `is_in_doaj`: ¿Está en DOAJ?
- `country_code`: País principal
- `download_date`: Fecha de descarga

### `data/latin_american_works.parquet`

Columnas:
- `id`: OpenAlex ID del trabajo
- `doi`: DOI
- `title`: Título
- `publication_year`: Año de publicación
- `type`: Tipo de trabajo
- `cited_by_count`: Número de citas
- `journal_id`: ID de la revista
- `journal_name`: Nombre de la revista
- `authorships`: JSON con autores e instituciones
- `concepts`: JSON con conceptos
- `topics`: JSON con tópicos (si disponible)
- `is_oa`: ¿Es Open Access?
- `oa_status`: Estado de OA
- `download_date`: Fecha de descarga

## Próximos Pasos

Después de extraer los datos:

1. **Verificar** con `diagnose_data.py`:
   ```bash
   python diagnose_data.py
   ```

2. **Calcular métricas**:
   ```bash
   python precompute_metrics_parallel_optimized.py
   ```

3. **Visualizar** en el dashboard:
   ```bash
   streamlit run dashboard.py
   ```

## Soporte

Si encuentras problemas:

1. Ejecuta `diagnose_postgres.py` y comparte el output
2. Verifica que todas las tablas necesarias existan
3. Crea los índices recomendados
4. Ajusta el batch size si hay problemas de memoria
