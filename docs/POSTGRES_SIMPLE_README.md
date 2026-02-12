# Data Collector Simplificado - PostgreSQL

## Situación

Tu base de datos PostgreSQL tiene un snapshot **parcial** de OpenAlex con solo estas tablas:

✅ **Disponibles**:
- `works` (3,404,980 registros)
- `works_authorships` (11,201,204 registros)

❌ **Faltantes/Vacías**:
- `sources` (0 registros)
- `institutions` (0 registros)
- `works_primary_location` (no existe)
- `works_concepts` (0 registros)
- `works_open_access` (no existe)

## Estrategia Alternativa

Como no tenemos la tabla `institutions` con `country_code`, el script `data_collector_postgres_simple.py` usa una **estrategia heurística**:

### 🔍 **Cómo Identifica Trabajos Latinoamericanos**

1. **Lee `raw_affiliation_string`** de `works_authorships`
2. **Busca nombres de países** en el texto (ej: "Mexico", "Brasil", "Universidad de Chile")
3. **Extrae el país** usando un diccionario de mapeo
4. **Filtra** solo trabajos con al menos una afiliación LATAM
5. **Determina país principal** por conteo de afiliaciones

### 📝 **Ejemplo**

```
Affiliation: "Universidad Nacional Autónoma de México, Mexico City"
           ↓
Detecta: "mexico" → Código: MX
           ↓
Incluye el trabajo como mexicano
```

## Uso

### **Modo Completo** (procesa todos los 3.4M de trabajos)

```bash
python data_collector_postgres_simple.py
```

**Advertencia**: Esto puede tomar **varias horas** porque:
- Procesa 3,404,980 trabajos en lotes de 10,000
- Para cada lote, extrae y analiza las afiliaciones
- Filtra por texto (no por índices)

### **Modo de Prueba** (solo primeros 100k trabajos)

```bash
python data_collector_postgres_simple.py --test
```

Útil para verificar que funciona antes de procesar todo.

## Salida

### **Archivo Generado**

`data/latin_american_works.parquet`

### **Columnas**

- `id`: OpenAlex ID del trabajo
- `doi`: DOI
- `title`: Título
- `display_name`: Nombre para mostrar
- `publication_year`: Año de publicación
- `publication_date`: Fecha de publicación
- `type`: Tipo de trabajo
- `cited_by_count`: Número de citas
- `is_retracted`: ¿Está retractado?
- `is_paratext`: ¿Es paratexto?
- `cited_by_api_url`: URL de la API de citas
- `abstract_inverted_index`: Índice invertido del abstract
- `language`: Idioma
- `authorships`: JSON con autores y afiliaciones
- `country_code`: **País principal detectado**
- `latam_countries`: **Lista de todos los países LATAM detectados**
- `download_date`: Fecha de descarga

## Limitaciones

### ⚠️ **Precisión Reducida**

Como usamos detección de texto en lugar de la tabla `institutions`:

- ✅ **Detecta** afiliaciones que mencionan el país explícitamente
- ❌ **Pierde** afiliaciones que solo mencionan la institución sin el país
- ❌ **Puede fallar** con abreviaturas o nombres en otros idiomas

**Ejemplo de lo que se pierde**:
```
"UNAM" → No detecta que es México (a menos que diga "UNAM, Mexico")
"USP" → No detecta que es Brasil
```

### 📊 **Estimación de Cobertura**

- **Mejor caso**: ~70-80% de trabajos LATAM reales
- **Peor caso**: ~50-60% de trabajos LATAM reales

Depende de qué tan completas sean las afiliaciones en el snapshot.

## Optimización

### **Crear Índice en Afiliaciones**

Para acelerar la búsqueda:

```sql
-- Índice en raw_affiliation_string para búsquedas de texto
CREATE INDEX idx_wa_affiliation 
ON openalex.works_authorships 
USING gin(to_tsvector('english', raw_affiliation_string));

-- Índice en work_id
CREATE INDEX idx_wa_work_id 
ON openalex.works_authorships(work_id);
```

Ejecutar:
```bash
psql -U postgres -d openalex -c "CREATE INDEX idx_wa_work_id ON openalex.works_authorships(work_id);"
```

## Progreso

El script muestra progreso cada lote:

```
Processing batch: 0 - 10,000
  → Found 234 LATAM works in this batch
  → Total LATAM works so far: 234

Processing batch: 10,000 - 20,000
  → Found 189 LATAM works in this batch
  → Total LATAM works so far: 423

💾 Saving intermediate results...
  ✓ Saved 423 works
```

Guarda automáticamente cada 50,000 trabajos procesados.

## Interrupción y Reanudación

### **Interrumpir**

Presiona `Ctrl+C` cuando veas "💾 Saving intermediate results..."

### **Reanudar**

El script **NO** tiene modo resume automático. Para evitar re-procesar:

1. **Opción A**: Renombra el archivo existente
   ```bash
   mv data/latin_american_works.parquet data/latin_american_works_backup.parquet
   ```

2. **Opción B**: Modifica el script para empezar en un offset específico
   ```python
   # En extract_latam_works(), cambiar:
   offset = 0  # ← Cambiar a 50000, 100000, etc.
   ```

## Alternativa: Cargar Más Datos

Si es posible, considera cargar estas tablas adicionales del snapshot:

### **Prioridad Alta**
1. `institutions` - Para detección precisa de países
2. `sources` - Para identificar journals
3. `works_primary_location` - Para relacionar works con sources

### **Prioridad Media**
4. `works_open_access` - Para métricas de OA
5. `works_concepts` - Para análisis temático

Con estas tablas, podrías usar el script completo `data_collector_postgres.py` que es mucho más preciso.

## Comparación de Scripts

| Script | Requiere | Precisión | Velocidad |
|--------|----------|-----------|-----------|
| `data_collector_postgres.py` | Todas las tablas | ✅ Alta (95%+) | ⚡ Rápida (con índices) |
| `data_collector_postgres_simple.py` | Solo works + authorships | ⚠️ Media (50-80%) | 🐌 Lenta (scan completo) |

## Próximos Pasos

Después de extraer los datos:

1. **Verificar resultados**:
   ```bash
   python diagnose_data.py
   ```

2. **Revisar muestra**:
   ```python
   import pandas as pd
   df = pd.read_parquet('data/latin_american_works.parquet')
   print(df[['display_name', 'country_code', 'publication_year']].head(20))
   ```

3. **Calcular métricas** (si tienes suficientes datos):
   ```bash
   python precompute_metrics_parallel_optimized.py
   ```

## Recomendación

Si tienes espacio en disco y ancho de banda, **carga las tablas faltantes** del snapshot de OpenAlex:

- `institutions` (~200 MB comprimido)
- `sources` (~50 MB comprimido)  
- `works_primary_location` (~500 MB comprimido)

Esto te permitirá usar el script completo con mucha mejor precisión.
