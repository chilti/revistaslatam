# Correcciones al Script de Descarga de Datos

## Problemas Identificados y Solucionados

### 🐛 **Problema 1: Journals se descargaban en cada ejecución**

**Síntoma**: Cada vez que ejecutabas el script, descargaba todos los journals de nuevo, aunque ya existieran.

**Causa**: El script no verificaba si `latin_american_journals.parquet` ya existía.

**Solución**: 
- Agregado parámetro `update_journals=False` (por defecto)
- El script ahora **carga** los journals existentes en lugar de descargarlos de nuevo
- Solo descarga journals si:
  - El archivo no existe
  - O se pasa `update_journals=True` explícitamente

```python
# Antes (SIEMPRE descargaba):
for country in LATAM_COUNTRIES:
    country_journals = fetch_journals_by_country(country)
    all_journals.extend(country_journals)

# Ahora (solo si es necesario):
if update_journals or not os.path.exists(PARQUET_FILE):
    # Descargar journals...
else:
    # Cargar journals existentes
    df = pd.read_parquet(PARQUET_FILE)
    all_journals = df.to_dict('records')
```

---

### 🐛 **Problema 2: Contador de progreso incorrecto**

**Síntoma**: El script mostraba progreso incorrecto, como "Processing 50/1000" cuando solo había 50 journals pendientes.

**Causa**: Usaba `len(all_journals)` en lugar de `len(journals_to_process)`.

**Líneas corregidas**:
- Línea 196: `if idx % batch_size == 0 or idx == len(journals_to_process):`
- Línea 310: `print(f"\n--- Progress: {idx}/{len(journals_to_process)} journals processed ---\n")`

**Impacto**: Esto podría haber causado que el script pensara que había más trabajo por hacer.

---

### 🐛 **Problema 3: Posibles re-descargas**

**Causa potencial**: Si el contador estaba mal, el script podría haber intentado procesar journals ya descargados.

**Solución**: Con los contadores corregidos, el script ahora:
1. Identifica correctamente cuántos journals faltan
2. Procesa solo los faltantes
3. Muestra progreso preciso

---

## Cómo Usar el Script Correctamente

### **Uso Normal (Recomendado)**

```bash
python src/data_collector.py
```

**Comportamiento**:
- ✅ Carga journals existentes (NO descarga de nuevo)
- ✅ Descarga solo works de journals faltantes
- ✅ Modo resume automático

---

### **Forzar Actualización de Journals**

Si quieres actualizar la metadata de journals (por ejemplo, si OpenAlex agregó nuevos campos):

```python
from src.data_collector import update_data

# Actualizar journals Y works
update_data(include_works=True, resume=True, update_journals=True)
```

---

### **Solo Actualizar Journals (sin works)**

```python
from src.data_collector import update_data

# Solo actualizar journals, no descargar works
update_data(include_works=False, update_journals=True)
```

---

## Script de Diagnóstico

He creado `diagnose_data.py` para verificar el estado actual:

```bash
python diagnose_data.py
```

**Información que muestra**:
1. ✅ Total de journals y distribución por país
2. ✅ Total de works descargados
3. ✅ Journals con/sin works
4. ✅ Revistas faltantes por país
5. ✅ Detección de duplicados
6. ✅ Tamaño de archivos

---

## Diagnóstico del Problema Actual

### **Posibles Causas de Re-descarga de Brasil**

1. **Journals duplicados**: Si el archivo de journals tiene duplicados, el script intentará descargar works para cada duplicado.

2. **Works duplicados**: Si hay works duplicados en el archivo, parecerá que hay más works de los que realmente hay.

3. **Contador incorrecto**: El bug del contador podría haber hecho que el script pensara que faltaban journals por procesar.

### **Pasos para Diagnosticar**

1. **Ejecuta el script de diagnóstico**:
   ```bash
   python diagnose_data.py
   ```

2. **Verifica**:
   - ¿Cuántos journals hay en total?
   - ¿Cuántos journals tienen works descargados?
   - ¿Hay duplicados en works?
   - ¿Qué journals faltan por país?

3. **Comparte el output** para que pueda ver exactamente qué está pasando.

---

## Solución de Problemas

### **Si hay duplicados en works**

```python
import pandas as pd

# Leer works
works_df = pd.read_parquet('data/latin_american_works.parquet')

# Eliminar duplicados por 'id'
works_df_clean = works_df.drop_duplicates(subset=['id'], keep='first')

# Guardar limpio
works_df_clean.to_parquet('data/latin_american_works.parquet', index=False)

print(f"Eliminados {len(works_df) - len(works_df_clean)} duplicados")
```

### **Si hay journals duplicados**

```python
import pandas as pd

# Leer journals
journals_df = pd.read_parquet('data/latin_american_journals.parquet')

# Eliminar duplicados por 'id'
journals_df_clean = journals_df.drop_duplicates(subset=['id'], keep='first')

# Guardar limpio
journals_df_clean.to_parquet('data/latin_american_journals.parquet', index=False)

print(f"Eliminados {len(journals_df) - len(journals_df_clean)} duplicados")
```

### **Si quieres empezar de cero solo con works**

```bash
# Respaldar
mv data/latin_american_works.parquet data/latin_american_works.parquet.backup

# Ejecutar script (mantendrá journals existentes)
python src/data_collector.py
```

---

## Mejoras Implementadas

### **1. Eficiencia**
- ✅ No descarga journals innecesariamente
- ✅ Ahorra tiempo y llamadas a la API
- ✅ Reduce riesgo de exceder límites de API

### **2. Precisión**
- ✅ Contadores de progreso correctos
- ✅ Mensajes claros sobre qué se está haciendo
- ✅ Mejor logging

### **3. Control**
- ✅ Parámetro `update_journals` para control explícito
- ✅ Modo resume funciona correctamente
- ✅ Fácil de diagnosticar problemas

---

## Ejemplo de Salida Esperada

### **Primera Ejecución (sin datos)**
```
Starting data update from OpenAlex...

============================================================
DOWNLOADING JOURNAL METADATA
============================================================
Fetching journals for MX...
Found 150 journals for MX.
Fetching journals for BR...
Found 450 journals for BR.
...
Saving 1200 journal records to data/latin_american_journals.parquet...
Journal data update complete.

============================================================
Starting Works (articles) download...
This will download articles for 1200 journals.
Will process 1200 journals.
============================================================
```

### **Ejecución Posterior (con journals existentes)**
```
Starting data update from OpenAlex...

============================================================
LOADING EXISTING JOURNAL METADATA
============================================================
Using existing journal file: data/latin_american_journals.parquet
Loaded 1200 journals from cache.

============================================================
Starting Works (articles) download...
This will download articles for 1200 journals.
Found 800 journals already downloaded.
RESUME MODE: Skipping 800 already downloaded journals.
Will process 400 journals.
============================================================
```

---

## Recomendaciones

1. **Ejecuta el diagnóstico primero**:
   ```bash
   python diagnose_data.py
   ```

2. **Revisa el output** para entender el estado actual

3. **Si hay duplicados**, límpialos con los scripts de arriba

4. **Continúa la descarga** con el script corregido:
   ```bash
   python src/data_collector.py
   ```

5. **Monitorea el progreso** - ahora debería ser preciso

---

## Contacto

Si después de ejecutar el diagnóstico sigues viendo comportamiento extraño, comparte:
- Output de `diagnose_data.py`
- Últimas líneas del log cuando ejecutas `data_collector.py`
- Cuántos journals/works esperas vs cuántos tienes

Esto me ayudará a identificar si hay algún otro problema.
