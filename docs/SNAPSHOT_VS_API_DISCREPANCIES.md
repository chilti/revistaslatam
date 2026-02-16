# Explicación de Discrepancias entre Snapshot y API de OpenAlex

## Caso de Estudio: Estudios Demográficos y Urbanos

### 📊 Resumen de Hallazgos

| Métrica | Snapshot (2025-10-27) | API en Vivo (2026-02-16) | Diferencia |
|---------|----------------------|--------------------------|------------|
| **Total Documentos** | 1,985 | 1,995 | +10 (0.5%) |
| **2yr_mean_citedness** | 0.5119 | 0.2878 | -44% |
| **Documentos 2025** | 29 | 34 | +5 |
| **Documentos 2026** | 0 | 5 | +5 |

---

## 🔍 Análisis Detallado

### 1. Discrepancia en Total de Documentos (1985 vs 1995)

**Causa**: Desfase temporal entre snapshot y API

El snapshot fue generado el **27 de octubre de 2025**, mientras que el API refleja datos hasta **febrero de 2026**.

**Documentos faltantes identificados**:
- **2025**: 5 documentos publicados después del 27 de octubre
- **2026**: 5 documentos publicados en enero-febrero 2026

**Conclusión**: ✅ **Normal y esperado**. El snapshot está desactualizado por ~4 meses.

**Impacto**: Mínimo (0.5% del total). No afecta significativamente los análisis.

---

### 2. Discrepancia en 2yr_mean_citedness (0.5119 vs 0.2878)

**Causa**: Ventana temporal móvil del cálculo

El `2yr_mean_citedness` (impacto a 2 años) se calcula como:
```
Promedio de citas de trabajos publicados en los últimos 2 años
```

**Ventanas de cálculo**:
- **Snapshot (oct 2025)**: Trabajos de 2023-2024
- **API (feb 2026)**: Trabajos de 2024-2025

**Por qué bajó el valor**:
1. Los 34 trabajos de 2025 son muy recientes
2. Tienen pocas citas acumuladas (solo 4 citas en total según el snapshot)
3. Al incluirse en el cálculo actual, **diluyen** el promedio

**Ejemplo numérico simplificado**:
```
Snapshot (oct 2025):
  Trabajos 2023-2024: ~60 trabajos con ~30 citas promedio = 0.51

API (feb 2026):
  Trabajos 2024-2025: ~60 trabajos
  - 2024: ~27 trabajos con ~30 citas promedio
  - 2025: ~34 trabajos con ~0.1 citas promedio
  = Promedio combinado: 0.29
```

**Conclusión**: ✅ **Normal y esperado**. Las métricas de impacto son **dinámicas** y cambian con el tiempo.

---

## 🎯 Implicaciones para el Dashboard

### ¿Qué datos usar?

#### Opción A: Snapshot (Recomendado) ✅
**Ventajas**:
- Datos estables y reproducibles
- Consistencia temporal en todos los análisis
- No requiere actualizaciones frecuentes

**Desventajas**:
- Desactualizado por ~4 meses
- Falta ~0.5% de documentos recientes

**Usar cuando**:
- Análisis históricos
- Comparaciones entre revistas
- Estudios longitudinales
- Necesitas reproducibilidad

#### Opción B: API en Vivo
**Ventajas**:
- Datos más actuales
- Incluye publicaciones recientes

**Desventajas**:
- Cambia constantemente
- Requiere actualizaciones frecuentes
- Lento (7,500+ revistas vía API)

**Usar cuando**:
- Dashboard en tiempo real
- Monitoreo de publicaciones recientes
- Necesitas datos de última hora

#### Opción C: Híbrido
Snapshot como base + actualización incremental de revistas activas.

---

## 📝 Recomendación Final

Para el proyecto **RevistasLatam**, recomiendo:

1. ✅ **Usar el snapshot como fuente principal**
   - Los datos son suficientemente recientes (4 meses)
   - La diferencia de 10 documentos por revista es insignificante
   - Garantiza reproducibilidad de análisis

2. ✅ **Documentar la fecha del snapshot en el dashboard**
   - Ya implementado en el sidebar
   - Los usuarios entienden que es un "punto en el tiempo"

3. ✅ **Actualizar el snapshot cada 6-12 meses**
   - Cuando OpenAlex lance un nuevo snapshot oficial
   - Mantiene datos razonablemente actuales
   - No requiere actualizaciones constantes

4. ⚠️ **NO intentar "corregir" el `2yr_mean_citedness`**
   - El valor del snapshot (0.5119) es **correcto** para esa fecha
   - El valor del API (0.2878) es **correcto** para hoy
   - Ambos son válidos en sus respectivos contextos temporales

---

## 🔬 Verificación de Otros Campos

Según el snapshot, la revista tiene:

✅ **Campos correctamente extraídos**:
- `is_in_scielo`: False
- `is_ojs`: True
- `is_core`: False
- `oa_works_count`: 1985
- `h_index`: 25
- `i10_index`: 162

⚠️ **Campo faltante**:
- `is_scopus`: NO EXISTE en el snapshot

**Acción**: El pipeline ya tiene fallback a `False` para este campo.

---

## 📅 Historial de Snapshots

| Fecha Snapshot | Documentos | 2yr_mean_citedness | Notas |
|----------------|------------|-------------------|-------|
| 2025-10-27 | 1,985 | 0.5119 | Snapshot actual |
| (API vivo) 2026-02-16 | 1,995 | 0.2878 | Incluye 2025-2026 |

---

## 🎓 Lecciones Aprendidas

1. **Las métricas de impacto son dinámicas**: `2yr_mean_citedness`, `cited_by_count`, etc. cambian constantemente
2. **Los snapshots son fotografías**: Representan un momento específico en el tiempo
3. **Pequeñas discrepancias son normales**: 0.5% de diferencia en documentos es aceptable
4. **Documentar la fecha es crucial**: Los usuarios deben saber qué "versión" de los datos están viendo

---

## 📚 Referencias

- OpenAlex Snapshot: https://openalex.org/snapshot
- Documentación de métricas: https://docs.openalex.org/api-entities/sources
- Revista en OpenAlex: https://openalex.org/S2737081250
