# Prueba Técnica — Data Engineer (Telecom)

**Candidato:** Leonardo Franco
**Email:** leonardofrancoch28@gmail.com
**Fecha de entrega:** 2026-06-18

---

## Estructura del repositorio

```
├── architecture/
│   ├── design_doc.md          # Punto 1 — Documento de arquitectura
│   └── diagram.md             # Punto 1 — Diagrama de capas (ASCII)
├── etl/
│   └── job.py                 # Punto 2 — Script PySpark standalone
├── notebook/
│   ├── analisys.ipynb         # Punto 3 — Notebook con queries SQL (SQLite)
│   └── Notebook_Colab_start.ipynb  # Versión inicial / setup Colab
├── data/
│   ├── raw/
│   │   ├── geo/               # Parquet de entrada — tabla geo
│   │   └── labels/            # Parquet de entrada — tabla labels
│   └── processed/
│       └── datalake.db        # SQLite generada por el notebook
└── docs/
    ├── Case_DataEngineer.docx.md
    └── Case_DataEngineer.spanish.md
```

---

## Punto 1 — Arquitectura

Archivos: `architecture/design_doc.md` y `architecture/diagram.md`

El documento cubre:
- Elección de cloud (AWS) con justificación vs GCP/Databricks/Snowflake
- Cuatro capas de almacenamiento: Raw → Staging → Analytics → Curated
- Herramientas por capa: S3 + Glue + Delta Lake + Athena + SageMaker
- Patrones de acceso diferenciados para Data Scientists y BI Analysts
- Orquestación con AWS Step Functions + SNS para alertas
- Control de acceso con Lake Formation (columnar/row-level)
- Estrategia de particionamiento por `date=` (raw) y `week=` (analytics/curated)

No requiere ejecución — es documentación.

---

## Punto 2 — Script PySpark ETL

Archivo: `etl/job.py`

### Requisitos

```bash
pip install pyspark
```

PySpark 3.3+ recomendado. Java 8 o 11 requerido (Spark dependency).

### Ejecución

```bash
python etl/job.py \
  --input_path data/raw \
  --output_path data/output
```

**O con spark-submit:**

```bash
spark-submit etl/job.py \
  --input_path data/raw \
  --output_path data/output
```

### Qué hace el pipeline (en orden)

| Paso | Función | Descripción |
|------|----------|-------------|
| 1 | `profilingTable()` | Row counts, schema, NULLs en ID/comuna/event/lat/lon, coordenadas fuera de rango UTM, IDs duplicados |
| 2 | `cleanAndDeduplicate()` | Geo: keep first por ID. Labels: agrega event por ID |
| 3 | `joinTables()` | Left join geo → labels (justificado por profiling: geo es tabla base) |
| 4–5 | `calculateProximity()` | Grid-cell bucketing (celda 50m) + Haversine. Evita cross-join O(n²) |
| 6 | `buildDsOutput()` / `buildBiOutput()` | Dos schemas de salida: feature-rich para DS, denormalizado para BI |
| 7 | `saveOutput()` | Parquet particionado por `week=YYYY-WXX` con historial semanal |

### Optimización espacial

Se usa **grid-cell bucketing** con celda de 50m:
- Cada cliente recibe una `cell_key` (x_cell, y_cell)
- Distancias se calculan solo entre clientes en la misma celda o celdas adyacentes (3×3 = 9 celdas)
- Elimina el producto cartesiano global; complejidad pasa de O(n²) a O(n × k) donde k = clientes por celda

### Salida generada

```
data/output/
├── ds/   # Schema para Data Scientists (todas las columnas + distancia_m)
└── bi/   # Schema para BI Analysts (agregados por comuna + conteos)
```

Ambas salidas en Parquet, particionadas por `week=YYYY-WXX`.

---

## Punto 3 — Notebook Analytics (SQLite)

Archivo: `notebook/analisys.ipynb`

### Ejecución local

```bash
pip install jupyter pandas sqlite3
jupyter notebook notebook/analisys.ipynb
```

### Ejecución en Google Colab

1. Subir el archivo `notebook/analisys.ipynb` a Colab
2. Subir `data/processed/datalake.db` o dejar que el notebook regenere la DB desde los parquet
3. Ejecutar celdas en orden

### Queries implementadas

| Query | Descripción |
|-------|-------------|
| Carga de tablas | Carga `labels` y resultado del Punto 2 en SQLite |
| NULL checks | Validación de campos clave post-carga |
| Detección de duplicados | Verifica deduplicación del Punto 2 |
| Outliers de coordenadas | Subquery con bounding box por comuna para detectar coords fuera de rango |
| RANK / DENSE_RANK | Top 20 comunas por eventos tipo 2 + share del total nacional |
| CTE tipo 1 | Avg/max/min lat/lon + count por comuna; filtra donde avg lat desvía >10% del promedio nacional |
| Agregación condicional | Conteo tipo 1, conteo tipo 2, ratio tipo_2/tipo_1 por comuna — sin JOINs |

---

## Decisiones técnicas clave

| Decisión | Elección | Razón |
|----------|----------|-------|
| Sistema de coordenadas | UTM (metros) | Los datos usan proyección UTM, no lat/lon decimal — Haversine no aplica directamente |
| Distancia | Euclidiana sobre coordenadas UTM | Válida en metros para distancias cortas (<50m); documentada en código |
| Join type | Left join (geo → labels) | Profiling mostró que geo tiene clientes sin eventos; labels nunca tiene IDs sin geo |
| Anti-explosión | Grid-cell bucketing 50m | Más simple que geohashing; suficiente para radio fijo de 50m |
| Particionamiento output | `week=YYYY-WXX` | Cumple requisito de historial semanal; compatible con Athena y Delta Lake |
| Dos schemas de salida | DS (feature-rich) / BI (denormalizado) | Patrones de acceso distintos; evita sobre-ingeniería en cada capa |

---

## Verificación rápida

```bash
# Confirmar que el ETL corre sin errores
python etl/job.py --input_path data/raw --output_path /tmp/test_output

# Confirmar salida generada
ls /tmp/test_output/ds/
ls /tmp/test_output/bi/

# Abrir notebook
jupyter notebook notebook/analisys.ipynb
```
