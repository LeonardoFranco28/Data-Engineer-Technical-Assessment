"""
ETL Standalone Job - Customer Proximity pipeline
Technical test for data engineer position at lla.
Telecom take-home Assessment

Usage:
    python job.py --input_path <input_path> --output_path <output_path>

Where:
    <input_path> es la ruta a los datos de entrada (ej: "data/raw/")
    <output_path> es la ruta donde se guardarán los resultados (ej: "data/processed/")
"""


import argparse
import logging
from datetime import datetime, timedelta
from typing import List

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("etl.proximity")


# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

# Rango válido de coordenadas UTM para Chile (zona 18-19S, en metros)
# "latitud" en los datos = Easting (eje X), "longitud" = Northing (eje Y)
# Los nombres de columna son engañosos pero los rangos son correctos
coordinates = {
    "east": {"min": 100_000, "max": 900_000},
    "nort": {"min": 4_500_000, "max": 9_500_000},
}

# Tamaño de celda para la grilla espacial (50m = radio de búsqueda)
cellSize = 50


# ---------------------------------------------------------------------------
# Configuración de Spark
# ---------------------------------------------------------------------------

sparkConfig = {
    "spark.app.name": "Customer Proximity ETL Job",
    "spark.master": "local[*]",
    # AQE: Spark ajusta particiones y joins automáticamente en tiempo de ejecución
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    # Particiones del shuffle — AQE las ajusta según el volumen real
    "spark.sql.shuffle.partitions": "200",
    # Snappy: buen balance velocidad vs compresión para parquet
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.sql.session.timeZone": "UTC",
    # Kryo: serialización 10x más rápida que Java (menos CPU en shuffles)
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    # Umbral para broadcast join: tablas menores a 50MB se transmiten a todos los nodos
    "spark.sql.autoBroadcastJoinThreshold": "52428800",  # 50MB
}


def generateSparkSession(config: dict) -> SparkSession:
    """Crea una SparkSession con la configuración dada."""
    try:
        builder = SparkSession.builder
        for key, value in config.items():
            builder = builder.config(key, value)
        return builder.getOrCreate()
    except Exception as e:
        logger.error("Error al crear SparkSession: %s", e)
        raise


# ---------------------------------------------------------------------------
# Step 1 — Perfilamiento de datos
# ---------------------------------------------------------------------------

def profilingTable(
    df: DataFrame,
    name: str = "DataFrame",
    tableType: str = "generic",
    Id: str = "ID",
    columns: List = None,
) -> dict:
    """
    Inspecciona el DataFrame y reporta métricas de calidad.
    No modifica los datos — solo lectura.

    Truco de performance: todas las métricas se calculan en UNA sola pasada
    del DataFrame (un solo .agg() con todas las expresiones juntas).
    Si se calcularan por separado serían N scans al disco — mucho más lento.

    Retorna un dict con los hallazgos para que pasos siguientes puedan
    tomar decisiones (ej: qué tipo de join usar según el overlap de IDs).
    """
    columns = columns or list(df.columns)
    # MEMORY_AND_DISK: si el dataset no cabe en RAM, hace spill a disco
    # en vez de desalojar el bloque — garantiza que las acciones siguientes
    # no re-lean el parquet desde el origen
    df.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        total      = df.count()                       # acción 1 — materializa cache
        distinct   = df.select(Id).distinct().count() # acción 2 — lee de RAM
        duplicates = total - distinct

        # NULLs por columna — una expresión por cada columna, todo en 1 sola acción
        aggExprs = [F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}") for c in columns]

        # Para geo: rango de coordenadas y conteo de valores fuera de rango UTM
        if tableType == "geo" and "latitud" in columns and "longitud" in columns:
            aggExprs += [
                F.min("latitud").alias("lat_min"),
                F.max("latitud").alias("lat_max"),
                F.min("longitud").alias("lon_min"),
                F.max("longitud").alias("lon_max"),
                F.sum(
                    (
                        (F.col("latitud")  < 100_000) | (F.col("latitud")  > 900_000) |
                        (F.col("longitud") < 4_500_000) | (F.col("longitud") > 9_500_000)
                    ).cast("int")
                ).alias("out_of_range"),
            ]

        # Para labels: conteo directo de cada tipo de evento.
        # Se usa suma condicional en vez de groupBy porque ya sabemos los valores
        # posibles (1 y 2) — evita un shuffle costoso innecesario.
        if tableType == "labels" and "event" in columns:
            aggExprs += [
                F.sum((F.col("event") == 1).cast("int")).alias("event_type1"),
                F.sum((F.col("event") == 2).cast("int")).alias("event_type2"),
                F.sum(F.col("event").isNull().cast("int")).alias("event_nulls"),
            ]

        stats = df.agg(*aggExprs).collect()[0].asDict()
        nulls = {k.replace("null_", ""): v for k, v in stats.items() if k.startswith("null_")}

        logger.info("=== PERFIL: %s ===", name)
        logger.info("Filas totales  : %s", f"{total:,}")
        logger.info("IDs duplicados : %s", f"{duplicates:,}")
        logger.info("NULLs          : %s", nulls)
        logger.info("Schema         :\n%s", df.schema.simpleString())

        findings = {
            "table": name,
            "total": total,
            "duplicates": duplicates,
            "nulls": nulls,
        }

        if tableType == "geo" and "latitud" in columns and "longitud" in columns:
            logger.info("Rango East     : [%.2f, %.2f]", stats["lat_min"], stats["lat_max"])
            logger.info("Rango North    : [%.2f, %.2f]", stats["lon_min"], stats["lon_max"])
            logger.info("Fuera de rango : %s", f"{stats['out_of_range']:,}")
            findings["coord_range"] = {
                "lat_min": stats["lat_min"], "lat_max": stats["lat_max"],
                "lon_min": stats["lon_min"], "lon_max": stats["lon_max"],
            }
            findings["out_of_range_coords"] = stats["out_of_range"]

        if tableType == "labels" and "event" in columns:
            dist = {
                "type 1": stats["event_type1"],
                "type 2": stats["event_type2"],
                "null":   stats["event_nulls"],
            }
            logger.info("Dist. eventos  : %s", dist)
            findings["event_distribution"] = dist

        # ── RESUMEN DE HALLAZGOS (muestra de 500K filas) ───────────────────────
        # Geo  : 611.959 filas | 10.250 IDs duplicados (1.7%) | 0 NULLs
        #        Rango East [129.897 – 34.405.839]: 1 outlier extremo fuera de UTM.
        #        cleanAndDeduplicate filtra ese outlier antes del join.
        # Labels: 84.435 filas | 8 duplicados | 2 NULLs en ID
        #        Evento es INTEGER (1 o 2). Mayoría type 2 (82%).
        # Overlap: solo ~2.9% de IDs de geo existen en labels → inner join
        #          reduce el volumen antes del costoso paso de proximidad.
        # ──────────────────────────────────────────────────────────────────────

        return findings

    except Exception as e:
        logger.warning("Perfilamiento de '%s' falló, se continúa sin métricas: %s", name, e)
        return {"table": name, "total": 0, "duplicates": 0, "nulls": {}}
    finally:
        df.unpersist()


# ---------------------------------------------------------------------------
# Step 2 — Limpieza y deduplicación
# ---------------------------------------------------------------------------

def cleanAndDeduplicate(tableType: str, df: DataFrame) -> DataFrame:
    """
    Limpia y deduplica los datos según el tipo de tabla.

    GEO — dropDuplicates(["ID"]):
        Mantiene la primera ocurrencia de cada ID.
        "First" es correcto porque: no hay timestamp para elegir "más reciente",
        los duplicados son el mismo evento de captura repetido (no variaciones),
        y no hay métrica que justifique elegir el más frecuente (ID es PK).
        Renombra latitud→east y longitud→north para reflejar el sistema UTM real.

    LABELS — collect_set:
        Un cliente puede tener type 1 Y type 2 al mismo tiempo.
        Usar "first" descartaría uno de los dos tipos.
        collect_set preserva todos los eventos distintos en un array.
    """
    if tableType == "geo":
        # Filtra coordenadas inválidas ANTES de dedup para no contaminar
        # el join ni el cálculo de distancias con valores basura
        return (
            df.filter(
                (F.col("latitud").between(coordinates["east"]["min"], coordinates["east"]["max"])) &
                (F.col("longitud").between(coordinates["nort"]["min"], coordinates["nort"]["max"]))
            )
            .dropna(subset=["ID"])
            .dropDuplicates(["ID"])
            .withColumnRenamed("latitud", "east")
            .withColumnRenamed("longitud", "north")
        )

    elif tableType == "labels":
        # Elimina filas sin ID o sin evento — no son útiles para el join ni el análisis
        return (
            df.dropna(subset=["ID", "event"])
              .groupBy("ID")
              .agg(F.collect_set("event").alias("events"))
        )

    return df.dropna(subset=["ID"]).dropDuplicates(["ID"])


# ---------------------------------------------------------------------------
# Step 3 — Join
# ---------------------------------------------------------------------------

def joinTables(geo: DataFrame, labels: DataFrame) -> DataFrame:
    """
    INNER JOIN entre geo y labels por ID.

    Por qué inner: el análisis de proximidad requiere que el cliente tenga
    AMBAS cosas — coordenadas (geo) Y tipo de evento (labels). Sin coordenadas
    no se puede ubicar al cliente en el mapa. Sin evento no hay nada que analizar.

    El inner join reduce el volumen de datos antes del paso más costoso
    (cálculo de distancias): del total de geo, solo ~2.9% tiene label → se
    descarta el 97% que no sirve para la proximidad.

    F.broadcast(labels): labels post-dedup pesa ~2MB. Se transmite a todos
    los nodos en vez de hacer shuffle — elimina la etapa de shuffle del join.

    Si se necesitara conservar clientes sin label, usar LEFT JOIN y marcar
    el campo evento como NULL en la salida.
    """
    return geo.join(F.broadcast(labels), on="ID", how="inner")


# ---------------------------------------------------------------------------
# Step 4 & 5 — Cálculo de proximidad con grilla espacial (grid-cell bucketing)
# ---------------------------------------------------------------------------

"""
ESTRATEGIA DE OPTIMIZACIÓN: Grid-Cell Bucketing
================================================
(1) Estrategia elegida:
    Se divide el espacio UTM en celdas de 50m x 50m.
    Cada cliente recibe un índice (cell_x, cell_y) según su posición.
    Dos clientes solo pueden estar a ≤50m si comparten celda o están
    en celdas adyacentes. Se hace self-join solo sobre la vecindad 3x3
    (9 pares de celdas por cliente) en vez de todos los N×N pares.

(2) Por qué es apropiada:
    Las coordenadas ya están en UTM (metros), así que cell_size=50 es exacto.
    La grilla reduce combinaciones de O(N²) a O(N × densidad_promedio_celda).
    Para datos uniformes, densidad ≈ N/total_celdas → complejidad casi lineal.
    Spark particiona naturalmente por (cell_x, cell_y) → join local sin shuffle.

(3) Trade-offs vs alternativas:
    - Geohashing: más portable entre proyecciones, pero requiere dependencia
      extra y afinar la precisión; la enumeración de vecinos es menos intuitiva
      que aritmética ±1 sobre enteros.
    - Apache Sedona (R-tree): óptimo para queries espaciales complejas, pero
      agrega una librería pesada y requiere setup de cluster. Overkill para
      un filtro de radio puntual sobre un solo dataset.
    - Cross-join naïve: O(N²) — 500K filas = 250B pares, inviable.
"""


def addCellKeys(df: DataFrame) -> DataFrame:
    """Asigna índice de celda (cell_x, cell_y) a cada cliente según su posición UTM."""
    return (
        df
        .withColumn("cell_x", (F.col("east")  / cellSize).cast("long"))
        .withColumn("cell_y", (F.col("north") / cellSize).cast("long"))
    )


def calculateProximity(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Encuentra todos los pares de clientes a 50m o menos usando grilla espacial.

    Pasos:
      1. Proyección temprana: solo columnas necesarias para reducir datos en memoria
      2. Asignar celda UTM a cada cliente y repartir por (cell_x, cell_y)
         → clientes de la misma celda quedan en la misma partición Spark
      3. Expandir cada cliente a sus 9 celdas vecinas (3x3 alrededor)
      4. Self-join: cada cliente A busca vecinos B que vivan en alguna de sus 9 celdas
         La condición a.ID < b.ID evita duplicar el par (A,B) y (B,A)
      5. Filtrar por distancia²≤2500 (= 50²) antes de calcular sqrt
         → sqrt es costoso; se aplica solo al resultado final
      6. Seleccionar y renombrar columnas para resolver ambigüedad del self-join
    """
    # Particiones dinámicas: 3x el paralelismo disponible del cluster
    numPartitions = spark.sparkContext.defaultParallelism * 3

    # Paso 1-2: columnas mínimas + índice de celda + repartición por celda
    dfCells = (
        addCellKeys(df.select("ID", "east", "north", "comuna", "events"))
        .repartition(numPartitions, "cell_x", "cell_y")
    )

    # dfCells se usa dos veces: lado A (expanded) y lado B (dfCells.alias("b")).
    # persist() lo materializa en RAM (con spill a disco si no cabe)
    # para que el self-join no recalcule desde el parquet origen en cada lado.
    dfCells.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        dfCells.count()  # fuerza materialización antes de expandir

        # Paso 3: expandir cada fila a 9 filas — una por cada celda vecina
        # F.explode(F.array([structs])) genera un plan plano (sin 9 unions separadas)
        offsets = [(dx, dy) for dx in (-1, 0, 1) for dy in (-1, 0, 1)]
        expanded = (
            dfCells.select(
                "*",
                F.explode(F.array([
                    F.struct(
                        (F.col("cell_x") + dx).alias("cell_x"),
                        (F.col("cell_y") + dy).alias("cell_y"),
                    )
                    for dx, dy in offsets
                ])).alias("neighbor"),
            )
            .select(
                "*",
                F.col("neighbor.cell_x").alias("neighbor_cell_x"),
                F.col("neighbor.cell_y").alias("neighbor_cell_y"),
            )
            .drop("neighbor")
        )

        # Paso 4: join equi-join por celda — local porque dfCells ya está particionado
        joined = expanded.alias("a").join(
            dfCells.alias("b"),
            (F.col("a.neighbor_cell_x") == F.col("b.cell_x")) &
            (F.col("a.neighbor_cell_y") == F.col("b.cell_y")) &
            (F.col("a.ID") < F.col("b.ID")),  # cada par solo una vez
            how="inner",
        )

        # Pasos 5-6: filtrar por distancia² y resolver nombres ambiguos del self-join
        result = (
            joined
            .withColumn(
                "distance_sq",
                F.pow(F.col("a.east") - F.col("b.east"), 2) +
                F.pow(F.col("a.north") - F.col("b.north"), 2),
            )
            .filter(F.col("distance_sq") <= 2500)  # 50² = 2500, sin sqrt todavía
            .withColumn("distance_m", F.sqrt(F.col("distance_sq")))
            .select(
                F.col("a.ID").alias("customer_id"),
                F.col("a.east").alias("east_m"),
                F.col("a.north").alias("north_m"),
                F.col("a.comuna").alias("comuna"),
                F.col("a.events").alias("customer_events"),
                F.col("b.ID").alias("nearby_customer_id"),
                F.col("b.events").alias("nearby_events"),
                F.col("distance_m"),
            )
        )

        return result

    except Exception as e:
        logger.error("Error en cálculo de proximidad: %s", e)
        raise
    finally:
        dfCells.unpersist()


# ---------------------------------------------------------------------------
# Step 6 — Esquemas de salida (DS vs BI)
# ---------------------------------------------------------------------------


def buildDsOutput(proximityDf: DataFrame, processing_date: str, week_start: str) -> DataFrame:
    """
    Salida para Data Scientists — 1 fila por PAR de clientes cercanos.

    Se preservan los arrays y coordenadas raw sin agregar para que el DS
    pueda hacer sus propias transformaciones: feature engineering, clustering
    espacial, análisis de densidad, modelos de segmentación, etc.

    Schema de salida:
        customer_id        string  — ID del cliente A
        east_m             double  — coordenada UTM Easting en metros
        north_m            double  — coordenada UTM Northing en metros
        comuna             string  — comuna del cliente A
        customer_events    array<int>  — tipos de evento de A ([1], [2] o [1,2])
        nearby_customer_id string  — ID del cliente B (a ≤50m de A)
        nearby_events      array<int>  — tipos de evento de B
        distance_m         double  — distancia Euclidean en metros
        processing_date    string  — fecha de ejecución del job
        week_start         string  — lunes de la semana de procesamiento
    """
    return (
        proximityDf
        .withColumn("processing_date", F.lit(processing_date))
        .withColumn("week_start", F.lit(week_start))
    )


def buildBiOutput(proximityDf: DataFrame, processing_date: str, week_start: str) -> DataFrame:
    """
    Salida para BI Analysts — 1 fila por (cliente, tipo_evento), pre-agregada.

    El array customer_events se explota para obtener una fila por tipo de evento.
    Sin arrays ni structs anidados — SQL puro compatible con Athena / BigQuery.

    Schema de salida:
        customer_id       string  — ID del cliente
        comuna            string  — comuna
        event_type        int     — 1 o 2 (explotado del array customer_events)
        nearby_count      long    — cantidad de clientes dentro de 50m
        avg_distance_m    double  — distancia promedio a los vecinos
        min_distance_m    double  — distancia al vecino más cercano
        processing_date   string
        week_start        string
    """
    return (
        proximityDf
        .select(
            "customer_id",
            "comuna",
            F.explode("customer_events").alias("event_type"),
            "distance_m",
        )
        .groupBy("customer_id", "comuna", "event_type")
        .agg(
            F.count("*").alias("nearby_count"),
            F.round(F.avg("distance_m"), 2).alias("avg_distance_m"),
            F.round(F.min("distance_m"), 2).alias("min_distance_m"),
        )
        .withColumn("processing_date", F.lit(processing_date))
        .withColumn("week_start", F.lit(week_start))
    )


# ---------------------------------------------------------------------------
# Step 7 — Guardar con historial semanal
# ---------------------------------------------------------------------------


def saveOutput(df: DataFrame, path: str, label: str, spark: SparkSession) -> None:
    """
    Escribe parquet particionado por week_start / processing_date.

    Partición doble:
        week_start      → agrupa los 7 días de cada semana en un directorio.
                          Facilita queries del tipo "dame toda la semana N".
        processing_date → permite re-correr el job del mismo día sin borrar
                          los otros días de la misma semana.

    Ejemplo de estructura en disco:
        output/ds/
          week_start=2026-06-09/
            processing_date=2026-06-10/  ← hoy (se sobreescribe si re-corre)
            processing_date=2026-06-11/  ← mañana (queda intacto)

    Por qué "dynamic": sin esta config, mode("overwrite") borra TODA la carpeta
    de salida al escribir, destruyendo el historial de semanas anteriores.
    Con "dynamic", Spark sobreescribe solo la partición del día actual.
    """
    try:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        (
            df.write
            .mode("overwrite")
            .partitionBy("week_start", "processing_date")
            .parquet(path)
        )
        logger.info("%s → %s", label, path)
    except Exception as e:
        logger.error("Error al guardar '%s' en %s: %s", label, path, e)
        raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="ETL Job — Customer Proximity Pipeline")
    parser.add_argument("--input_path",  required=True, help="Ruta a los datos de entrada")
    parser.add_argument("--output_path", required=True, help="Ruta donde se guardan los resultados")
    args = parser.parse_args()

    logger.info("Input  : %s", args.input_path)
    logger.info("Output : %s", args.output_path)

    spark = generateSparkSession(sparkConfig)
    try:
        spark.sparkContext.setLogLevel("ERROR")

        # Lectura de datos
        try:
            geo    = spark.read.parquet(args.input_path + "/geo/")
            labels = spark.read.parquet(args.input_path + "/labels/")
        except Exception as e:
            logger.error("Error al leer datos de entrada desde '%s': %s", args.input_path, e)
            raise

        # Columnas a perfilar por tabla
        geoColumns   = ["ID", "latitud", "longitud", "comuna"]
        labelColumns = ["ID", "event"]

        # Step 1 — Perfilamiento
        geoFindings    = profilingTable(geo,    "Geo Data",    tableType="geo",    Id="ID", columns=geoColumns)
        labelsFindings = profilingTable(labels, "Labels Data", tableType="labels", Id="ID", columns=labelColumns)

        # Step 2 — Limpieza y deduplicación
        geo    = cleanAndDeduplicate("geo",    geo)
        labels = cleanAndDeduplicate("labels", labels)

        # Step 3 — Join
        dataset = joinTables(geo, labels)

        # Steps 4 & 5 — Proximidad
        dfProximity = calculateProximity(dataset, spark)

        # Cache del resultado de proximidad: se usa dos veces (DS y BI).
        # Sin cache, Spark recalcularía todo el pipeline de proximidad dos veces.
        dfProximity.cache()
        try:
            dfProximity.count()

            # Step 6 — Construir schemas de salida
            processingDate = datetime.now().strftime("%Y-%m-%d")
            weekStart      = (datetime.now() - timedelta(days=datetime.now().weekday())).strftime("%Y-%m-%d")

            dsOutput = buildDsOutput(dfProximity, processingDate, weekStart)
            biOutput = buildBiOutput(dfProximity, processingDate, weekStart)

            dsOutput.show(5)
            biOutput.show(5)

            # Step 7 — Guardar con historial semanal
            saveOutput(dsOutput, args.output_path + "/ds/", "DS output", spark)
            saveOutput(biOutput, args.output_path + "/bi/", "BI output", spark)

        finally:
            dfProximity.unpersist()

    except Exception as e:
        logger.error("Pipeline falló: %s", e)
        raise
    finally:
        spark.stop()
        print("ETL Job finalizado.")


if __name__ == "__main__":
    main()
