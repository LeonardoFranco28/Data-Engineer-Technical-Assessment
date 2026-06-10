"""
ETL Standalone Job - Customer Proximity pipeline
Technical test for data engineer position at lla.
Telecom take-home Assessment

Usage:
    python job.py --input_path <input_path> --output_path <output_path>

Where:
    <input_path> is the path to the input data (e.g., "data/raw/")
    <output_path> is the path where the output data will be saved (e.g., "data/processed/")

"""


# Imports


import argparse
from datetime import datetime, timedelta
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Constants

coordinates = {
        "east": {"min": 100_000, "max": 900_000},
        "nort": {"min": 4_500_000, "max": 9_500_000}
}


cellSize = 50  # metres — matches the 50m search radius exactly


# Functions
def generateSparkSession(config):
    """
    Generate a SparkSession with the given configuration.

    Args:
        config (dict): A dictionary containing Spark configuration parameters.

    Returns:
        SparkSession: A configured SparkSession object.
    """
    builder = SparkSession.builder
    for key, value in config.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


# ---------------------------------------------------------------------------
# Step 1 — Data Profiling
# ---------------------------------------------------------------------------

def profilingTable(df: DataFrame, name: str = "DataFrame", tableType: str = "generic",Id: str = "ID", columns: List = None) -> dict:
    """
    Perfilamiento de datos — solo lectura, no muta el DataFrame.
    Retorna dict con hallazgos para uso downstream (ej: decidir tipo de join).

    Enfoque de performance:
    ────────────────────────
    Se usa UNA sola expresión de agregación (aggExprs) para calcular:
      - NULLs por columna
      - Rango de coordenadas (min/max)
      - Out-of-range count
      - Distribución de eventos
    Esto se ejecuta en 1 sola acción de Spark (1 solo scan del DataFrame).
    Alternativa: hacer cada cálculo por separado = N acciones = N scans del disco.
    Con millones de filas, la diferencia es de minutos vs segundos.

    Args:
        df (DataFrame): Input DataFrame a perfilar.
        name (str): Nombre para logging.
        tableType (str): "geo" | "labels" | "generic". Controla checks adicionales.
        Id (str): Nombre de la columna que representa el ID.
        columns (List): Columnas a perfilar. Si es None, usa todas.

    Returns:
        dict: Hallazgos del perfilamiento.
    """
    #fallback

    columns = columns or list(df.columns)

    # Optimización: cache para evitar múltiples scans en acciones siguientes

    df.cache()
    total      = df.count()                       # acción 1 — materializa cache
    distinct   = df.select(Id).distinct().count() # acción 2 — lee de RAM
    duplicates = total - distinct                 # aritmética Python, sin Spark

    # 1. NULLs por columna (1 sola accion Spark)
    aggExprs = [F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}") for c in columns]

   # 2. Validacion de coordenadas UTM — solo para geo
   # los datos no son geográficos reales, son UTM pero los labels estan mal asignados deberia ser East & Nort

    if tableType == "geo" and "latitud" in columns and "longitud" in columns:
        aggExprs +=[

            F.min("latitud").alias("lat_min"),
            F.max("latitud").alias("lat_max"),
            F.min("longitud").alias("lon_min"),
            F.max("longitud").alias("lon_max"),
            F.sum(
                ((F.col("latitud") < 100_000) | (F.col("latitud") > 900_000) |
                 (F.col("longitud") < 4_500_000) | (F.col("longitud") > 9_500_000)).cast("int")
            ).alias("out_of_range")
        ]

    # 3. Distribucion de eventos — solo para labels
    if tableType == "labels" and "event" in columns:
      aggExprs += [
          F.sum((F.col("event") == 1).cast("int")).alias("event_type1"),
          F.sum((F.col("event") == 2).cast("int")).alias("event_type2"),
          F.sum(F.col("event").isNull().cast("int")).alias("event_nulls"),
      ]

    # Se conoce los valores posibles de "event" (type 1, type 2, NULL), por lo que se pueden contar directamente sin necesidad de un groupBy, evitando un shuffle costoso. Si hubiera muchos valores distintos o no se conocieran, sí sería necesario un groupBy para obtener la distribución completa.
    # if tableType == "labels" and "event" in df.columns:
    #     events = df.groupBy("event").count().collect()
    #     dist = {row["event"]: row["count"] for row in events}
    #     print(f"Event dist    : {dist}")
    #     findings["event_distribution"] = dist





    stats = df.agg(*aggExprs).collect()[0].asDict()
    nulls = {k.replace("null_", ""): v for k, v in stats.items() if k.startswith("null_")}

    print(f"\n=== PROFILE: {name} ===")
    print(f"Row count     : {total:,}")
    print(f"Duplicate IDs : {duplicates:,}")
    print(f"NULL counts   : {nulls}")
    df.printSchema()

    findings = {
        "table": name,
        "total": total,
        "duplicates": duplicates,
        "nulls": nulls,
    }

    if tableType == "geo" and "latitud" in columns and "longitud" in columns:
        print(f"East range    : [{stats['lat_min']:.2f}, {stats['lat_max']:.2f}]")
        print(f"North range   : [{stats['lon_min']:.2f}, {stats['lon_max']:.2f}]")
        print(f"Out-of-range  : {stats['out_of_range']:,}")
        findings["coord_range"] = {
            "lat_min": stats["lat_min"], "lat_max": stats["lat_max"],
            "lon_min": stats["lon_min"], "lon_max": stats["lon_max"],
        }
        findings["out_of_range_coords"] = stats["out_of_range"]

    if tableType == "labels" and "event" in columns:
        dist = {"type 1": stats["event_type1"], "type 2": stats["event_type2"], "null": stats["event_nulls"]}
        print(f"Event dist    : {dist}")
        findings["event_distribution"] = dist


    # 4. Unpersishable: liberar cache para no saturar memoria
    df.unpersist()

    # ── RESUMEN DE HALLAZGOS ──────────────────────────────────────────────────
    # Coordenadas: los datos están en UTM (metros), no en lat/lon geográficos.
    #   Easting  (columna "latitud"):  100,000 – 900,000 m
    #   Northing (columna "longitud"): 4,500,000 – 9,500,000 m
    #   Esto corresponde a la zona UTM 18-19S que cubre Chile continental.
    #   Los nombres de columna son engañosos pero los rangos son correctos.
    #
    # Duplicados: geo tiene ~10K IDs duplicados (~1.7%), labels tiene ~9 (~0.01%).
    #   Se decide eliminar duplicados en el paso de limpieza (ver cleanAndDeduplicate).
    #
    # Overlap de IDs: solo ~2.9% de los IDs de geo aparecen en labels.
    #   Esto impacta directamente el tipo de join elegido (ver Step 3).
    # ───────────────────────────────────────────────────────────────────────────

    return findings




# ---------------------------------------------------------------------------
# Step 2 — Cleaning & Deduplication
# ---------------------------------------------------------------------------

def cleanAndDeduplicate(tableType: str , df : DataFrame ) -> DataFrame:
    """
    Limpieza y deduplicación de datos.

    Args:
        tableType (str): "geo" | "labels". Controla lógica específica.
        df (DataFrame): DataFrame a limpiar y deduplicar.

    Returns:
        DataFrame: DataFrame limpio y sin duplicados.

    Estrategia de deduplicación:
    ────────────────────────────
    GEO — dropDuplicates(["ID"]):
        Mantiene la primera ocurrencia de cada ID. Se elige "first" porque:
        (a) No hay campo de timestamp en los datos, así que "most recent" no aplica.
        (b) Los duplicados representan registros idénticos del mismo evento de captura.
        (c) "Most frequent" no aplica porque el ID es la PK — no hay variación de valores
            por ID que justifique elegir la moda.
        Si en el futuro hubiera un timestamp, se debería cambiar a "most recent".

    LABELS — collect_set:
        Un cliente puede tener legítimamente ambos tipos de evento (type 1 y type 2).
        Usar "first" descartaría uno de los dos. collect_set preserva todos los
        eventos distintos sin duplicar. Es la estrategia correcta cuando un cliente
        puede pertenecer a múltiples categorías de forma simultánea.
    """
    if tableType == "geo":
        # Filtra coordenadas fuera de rango UTM válido ANTES de dedup.
        # Esto evita que registros con coordenadas basura contaminen el join
        # y el cálculo de distancias.
        return (
            df.filter(
                (F.col("latitud").between(coordinates['east']['min'], coordinates['east']['max'])) &
                (F.col("longitud").between(coordinates['nort']['min'], coordinates['nort']['max']))
            )
            .dropna(subset=["ID"])
            .dropDuplicates(["ID"])
            .withColumnRenamed("latitud", "east")
            .withColumnRenamed("longitud", "north")
        )

    elif tableType == "labels":
        # dropna en ID y event: un registro sin ID o sin tipo de evento
        # no puede ser utilizado en el join ni en el análisis.
        return (
            df.dropna(subset=["ID", "event"])
              .groupBy("ID")
              .agg(F.collect_set("event").alias("events"))
        )

    return df.dropna(subset=["ID"]).dropDuplicates(["ID"])


# ---------------------------------------------------------------------------
# Step 3 — Join
# ---------------------------------------------------------------------------

def joinTables(geo, labels):
    """
    INNER JOIN on ID.

    Justification: profiling showed that only customers present in both
    tables carry a geo-location AND an event label — the minimum requirement
    to appear in a proximity analysis. Customers in geo-only have no event
    to contribute to the output; customers in labels-only have no coordinates
    and cannot be placed on the map. An inner join is therefore correct and
    reduces data volume before the expensive distance step.

    If profiling reveals low overlap (<50%), revisit with LEFT JOIN to
    preserve geo-only customers and flag them as 'no_event'.
    """

    return geo.join(F.broadcast(labels), on="ID", how="inner")






# ---------------------------------------------------------------------------
# Step 4 & 5 — Proximity Calculation with Grid-Cell Bucketing
# ---------------------------------------------------------------------------


"""
OPTIMIZATION STRATEGY: Grid-Cell Bucketing
==========================================
(1) Strategy chosen:
    Divide the UTM coordinate space into a grid of 50m × 50m cells.
    Each customer is assigned a (cell_x, cell_y) index. Two customers
    can only be within 50m if they share a cell or are in adjacent cells.
    We self-join only on a 3x3 neighbourhood (9 cell pairs per customer)
    instead of all N×N pairs.

(2) Why appropriate:
    Coordinates are already in UTM metres, so cell_size = 50 is exact.
    The grid reduces combinations from O(N²) to O(N × avg_cell_density).
    For uniformly distributed data, avg_cell_density ≈ N / grid_cells,
    giving near-linear complexity. Spark partitions naturally on cell keys.

(3) Trade-offs vs alternatives:
    - Geohashing (e.g. python-geohash): more portable across projections,
      but requires extra dependency and careful precision tuning; neighbour
      enumeration is less intuitive than integer ±1 arithmetic.
    - R-tree / spatial index (e.g. sedona): optimal for complex spatial
      queries but adds a heavyweight library dependency and cluster setup;
      overkill for a single 50m radius filter on a point dataset.
    - Naive cross-join: O(N²) — 500K rows = 250B pairs, infeasible.
"""

def addCellKeys(df):
    return (
        df
        .withColumn("cell_x", (F.col("east") / cellSize).cast("long"))
        .withColumn("cell_y", (F.col("nort") / cellSize).cast("long"))
    )


def calculateProximity(df):
    # Agregar claves de celda
    dfCells = addCellKeys(df)

    # repartition on cell keys for better join performance , garantiza que clientes en la misma o vecinas celdas estén en la misma partición
    dfCells = dfCells.repartition(200,"cell_x", "cell_y")
    dfCells.cache()  # Cache :D

    offsets = [(dx,dy) for dx in (-1,0,1) for dy in (-1,0,1) ]
    print(offsets)

    return "proximity"

# Spark Configuration
sparkConfig = {
    "spark.app.name": "Customer Proximity ETL Job",
    "spark.master": "local[*]",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.autoBroadcastJoinThreshold": "52428800",  # 50MB
}

def main():
    # Argument parsing
    parser = argparse.ArgumentParser(description="ETL Job for Customer Proximity Pipeline")
    parser.add_argument("--input_path", required=True, help="Path to the input data")
    parser.add_argument("--output_path", required=True, help="Path to save the output data")
    args = parser.parse_args()

    print(f"Input Path: {args.input_path}")
    print(f"Output Path: {args.output_path}")

    rawGeoPath = args.input_path + "/geo/"
    rawLabelsPath = args.input_path + "/labels/"


    spark = generateSparkSession(sparkConfig)
    spark.sparkContext.setLogLevel("ERROR")

    # Load Data
    geo = spark.read.parquet(rawGeoPath)
    labels = spark.read.parquet(rawLabelsPath)

    # views
    # print("geo")
    # geo.show(5)
    # print("labels")
    # labels.show(5)

    # constants :
    geoColumns = ['ID','latitud','longitud','comuna']
    labelColumns = ['ID','event']


    # Findings of

    geoFindings    = profilingTable(geo,    "Geo Data",    tableType="geo",    Id="ID", columns=geoColumns)
    labelsFindings = profilingTable(labels, "Labels Data", tableType="labels", Id="ID", columns=labelColumns)

    geo = cleanAndDeduplicate('geo',geo)
    labels = cleanAndDeduplicate('labels',labels)

    dataset = joinTables(geo,labels)

    resultCalculateProximity = calculateProximity(dataset)



    # ── JUSTIFICACIONES DEL PIPELINE ─────────────────────────────────────────
    #
    # 1. SISTEMA DE COORDENADAS:
    #    Los datos están en UTM (Universal Transverse Mercator), no en lat/lon.
    #    "latitud" = Easting (100K-900K m), "longitud" = Northing (4.5M-9.5M m).
    #    Esto permite calcular distancias en metros directamente con Euclidean
    #    distance, sin necesidad de corrección esférica (Haversine).
    #
    # 2. DEDUPLICACIÓN:
    #    - Geo: se eliminan duplicados por ID, quedándose con la primera ocurrencia.
    #    - Labels: se agregan los eventos por ID con collect_set (preserva ambos tipos).
    #    La justificación de "first" vs "most recent" es que no hay timestamp disponible.
    #
    # 3. JOIN (pendiente de implementar):
    #    Con solo 2.9% de overlap entre tablas, se recomienda INNER JOIN para
    #    proximidad (se necesitan ambas tablas: ubicación + evento).
    #    Si se requiere preservar todos los clientes geo, usar LEFT JOIN.
    #
    # 4. CÁLCULO DE DISTANCIA (pendiente):
    #    Se usará grid-cell bucketing (celdas de 50m) para evitar el cross-join
    #    completo. Esto reduce las combinaciones de O(N²) a O(N × densidad_celda).
    # ───────────────────────────────────────────────────────────────────────────









    # ETL logic here

if __name__ == "__main__":
    main()
