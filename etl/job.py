"""
ETL Standalone Job - Customer Proximity pipeline
Technical test for data engineer position at lla.
Telecom take-home assessment

Usage:
    python job.py --input_path <input_path> --output_path <output_path>

Where:
    <input_path> is the path to the input data (e.g., "data/raw/")
    <output_path> is the path where the output data will be saved (e.g., "data/processed/")

"""


# Imports


import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window



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






# Spark Configuration
sparkConfig = {
    "spark.app.name": "Customer Proximity ETL Job",
    "spark.master": "local[*]",
    "spark.sql.shuffle.partitions": "4",
    "spark.driver.memory": "8g",
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.sql.session.timeZone": "UTC",
}

def main():
    # Argument parsing
    parser = argparse.ArgumentParser(description="ETL Job for Customer Proximity Pipeline")
    parser.add_argument("--input_path", required=True, help="Path to the input data")
    parser.add_argument("--output_path", required=True, help="Path to save the output data")
    args = parser.parse_args()

    print(f"Input Path: {args.input_path}")
    print(f"Output Path: {args.output_path}")

    spark = generateSparkSession(sparkConfig)
    spark.sparkContext.setLogLevel("ERROR")

    print(spark.conf.getAll)



    # ETL logic here

if __name__ == "__main__":
    main()
