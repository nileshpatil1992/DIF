# local csv to snowflake
from src.config.config_loader import SourceConfig, TargetConfig
from tests.ingestion_scenario_test_helper import assert_ingestion_flow

import os
from pyspark.sql import SparkSession


def test_local_csv_to_snowflake():

    # Hadoop configuration
    os.environ["HADOOP_HOME"] = r"C:/hadoop"
    os.environ["PATH"] = r"C:/hadoop/bin;" + os.environ["PATH"]

    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("Local_CSV_to_Snowflake_Test")
        .config(
            "spark.jars.packages",
            "net.snowflake:spark-snowflake_2.12:3.2.0-spark_3.5"
        )
        .getOrCreate()
    )

    try:

        # Snowflake configuration
        sf_options = {
            "sfURL": "UOYZKCV-GAB60258.snowflakecomputing.com",
            "sfUser": "TEJASMORE910",
            "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
            "sfDatabase": "PYSPARK_DB",
            "sfSchema": "PUBLIC",
            "sfWarehouse": "COMPUTE_WH",
            "sfRole": "ACCOUNTADMIN"
        }

        # Read local CSV
        source_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv("tests/Data/all_datatypes.csv")
        )

        source_count = source_df.count()

        assert source_count > 0

        # Write to Snowflake
        (
            source_df.write
            .format("snowflake")
            .options(**sf_options)
            .option("dbtable", "ALL_DATATYPES_CSV")
            .mode("overwrite")
            .save()
        )

        # Read target from Snowflake
        target_df = (
            spark.read
            .format("snowflake")
            .options(**sf_options)
            .option("dbtable", "ALL_DATATYPES_CSV")
            .load()
        )

        target_count = target_df.count()

        # Validate row count
        assert target_count == source_count

    finally:
        spark.stop()