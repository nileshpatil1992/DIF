
from src.config.config_loader import SourceConfig, TargetConfig
from tests.ingestion_scenario_test_helper import assert_ingestion_flow




import os

from pyspark.sql import SparkSession

# Local JSON to Snowflake
def test_local_json_to_snowflake():

    # Hadoop configuration for Windows
    os.environ["HADOOP_HOME"] = r"C:/hadoop"
    os.environ["PATH"] = r"C:/hadoop/bin;" + os.environ["PATH"]

    spark = (
        SparkSession.builder
        .appName("Local_JSON_to_Snowflake_Test")
        .config(
            "spark.jars.packages",
            "net.snowflake:spark-snowflake_2.12:3.2.0-spark_3.5"
        )
        .getOrCreate()
    )

    try:
        # Snowflake connection
        sf_options = {
            "sfURL": "UOYZKCV-GAB60258.snowflakecomputing.com",
            "sfUser": "TEJASMORE910",
            "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
            "sfDatabase": "PYSPARK_DB",
            "sfSchema": "PUBLIC",
            "sfWarehouse": "COMPUTE_WH",
            "sfRole": "ACCOUNTADMIN"
        }

        # Read JSON
        source_df = (
            spark.read
            .option("multiLine", True)
            .option("inferSchema", True)
            .json("tests/Data/all_datatypes.json")
        )

        source_count = source_df.count()

        assert source_count == 4

        # Write JSON data to Snowflake
        (
            source_df.write
            .format("snowflake")
            .options(**sf_options)
            .option("dbtable", "ALL_DATATYPES_JSON")
            .mode("overwrite")
            .save()
        )

        # Read data back from Snowflake
        target_df = (
            spark.read
            .format("snowflake")
            .options(**sf_options)
            .option("dbtable", "ALL_DATATYPES_JSON")
            .load()
        )

        target_count = target_df.count()

        # Validate row count
        assert target_count == source_count

        # Validate data
        source_ids = {
            row.id for row in source_df.select("id").collect()
        }

        target_ids = {
            row.ID for row in target_df.select("ID").collect()
        }

        assert source_ids == target_ids

        print("Local JSON → Snowflake test passed.")

    finally:
        spark.stop()