from src.config.config_loader import SourceConfig, TargetConfig
from tests.ingestion_scenario_test_helper import assert_ingestion_flow

# Local JSON to Snowflake with overwrite

def test_local_json_to_snowflake_overwrite():

    import os
    from pyspark.sql import SparkSession

    os.environ["HADOOP_HOME"] = r"C:/hadoop"
    os.environ["PATH"] = r"C:/hadoop/bin;" + os.environ["PATH"]

    spark = (
        SparkSession.builder
        .appName("Local_JSON_to_Snowflake_Overwrite_Test")
        .config(
            "spark.jars.packages",
            "net.snowflake:spark-snowflake_2.12:3.2.0-spark_3.5"
        )
        .getOrCreate()
    )

    try:

        # Read JSON file
        json_path = r"C:/Users/tejas/Downloads/Customer.json"

        df = (
            spark.read
            .option("multiLine", "true")
            .option("inferSchema", "true")
            .json(json_path)
        )

        # Check source data
        row_count = df.count()

        print("JSON row count:", row_count)

        assert row_count == 20

        # Snowflake configuration
        sfOptions = {
            "sfURL": "UOYZKCV-GAB60258.snowflakecomputing.com",
            "sfUser": "TEJASMORE910",
            "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
            "sfDatabase": "PYSPARK_DB",
            "sfSchema": "PUBLIC",
            "sfWarehouse": "COMPUTE_WH",
            "sfRole": "ACCOUNTADMIN"
        }

        # Write using overwrite mode
        (
            df.write
            .format("snowflake")
            .options(**sfOptions)
            .option("dbtable", "CUSTOMERS_JSON")
            .mode("overwrite")
            .save()
        )

        print("JSON data successfully overwritten in Snowflake!")

    finally:
        spark.stop()