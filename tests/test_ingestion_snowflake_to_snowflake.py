from src.config.config_loader import SourceConfig, TargetConfig

from tests.ingestion_scenario_test_helper import assert_ingestion_flow


def test_snowflake_to_snowflake():

    import os
    from pyspark.sql import SparkSession

    # Hadoop configuration
    os.environ["HADOOP_HOME"] = r"C:/hadoop"
    os.environ["PATH"] = r"C:/hadoop/bin;" + os.environ["PATH"]

    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("Snowflake_to_Snowflake_Test")
        .config(
            "spark.jars.packages",
            "net.snowflake:spark-snowflake_2.12:3.2.0-spark_3.5"
        )
        .getOrCreate()
    )

    try:

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

        # Read source table from Snowflake
        source_df = (
            spark.read
            .format("snowflake")
            .options(**sfOptions)
            .option("dbtable", "CUSTOMERS")
            .load()
        )

        # Display source data
        source_df.printSchema()
        source_df.show(10, truncate=False)

        # Count source records
        source_count = source_df.count()

        print("Source Snowflake row count:", source_count)

        # Verify source data exists
        assert source_count > 0

        # Write to target Snowflake table
        (
            source_df.write
            .format("snowflake")
            .options(**sfOptions)
            .option("dbtable", "CUSTOMERS_SNOWFLAKE_COPY")
            .mode("overwrite")
            .save()
        )

        print("Snowflake to Snowflake transfer completed!")

        # Read target table back from Snowflake
        target_df = (
            spark.read
            .format("snowflake")
            .options(**sfOptions)
            .option("dbtable", "CUSTOMERS_SNOWFLAKE_COPY")
            .load()
        )

        # Count target records
        target_count = target_df.count()

        print("Target Snowflake row count:", target_count)

        # Verify source and target counts
        assert target_count == source_count

        print("Source and target row counts match!")

    finally:
        spark.stop()
