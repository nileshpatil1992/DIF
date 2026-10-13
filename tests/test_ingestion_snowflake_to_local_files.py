import os
import shutil
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


# Snowflake source table
SNOWFLAKE_TABLE = "ALL_DATATYPES_CSV"

# Output directory for all exported formats
OUTPUT_ROOT = Path("tests/Data/snowflake_exports")


@pytest.fixture(scope="module")
def spark():
    # Windows Hadoop configuration
    os.environ["HADOOP_HOME"] = r"C:/hadoop"
    os.environ["PATH"] = (
        r"C:/hadoop/bin;" + os.environ.get("PATH", "")
    )

    # Packages for Snowflake, XML, and Avro
    packages = ",".join([
        "net.snowflake:spark-snowflake_2.12:3.2.0-spark_3.5",
        "com.databricks:spark-xml_2.12:0.18.0",
        "org.apache.spark:spark-avro_2.12:3.5.6",
    ])

    session = (
        SparkSession.builder
        .appName("Snowflake_To_Local_Files")
        .config("spark.jars.packages", packages)
        .getOrCreate()
    )

    yield session
    session.stop()


def get_snowflake_options():
    password = os.environ.get("SNOWFLAKE_PASSWORD")

    if not password:
        pytest.fail(
            "SNOWFLAKE_PASSWORD is not set in PowerShell."
        )

    return {
        "sfURL": "UOYZKCV-GAB60258.snowflakecomputing.com",
        "sfUser": "TEJASMORE910",
        "sfPassword": password,
        "sfDatabase": "PYSPARK_DB",
        "sfSchema": "PUBLIC",
        "sfWarehouse": "COMPUTE_WH",
        "sfRole": "ACCOUNTADMIN",
    }


@pytest.mark.parametrize(
    "file_format",
    ["csv", "json", "xml", "orc", "parquet", "avro"],
)
def test_snowflake_to_local_file(spark, file_format):
    sf_options = get_snowflake_options()

    # Step 1: Read data from Snowflake
    source_df = (
        spark.read
        .format("snowflake")
        .options(**sf_options)
        .option("dbtable", SNOWFLAKE_TABLE)
        .load()
    )

    source_count = source_df.count()
    assert source_count > 0, "Snowflake table is empty"

    # Step 2: Create output path
    output_path = OUTPUT_ROOT / file_format
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    # Remove previous output so old data cannot affect this test
    if output_path.exists():
        shutil.rmtree(output_path)

    # Step 3: Write data in the selected format
    writer = source_df.write.mode("overwrite")

    if file_format == "csv":
        writer.option("header", True).csv(str(output_path))

    elif file_format == "json":
        writer.json(str(output_path))

    elif file_format == "xml":
        (
            writer.format("xml")
            .option("rowTag", "row")
            .save(str(output_path))
        )

    elif file_format == "orc":
        writer.orc(str(output_path))

    elif file_format == "parquet":
        writer.parquet(str(output_path))

    elif file_format == "avro":
        writer.format("avro").save(str(output_path))

    # Step 4: Read exported data back
    if file_format == "csv":
        result_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(output_path))
        )

    elif file_format == "json":
        result_df = spark.read.json(str(output_path))

    elif file_format == "xml":
        (
            result_df := (
                spark.read
                .format("xml")
                .option("rowTag", "row")
                .load(str(output_path))
            )
        )

    elif file_format == "orc":
        result_df = spark.read.orc(str(output_path))

    elif file_format == "parquet":
        result_df = spark.read.parquet(str(output_path))

    elif file_format == "avro":
        result_df = (
            spark.read
            .format("avro")
            .load(str(output_path))
        )

    # Step 5: Validate exported row count
    exported_count = result_df.count()

    assert exported_count == source_count, (
        f"{file_format.upper()} failed: "
        f"source={source_count}, exported={exported_count}"
    )

    # Step 6: Validate columns exist
    assert len(result_df.columns) > 0

    print(
        f"PASSED: Snowflake -> {file_format.upper()}, "
        f"rows={exported_count}"
    )
