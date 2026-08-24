# Pytest configuration and fixtures

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import tempfile
import os


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for entire test session."""
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-pyspark") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def spark_with_catalog(spark):
    """Spark session with temporary warehouse."""
    with tempfile.TemporaryDirectory() as tmpdir:
        spark.sql(f"SET spark.sql.warehouse.dir={tmpdir}")
        yield spark


@pytest.fixture
def sample_csv_data(spark, tmp_path):
    """Create sample CSV data for testing."""
    df = spark.createDataFrame(
        [
            (1, 'Alice', 25000.0, 'Engineering'),
            (2, 'Bob', 30000.0, 'Sales'),
            (3, 'Charlie', 35000.0, 'Engineering'),
            (4, 'Diana', 40000.0, 'Management'),
            (5, 'Eve', 45000.0, 'Engineering'),
        ],
        ['id', 'name', 'salary', 'department']
    )
    
    output_path = str(tmp_path / 'sample.csv')
    df.write.mode('overwrite').option('header', 'true').csv(output_path)
    
    return output_path, df


@pytest.fixture
def sample_json_data(spark, tmp_path):
    """Create sample JSON data for testing."""
    df = spark.createDataFrame(
        [
            (1, 'Alice', 25000.0),
            (2, 'Bob', 30000.0),
            (3, 'Charlie', 35000.0),
        ],
        ['id', 'name', 'salary']
    )
    
    output_path = str(tmp_path / 'sample.json')
    df.write.mode('overwrite').json(output_path)
    
    return output_path, df


@pytest.fixture
def sample_parquet_data(spark, tmp_path):
    """Create sample Parquet data for testing."""
    df = spark.createDataFrame(
        [
            (1, 'Alice', 25000.0),
            (2, 'Bob', 30000.0),
            (3, 'Charlie', 35000.0),
        ],
        ['id', 'name', 'salary']
    )
    
    output_path = str(tmp_path / 'sample.parquet')
    df.write.mode('overwrite').parquet(output_path)
    
    return output_path, df


@pytest.fixture
def sample_config():
    """Sample configuration dictionary."""
    return {
        'ingestion': {
            'source': {
                'type': 's3',
                'format': 'csv',
                'path': 's3://test-bucket/input/data.csv',
                'options': {
                    'header': True,
                    'inferSchema': True,
                    'delimiter': ','
                },
                'credentials': {
                    'access_key': 'test-key',
                    'secret_key': 'test-secret'
                }
            },
            'target': {
                'type': 's3',
                'format': 'parquet',
                'path': 's3://test-bucket/output/',
                'options': {
                    'mode': 'overwrite',
                    'compression': 'snappy',
                    'repartition': 4
                }
            },
            'spark': {
                'master': 'local[*]',
                'app_name': 'test-app',
                'cores': 4,
                'memory': '4g',
                'driver_memory': '2g',
                'executor_memory': '2g',
                'shuffle_partitions': 200
            },
            'processing': {
                'validate': True,
                'dedup': False,
                'sample_data': False,
                'cache_intermediate': False
            }
        }
    }


@pytest.fixture
def empty_dataframe(spark):
    """Create empty DataFrame with schema."""
    schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('name', StringType(), True),
        StructField('value', DoubleType(), True)
    ])
    return spark.createDataFrame([], schema)


@pytest.fixture
def dataframe_with_nulls(spark):
    """Create DataFrame with null values."""
    df = spark.createDataFrame(
        [
            (1, 'Alice', 100.0),
            (2, None, 200.0),
            (None, 'Charlie', None),
            (4, 'Diana', 400.0),
        ],
        ['id', 'name', 'value']
    )
    return df


@pytest.fixture
def dataframe_with_duplicates(spark):
    """Create DataFrame with duplicate rows."""
    df = spark.createDataFrame(
        [
            (1, 'Alice', 100.0),
            (1, 'Alice', 100.0),  # Duplicate
            (2, 'Bob', 200.0),
            (2, 'Bob', 200.0),    # Duplicate
            (3, 'Charlie', 300.0),
        ],
        ['id', 'name', 'value']
    )
    return df


pytest.mark.unit = pytest.mark.unit
pytest.mark.integration = pytest.mark.integration
pytest.mark.slow = pytest.mark.slow
