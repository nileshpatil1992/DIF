# Testing Guide

## Overview

Comprehensive testing strategy for the Data Ingestion Framework (DIF) ensuring code quality, reliability, and maintainability.

## Table of Contents

1. [Testing Pyramid](#testing-pyramid)
2. [Unit Tests](#unit-tests)
3. [Integration Tests](#integration-tests)
4. [Configuration Tests](#configuration-tests)
5. [Test Organization](#test-organization)
6. [Running Tests](#running-tests)
7. [Test Coverage](#test-coverage)
8. [Mock Objects](#mock-objects)
9. [Test Data](#test-data)
10. [CI/CD Integration](#cicd-integration)

## Testing Pyramid

```
        /\
       /  \
      / E2E \
     /------\
    / Integ  \
   /--------\
  / Unit     \
 /___________\
```

- **Unit Tests (70%)**: Test individual components in isolation
- **Integration Tests (20%)**: Test component interactions
- **End-to-End Tests (10%)**: Test complete workflows

## Unit Tests

### 1. Testing Readers

```python
# tests/test_readers.py

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.readers.file_readers import CSVReader, JSONReader
from src.readers.database_readers import PostgreSQLReader
from src.readers.reader_factory import ReaderFactory


class TestCSVReader:
    """Test suite for CSV reader."""
    
    @pytest.fixture
    def spark_session(self):
        """Create test Spark session."""
        return SparkSession.builder \
            .master("local[1]") \
            .appName("test-reader") \
            .getOrCreate()
    
    @pytest.fixture
    def csv_config(self):
        """Sample CSV configuration."""
        return {
            'path': 's3://test-bucket/data.csv',
            'options': {
                'header': True,
                'inferSchema': True,
                'delimiter': ',',
                'encoding': 'utf-8'
            }
        }
    
    def test_csv_reader_initialization(self, spark_session, csv_config):
        """Test CSV reader initialization."""
        reader = CSVReader(spark_session, csv_config)
        
        assert reader.path == 's3://test-bucket/data.csv'
        assert reader.options['header'] is True
        assert reader.options['delimiter'] == ','
    
    @patch('src.readers.file_readers.CSVReader.read')
    def test_csv_reader_read(self, mock_read, spark_session, csv_config):
        """Test CSV reader read method."""
        # Create mock DataFrame
        mock_df = MagicMock(spec=DataFrame)
        mock_df.count.return_value = 100
        mock_df.columns = ['id', 'name', 'value']
        mock_read.return_value = mock_df
        
        reader = CSVReader(spark_session, csv_config)
        df = reader.read()
        
        assert df is not None
        assert df.count() == 100
        assert len(df.columns) == 3
    
    def test_csv_reader_missing_path(self, spark_session):
        """Test CSV reader with missing path."""
        config = {'options': {}}
        
        reader = CSVReader(spark_session, config)
        assert reader.path is None
    
    def test_csv_reader_options_defaults(self, spark_session):
        """Test CSV reader option defaults."""
        config = {'path': 'data.csv', 'options': {}}
        
        reader = CSVReader(spark_session, config)
        assert reader.options.get('header', True) is True
        assert reader.options.get('inferSchema', True) is True


class TestJSONReader:
    """Test suite for JSON reader."""
    
    @pytest.fixture
    def spark_session(self):
        return SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    
    @pytest.fixture
    def json_config(self):
        return {
            'path': 's3://test-bucket/data.json',
            'options': {'inferSchema': True}
        }
    
    def test_json_reader_initialization(self, spark_session, json_config):
        """Test JSON reader initialization."""
        reader = JSONReader(spark_session, json_config)
        
        assert reader.path == 's3://test-bucket/data.json'
        assert reader.options['inferSchema'] is True
    
    def test_json_reader_encoding(self, spark_session, json_config):
        """Test JSON reader encoding option."""
        json_config['options']['encoding'] = 'utf-16'
        reader = JSONReader(spark_session, json_config)
        
        assert reader.options['encoding'] == 'utf-16'


class TestReaderFactory:
    """Test suite for reader factory."""
    
    @pytest.fixture
    def spark_session(self):
        return SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    
    def test_factory_creates_csv_reader(self, spark_session):
        """Test factory creates CSV reader."""
        config = {'path': 'data.csv', 'options': {}}
        
        reader = ReaderFactory.create_reader(
            spark_session,
            's3',
            'csv',
            config
        )
        
        assert isinstance(reader, CSVReader)
    
    def test_factory_creates_json_reader(self, spark_session):
        """Test factory creates JSON reader."""
        config = {'path': 'data.json', 'options': {}}
        
        reader = ReaderFactory.create_reader(
            spark_session,
            'gcs',
            'json',
            config
        )
        
        assert isinstance(reader, JSONReader)
    
    def test_factory_unsupported_format(self, spark_session):
        """Test factory raises error for unsupported format."""
        config = {'path': 'data.xyz', 'options': {}}
        
        with pytest.raises(ValueError, match="Unsupported file format"):
            ReaderFactory.create_reader(
                spark_session,
                's3',
                'xyz',  # Unsupported format
                config
            )
    
    def test_factory_unsupported_source(self, spark_session):
        """Test factory raises error for unsupported source."""
        config = {'path': 'data.csv', 'options': {}}
        
        with pytest.raises(ValueError, match="Unsupported source type"):
            ReaderFactory.create_reader(
                spark_session,
                'ftp',  # Unsupported source
                'csv',
                config
            )
    
    def test_factory_supported_formats(self):
        """Test factory reports supported formats."""
        supported = ReaderFactory.get_supported_formats()
        
        assert 'csv' in supported['file_formats']
        assert 'json' in supported['file_formats']
        assert 'parquet' in supported['file_formats']
        assert 's3' in supported['cloud_storage']
        assert 'snowflake' in supported['database_sources']
```

### 2. Testing Writers

```python
# tests/test_writers.py

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame

from src.writers.file_writers import CSVWriter, ParquetWriter
from src.writers.writer_factory import WriterFactory


class TestCSVWriter:
    """Test suite for CSV writer."""
    
    @pytest.fixture
    def spark_session(self):
        return SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    
    @pytest.fixture
    def writer_config(self):
        return {
            'path': 's3://test-bucket/output/data.csv',
            'options': {
                'mode': 'overwrite',
                'header': True,
                'delimiter': ','
            }
        }
    
    def test_csv_writer_initialization(self, spark_session, writer_config):
        """Test CSV writer initialization."""
        writer = CSVWriter(spark_session, writer_config)
        
        assert writer.path == 's3://test-bucket/output/data.csv'
        assert writer.options['mode'] == 'overwrite'
        assert writer.get_write_mode() == 'overwrite'
    
    def test_csv_writer_write_mode(self, spark_session, writer_config):
        """Test CSV writer write mode."""
        writer = CSVWriter(spark_session, writer_config)
        
        assert writer.get_write_mode() == 'overwrite'
    
    def test_csv_writer_partition_columns(self, spark_session, writer_config):
        """Test CSV writer partition columns."""
        writer_config['options']['partitionBy'] = ['date', 'region']
        writer = CSVWriter(spark_session, writer_config)
        
        assert writer.get_partition_columns() == ['date', 'region']
    
    def test_csv_writer_repartition(self, spark_session, writer_config):
        """Test CSV writer repartition count."""
        writer_config['options']['repartition'] = 8
        writer = CSVWriter(spark_session, writer_config)
        
        assert writer.get_repartition_count() == 8


class TestWriterFactory:
    """Test suite for writer factory."""
    
    @pytest.fixture
    def spark_session(self):
        return SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    
    def test_factory_creates_csv_writer(self, spark_session):
        """Test factory creates CSV writer."""
        config = {'path': 'output.csv', 'options': {}}
        
        writer = WriterFactory.create_writer(
            spark_session,
            's3',
            'csv',
            config
        )
        
        assert isinstance(writer, CSVWriter)
    
    def test_factory_creates_parquet_writer(self, spark_session):
        """Test factory creates Parquet writer."""
        config = {'path': 'output.parquet', 'options': {}}
        
        writer = WriterFactory.create_writer(
            spark_session,
            's3',
            'parquet',
            config
        )
        
        assert isinstance(writer, ParquetWriter)
    
    def test_factory_unsupported_format(self, spark_session):
        """Test factory raises error for unsupported format."""
        config = {'path': 'output.xyz', 'options': {}}
        
        with pytest.raises(ValueError, match="Unsupported file format"):
            WriterFactory.create_writer(
                spark_session,
                's3',
                'xyz',
                config
            )
```

### 3. Testing Configuration Loader

```python
# tests/test_config_loader.py

import pytest
import os
import tempfile
import yaml
from src.config.config_loader import ConfigLoader, SourceConfig, TargetConfig


class TestConfigLoader:
    """Test suite for configuration loader."""
    
    @pytest.fixture
    def sample_config(self):
        """Create sample configuration."""
        return {
            'ingestion': {
                'source': {
                    'type': 's3',
                    'format': 'csv',
                    'path': 's3://bucket/input/',
                    'options': {'header': True}
                },
                'target': {
                    'type': 'snowflake',
                    'format': 'parquet',
                    'path': 's3://bucket/output/',
                    'options': {'mode': 'overwrite'}
                },
                'spark': {
                    'master': 'local[*]',
                    'cores': 4
                },
                'processing': {
                    'validate': True,
                    'dedup': False
                }
            }
        }
    
    def test_config_loader_with_file(self, sample_config):
        """Test loading configuration from file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(sample_config, f)
            config_path = f.name
        
        try:
            config = ConfigLoader.load_config(config_path)
            
            assert 'ingestion' in config
            assert 'source' in config['ingestion']
            assert 'target' in config['ingestion']
        finally:
            os.unlink(config_path)
    
    def test_config_loader_file_not_found(self):
        """Test loading from non-existent file."""
        with pytest.raises(FileNotFoundError):
            ConfigLoader.load_config('/nonexistent/path/config.yaml')
    
    def test_config_loader_env_substitution(self, sample_config):
        """Test environment variable substitution."""
        os.environ['TEST_BUCKET'] = 'my-bucket'
        os.environ['TEST_REGION'] = 'us-east-1'
        
        sample_config['ingestion']['source']['path'] = '${TEST_BUCKET}'
        sample_config['ingestion']['source']['options']['region'] = '${TEST_REGION}'
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(sample_config, f)
            config_path = f.name
        
        try:
            config = ConfigLoader.load_config(config_path)
            
            assert config['ingestion']['source']['path'] == 'my-bucket'
            assert config['ingestion']['source']['options']['region'] == 'us-east-1'
        finally:
            os.unlink(config_path)
    
    def test_get_source_config(self, sample_config):
        """Test extracting source configuration."""
        source_config = ConfigLoader.get_source_config(sample_config)
        
        assert isinstance(source_config, SourceConfig)
        assert source_config.type == 's3'
        assert source_config.format == 'csv'
        assert source_config.path == 's3://bucket/input/'
    
    def test_get_target_config(self, sample_config):
        """Test extracting target configuration."""
        target_config = ConfigLoader.get_target_config(sample_config)
        
        assert isinstance(target_config, TargetConfig)
        assert target_config.type == 'snowflake'
        assert target_config.format == 'parquet'
```

### 4. Testing Facade

```python
# tests/test_facade.py

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame

from src.facade.ingestion_facade import IngestionFacade


class TestIngestionFacade:
    """Test suite for ingestion facade."""
    
    @pytest.fixture
    def config_path(self):
        """Return path to sample config."""
        return 'config/sample_config.yaml'
    
    @patch('src.facade.ingestion_facade.ConfigLoader')
    @patch('src.facade.ingestion_facade.SparkManager')
    def test_facade_initialization(
        self,
        mock_spark_manager,
        mock_config_loader,
        config_path
    ):
        """Test facade initialization."""
        # Mock config loader
        mock_config = {
            'ingestion': {
                'source': {'type': 's3', 'format': 'csv'},
                'target': {'type': 's3', 'format': 'parquet'},
                'spark': {'master': 'local[*]'},
                'processing': {}
            }
        }
        mock_config_loader.load_config.return_value = mock_config
        mock_config_loader.get_source_config.return_value = Mock()
        mock_config_loader.get_target_config.return_value = Mock()
        mock_config_loader.get_spark_config.return_value = Mock()
        mock_config_loader.get_processing_config.return_value = {}
        
        # Mock spark manager
        mock_spark_session = MagicMock(spec=SparkSession)
        mock_spark_manager_instance = Mock()
        mock_spark_manager_instance.create_session.return_value = mock_spark_session
        mock_spark_manager.return_value = mock_spark_manager_instance
        
        # Create facade
        facade = IngestionFacade(config_path)
        
        # Verify initialization
        assert facade.config is not None
        assert facade.spark is not None
    
    @patch('src.facade.ingestion_facade.ReaderFactory')
    @patch('src.facade.ingestion_facade.WriterFactory')
    @patch('src.facade.ingestion_facade.ConfigLoader')
    @patch('src.facade.ingestion_facade.SparkManager')
    def test_facade_read_only(
        self,
        mock_spark_manager,
        mock_config_loader,
        mock_writer_factory,
        mock_reader_factory,
        config_path
    ):
        """Test facade read-only operation."""
        # Setup mocks
        mock_config = {'ingestion': {'source': {}, 'target': {}, 'spark': {}, 'processing': {}}}
        mock_config_loader.load_config.return_value = mock_config
        mock_config_loader.get_source_config.return_value = Mock()
        mock_config_loader.get_target_config.return_value = Mock()
        mock_config_loader.get_spark_config.return_value = Mock()
        mock_config_loader.get_processing_config.return_value = {}
        
        mock_spark_session = MagicMock(spec=SparkSession)
        mock_spark_manager_instance = Mock()
        mock_spark_manager_instance.create_session.return_value = mock_spark_session
        mock_spark_manager.return_value = mock_spark_manager_instance
        
        # Create mock DataFrame
        mock_df = MagicMock(spec=DataFrame)
        mock_reader = Mock()
        mock_reader.read.return_value = mock_df
        mock_reader_factory.create_reader.return_value = mock_reader
        
        # Test read-only
        facade = IngestionFacade(config_path)
        result = facade.read_only()
        
        assert result is not None
        mock_reader.read.assert_called_once()
```

## Integration Tests

```python
# tests/test_integration.py

import pytest
from pyspark.sql import SparkSession
from tempfile import TemporaryDirectory
import os

from src.facade.ingestion_facade import IngestionFacade


class TestIntegration:
    """Integration tests for complete workflows."""
    
    @pytest.fixture
    def spark_session(self):
        """Create Spark session for testing."""
        spark = SparkSession.builder \
            .master("local[1]") \
            .appName("integration-test") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    def test_csv_to_csv_integration(self, spark_session):
        """Test CSV to CSV ingestion."""
        with TemporaryDirectory() as tmpdir:
            # Create sample CSV
            input_file = os.path.join(tmpdir, 'input.csv')
            output_file = os.path.join(tmpdir, 'output.csv')
            
            # Write sample data
            df = spark_session.createDataFrame(
                [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')],
                ['id', 'name']
            )
            df.write.mode('overwrite').csv(input_file, header=True)
            
            # Verify output exists
            assert os.path.exists(input_file)
    
    def test_data_quality(self, spark_session):
        """Test data quality after ingestion."""
        # Create DataFrame with known data
        df = spark_session.createDataFrame(
            [(1, 'test1'), (2, 'test2'), (3, 'test3')],
            ['id', 'value']
        )
        
        # Verify quality metrics
        row_count = df.count()
        assert row_count == 3
        
        col_count = len(df.columns)
        assert col_count == 2
        
        # Check for nulls
        null_counts = df.select(
            [sum(df[col].isNull().cast('int')).alias(col) for col in df.columns]
        ).collect()[0]
        
        assert all(count == 0 for count in null_counts)
```

## Running Tests

### Run All Tests

```bash
pytest tests/ -v
```

### Run Specific Test File

```bash
pytest tests/test_readers.py -v
```

### Run Specific Test Class

```bash
pytest tests/test_readers.py::TestCSVReader -v
```

### Run Specific Test Method

```bash
pytest tests/test_readers.py::TestCSVReader::test_csv_reader_initialization -v
```

### Run with Coverage

```bash
pytest tests/ --cov=src --cov-report=html --cov-report=term
```

### Run with Markers

```bash
# Run only unit tests
pytest tests/ -m unit -v

# Run only integration tests
pytest tests/ -m integration -v
```

## Test Coverage

**Target Coverage: > 80%**

```bash
# Generate coverage report
pytest tests/ --cov=src --cov-report=html

# View report
open htmlcov/index.html
```

## Test Data

### Sample Data Generation

```python
# tests/conftest.py

import pytest
from pyspark.sql import SparkSession
import tempfile
import os


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for tests."""
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("pytest-pyspark") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_csv_data(spark, tmp_path):
    """Create sample CSV data for testing."""
    df = spark.createDataFrame(
        [
            (1, 'Alice', 25000),
            (2, 'Bob', 30000),
            (3, 'Charlie', 35000),
            (4, 'Diana', 40000),
            (5, 'Eve', 45000),
        ],
        ['id', 'name', 'salary']
    )
    
    output_path = str(tmp_path / 'sample.csv')
    df.write.mode('overwrite').csv(output_path, header=True)
    
    return output_path, df
```

## CI/CD Integration

### GitHub Actions

```yaml
# .github/workflows/tests.yml

name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v2
    
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.9'
    
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install pytest pytest-cov
    
    - name: Run tests
      run: pytest tests/ --cov=src --cov-report=xml
    
    - name: Upload coverage
      uses: codecov/codecov-action@v2
      with:
        files: ./coverage.xml
```

## Summary

- **Test Everything**: Unit, integration, and end-to-end tests
- **High Coverage**: Aim for > 80% code coverage
- **Mock External Dependencies**: Use mocks for external systems
- **Use Fixtures**: Keep tests DRY with pytest fixtures
- **Descriptive Names**: Test names should describe what they test
- **One Assertion Per Test**: Focus on single behavior per test
- **Run Tests Locally**: Before committing code
- **CI/CD Integration**: Automate testing in pipeline
