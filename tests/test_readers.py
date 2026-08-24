# Unit tests for readers

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame

from src.readers.file_readers import CSVReader, JSONReader, ParquetReader
from src.readers.reader_factory import ReaderFactory
from src.readers.base_reader import BaseReader


class TestCSVReader:
    """Test suite for CSV reader."""
    
    def test_csv_reader_initialization(self, spark, sample_config):
        """Test CSV reader initialization."""
        source_config = sample_config['ingestion']['source']
        reader = CSVReader(spark, source_config)
        
        assert reader.path == source_config['path']
        assert reader.options['header'] is True
        assert reader.options['delimiter'] == ','
    
    def test_csv_reader_config_extraction(self, spark, sample_config):
        """Test CSV reader configuration extraction."""
        source_config = sample_config['ingestion']['source']
        reader = CSVReader(spark, source_config)
        
        assert 'header' in reader.options
        assert 'inferSchema' in reader.options
        assert 'delimiter' in reader.options
        assert 'encoding' in reader.options
    
    def test_csv_reader_missing_path(self, spark):
        """Test CSV reader with missing path."""
        config = {'options': {}}
        reader = CSVReader(spark, config)
        
        assert reader.path is None
    
    def test_csv_reader_default_options(self, spark):
        """Test CSV reader with default options."""
        config = {'path': 'data.csv', 'options': {}}
        reader = CSVReader(spark, config)
        
        assert 'header' in reader.options
        assert 'inferSchema' in reader.options
        assert 'delimiter' in reader.options


class TestJSONReader:
    """Test suite for JSON reader."""
    
    def test_json_reader_initialization(self, spark, sample_config):
        """Test JSON reader initialization."""
        source_config = {
            'path': 's3://bucket/data.json',
            'options': {'inferSchema': True},
            'credentials': {}
        }
        reader = JSONReader(spark, source_config)
        
        assert reader.path == 's3://bucket/data.json'
        assert reader.options['inferSchema'] is True
    
    def test_json_reader_encoding_option(self, spark):
        """Test JSON reader with encoding option."""
        config = {
            'path': 'data.json',
            'options': {'encoding': 'utf-16'},
            'credentials': {}
        }
        reader = JSONReader(spark, config)
        
        assert reader.options['encoding'] == 'utf-16'


class TestParquetReader:
    """Test suite for Parquet reader."""
    
    def test_parquet_reader_initialization(self, spark):
        """Test Parquet reader initialization."""
        config = {'path': 'data.parquet', 'options': {}}
        reader = ParquetReader(spark, config)
        
        assert reader.path == 'data.parquet'


class TestReaderFactory:
    """Test suite for reader factory."""
    
    def test_factory_creates_csv_reader(self, spark):
        """Test factory creates CSV reader."""
        config = {'path': 'data.csv', 'options': {}}
        
        reader = ReaderFactory.create_reader(
            spark,
            's3',
            'csv',
            config
        )
        
        assert isinstance(reader, CSVReader)
    
    def test_factory_creates_json_reader(self, spark):
        """Test factory creates JSON reader."""
        config = {'path': 'data.json', 'options': {}}
        
        reader = ReaderFactory.create_reader(
            spark,
            'gcs',
            'json',
            config
        )
        
        assert isinstance(reader, JSONReader)
    
    def test_factory_creates_parquet_reader(self, spark):
        """Test factory creates Parquet reader."""
        config = {'path': 'data.parquet', 'options': {}}
        
        reader = ReaderFactory.create_reader(
            spark,
            'local',
            'parquet',
            config
        )
        
        assert isinstance(reader, ParquetReader)
    
    def test_factory_unsupported_format(self, spark):
        """Test factory raises error for unsupported format."""
        config = {'path': 'data.xyz', 'options': {}}
        
        with pytest.raises(ValueError, match="Unsupported file format"):
            ReaderFactory.create_reader(
                spark,
                's3',
                'xyz',
                config
            )
    
    def test_factory_unsupported_source(self, spark):
        """Test factory raises error for unsupported source."""
        config = {'path': 'data.csv', 'options': {}}
        
        with pytest.raises(ValueError, match="Unsupported source type"):
            ReaderFactory.create_reader(
                spark,
                'ftp',
                'csv',
                config
            )
    
    def test_factory_supported_formats(self):
        """Test factory reports supported formats."""
        supported = ReaderFactory.get_supported_formats()
        
        assert 'csv' in supported['file_formats']
        assert 'json' in supported['file_formats']
        assert 'parquet' in supported['file_formats']
        assert 'orc' in supported['file_formats']
        assert 'avro' in supported['file_formats']
        assert 's3' in supported['cloud_storage']
        assert 'gcs' in supported['cloud_storage']
        assert 'local' in supported['cloud_storage']
    
    def test_factory_supported_databases(self):
        """Test factory reports supported database sources."""
        supported = ReaderFactory.get_supported_formats()
        
        assert 'snowflake' in supported['database_sources']
        assert 'bigquery' in supported['database_sources']
        assert 'redshift' in supported['database_sources']
        assert 'postgresql' in supported['database_sources']
        assert 'mysql' in supported['database_sources']


class TestBaseReader:
    """Test suite for base reader."""
    
    def test_base_reader_validate_success(self, spark, dataframe_with_duplicates):
        """Test base reader validation with valid data."""
        config = {'path': 'test.csv', 'options': {}}
        reader = CSVReader(spark, config)
        
        # Validation should pass with actual data
        result = reader.validate(dataframe_with_duplicates)
        assert result is True
    
    def test_base_reader_validate_empty(self, spark, empty_dataframe):
        """Test base reader validation with empty data."""
        config = {'path': 'test.csv', 'options': {}}
        reader = CSVReader(spark, config)
        
        # Empty data should still pass validation
        result = reader.validate(empty_dataframe)
        assert result is True
