# Unit tests for writers

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame

from src.writers.file_writers import CSVWriter, ParquetWriter, JSONWriter
from src.writers.writer_factory import WriterFactory
from src.writers.base_writer import BaseWriter


class TestCSVWriter:
    """Test suite for CSV writer."""
    
    def test_csv_writer_initialization(self, spark):
        """Test CSV writer initialization."""
        config = {
            'path': 's3://test-bucket/output/data.csv',
            'options': {
                'mode': 'overwrite',
                'header': True,
                'delimiter': ','
            }
        }
        writer = CSVWriter(spark, config)
        
        assert writer.path == 's3://test-bucket/output/data.csv'
        assert writer.options['mode'] == 'overwrite'
    
    def test_csv_writer_write_mode(self, spark):
        """Test CSV writer write mode."""
        config = {
            'path': 'output.csv',
            'options': {'mode': 'append'}
        }
        writer = CSVWriter(spark, config)
        
        assert writer.get_write_mode() == 'append'
    
    def test_csv_writer_default_write_mode(self, spark):
        """Test CSV writer default write mode."""
        config = {'path': 'output.csv', 'options': {}}
        writer = CSVWriter(spark, config)
        
        assert writer.get_write_mode() == 'overwrite'
    
    def test_csv_writer_partition_columns(self, spark):
        """Test CSV writer partition columns."""
        config = {
            'path': 'output.csv',
            'options': {'partitionBy': ['date', 'region']}
        }
        writer = CSVWriter(spark, config)
        
        assert writer.get_partition_columns() == ['date', 'region']
    
    def test_csv_writer_repartition(self, spark):
        """Test CSV writer repartition count."""
        config = {
            'path': 'output.csv',
            'options': {'repartition': 8}
        }
        writer = CSVWriter(spark, config)
        
        assert writer.get_repartition_count() == 8
    
    def test_csv_writer_default_repartition(self, spark):
        """Test CSV writer default repartition count."""
        config = {'path': 'output.csv', 'options': {}}
        writer = CSVWriter(spark, config)
        
        assert writer.get_repartition_count() == 4


class TestParquetWriter:
    """Test suite for Parquet writer."""
    
    def test_parquet_writer_initialization(self, spark):
        """Test Parquet writer initialization."""
        config = {
            'path': 's3://bucket/output/data.parquet',
            'options': {'compression': 'snappy'}
        }
        writer = ParquetWriter(spark, config)
        
        assert writer.path == 's3://bucket/output/data.parquet'
        assert writer.options['compression'] == 'snappy'
    
    def test_parquet_writer_default_compression(self, spark):
        """Test Parquet writer default compression."""
        config = {'path': 'output.parquet', 'options': {}}
        writer = ParquetWriter(spark, config)
        
        assert writer.options.get('compression', 'snappy') == 'snappy'


class TestJSONWriter:
    """Test suite for JSON writer."""
    
    def test_json_writer_initialization(self, spark):
        """Test JSON writer initialization."""
        config = {
            'path': 's3://bucket/output/data.json',
            'options': {}
        }
        writer = JSONWriter(spark, config)
        
        assert writer.path == 's3://bucket/output/data.json'


class TestWriterFactory:
    """Test suite for writer factory."""
    
    def test_factory_creates_csv_writer(self, spark):
        """Test factory creates CSV writer."""
        config = {'path': 'output.csv', 'options': {}}
        
        writer = WriterFactory.create_writer(
            spark,
            's3',
            'csv',
            config
        )
        
        assert isinstance(writer, CSVWriter)
    
    def test_factory_creates_parquet_writer(self, spark):
        """Test factory creates Parquet writer."""
        config = {'path': 'output.parquet', 'options': {}}
        
        writer = WriterFactory.create_writer(
            spark,
            's3',
            'parquet',
            config
        )
        
        assert isinstance(writer, ParquetWriter)
    
    def test_factory_creates_json_writer(self, spark):
        """Test factory creates JSON writer."""
        config = {'path': 'output.json', 'options': {}}
        
        writer = WriterFactory.create_writer(
            spark,
            'gcs',
            'json',
            config
        )
        
        assert isinstance(writer, JSONWriter)
    
    def test_factory_unsupported_format(self, spark):
        """Test factory raises error for unsupported format."""
        config = {'path': 'output.xyz', 'options': {}}
        
        with pytest.raises(ValueError, match="Unsupported file format"):
            WriterFactory.create_writer(
                spark,
                's3',
                'xyz',
                config
            )
    
    def test_factory_unsupported_target(self, spark):
        """Test factory raises error for unsupported target."""
        config = {'path': 'output.csv', 'options': {}}
        
        with pytest.raises(ValueError, match="Unsupported target type"):
            WriterFactory.create_writer(
                spark,
                'ftp',
                'csv',
                config
            )
    
    def test_factory_supported_formats(self):
        """Test factory reports supported formats."""
        supported = WriterFactory.get_supported_formats()
        
        assert 'csv' in supported['file_formats']
        assert 'json' in supported['file_formats']
        assert 'parquet' in supported['file_formats']
        assert 'orc' in supported['file_formats']
        assert 'avro' in supported['file_formats']
    
    def test_factory_supported_cloud_storage(self):
        """Test factory reports supported cloud storage."""
        supported = WriterFactory.get_supported_formats()
        
        assert 's3' in supported['cloud_storage']
        assert 'gcs' in supported['cloud_storage']
        assert 'azure_blob' in supported['cloud_storage']
        assert 'local' in supported['cloud_storage']
