"""
Reader Factory Module
Factory pattern implementation for creating appropriate readers based on source type and format
"""

from pyspark.sql import SparkSession
from typing import Dict, Any
import logging
from src.readers.base_reader import BaseReader
from src.readers.file_readers import (
    CSVReader, JSONReader, ParquetReader, ORCReader, 
    AvroReader, TextReader, XMLReader, ExcelReader
)
from src.readers.database_readers import (
    SnowflakeReader, BigQueryReader, RedshiftReader,
    PostgreSQLReader, MySQLReader, DatabricksReader
)

logger = logging.getLogger(__name__)


class ReaderFactory:
    """Factory class for creating readers"""

    # File format readers mapping
    FILE_READERS = {
        'csv': CSVReader,
        'json': JSONReader,
        'parquet': ParquetReader,
        'orc': ORCReader,
        'avro': AvroReader,
        'txt': TextReader,
        'text': TextReader,
        'xml': XMLReader,
        'xlsx': ExcelReader,
        'xls': ExcelReader,
    }

    # Database readers mapping
    DATABASE_READERS = {
        'snowflake': SnowflakeReader,
        'bigquery': BigQueryReader,
        'redshift': RedshiftReader,
        'postgresql': PostgreSQLReader,
        'postgres': PostgreSQLReader,
        'mysql': MySQLReader,
        'databricks': DatabricksReader,
    }

    # File sources
    FILE_SOURCES = ['s3', 'gcs', 'azure_blob', 'local', 'hdfs']

    # Database sources
    DATABASE_SOURCES = ['snowflake', 'bigquery', 'redshift', 'postgresql', 'postgres', 'mysql', 'databricks']

    @staticmethod
    def create_reader(spark: SparkSession, source_type: str, file_format: str, config: Dict[str, Any]) -> BaseReader:
        """
        Create appropriate reader based on source type and format
        
        Args:
            spark: SparkSession instance
            source_type: Type of source (s3, gcs, snowflake, etc.)
            file_format: File format (csv, json, parquet, etc.)
            config: Configuration dictionary
            
        Returns:
            Appropriate reader instance
            
        Raises:
            ValueError: If source type or format is not supported
        """
        source_type_lower = source_type.lower()
        format_lower = file_format.lower()

        # Check if it's a file source
        if source_type_lower in ReaderFactory.FILE_SOURCES:
            logger.info(f"Creating file reader for format: {format_lower}")
            
            if format_lower not in ReaderFactory.FILE_READERS:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            reader_class = ReaderFactory.FILE_READERS[format_lower]
            return reader_class(spark, config)

        # Check if it's a database source
        elif source_type_lower in ReaderFactory.DATABASE_SOURCES:
            logger.info(f"Creating database reader for: {source_type_lower}")
            
            if source_type_lower not in ReaderFactory.DATABASE_READERS:
                raise ValueError(f"Unsupported database source: {source_type}")
            
            reader_class = ReaderFactory.DATABASE_READERS[source_type_lower]
            return reader_class(spark, config)

        else:
            raise ValueError(f"Unsupported source type: {source_type}")

    @staticmethod
    def get_supported_formats() -> Dict[str, list]:
        """
        Get list of supported file formats and database sources
        
        Returns:
            Dictionary with supported file formats and database sources
        """
        return {
            'file_formats': list(ReaderFactory.FILE_READERS.keys()),
            'database_sources': list(ReaderFactory.DATABASE_READERS.keys()),
            'cloud_storage': ReaderFactory.FILE_SOURCES
        }
