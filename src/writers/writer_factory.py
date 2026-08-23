"""
Writer Factory Module
Factory pattern implementation for creating appropriate writers based on target type and format
"""

from pyspark.sql import SparkSession
from typing import Dict, Any
import logging
from src.writers.base_writer import BaseWriter
from src.writers.file_writers import (
    CSVWriter, JSONWriter, ParquetWriter, ORCWriter,
    AvroWriter, TextWriter, XMLWriter, ExcelWriter
)
from src.writers.database_writers import (
    SnowflakeWriter, BigQueryWriter, RedshiftWriter,
    PostgreSQLWriter, MySQLWriter, DatabricksWriter
)

logger = logging.getLogger(__name__)


class WriterFactory:
    """Factory class for creating writers"""

    # File format writers mapping
    FILE_WRITERS = {
        'csv': CSVWriter,
        'json': JSONWriter,
        'parquet': ParquetWriter,
        'orc': ORCWriter,
        'avro': AvroWriter,
        'txt': TextWriter,
        'text': TextWriter,
        'xml': XMLWriter,
        'xlsx': ExcelWriter,
        'xls': ExcelWriter,
    }

    # Database writers mapping
    DATABASE_WRITERS = {
        'snowflake': SnowflakeWriter,
        'bigquery': BigQueryWriter,
        'redshift': RedshiftWriter,
        'postgresql': PostgreSQLWriter,
        'postgres': PostgreSQLWriter,
        'mysql': MySQLWriter,
        'databricks': DatabricksWriter,
    }

    # File destinations
    FILE_DESTINATIONS = ['s3', 'gcs', 'azure_blob', 'local', 'hdfs']

    # Database destinations
    DATABASE_DESTINATIONS = ['snowflake', 'bigquery', 'redshift', 'postgresql', 'postgres', 'mysql', 'databricks']

    @staticmethod
    def create_writer(spark: SparkSession, target_type: str, file_format: str, config: Dict[str, Any]) -> BaseWriter:
        """
        Create appropriate writer based on target type and format
        
        Args:
            spark: SparkSession instance
            target_type: Type of target (s3, gcs, snowflake, etc.)
            file_format: File format (csv, json, parquet, etc.)
            config: Configuration dictionary
            
        Returns:
            Appropriate writer instance
            
        Raises:
            ValueError: If target type or format is not supported
        """
        target_type_lower = target_type.lower()
        format_lower = file_format.lower()

        # Check if it's a file destination
        if target_type_lower in WriterFactory.FILE_DESTINATIONS:
            logger.info(f"Creating file writer for format: {format_lower}")
            
            if format_lower not in WriterFactory.FILE_WRITERS:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            writer_class = WriterFactory.FILE_WRITERS[format_lower]
            return writer_class(spark, config)

        # Check if it's a database destination
        elif target_type_lower in WriterFactory.DATABASE_WRITERS:
            logger.info(f"Creating database writer for: {target_type_lower}")
            
            if target_type_lower not in WriterFactory.DATABASE_WRITERS:
                raise ValueError(f"Unsupported database target: {target_type}")
            
            writer_class = WriterFactory.DATABASE_WRITERS[target_type_lower]
            return writer_class(spark, config)

        else:
            raise ValueError(f"Unsupported target type: {target_type}")

    @staticmethod
    def get_supported_formats() -> Dict[str, list]:
        """
        Get list of supported file formats and database destinations
        
        Returns:
            Dictionary with supported file formats and database destinations
        """
        return {
            'file_formats': list(WriterFactory.FILE_WRITERS.keys()),
            'database_destinations': list(WriterFactory.DATABASE_WRITERS.keys()),
            'cloud_storage': WriterFactory.FILE_DESTINATIONS
        }
