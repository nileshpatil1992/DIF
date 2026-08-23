"""
Base Writer Module
Abstract base class for all writers
"""

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class BaseWriter(ABC):
    """Abstract base class for all writers"""

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize writer
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.path = config.get('path')
        self.options = config.get('options', {})
        self.database_config = config.get('database_config', {})

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        """
        Write data to target
        
        Args:
            df: Spark DataFrame to write
        """
        pass

    def get_write_mode(self) -> str:
        """
        Get write mode from configuration
        
        Returns:
            Write mode (overwrite, append, ignore, error)
        """
        return self.options.get('mode', 'overwrite')

    def get_partition_columns(self) -> list:
        """
        Get partition columns from configuration
        
        Returns:
            List of partition columns
        """
        return self.options.get('partitionBy', [])

    def get_repartition_count(self) -> int:
        """
        Get repartition count from configuration
        
        Returns:
            Number of partitions
        """
        return self.options.get('repartition', 4)
