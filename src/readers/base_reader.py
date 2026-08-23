"""
Base Reader Module
Abstract base class for all readers
"""

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class BaseReader(ABC):
    """Abstract base class for all readers"""

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize reader
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.path = config.get('path')
        self.options = config.get('options', {})
        self.credentials = config.get('credentials', {})

    @abstractmethod
    def read(self) -> DataFrame:
        """
        Read data from source
        
        Returns:
            Spark DataFrame
        """
        pass

    def validate(self, df: DataFrame) -> bool:
        """
        Validate data after reading
        
        Args:
            df: Spark DataFrame
            
        Returns:
            True if valid, False otherwise
        """
        try:
            row_count = df.count()
            col_count = len(df.columns)
            logger.info(f"Data validation successful - Rows: {row_count}, Columns: {col_count}")
            return True
        except Exception as e:
            logger.error(f"Data validation failed: {e}")
            return False
