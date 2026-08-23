"""
Ingestion Facade Module
Facade pattern implementation providing simplified interface for data ingestion
"""

from pyspark.sql import DataFrame
from typing import Dict, Any
import logging
from src.config.config_loader import ConfigLoader, SourceConfig, TargetConfig, SparkConfig
from src.spark.spark_manager import SparkManager
from src.readers.reader_factory import ReaderFactory
from src.writers.writer_factory import WriterFactory

logger = logging.getLogger(__name__)


class IngestionFacade:
    """
    Facade class providing simplified interface for complete data ingestion process
    Handles configuration loading, reader/writer creation, and data pipeline execution
    """

    def __init__(self, config_path: str):
        """
        Initialize Ingestion Facade
        
        Args:
            config_path: Path to YAML configuration file
        """
        logger.info(f"Initializing Ingestion Facade with config: {config_path}")
        
        # Load configuration
        self.config = ConfigLoader.load_config(config_path)
        self.source_config = ConfigLoader.get_source_config(self.config)
        self.target_config = ConfigLoader.get_target_config(self.config)
        self.spark_config = ConfigLoader.get_spark_config(self.config)
        self.processing_config = ConfigLoader.get_processing_config(self.config)

        # Initialize Spark Manager
        self.spark_manager = SparkManager(self.spark_config.__dict__)
        self.spark = self.spark_manager.create_session()

        # Initialize reader and writer
        self.reader = None
        self.writer = None

    def ingest(self) -> None:
        """
        Execute complete ingestion pipeline
        1. Create reader based on source configuration
        2. Read data
        3. Validate data (if configured)
        4. Perform transformations (if configured)
        5. Create writer based on target configuration
        6. Write data
        """
        logger.info("Starting ingestion pipeline")
        
        try:
            # Step 1: Create reader
            logger.info(f"Creating reader for source: {self.source_config.type} ({self.source_config.format})")
            self.reader = ReaderFactory.create_reader(
                self.spark,
                self.source_config.type,
                self.source_config.format,
                self.source_config.__dict__
            )

            # Step 2: Read data
            logger.info("Reading source data")
            df = self.reader.read()
            
            # Step 3: Validate (if enabled)
            if self.processing_config.get('validate', False):
                logger.info("Validating data")
                if not self.reader.validate(df):
                    raise RuntimeError("Data validation failed")

            # Step 4: Perform transformations
            df = self._apply_transformations(df)

            # Step 5: Create writer
            logger.info(f"Creating writer for target: {self.target_config.type} ({self.target_config.format})")
            self.writer = WriterFactory.create_writer(
                self.spark,
                self.target_config.type,
                self.target_config.format,
                self.target_config.__dict__
            )

            # Step 6: Write data
            logger.info("Writing data to target")
            self.writer.write(df)

            logger.info("Ingestion pipeline completed successfully")
        
        except Exception as e:
            logger.error(f"Ingestion pipeline failed: {e}")
            raise
        
        finally:
            # Cleanup
            self.cleanup()

    def _apply_transformations(self, df: DataFrame) -> DataFrame:
        """
        Apply transformations to DataFrame based on processing configuration
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Applying transformations")

        # Deduplication
        if self.processing_config.get('dedup', False):
            logger.info("Removing duplicates")
            df = df.dropDuplicates()

        # Sampling
        if self.processing_config.get('sample_data', False):
            sample_fraction = self.processing_config.get('sample_fraction', 0.1)
            logger.info(f"Sampling data with fraction: {sample_fraction}")
            df = df.sample(fraction=sample_fraction)

        # Caching
        if self.processing_config.get('cache_intermediate', False):
            logger.info("Caching intermediate data")
            df = df.cache()

        return df

    def read_only(self) -> DataFrame:
        """
        Read data without writing (useful for validation and inspection)
        
        Returns:
            Spark DataFrame
        """
        logger.info("Executing read-only operation")
        
        try:
            logger.info(f"Creating reader for source: {self.source_config.type} ({self.source_config.format})")
            reader = ReaderFactory.create_reader(
                self.spark,
                self.source_config.type,
                self.source_config.format,
                self.source_config.__dict__
            )

            logger.info("Reading source data")
            df = reader.read()

            if self.processing_config.get('validate', False):
                logger.info("Validating data")
                reader.validate(df)

            return df

        except Exception as e:
            logger.error(f"Read-only operation failed: {e}")
            raise

    def get_source_preview(self, num_rows: int = 10) -> None:
        """
        Display preview of source data
        
        Args:
            num_rows: Number of rows to display
        """
        logger.info(f"Getting preview of source data ({num_rows} rows)")
        
        try:
            df = self.read_only()
            df.show(num_rows)
            logger.info(f"Schema: {df.printSchema()}")
        except Exception as e:
            logger.error(f"Error getting source preview: {e}")
            raise

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about source data
        
        Returns:
            Dictionary with data statistics
        """
        logger.info("Computing data statistics")
        
        try:
            df = self.read_only()
            
            stats = {
                'row_count': df.count(),
                'column_count': len(df.columns),
                'columns': df.columns,
                'schema': str(df.schema),
            }
            
            return stats
        except Exception as e:
            logger.error(f"Error computing statistics: {e}")
            raise

    def cleanup(self) -> None:
        """Clean up resources"""
        logger.info("Cleaning up resources")
        if self.spark_manager:
            self.spark_manager.stop_session()

    def __del__(self):
        """Destructor to ensure cleanup"""
        self.cleanup()
