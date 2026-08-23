"""
File Format Writers Module
Implementations for different file format writers
"""

from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any
import logging
from src.writers.base_writer import BaseWriter

logger = logging.getLogger(__name__)


class CSVWriter(BaseWriter):
    """CSV file writer"""

    def write(self, df: DataFrame) -> None:
        """Write data to CSV"""
        logger.info(f"Writing CSV to: {self.path}")
        try:
            writer = df.write.mode(self.get_write_mode())
            
            if self.get_partition_columns():
                writer = writer.partitionBy(*self.get_partition_columns())
            
            if self.get_repartition_count() > 0:
                df = df.repartition(self.get_repartition_count())
            
            writer.option("header", self.options.get("header", True)) \
                  .option("delimiter", self.options.get("delimiter", ",")) \
                  .option("encoding", self.options.get("encoding", "utf-8")) \
                  .csv(self.path)
            
            logger.info("Successfully wrote CSV file")
        except Exception as e:
            logger.error(f"Error writing CSV file: {e}")
            raise


class JSONWriter(BaseWriter):
    """JSON file writer"""

    def write(self, df: DataFrame) -> None:
        """Write data to JSON"""
        logger.info(f"Writing JSON to: {self.path}")
        try:
            writer = df.write.mode(self.get_write_mode())
            
            if self.get_partition_columns():
                writer = writer.partitionBy(*self.get_partition_columns())
            
            if self.get_repartition_count() > 0:
                df = df.repartition(self.get_repartition_count())
            
            writer.option("encoding", self.options.get("encoding", "utf-8")) \
                  .json(self.path)
            
            logger.info("Successfully wrote JSON file")
        except Exception as e:
            logger.error(f"Error writing JSON file: {e}")
            raise


class ParquetWriter(BaseWriter):
    """Parquet file writer"""

    def write(self, df: DataFrame) -> None:
        """Write data to Parquet"""
        logger.info(f"Writing Parquet to: {self.path}")
        try:
            writer = df.write.mode(self.get_write_mode())
            
            if self.get_partition_columns():
                writer = writer.partitionBy(*self.get_partition_columns())
            
            if self.get_repartition_count() > 0:
                df = df.repartition(self.get_repartition_count())
            
            writer.option("compression", self.options.get("compression", "snappy")) \
                  .parquet(self.path)
            
            logger.info("Successfully wrote Parquet file")
        except Exception as e:
            logger.error(f"Error writing Parquet file: {e}")
            raise


class ORCWriter(BaseWriter):
    """ORC file writer"""

    def write(self, df: DataFrame) -> None:
        """Write data to ORC"""
        logger.info(f"Writing ORC to: {self.path}")
        try:
            writer = df.write.mode(self.get_write_mode())
            
            if self.get_partition_columns():
                writer = writer.partitionBy(*self.get_partition_columns())
            
            if self.get_repartition_count() > 0:
                df = df.repartition(self.get_repartition_count())
            
            writer.option("compression", self.options.get("compression", "SNAPPY")) \
                  .orc(self.path)
            
            logger.info("Successfully wrote ORC file")
        except Exception as e:
            logger.error(f"Error writing ORC file: {e}")
            raise


class AvroWriter(BaseWriter):
    """Avro file writer"""

    def write(self, df: DataFrame) -> None:
        """Write data to Avro"""
        logger.info(f"Writing Avro to: {self.path}")
        try:
            writer = df.write.mode(self.get_write_mode()).format("avro")
            
            if self.get_partition_columns():
                writer = writer.partitionBy(*self.get_partition_columns())
            
            if self.get_repartition_count() > 0:
                df = df.repartition(self.get_repartition_count())
            
            writer.option("compression", self.options.get("compression", "snappy")) \
                  .save(self.path)
            
            logger.info("Successfully wrote Avro file")
        except Exception as e:
            logger.error(f"Error writing Avro file: {e}")
            raise


class TextWriter(BaseWriter):
    """Text file writer"""

    def write(self, df: DataFrame) -> None:
        """Write data to Text"""
        logger.info(f"Writing Text to: {self.path}")
        try:
            writer = df.write.mode(self.get_write_mode())
            
            if self.get_repartition_count() > 0:
                df = df.repartition(self.get_repartition_count())
            
            writer.text(self.path)
            
            logger.info("Successfully wrote Text file")
        except Exception as e:
            logger.error(f"Error writing Text file: {e}")
            raise


class XMLWriter(BaseWriter):
    """XML file writer"""

    def write(self, df: DataFrame) -> None:
        """Write data to XML"""
        logger.info(f"Writing XML to: {self.path}")
        try:
            writer = df.write.mode(self.get_write_mode()).format("xml")
            
            if self.get_repartition_count() > 0:
                df = df.repartition(self.get_repartition_count())
            
            writer.option("rootTag", self.options.get("rootTag", "root")) \
                  .option("rowTag", self.options.get("rowTag", "row")) \
                  .save(self.path)
            
            logger.info("Successfully wrote XML file")
        except Exception as e:
            logger.error(f"Error writing XML file: {e}")
            raise


class ExcelWriter(BaseWriter):
    """Excel file writer"""

    def write(self, df: DataFrame) -> None:
        """Write data to Excel"""
        logger.info(f"Writing Excel to: {self.path}")
        try:
            writer = df.write.mode(self.get_write_mode()).format("com.crealytics.spark.excel")
            
            if self.get_repartition_count() > 0:
                df = df.repartition(self.get_repartition_count())
            
            writer.option("header", self.options.get("header", True)) \
                  .option("sheetName", self.options.get("sheetName", "Sheet1")) \
                  .save(self.path)
            
            logger.info("Successfully wrote Excel file")
        except Exception as e:
            logger.error(f"Error writing Excel file: {e}")
            raise
