"""
File Format Readers Module
Implementations for different file format readers
"""

from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any
import logging
from src.readers.base_reader import BaseReader

logger = logging.getLogger(__name__)


class CSVReader(BaseReader):
    """CSV file reader"""

    def read(self) -> DataFrame:
        """Read CSV file"""
        logger.info(f"Reading CSV from: {self.path}")
        try:
            df = self.spark.read.option("inferSchema", self.options.get("inferSchema", True)) \
                .option("header", self.options.get("header", True)) \
                .option("delimiter", self.options.get("delimiter", ",")) \
                .option("encoding", self.options.get("encoding", "utf-8")) \
                .csv(self.path)
            logger.info(f"Successfully read CSV file with {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise


class JSONReader(BaseReader):
    """JSON file reader"""

    def read(self) -> DataFrame:
        """Read JSON file"""
        logger.info(f"Reading JSON from: {self.path}")
        try:
            df = self.spark.read.option("inferSchema", self.options.get("inferSchema", True)) \
                .option("encoding", self.options.get("encoding", "utf-8")) \
                .json(self.path)
            logger.info(f"Successfully read JSON file with {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading JSON file: {e}")
            raise


class ParquetReader(BaseReader):
    """Parquet file reader"""

    def read(self) -> DataFrame:
        """Read Parquet file"""
        logger.info(f"Reading Parquet from: {self.path}")
        try:
            df = self.spark.read.parquet(self.path)
            logger.info(f"Successfully read Parquet file with {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading Parquet file: {e}")
            raise


class ORCReader(BaseReader):
    """ORC file reader"""

    def read(self) -> DataFrame:
        """Read ORC file"""
        logger.info(f"Reading ORC from: {self.path}")
        try:
            df = self.spark.read.orc(self.path)
            logger.info(f"Successfully read ORC file with {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading ORC file: {e}")
            raise


class AvroReader(BaseReader):
    """Avro file reader"""

    def read(self) -> DataFrame:
        """Read Avro file"""
        logger.info(f"Reading Avro from: {self.path}")
        try:
            df = self.spark.read.format("avro").load(self.path)
            logger.info(f"Successfully read Avro file with {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading Avro file: {e}")
            raise


class TextReader(BaseReader):
    """Text file reader"""

    def read(self) -> DataFrame:
        """Read Text file"""
        logger.info(f"Reading Text from: {self.path}")
        try:
            df = self.spark.read.text(self.path)
            logger.info(f"Successfully read Text file with {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading Text file: {e}")
            raise


class XMLReader(BaseReader):
    """XML file reader"""

    def read(self) -> DataFrame:
        """Read XML file"""
        logger.info(f"Reading XML from: {self.path}")
        try:
            # XML reader using spark-xml package (com.databricks:spark-xml)
            df = self.spark.read.format("xml") \
                .option("rootTag", self.options.get("rootTag", "root")) \
                .option("rowTag", self.options.get("rowTag", "row")) \
                .load(self.path)
            logger.info(f"Successfully read XML file with {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading XML file: {e}")
            raise


class ExcelReader(BaseReader):
    """Excel file reader"""

    def read(self) -> DataFrame:
        """Read Excel file"""
        logger.info(f"Reading Excel from: {self.path}")
        try:
            # Excel reader using spark-excel package (com.crealytics:spark-excel)
            df = self.spark.read.format("com.crealytics.spark.excel") \
                .option("header", self.options.get("header", True)) \
                .option("inferSchema", self.options.get("inferSchema", True)) \
                .option("sheetName", self.options.get("sheetName", "0")) \
                .load(self.path)
            logger.info(f"Successfully read Excel file with {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading Excel file: {e}")
            raise
