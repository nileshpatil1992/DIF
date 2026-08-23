"""
Database Readers Module
Implementations for different database readers
"""

from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any
import logging
from src.readers.base_reader import BaseReader

logger = logging.getLogger(__name__)


class SnowflakeReader(BaseReader):
    """Snowflake database reader"""

    def read(self) -> DataFrame:
        """Read data from Snowflake"""
        logger.info("Reading data from Snowflake")
        try:
            db_config = self.config.get('database_config', {})
            
            df = self.spark.read.format("snowflake") \
                .option("sfUrl", db_config.get('host')) \
                .option("sfUser", db_config.get('user')) \
                .option("sfPassword", db_config.get('password')) \
                .option("sfDatabase", db_config.get('database')) \
                .option("sfSchema", db_config.get('schema', 'PUBLIC')) \
                .option("dbtable", db_config.get('table')) \
                .load()
            
            logger.info(f"Successfully read from Snowflake with {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading from Snowflake: {e}")
            raise


class BigQueryReader(BaseReader):
    """Google BigQuery reader"""

    def read(self) -> DataFrame:
        """Read data from BigQuery"""
        logger.info("Reading data from BigQuery")
        try:
            db_config = self.config.get('database_config', {})
            
            df = self.spark.read.format("bigquery") \
                .option("project", db_config.get('project_id')) \
                .option("parentProject", db_config.get('parent_project')) \
                .option("dataset", db_config.get('dataset')) \
                .option("table", db_config.get('table')) \
                .load()
            
            logger.info(f"Successfully read from BigQuery with {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading from BigQuery: {e}")
            raise


class RedshiftReader(BaseReader):
    """AWS Redshift reader"""

    def read(self) -> DataFrame:
        """Read data from Redshift"""
        logger.info("Reading data from Redshift")
        try:
            db_config = self.config.get('database_config', {})
            
            df = self.spark.read.format("redshift") \
                .option("url", f"jdbc:redshift://{db_config.get('host')}:{db_config.get('port', 5439)}/{db_config.get('database')}") \
                .option("user", db_config.get('user')) \
                .option("password", db_config.get('password')) \
                .option("dbtable", db_config.get('table')) \
                .load()
            
            logger.info(f"Successfully read from Redshift with {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading from Redshift: {e}")
            raise


class PostgreSQLReader(BaseReader):
    """PostgreSQL database reader"""

    def read(self) -> DataFrame:
        """Read data from PostgreSQL"""
        logger.info("Reading data from PostgreSQL")
        try:
            db_config = self.config.get('database_config', {})
            
            df = self.spark.read.format("jdbc") \
                .option("url", f"jdbc:postgresql://{db_config.get('host')}:{db_config.get('port', 5432)}/{db_config.get('database')}") \
                .option("dbtable", db_config.get('table')) \
                .option("user", db_config.get('user')) \
                .option("password", db_config.get('password')) \
                .option("driver", "org.postgresql.Driver") \
                .load()
            
            logger.info(f"Successfully read from PostgreSQL with {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading from PostgreSQL: {e}")
            raise


class MySQLReader(BaseReader):
    """MySQL database reader"""

    def read(self) -> DataFrame:
        """Read data from MySQL"""
        logger.info("Reading data from MySQL")
        try:
            db_config = self.config.get('database_config', {})
            
            df = self.spark.read.format("jdbc") \
                .option("url", f"jdbc:mysql://{db_config.get('host')}:{db_config.get('port', 3306)}/{db_config.get('database')}") \
                .option("dbtable", db_config.get('table')) \
                .option("user", db_config.get('user')) \
                .option("password", db_config.get('password')) \
                .option("driver", "com.mysql.jdbc.Driver") \
                .load()
            
            logger.info(f"Successfully read from MySQL with {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading from MySQL: {e}")
            raise


class DatabricksReader(BaseReader):
    """Databricks database reader"""

    def read(self) -> DataFrame:
        """Read data from Databricks"""
        logger.info("Reading data from Databricks")
        try:
            db_config = self.config.get('database_config', {})
            
            df = self.spark.read.format("databricks") \
                .option("host", db_config.get('host')) \
                .option("port", db_config.get('port', 443)) \
                .option("http_path", db_config.get('http_path')) \
                .option("user_agent_entry", "spark-python") \
                .option("schema", db_config.get('schema')) \
                .option("table", db_config.get('table')) \
                .load()
            
            logger.info(f"Successfully read from Databricks with {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading from Databricks: {e}")
            raise
