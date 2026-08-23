"""
Database Writers Module
Implementations for different database writers
"""

from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any
import logging
from src.writers.base_writer import BaseWriter

logger = logging.getLogger(__name__)


class SnowflakeWriter(BaseWriter):
    """Snowflake database writer"""

    def write(self, df: DataFrame) -> None:
        """Write data to Snowflake"""
        logger.info("Writing data to Snowflake")
        try:
            db_config = self.database_config
            
            writer = df.write.format("snowflake") \
                .option("sfUrl", db_config.get('host')) \
                .option("sfUser", db_config.get('user')) \
                .option("sfPassword", db_config.get('password')) \
                .option("sfDatabase", db_config.get('database')) \
                .option("sfSchema", db_config.get('schema', 'PUBLIC')) \
                .option("dbtable", db_config.get('table')) \
                .mode(self.get_write_mode())
            
            writer.save()
            logger.info("Successfully wrote to Snowflake")
        except Exception as e:
            logger.error(f"Error writing to Snowflake: {e}")
            raise


class BigQueryWriter(BaseWriter):
    """Google BigQuery writer"""

    def write(self, df: DataFrame) -> None:
        """Write data to BigQuery"""
        logger.info("Writing data to BigQuery")
        try:
            db_config = self.database_config
            
            writer = df.write.format("bigquery") \
                .option("project", db_config.get('project_id')) \
                .option("parentProject", db_config.get('parent_project')) \
                .option("dataset", db_config.get('dataset')) \
                .option("table", db_config.get('table')) \
                .mode(self.get_write_mode())
            
            writer.save()
            logger.info("Successfully wrote to BigQuery")
        except Exception as e:
            logger.error(f"Error writing to BigQuery: {e}")
            raise


class RedshiftWriter(BaseWriter):
    """AWS Redshift writer"""

    def write(self, df: DataFrame) -> None:
        """Write data to Redshift"""
        logger.info("Writing data to Redshift")
        try:
            db_config = self.database_config
            
            writer = df.write.format("redshift") \
                .option("url", f"jdbc:redshift://{db_config.get('host')}:{db_config.get('port', 5439)}/{db_config.get('database')}") \
                .option("user", db_config.get('user')) \
                .option("password", db_config.get('password')) \
                .option("dbtable", db_config.get('table')) \
                .mode(self.get_write_mode())
            
            writer.save()
            logger.info("Successfully wrote to Redshift")
        except Exception as e:
            logger.error(f"Error writing to Redshift: {e}")
            raise


class PostgreSQLWriter(BaseWriter):
    """PostgreSQL database writer"""

    def write(self, df: DataFrame) -> None:
        """Write data to PostgreSQL"""
        logger.info("Writing data to PostgreSQL")
        try:
            db_config = self.database_config
            
            writer = df.write.format("jdbc") \
                .option("url", f"jdbc:postgresql://{db_config.get('host')}:{db_config.get('port', 5432)}/{db_config.get('database')}") \
                .option("dbtable", db_config.get('table')) \
                .option("user", db_config.get('user')) \
                .option("password", db_config.get('password')) \
                .option("driver", "org.postgresql.Driver") \
                .mode(self.get_write_mode())
            
            writer.save()
            logger.info("Successfully wrote to PostgreSQL")
        except Exception as e:
            logger.error(f"Error writing to PostgreSQL: {e}")
            raise


class MySQLWriter(BaseWriter):
    """MySQL database writer"""

    def write(self, df: DataFrame) -> None:
        """Write data to MySQL"""
        logger.info("Writing data to MySQL")
        try:
            db_config = self.database_config
            
            writer = df.write.format("jdbc") \
                .option("url", f"jdbc:mysql://{db_config.get('host')}:{db_config.get('port', 3306)}/{db_config.get('database')}") \
                .option("dbtable", db_config.get('table')) \
                .option("user", db_config.get('user')) \
                .option("password", db_config.get('password')) \
                .option("driver", "com.mysql.jdbc.Driver") \
                .mode(self.get_write_mode())
            
            writer.save()
            logger.info("Successfully wrote to MySQL")
        except Exception as e:
            logger.error(f"Error writing to MySQL: {e}")
            raise


class DatabricksWriter(BaseWriter):
    """Databricks database writer"""

    def write(self, df: DataFrame) -> None:
        """Write data to Databricks"""
        logger.info("Writing data to Databricks")
        try:
            db_config = self.database_config
            
            writer = df.write.format("databricks") \
                .option("host", db_config.get('host')) \
                .option("port", db_config.get('port', 443)) \
                .option("http_path", db_config.get('http_path')) \
                .option("user_agent_entry", "spark-python") \
                .option("schema", db_config.get('schema')) \
                .option("table", db_config.get('table')) \
                .mode(self.get_write_mode())
            
            writer.save()
            logger.info("Successfully wrote to Databricks")
        except Exception as e:
            logger.error(f"Error writing to Databricks: {e}")
            raise
