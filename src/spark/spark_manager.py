"""
Spark Manager Module
Handles Spark session creation and configuration
"""

from pyspark.sql import SparkSession
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class SparkManager:
    """Manages Spark session creation and configuration"""

    def __init__(self, spark_config: Dict[str, Any]):
        """
        Initialize Spark Manager
        
        Args:
            spark_config: Spark configuration dictionary
        """
        self.spark_config = spark_config
        self.spark = None

    def create_session(self) -> SparkSession:
        """
        Create and configure Spark session
        
        Returns:
            Configured SparkSession instance
        """
        logger.info("Creating Spark session")
        
        try:
            builder = SparkSession.builder \
                .master(self.spark_config.get('master', 'local[*]')) \
                .appName(self.spark_config.get('app_name', 'DIF_Ingestion')) \
                .config("spark.cores.max", str(self.spark_config.get('cores', 4))) \
                .config("spark.executor.memory", self.spark_config.get('executor_memory', '2g')) \
                .config("spark.driver.memory", self.spark_config.get('driver_memory', '2g')) \
                .config("spark.sql.shuffle.partitions", str(self.spark_config.get('shuffle_partitions', 200)))

            # Add cloud storage support packages if needed
            self.spark = builder.getOrCreate()
            
            # Configure additional settings
            self.spark.conf.set("spark.sql.adaptive.enabled", "true")
            self.spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
            
            logger.info("Spark session created successfully")
            return self.spark
        except Exception as e:
            logger.error(f"Error creating Spark session: {e}")
            raise

    def get_session(self) -> SparkSession:
        """
        Get existing Spark session or create new one
        
        Returns:
            SparkSession instance
        """
        if self.spark is None:
            self.spark = self.create_session()
        return self.spark

    def stop_session(self) -> None:
        """Stop Spark session"""
        if self.spark is not None:
            logger.info("Stopping Spark session")
            self.spark.stop()
            self.spark = None

    def add_jars(self, jar_paths: list) -> None:
        """
        Add external JAR files to Spark
        
        Args:
            jar_paths: List of JAR file paths
        """
        if self.spark is None:
            logger.warning("Spark session not initialized. Create session first.")
            return
        
        for jar_path in jar_paths:
            logger.info(f"Adding JAR: {jar_path}")
            self.spark.sparkContext.addJar(jar_path)

    @staticmethod
    def get_required_packages() -> list:
        """
        Get list of required Maven packages for the framework
        
        Returns:
            List of Maven package coordinates
        """
        return [
            # Spark XML support
            "com.databricks:spark-xml_2.12:0.17.0",
            # Spark Excel support
            "com.crealytics:spark-excel_2.12:3.5.0",
            # Snowflake connector
            "net.snowflake:snowflake-jdbc:3.14.4",
            "net.snowflake:spark-snowflake_2.12:2.14.0",
            # BigQuery connector
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.33.0",
            # PostgreSQL JDBC
            "org.postgresql:postgresql:42.7.0",
            # MySQL JDBC
            "mysql:mysql-connector-java:8.0.33",
            # AWS Redshift
            "com.amazon.redshift:redshift-jdbc42:2.1.0.5",
            # Databricks connector
            "com.databricks:databricks-sql-connector:1.0.8",
        ]
