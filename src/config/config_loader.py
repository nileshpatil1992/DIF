"""
Configuration Loader Module
Handles loading and parsing YAML configuration files with environment variable substitution
"""

import os
import yaml
from typing import Dict, Any
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class SourceConfig:
    """Source configuration data class"""
    type: str
    format: str
    path: str
    options: Dict[str, Any]
    credentials: Dict[str, str]


@dataclass
class TargetConfig:
    """Target configuration data class"""
    type: str
    format: str
    path: str
    options: Dict[str, Any]
    database_config: Dict[str, Any]


@dataclass
class SparkConfig:
    """Spark configuration data class"""
    master: str
    app_name: str
    cores: int
    memory: str
    driver_memory: str
    executor_memory: str
    shuffle_partitions: int


class ConfigLoader:
    """Load and parse YAML configuration files"""

    @staticmethod
    def load_config(config_path: str) -> Dict[str, Any]:
        """
        Load YAML configuration file with environment variable substitution
        
        Args:
            config_path: Path to YAML configuration file
            
        Returns:
            Parsed configuration dictionary
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            # Substitute environment variables
            config = ConfigLoader._substitute_env_variables(config)
            logger.info(f"Configuration loaded successfully from {config_path}")
            return config
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file: {e}")
            raise
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise

    @staticmethod
    def _substitute_env_variables(obj: Any) -> Any:
        """
        Recursively substitute environment variables in configuration
        Format: ${ENV_VAR_NAME}
        
        Args:
            obj: Configuration object (dict, list, or string)
            
        Returns:
            Configuration object with substituted environment variables
        """
        if isinstance(obj, dict):
            return {key: ConfigLoader._substitute_env_variables(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [ConfigLoader._substitute_env_variables(item) for item in obj]
        elif isinstance(obj, str):
            # Replace ${ENV_VAR} with environment variable value
            import re
            pattern = r'\$\{([^}]+)\}'
            
            def replace_env(match):
                env_var = match.group(1)
                value = os.getenv(env_var)
                if value is None:
                    logger.warning(f"Environment variable not found: {env_var}")
                    return match.group(0)
                return value
            
            return re.sub(pattern, replace_env, obj)
        else:
            return obj

    @staticmethod
    def get_source_config(config: Dict[str, Any]) -> SourceConfig:
        """Extract source configuration"""
        source = config.get('ingestion', {}).get('source', {})
        return SourceConfig(
            type=source.get('type'),
            format=source.get('format'),
            path=source.get('path'),
            options=source.get('options', {}),
            credentials=source.get('credentials', {})
        )

    @staticmethod
    def get_target_config(config: Dict[str, Any]) -> TargetConfig:
        """Extract target configuration"""
        target = config.get('ingestion', {}).get('target', {})
        return TargetConfig(
            type=target.get('type'),
            format=target.get('format'),
            path=target.get('path'),
            options=target.get('options', {}),
            database_config=target.get('database_config', {})
        )

    @staticmethod
    def get_spark_config(config: Dict[str, Any]) -> SparkConfig:
        """Extract spark configuration"""
        spark = config.get('ingestion', {}).get('spark', {})
        return SparkConfig(
            master=spark.get('master', 'local[*]'),
            app_name=spark.get('app_name', 'DIF_Ingestion'),
            cores=spark.get('cores', 4),
            memory=spark.get('memory', '4g'),
            driver_memory=spark.get('driver_memory', '2g'),
            executor_memory=spark.get('executor_memory', '2g'),
            shuffle_partitions=spark.get('shuffle_partitions', 200)
        )

    @staticmethod
    def get_processing_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract processing configuration"""
        return config.get('ingestion', {}).get('processing', {})
