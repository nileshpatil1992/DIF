# Unit tests for configuration loader

import pytest
import os
import tempfile
import yaml
from src.config.config_loader import ConfigLoader, SourceConfig, TargetConfig, SparkConfig


class TestConfigLoaderBasic:
    """Test basic configuration loading."""
    
    def test_config_loader_file_not_found(self):
        """Test loading from non-existent file."""
        with pytest.raises(FileNotFoundError):
            ConfigLoader.load_config('/nonexistent/path/config.yaml')
    
    def test_config_loader_with_file(self, sample_config):
        """Test loading configuration from file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(sample_config, f)
            config_path = f.name
        
        try:
            config = ConfigLoader.load_config(config_path)
            
            assert 'ingestion' in config
            assert 'source' in config['ingestion']
            assert 'target' in config['ingestion']
            assert 'spark' in config['ingestion']
        finally:
            os.unlink(config_path)
    
    def test_config_loader_invalid_yaml(self):
        """Test loading invalid YAML file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("{ invalid yaml: [")
            config_path = f.name
        
        try:
            with pytest.raises(yaml.YAMLError):
                ConfigLoader.load_config(config_path)
        finally:
            os.unlink(config_path)


class TestConfigLoaderEnvSubstitution:
    """Test environment variable substitution."""
    
    def test_env_substitution_string(self, sample_config):
        """Test environment variable substitution in strings."""
        os.environ['TEST_BUCKET'] = 'my-test-bucket'
        os.environ['TEST_REGION'] = 'us-west-2'
        
        sample_config['ingestion']['source']['path'] = '${TEST_BUCKET}'
        sample_config['ingestion']['source']['options']['region'] = '${TEST_REGION}'
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(sample_config, f)
            config_path = f.name
        
        try:
            config = ConfigLoader.load_config(config_path)
            
            assert config['ingestion']['source']['path'] == 'my-test-bucket'
            assert config['ingestion']['source']['options']['region'] == 'us-west-2'
        finally:
            os.unlink(config_path)
            del os.environ['TEST_BUCKET']
            del os.environ['TEST_REGION']
    
    def test_env_substitution_missing_var(self, sample_config):
        """Test behavior with missing environment variable."""
        sample_config['ingestion']['source']['path'] = '${NONEXISTENT_VAR}'
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(sample_config, f)
            config_path = f.name
        
        try:
            config = ConfigLoader.load_config(config_path)
            # Variable should remain unchanged if not found
            assert config['ingestion']['source']['path'] == '${NONEXISTENT_VAR}'
        finally:
            os.unlink(config_path)


class TestConfigLoaderExtraction:
    """Test configuration extraction methods."""
    
    def test_get_source_config(self, sample_config):
        """Test extracting source configuration."""
        source_config = ConfigLoader.get_source_config(sample_config)
        
        assert isinstance(source_config, SourceConfig)
        assert source_config.type == 's3'
        assert source_config.format == 'csv'
        assert source_config.path == 's3://test-bucket/input/data.csv'
        assert source_config.database_config == {}

    def test_get_source_config_with_database_config(self, sample_config):
        """Test extracting database source configuration."""
        sample_config['ingestion']['source'] = {
            'type': 'snowflake',
            'format': 'snowflake',
            'path': None,
            'options': {},
            'credentials': {},
            'database_config': {
                'host': 'test-account.snowflakecomputing.com',
                'user': 'test-user',
                'database': 'raw',
                'schema': 'public',
                'table': 'customers'
            }
        }

        source_config = ConfigLoader.get_source_config(sample_config)

        assert isinstance(source_config, SourceConfig)
        assert source_config.type == 'snowflake'
        assert source_config.database_config['host'] == 'test-account.snowflakecomputing.com'
        assert source_config.database_config['table'] == 'customers'
    
    def test_get_target_config(self, sample_config):
        """Test extracting target configuration."""
        target_config = ConfigLoader.get_target_config(sample_config)
        
        assert isinstance(target_config, TargetConfig)
        assert target_config.type == 's3'
        assert target_config.format == 'parquet'
        assert target_config.path == 's3://test-bucket/output/'
    
    def test_get_spark_config(self, sample_config):
        """Test extracting Spark configuration."""
        spark_config = ConfigLoader.get_spark_config(sample_config)
        
        assert isinstance(spark_config, SparkConfig)
        assert spark_config.master == 'local[*]'
        assert spark_config.app_name == 'test-app'
        assert spark_config.cores == 4
    
    def test_get_processing_config(self, sample_config):
        """Test extracting processing configuration."""
        processing_config = ConfigLoader.get_processing_config(sample_config)
        
        assert isinstance(processing_config, dict)
        assert processing_config['validate'] is True
        assert processing_config['dedup'] is False


class TestConfigLoaderDefaults:
    """Test configuration defaults."""
    
    def test_spark_config_defaults(self):
        """Test Spark configuration defaults."""
        minimal_config = {
            'ingestion': {'spark': {}}
        }
        
        spark_config = ConfigLoader.get_spark_config(minimal_config)
        
        assert spark_config.master == 'local[*]'
        assert spark_config.app_name == 'DIF_Ingestion'
        assert spark_config.cores == 4
    
    def test_processing_config_defaults(self):
        """Test processing configuration defaults."""
        minimal_config = {
            'ingestion': {'processing': {}}
        }
        
        processing_config = ConfigLoader.get_processing_config(minimal_config)
        
        assert isinstance(processing_config, dict)
