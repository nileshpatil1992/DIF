# Unit tests for facade

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame

from src.facade.ingestion_facade import IngestionFacade
from src.config.config_loader import SourceConfig, TargetConfig, SparkConfig


class TestIngestionFacadeInitialization:
    """Test facade initialization."""
    
    @patch('src.facade.ingestion_facade.ConfigLoader')
    @patch('src.facade.ingestion_facade.SparkManager')
    def test_facade_initialization(self, mock_spark_manager, mock_config_loader):
        """Test facade initialization."""
        # Mock configuration
        mock_config = {
            'ingestion': {
                'source': {'type': 's3', 'format': 'csv'},
                'target': {'type': 's3', 'format': 'parquet'},
                'spark': {'master': 'local[*]'},
                'processing': {}
            }
        }
        mock_config_loader.load_config.return_value = mock_config
        mock_config_loader.get_source_config.return_value = Mock()
        mock_config_loader.get_target_config.return_value = Mock()
        mock_config_loader.get_spark_config.return_value = Mock()
        mock_config_loader.get_processing_config.return_value = {}
        
        # Mock Spark manager
        mock_spark_session = MagicMock(spec=SparkSession)
        mock_spark_manager_instance = Mock()
        mock_spark_manager_instance.create_session.return_value = mock_spark_session
        mock_spark_manager.return_value = mock_spark_manager_instance
        
        # Test initialization
        facade = IngestionFacade('config.yaml')
        
        assert facade.config is not None
        assert facade.spark is not None


class TestIngestionFacadeReadOnly:
    """Test facade read-only operations."""
    
    @patch('src.facade.ingestion_facade.ReaderFactory')
    @patch('src.facade.ingestion_facade.ConfigLoader')
    @patch('src.facade.ingestion_facade.SparkManager')
    def test_read_only_operation(
        self,
        mock_spark_manager,
        mock_config_loader,
        mock_reader_factory
    ):
        """Test read-only operation."""
        # Setup mocks
        mock_config = {'ingestion': {'source': {}, 'target': {}, 'spark': {}, 'processing': {}}}
        mock_config_loader.load_config.return_value = mock_config
        mock_config_loader.get_source_config.return_value = Mock()
        mock_config_loader.get_target_config.return_value = Mock()
        mock_config_loader.get_spark_config.return_value = Mock()
        mock_config_loader.get_processing_config.return_value = {}
        
        mock_spark_session = MagicMock(spec=SparkSession)
        mock_spark_manager_instance = Mock()
        mock_spark_manager_instance.create_session.return_value = mock_spark_session
        mock_spark_manager.return_value = mock_spark_manager_instance
        
        # Mock reader
        mock_df = MagicMock(spec=DataFrame)
        mock_reader = Mock()
        mock_reader.read.return_value = mock_df
        mock_reader_factory.create_reader.return_value = mock_reader
        
        # Test read-only
        facade = IngestionFacade('config.yaml')
        result = facade.read_only()
        
        assert result is not None
        mock_reader.read.assert_called_once()


class TestIngestionFacadePreview:
    """Test facade preview operations."""
    
    @patch('src.facade.ingestion_facade.ReaderFactory')
    @patch('src.facade.ingestion_facade.ConfigLoader')
    @patch('src.facade.ingestion_facade.SparkManager')
    def test_source_preview(
        self,
        mock_spark_manager,
        mock_config_loader,
        mock_reader_factory
    ):
        """Test source preview."""
        # Setup mocks
        mock_config = {'ingestion': {'source': {}, 'target': {}, 'spark': {}, 'processing': {}}}
        mock_config_loader.load_config.return_value = mock_config
        mock_config_loader.get_source_config.return_value = Mock()
        mock_config_loader.get_target_config.return_value = Mock()
        mock_config_loader.get_spark_config.return_value = Mock()
        mock_config_loader.get_processing_config.return_value = {}
        
        mock_spark_session = MagicMock(spec=SparkSession)
        mock_spark_manager_instance = Mock()
        mock_spark_manager_instance.create_session.return_value = mock_spark_session
        mock_spark_manager.return_value = mock_spark_manager_instance
        
        # Mock reader and dataframe
        mock_df = MagicMock(spec=DataFrame)
        mock_df.show = Mock()
        mock_df.printSchema = Mock()
        mock_reader = Mock()
        mock_reader.read.return_value = mock_df
        mock_reader_factory.create_reader.return_value = mock_reader
        
        # Test preview
        facade = IngestionFacade('config.yaml')
        facade.get_source_preview(num_rows=5)
        
        mock_df.show.assert_called_with(5)


class TestIngestionFacadeStatistics:
    """Test facade statistics operations."""
    
    @patch('src.facade.ingestion_facade.ReaderFactory')
    @patch('src.facade.ingestion_facade.ConfigLoader')
    @patch('src.facade.ingestion_facade.SparkManager')
    def test_get_statistics(
        self,
        mock_spark_manager,
        mock_config_loader,
        mock_reader_factory
    ):
        """Test statistics retrieval."""
        # Setup mocks
        mock_config = {'ingestion': {'source': {}, 'target': {}, 'spark': {}, 'processing': {}}}
        mock_config_loader.load_config.return_value = mock_config
        mock_config_loader.get_source_config.return_value = Mock()
        mock_config_loader.get_target_config.return_value = Mock()
        mock_config_loader.get_spark_config.return_value = Mock()
        mock_config_loader.get_processing_config.return_value = {}
        
        mock_spark_session = MagicMock(spec=SparkSession)
        mock_spark_manager_instance = Mock()
        mock_spark_manager_instance.create_session.return_value = mock_spark_session
        mock_spark_manager.return_value = mock_spark_manager_instance
        
        # Mock reader and dataframe
        mock_df = MagicMock(spec=DataFrame)
        mock_df.count.return_value = 1000
        mock_df.columns = ['id', 'name', 'value']
        mock_df.schema = 'StructType([...])'
        mock_reader = Mock()
        mock_reader.read.return_value = mock_df
        mock_reader_factory.create_reader.return_value = mock_reader
        
        # Test statistics
        facade = IngestionFacade('config.yaml')
        stats = facade.get_statistics()
        
        assert stats['row_count'] == 1000
        assert stats['column_count'] == 3
        assert len(stats['columns']) == 3


class TestIngestionFacadeIngestionScenarios:
    """Test ingestion for supported source-target scenarios."""

    @pytest.mark.parametrize(
        ("source_config", "target_config"),
        [
            (
                SourceConfig(
                    type='s3',
                    format='csv',
                    path='s3://test-bucket/input/customers.csv',
                    options={'header': True, 'inferSchema': True},
                    credentials={'access_key': 'test-key', 'secret_key': 'test-secret'},
                    database_config={}
                ),
                TargetConfig(
                    type='s3',
                    format='parquet',
                    path='s3://test-bucket/output/customers/',
                    options={'mode': 'overwrite'},
                    database_config={}
                ),
            ),
            (
                SourceConfig(
                    type='snowflake',
                    format='snowflake',
                    path=None,
                    options={},
                    credentials={},
                    database_config={
                        'host': 'account.snowflakecomputing.com',
                        'user': 'test-user',
                        'password': 'test-password',
                        'database': 'raw',
                        'schema': 'public',
                        'table': 'customers'
                    }
                ),
                TargetConfig(
                    type='snowflake',
                    format='snowflake',
                    path=None,
                    options={'mode': 'overwrite'},
                    database_config={
                        'host': 'account.snowflakecomputing.com',
                        'user': 'test-user',
                        'password': 'test-password',
                        'database': 'analytics',
                        'schema': 'public',
                        'table': 'customers_copy'
                    }
                ),
            ),
            (
                SourceConfig(
                    type='snowflake',
                    format='snowflake',
                    path=None,
                    options={},
                    credentials={},
                    database_config={
                        'host': 'account.snowflakecomputing.com',
                        'user': 'test-user',
                        'password': 'test-password',
                        'database': 'raw',
                        'schema': 'public',
                        'table': 'orders'
                    }
                ),
                TargetConfig(
                    type='s3',
                    format='parquet',
                    path='s3://test-bucket/output/orders/',
                    options={'mode': 'overwrite', 'compression': 'snappy'},
                    database_config={}
                ),
            ),
            (
                SourceConfig(
                    type='s3',
                    format='json',
                    path='s3://test-bucket/input/events.json',
                    options={'inferSchema': True},
                    credentials={'access_key': 'test-key', 'secret_key': 'test-secret'},
                    database_config={}
                ),
                TargetConfig(
                    type='snowflake',
                    format='snowflake',
                    path=None,
                    options={'mode': 'append'},
                    database_config={
                        'host': 'account.snowflakecomputing.com',
                        'user': 'test-user',
                        'password': 'test-password',
                        'database': 'analytics',
                        'schema': 'public',
                        'table': 'events'
                    }
                ),
            ),
        ],
        ids=[
            's3-to-s3',
            'snowflake-to-snowflake',
            'snowflake-to-s3',
            's3-to-snowflake',
        ]
    )
    @patch('src.facade.ingestion_facade.WriterFactory')
    @patch('src.facade.ingestion_facade.ReaderFactory')
    @patch('src.facade.ingestion_facade.ConfigLoader')
    @patch('src.facade.ingestion_facade.SparkManager')
    def test_ingest_supported_source_target_combinations(
        self,
        mock_spark_manager,
        mock_config_loader,
        mock_reader_factory,
        mock_writer_factory,
        source_config,
        target_config
    ):
        """Test ingestion for supported source and target combinations."""
        mock_config_loader.load_config.return_value = {'ingestion': {}}
        mock_config_loader.get_source_config.return_value = source_config
        mock_config_loader.get_target_config.return_value = target_config
        mock_config_loader.get_spark_config.return_value = SparkConfig(
            master='local[*]',
            app_name='test-app',
            cores=1,
            memory='1g',
            driver_memory='1g',
            executor_memory='1g',
            shuffle_partitions=1
        )
        mock_config_loader.get_processing_config.return_value = {}

        mock_spark_session = MagicMock(spec=SparkSession)
        mock_spark_manager_instance = Mock()
        mock_spark_manager_instance.create_session.return_value = mock_spark_session
        mock_spark_manager.return_value = mock_spark_manager_instance

        mock_df = MagicMock(spec=DataFrame)
        mock_reader = Mock()
        mock_reader.read.return_value = mock_df
        mock_reader_factory.create_reader.return_value = mock_reader

        mock_writer = Mock()
        mock_writer_factory.create_writer.return_value = mock_writer

        facade = IngestionFacade('config.yaml')
        facade.ingest()

        mock_reader_factory.create_reader.assert_called_once_with(
            mock_spark_session,
            source_config.type,
            source_config.format,
            source_config.__dict__
        )
        mock_reader.read.assert_called_once_with()
        mock_writer_factory.create_writer.assert_called_once_with(
            mock_spark_session,
            target_config.type,
            target_config.format,
            target_config.__dict__
        )
        mock_writer.write.assert_called_once_with(mock_df)
        mock_spark_manager_instance.stop_session.assert_called_once_with()
