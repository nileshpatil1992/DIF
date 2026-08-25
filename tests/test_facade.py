# Unit tests for facade

from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame

from src.facade.ingestion_facade import IngestionFacade


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
