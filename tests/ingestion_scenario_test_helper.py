from unittest.mock import Mock, patch, MagicMock

from pyspark.sql import SparkSession, DataFrame

from src.config.config_loader import SourceConfig, TargetConfig, SparkConfig
from src.facade.ingestion_facade import IngestionFacade


def build_spark_config() -> SparkConfig:
    return SparkConfig(
        master='local[*]',
        app_name='test-app',
        cores=1,
        memory='1g',
        driver_memory='1g',
        executor_memory='1g',
        shuffle_partitions=1
    )


def assert_ingestion_flow(source_config: SourceConfig, target_config: TargetConfig) -> None:
    with patch('src.facade.ingestion_facade.WriterFactory') as mock_writer_factory, \
         patch('src.facade.ingestion_facade.ReaderFactory') as mock_reader_factory, \
         patch('src.facade.ingestion_facade.ConfigLoader') as mock_config_loader, \
         patch('src.facade.ingestion_facade.SparkManager') as mock_spark_manager:
        mock_config_loader.load_config.return_value = {'ingestion': {}}
        mock_config_loader.get_source_config.return_value = source_config
        mock_config_loader.get_target_config.return_value = target_config
        mock_config_loader.get_spark_config.return_value = build_spark_config()
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
