from src.config.config_loader import SourceConfig, TargetConfig

from tests.ingestion_scenario_test_helper import assert_ingestion_flow


def test_s3_to_s3_ingestion():
    source_config = SourceConfig(
        type='s3',
        format='csv',
        path='s3://test-bucket/input/customers.csv',
        options={'header': True, 'inferSchema': True},
        credentials={'access_key': 'test-key', 'secret_key': 'test-secret'},
        database_config={}
    )
    target_config = TargetConfig(
        type='s3',
        format='parquet',
        path='s3://test-bucket/output/customers/',
        options={'mode': 'overwrite'},
        database_config={}
    )

    assert_ingestion_flow(source_config, target_config)
