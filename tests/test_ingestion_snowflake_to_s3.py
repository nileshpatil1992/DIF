from src.config.config_loader import SourceConfig, TargetConfig

from tests.ingestion_scenario_test_helper import assert_ingestion_flow


def test_snowflake_to_s3_ingestion():
    source_config = SourceConfig(
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
    )
    target_config = TargetConfig(
        type='s3',
        format='parquet',
        path='s3://test-bucket/output/orders/',
        options={'mode': 'overwrite', 'compression': 'snappy'},
        database_config={}
    )

    assert_ingestion_flow(source_config, target_config)
