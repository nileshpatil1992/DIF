from src.config.config_loader import SourceConfig, TargetConfig

from tests.ingestion_scenario_test_helper import assert_ingestion_flow


def test_s3_to_snowflake_ingestion():
    source_config = SourceConfig(
        type='s3',
        format='json',
        path='s3://test-bucket/input/events.json',
        options={'inferSchema': True},
        credentials={'access_key': 'test-key', 'secret_key': 'test-secret'},
        database_config={}
    )
    target_config = TargetConfig(
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
    )

    assert_ingestion_flow(source_config, target_config)
