from src.config.config_loader import SourceConfig, TargetConfig

from tests.ingestion_scenario_test_helper import assert_ingestion_flow


def test_snowflake_to_snowflake_ingestion():
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
            'table': 'customers'
        }
    )
    target_config = TargetConfig(
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
    )

    assert_ingestion_flow(source_config, target_config)
