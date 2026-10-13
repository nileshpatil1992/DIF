

from src.config.config_loader import SourceConfig, TargetConfig
from tests.ingestion_scenario_test_helper import assert_ingestion_flow

# local csv to snowflake with overwrite
def test_local_csv_to_snowflake_overwrite():

    source_config = SourceConfig(
        type="local",
        format="csv",
        path=r"C:/Users/tejas/Downloads/sample-5mb.csv",
        options={
            "header": True,
            "inferSchema": True
        },
        credentials={},
        database_config={}
    )

    target_config = TargetConfig(
        type="snowflake",
        format="snowflake",
        path=None,
        options={
            "mode": "overwrite"
        },
        database_config={
            "host": "UOYZKCV-GAB60258.snowflakecomputing.com",
            "user": "TEJASMORE910",
            "password": "Tejasmore065@gmail.com",
            "database": "PYSPARK_DB",
            "schema": "PUBLIC",
            "table": "CUSTOMERS"
        }
    )

    assert_ingestion_flow(source_config, target_config)