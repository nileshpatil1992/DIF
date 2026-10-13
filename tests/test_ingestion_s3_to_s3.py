from src.config.config_loader import SourceConfig, TargetConfig

from tests.ingestion_scenario_test_helper import assert_ingestion_flow
from src.facade.ingestion_facade import IngestionFacade


def ingest() -> None:
        try:
            # Step 1: Create reader

            config = ConfigLoader.load_config(config_path)
            source_config = ConfigLoader.get_source_config(config)
            target_config = ConfigLoader.get_target_config(config)
            spark_config = ConfigLoader.get_spark_config(config)
            processing_config = ConfigLoader.get_processing_config(config)

            # Initialize Spark Manager
            spark_manager = SparkManager(spark_config.__dict__)
            spark = spark_manager.create_session()

            # Initialize reader and writer
            reader = None
            writer = None

            reader = ReaderFactory.create_reader(
                spark,
                source_config.type,
                source_config.format,
                source_config.__dict__
            )

            # Step 2: Read data

            df = reader.read()


            # Step 5: Create writer

            writer = WriterFactory.create_writer(
                spark,
                target_config.type,
                target_config.format,
                target_config.__dict__
            )

            # Step 6: Write data

            writer.write(df)



        except Exception as e:
            print("Ingestion Failed")
            raise

        finally:
            # Cleanup
            facade = IngestionFacade("config\sample_config.yaml")
            facade.cleanup()





ingest()
