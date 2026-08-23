# Data Ingestion Framework (DIF)

**Scalable, config-driven data ingestion framework using PySpark with Facade and Factory design patterns**

## Overview

The Data Ingestion Framework (DIF) is a production-ready, enterprise-grade framework designed for seamless data ingestion from multiple cloud storage providers and databases into various formats and destinations. It leverages:

- **Design Patterns**: Facade and Factory patterns for clean, extensible architecture
- **PySpark**: Distributed data processing at scale
- **Configuration-Driven**: All settings managed via YAML, no code changes needed
- **Multi-Format Support**: CSV, JSON, Parquet, ORC, Avro, XML, Excel, and Text
- **Multi-Source/Target Support**: Cloud storage (S3, GCS, Azure) and Databases (Snowflake, BigQuery, Redshift, PostgreSQL, MySQL, Databricks)

## Key Features

### 1. **Facade Pattern - Simplified Interface**
The `IngestionFacade` provides a single, simple interface for:
- Complete ingestion pipelines
- Read-only operations
- Data preview and statistics
- Validation and transformation

### 2. **Factory Pattern - Reader/Writer Creation**
- **ReaderFactory**: Automatically creates appropriate readers based on source type and format
- **WriterFactory**: Automatically creates appropriate writers based on target type and format
- No manual instantiation needed

### 3. **Configuration-Driven Architecture**
All parameters externalized via YAML configuration:
- Source and target configuration
- Spark settings
- Data processing options
- Environment variable support for sensitive data

### 4. **Supported Formats**

**File Formats:**
- CSV with custom delimiters
- JSON with schema inference
- Parquet with compression
- ORC for columnar storage
- Avro for schema evolution
- XML with custom tags
- Excel (XLSX/XLS)
- Text/Plaintext

**Cloud Storage:**
- AWS S3
- Google Cloud Storage (GCS)
- Azure Blob Storage
- HDFS
- Local filesystem

**Databases/Warehouses:**
- Snowflake
- Google BigQuery
- AWS Redshift
- PostgreSQL
- MySQL
- Databricks

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   IngestionFacade                           │
│        (Simplified Interface - Entry Point)                 │
└────────────┬────────────────────────────────────────────────┘
             │
     ┌───────┴────────┐
     │                │
┌────▼─────────┐  ┌───▼──────────┐
│ReaderFactory  │  │WriterFactory │
│               │  │              │
└────┬─────────┘  └───┬──────────┘
     │                │
┌────▼──────────────┬─▼──────────────────┐
│  File Readers    │  Database Readers  │
│  ├─ CSV          │  ├─ Snowflake      │
│  ├─ JSON         │  ├─ BigQuery       │
│  ├─ Parquet      │  ├─ Redshift       │
│  ├─ ORC          │  ├─ PostgreSQL     │
│  ├─ Avro         │  ├─ MySQL          │
│  ├─ XML          │  └─ Databricks     │
│  ├─ Excel        │                    │
│  └─ Text         │                    │
└────────────────────────────────────────┘

┌────────────────────────────────────────┐
│  File Writers    │  Database Writers  │
│  ├─ CSV          │  ├─ Snowflake      │
│  ├─ JSON         │  ├─ BigQuery       │
│  ├─ Parquet      │  ├─ Redshift       │
│  ├─ ORC          │  ├─ PostgreSQL     │
│  ├─ Avro         │  ├─ MySQL          │
│  ├─ XML          │  └─ Databricks     │
│  ├─ Excel        │                    │
│  └─ Text         │                    │
└────────────────────────────────────────┘

┌──────────────────────┐
│  SparkManager        │
│  ├─ Session mgmt    │
│  ├─ Configuration   │
│  └─ Optimization    │
└──────────────────────┘

┌──────────────────────┐
│  ConfigLoader        │
│  ├─ YAML parsing    │
│  ├─ Env substitution│
│  └─ Validation      │
└──────────────────────┘
```

## Directory Structure

```
DIF/
├── main.py                          # Entry point
├── requirements.txt                 # Dependencies
├── README.md                        # This file
├── config/
│   └── sample_config.yaml          # Sample configuration
├── logs/                           # Log files
│   └── ingestion.log
├── src/
│   ├── __init__.py
│   ├── config/
│   │   ├── __init__.py
│   │   └── config_loader.py        # YAML config loading with env substitution
│   ├── spark/
│   │   ├── __init__.py
│   │   └── spark_manager.py        # Spark session management
│   ├── facade/
│   │   ├── __init__.py
│   │   └── ingestion_facade.py     # Main facade implementation
│   ├── readers/
│   │   ├── __init__.py
│   │   ├── base_reader.py          # Abstract base class
│   │   ├── reader_factory.py       # Factory for reader creation
│   │   ├── file_readers.py         # CSV, JSON, Parquet, etc.
│   │   └── database_readers.py     # Snowflake, BigQuery, etc.
│   └── writers/
│       ├── __init__.py
│       ├── base_writer.py          # Abstract base class
│       ├── writer_factory.py       # Factory for writer creation
│       ├── file_writers.py         # CSV, JSON, Parquet, etc.
│       └── database_writers.py     # Snowflake, BigQuery, etc.
└── tests/                          # Unit tests
    ├── test_readers.py
    ├── test_writers.py
    ├── test_facade.py
    └── test_config_loader.py
```

## Installation

### Prerequisites
- Python 3.8+
- Java 8+ (for Spark)
- Spark 3.5.0+

### Setup

```bash
# Clone the repository
git clone https://github.com/nileshpatil1992/DIF.git
cd DIF

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create logs directory
mkdir -p logs
```

## Configuration

### Configuration File Format

The framework uses YAML for configuration. Here's the structure:

```yaml
ingestion:
  # Source Configuration
  source:
    type: "s3"                    # Cloud storage or database type
    format: "csv"                 # File format
    path: "s3://bucket/path"      # Source path
    options:
      header: true
      inferSchema: true
      delimiter: ","
      encoding: "utf-8"
    credentials:
      access_key: "${AWS_ACCESS_KEY_ID}"    # Env var substitution
      secret_key: "${AWS_SECRET_ACCESS_KEY}"
      region: "us-east-1"

  # Target Configuration
  target:
    type: "snowflake"             # Target destination
    format: "parquet"
    path: "s3://bucket/output"
    options:
      mode: "overwrite"            # overwrite, append, ignore, error
      partitionBy: ["date", "region"]
      repartition: 4
    database_config:               # For database targets
      host: "${DB_HOST}"
      port: 5432
      database: "${DB_NAME}"
      user: "${DB_USER}"
      password: "${DB_PASSWORD}"
      table: "target_table"

  # Spark Configuration
  spark:
    master: "local[*]"             # local[*], yarn, k8s
    app_name: "DIF_Ingestion"
    cores: 4
    memory: "4g"
    driver_memory: "2g"
    executor_memory: "2g"
    shuffle_partitions: 200

  # Processing Configuration
  processing:
    validate: true                 # Data validation
    dedup: false                   # Remove duplicates
    sample_data: false             # Sample data
    sample_fraction: 0.1
    cache_intermediate: false      # Cache intermediate data

  # Logging
  logging:
    level: "INFO"
    log_file: "logs/ingestion.log"
```

### Environment Variables

The framework supports environment variable substitution in configuration using `${VAR_NAME}` syntax:

```bash
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"
export DB_HOST="your-db-host"
export DB_NAME="your-db-name"
export DB_USER="your-db-user"
export DB_PASSWORD="your-db-password"
```

## Usage

### 1. Full Ingestion Pipeline

```bash
python main.py --config config/sample_config.yaml --mode ingest
```

This will:
1. Load configuration from YAML
2. Create reader based on source type and format
3. Read data from source
4. Validate data (if enabled)
5. Apply transformations (dedup, sampling, etc.)
6. Create writer based on target type and format
7. Write data to target
8. Cleanup resources

### 2. Read-Only Mode

```bash
python main.py --config config/sample_config.yaml --mode read-only
```

Reads data without writing. Useful for validation and inspection.

### 3. Preview Data

```bash
python main.py --config config/sample_config.yaml --mode preview --preview 20
```

Displays first 20 rows from source with schema information.

### 4. Get Statistics

```bash
python main.py --config config/sample_config.yaml --mode statistics
```

Computes and displays:
- Row count
- Column count
- Column names
- Data schema

### Python API Usage

```python
from src.facade.ingestion_facade import IngestionFacade

# Initialize facade
facade = IngestionFacade('config/sample_config.yaml')

# Full ingestion
facade.ingest()

# Or read-only
df = facade.read_only()
df.show()

# Or get preview
facade.get_source_preview(num_rows=10)

# Or get statistics
stats = facade.get_statistics()
print(stats)
```

## Configuration Examples

### Example 1: S3 CSV to Snowflake Parquet

```yaml
ingestion:
  source:
    type: "s3"
    format: "csv"
    path: "s3://data-bucket/input/customers.csv"
    options:
      header: true
      inferSchema: true
      delimiter: ","
    credentials:
      access_key: "${AWS_ACCESS_KEY_ID}"
      secret_key: "${AWS_SECRET_ACCESS_KEY}"
      region: "us-east-1"

  target:
    type: "snowflake"
    format: "parquet"
    options:
      mode: "overwrite"
      repartition: 4
    database_config:
      host: "${SNOWFLAKE_HOST}"
      user: "${SNOWFLAKE_USER}"
      password: "${SNOWFLAKE_PASSWORD}"
      database: "analytics_db"
      schema: "raw"
      table: "customers"

  spark:
    master: "local[*]"
    cores: 4
    executor_memory: "4g"

  processing:
    validate: true
    dedup: false
```

### Example 2: JSON from GCS to BigQuery

```yaml
ingestion:
  source:
    type: "gcs"
    format: "json"
    path: "gs://data-bucket/input/events.json"
    options:
      inferSchema: true
    credentials:
      project_id: "${GCP_PROJECT_ID}"
      service_account: "${GCP_SERVICE_ACCOUNT_JSON}"

  target:
    type: "bigquery"
    format: "json"
    database_config:
      project_id: "${GCP_PROJECT_ID}"
      dataset: "raw_data"
      table: "events"

  spark:
    master: "local[*]"
    cores: 8
    executor_memory: "8g"

  processing:
    validate: true
    cache_intermediate: true
```

### Example 3: PostgreSQL to Parquet (Local)

```yaml
ingestion:
  source:
    type: "postgresql"
    format: "jdbc"
    database_config:
      host: "${DB_HOST}"
      port: 5432
      database: "${DB_NAME}"
      user: "${DB_USER}"
      password: "${DB_PASSWORD}"
      table: "customers"

  target:
    type: "local"
    format: "parquet"
    path: "/data/output/customers.parquet"
    options:
      mode: "overwrite"
      compression: "snappy"
      repartition: 4

  spark:
    master: "local[*]"
    executor_memory: "4g"

  processing:
    validate: true
    dedup: true
```

### Example 4: Excel to CSV with Transformations

```yaml
ingestion:
  source:
    type: "local"
    format: "xlsx"
    path: "/data/input/sales.xlsx"
    options:
      header: true
      inferSchema: true
      sheetName: "0"

  target:
    type: "s3"
    format: "csv"
    path: "s3://data-bucket/output/sales.csv"
    options:
      mode: "overwrite"
      header: true
      delimiter: ","
      repartition: 2

  spark:
    master: "local[*]"
    executor_memory: "2g"

  processing:
    validate: true
    dedup: true
    sample_data: false
```

## Design Patterns

### 1. Facade Pattern

**Purpose**: Provide unified interface to complex subsystems

**Implementation**: `IngestionFacade` class

**Benefits**:
- Simple API for end users
- Hides complexity of reader/writer creation
- Orchestrates the complete pipeline
- Single point of entry

```python
# User only interacts with facade
facade = IngestionFacade(config_path)
facade.ingest()  # All complexity hidden
```

### 2. Factory Pattern

**Purpose**: Create objects without specifying exact classes

**Implementation**: `ReaderFactory` and `WriterFactory` classes

**Benefits**:
- Easy to add new format/source support
- No tight coupling to specific implementations
- Configuration-driven object creation
- Extensible without modifying existing code

```python
# Factory creates appropriate reader based on type/format
reader = ReaderFactory.create_reader(
    spark,
    source_type='s3',
    file_format='csv',
    config={...}
)
# Factory handles instantiation details
```

### 3. Abstract Base Classes

**Purpose**: Define interface that subclasses must implement

**Implementation**: `BaseReader` and `BaseWriter` classes

**Benefits**:
- Enforces consistent interface
- Enables polymorphism
- Easier to add new implementations

```python
class CSVReader(BaseReader):
    def read(self) -> DataFrame:
        # Implementation specific to CSV
        pass
```

## Extending the Framework

### Adding a New File Format

1. Create reader in `src/readers/file_readers.py`:

```python
class JSONLReader(BaseReader):
    """JSONL (JSON Lines) file reader"""
    
    def read(self) -> DataFrame:
        logger.info(f"Reading JSONL from: {self.path}")
        try:
            df = self.spark.read.json(self.path)
            logger.info(f"Successfully read JSONL with {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading JSONL: {e}")
            raise
```

2. Create writer in `src/writers/file_writers.py`:

```python
class JSONLWriter(BaseWriter):
    """JSONL (JSON Lines) file writer"""
    
    def write(self, df: DataFrame) -> None:
        logger.info(f"Writing JSONL to: {self.path}")
        try:
            df.write.mode(self.get_write_mode()).json(self.path)
            logger.info("Successfully wrote JSONL file")
        except Exception as e:
            logger.error(f"Error writing JSONL: {e}")
            raise
```

3. Register in factories:

```python
# In reader_factory.py
FILE_READERS = {
    'csv': CSVReader,
    'json': JSONReader,
    'jsonl': JSONLReader,  # Add new format
    # ...
}

# In writer_factory.py
FILE_WRITERS = {
    'csv': CSVWriter,
    'json': JSONWriter,
    'jsonl': JSONLWriter,  # Add new format
    # ...
}
```

4. Use in configuration:

```yaml
ingestion:
  source:
    type: "s3"
    format: "jsonl"  # Use new format
    path: "s3://bucket/data.jsonl"
```

### Adding a New Database Source

1. Create reader in `src/readers/database_readers.py`:

```python
class MongoDBReader(BaseReader):
    """MongoDB database reader"""
    
    def read(self) -> DataFrame:
        logger.info("Reading from MongoDB")
        try:
            db_config = self.config.get('database_config', {})
            df = self.spark.read.format("mongo") \
                .option("uri", db_config.get('connection_string')) \
                .option("database", db_config.get('database')) \
                .option("collection", db_config.get('collection')) \
                .load()
            logger.info(f"Successfully read from MongoDB with {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading from MongoDB: {e}")
            raise
```

2. Register in factory and use in configuration.

## Performance Optimization

### 1. Partitioning

```yaml
target:
  options:
    partitionBy: ["date", "region"]  # Partition by columns
    repartition: 4                   # Number of partitions
```

### 2. Spark Configuration

```yaml
spark:
  shuffle_partitions: 200            # For shuffle operations
  executor_memory: "8g"              # Increase for large datasets
  driver_memory: "4g"
  cores: 16                          # Use available cores
```

### 3. Data Caching

```yaml
processing:
  cache_intermediate: true            # Cache for multiple operations
```

### 4. Compression

```yaml
target:
  options:
    compression: "snappy"             # For Parquet: snappy, gzip, lz4
```

## Error Handling

The framework provides comprehensive error handling:

- Configuration validation errors
- Reader/writer instantiation errors
- Data reading/writing errors
- Spark session errors
- Validation errors

All errors are logged with full stack traces in `logs/ingestion.log`.

## Logging

Logging is configured to output to both console and file:

- **Console**: INFO level messages
- **File** (`logs/ingestion.log`): All messages including DEBUG

```python
import logging

logger = logging.getLogger(__name__)
logger.info("Information message")
logger.error("Error message", exc_info=True)
```

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run specific test
pytest tests/test_facade.py -v
```

## Troubleshooting

### Issue: Configuration file not found

```bash
# Ensure config path is correct
python main.py --config config/sample_config.yaml

# Use absolute path if needed
python main.py --config /absolute/path/to/config.yaml
```

### Issue: Environment variable not substituted

```bash
# Verify environment variable is set
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"

# Check logs for warning about missing variables
grep "Environment variable not found" logs/ingestion.log
```

### Issue: Out of memory

```yaml
spark:
  executor_memory: "16g"        # Increase memory
  driver_memory: "8g"
  cores: 8                      # Reduce cores if needed
```

### Issue: Slow ingestion

```yaml
processing:
  cache_intermediate: true       # Cache intermediate results

target:
  options:
    repartition: 32             # Increase partitions for parallelism
```

## Production Deployment

### 1. Docker Deployment

```dockerfile
FROM pyspark:3.5.0-python3

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

ENTRYPOINT ["python", "main.py"]
```

### 2. Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dif-ingestion
spec:
  replicas: 1
  template:
    spec:
      containers:
      - name: dif
        image: dif:latest
        env:
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: aws-credentials
              key: access-key
```

### 3. Environment Setup

```bash
# Production secrets management
export ENVIRONMENT="production"
export AWS_ACCESS_KEY_ID="prod-key"
export AWS_SECRET_ACCESS_KEY="prod-secret"
export DB_HOST="prod-db-host"
export DB_PASSWORD="prod-db-password"
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes
4. Add tests
5. Submit a pull request

## License

MIT License - See LICENSE file for details

## Support

For issues, questions, or suggestions:
- Open an issue on GitHub
- Check existing documentation
- Review logs in `logs/ingestion.log`

## Roadmap

- [ ] Delta Lake support
- [ ] Apache Iceberg support
- [ ] Streaming ingestion
- [ ] Data quality frameworks (Great Expectations)
- [ ] Cost optimization insights
- [ ] Web UI for configuration
- [ ] Scheduled ingestion support
- [ ] Data lineage tracking

## Version History

**v1.0.0** (2024-08-23)
- Initial release
- Support for 8+ file formats
- Support for 6+ databases
- Facade and Factory patterns
- Configuration-driven architecture
- Comprehensive logging
