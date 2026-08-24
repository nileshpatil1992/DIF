# Architecture & Design Decisions

## Overview

This document outlines the architectural decisions, design patterns, and rationale behind the Data Ingestion Framework implementation.

## System Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Client Application                        │
└────────────────────────┬────────────────────────────────────┘
                         │
         ┌───────────────▼────────────────┐
         │   IngestionFacade (Facade)    │
         │  - Pipeline Orchestration     │
         │  - Error Management           │
         │  - Resource Cleanup           │
         └───────────────┬────────────────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
    ┌───▼────┐    ┌──────▼──────┐   ┌────▼────┐
    │  Config │    │   Spark     │   │  Reader │
    │ Loader  │    │   Manager   │   │ Factory │
    │         │    │             │   │         │
    └────────┘    └─────────────┘   └────┬────┘
                                           │
                    ┌──────────────────────┼──────────────────────┐
                    │                      │                      │
             ┌──────▼──────┐       ┌───────▼────────┐      ┌────▼────────┐
             │ File Readers│       │ Database Readers│      �� Writer      │
             │ - CSV       │       │ - Snowflake     │      │ Factory     │
             │ - JSON      │       │ - BigQuery      │      │             │
             │ - Parquet   │       │ - Redshift      │      └────┬────────┘
             │ - ORC       │       │ - PostgreSQL    │           │
             │ - Avro      │       │ - MySQL         │    ┌──────▼──────────┐
             │ - XML       │       │ - Databricks    │    │  File Writers    │
             │ - Excel     │       └─────────────────┘    │  Database Writers│
             └─────────────┘                              └──────────────────┘
```

### Component Responsibilities

#### 1. IngestionFacade (Facade Pattern)
**Responsibility:** Simplify complex subsystem interactions

**Provides:**
- Single entry point for ingestion
- Pipeline orchestration
- Error handling and recovery
- Resource lifecycle management
- Data validation and statistics

**Does NOT:**
- Handle actual data reading/writing
- Manage Spark sessions directly
- Load configuration files
- Validate individual records

#### 2. ConfigLoader (Configuration)
**Responsibility:** Load and parse configuration

**Provides:**
- YAML file parsing
- Environment variable substitution
- Configuration validation
- Type-safe configuration objects

**Features:**
- Supports `${VAR_NAME}` syntax
- Recursive substitution
- Default values
- Error reporting

#### 3. SparkManager (Spark Management)
**Responsibility:** Manage Spark session lifecycle

**Provides:**
- Session creation and configuration
- Optimization settings
- Required package management
- Clean shutdown

**Separates:**
- Infrastructure concerns from business logic
- Configuration from instantiation
- Lifecycle from usage

#### 4. ReaderFactory & WriterFactory (Factory Pattern)
**Responsibility:** Create appropriate reader/writer instances

**Benefits:**
- Decouples creation from usage
- Enables easy format support addition
- Configuration-driven instantiation
- Error handling at creation time

**Strategy:**
- Type checking at factory, not usage
- Clear error messages
- Support matrix built-in

#### 5. BaseReader & BaseWriter (Abstract Base Classes)
**Responsibility:** Define contracts for implementations

**Provides:**
- Consistent interface across formats
- Common functionality (validation, options)
- Polymorphic behavior
- Type safety with ABC

### Design Pattern Decisions

#### Why Facade?
**Problem:** Data ingestion requires coordinating multiple complex subsystems
- Configuration loading
- Spark session management
- Reader/writer creation
- Pipeline orchestration
- Error handling and recovery

**Solution:** Facade pattern
- Simplifies client code
- Encapsulates complexity
- Provides consistent interface
- Easier testing and maintenance

**Example:**
```python
# Without Facade - Complex!
config = ConfigLoader.load_config('config.yaml')
spark_manager = SparkManager(config['spark'])
spark = spark_manager.create_session()
reader = ReaderFactory.create_reader(spark, ...)
df = reader.read()
df = apply_transformations(df)
writer = WriterFactory.create_writer(spark, ...)
writer.write(df)
spark_manager.stop_session()

# With Facade - Simple!
facade = IngestionFacade('config.yaml')
facade.ingest()
```

#### Why Factory?
**Problem:** Different file formats and data sources require different readers/writers
- Adding new format requires modifying factory logic
- Hard to extend without breaking existing code
- Client code coupled to concrete classes

**Solution:** Factory pattern
- Defers creation decisions to specialized classes
- New formats added via registration only
- Clients depend on abstractions
- Follows Open/Closed Principle

**Example:**
```python
# Without Factory - Tight coupling
def get_reader(format, spark, config):
    if format == 'csv':
        return CSVReader(spark, config)
    elif format == 'json':
        return JSONReader(spark, config)
    elif format == 'parquet':
        return ParquetReader(spark, config)
    # Adding new format requires modifying this method!

# With Factory - Extensible
class ReaderFactory:
    FILE_READERS = {
        'csv': CSVReader,
        'json': JSONReader,
        'parquet': ParquetReader,
        # New formats added via dictionary only!
    }
    
    @staticmethod
    def create_reader(spark, source_type, format, config):
        reader_class = FILE_READERS[format]
        return reader_class(spark, config)
```

#### Why Abstract Base Classes?
**Problem:** Different readers/writers need consistent interface
- Need to enforce contract
- Avoid implementation details in base class
- Enable polymorphic usage

**Solution:** Abstract Base Classes (ABC)
- Define required methods
- Enforce implementation
- Support polymorphism
- IDE autocompletion support

```python
from abc import ABC, abstractmethod

class BaseReader(ABC):
    @abstractmethod
    def read(self) -> DataFrame:
        """Must be implemented by subclasses."""
        pass
```

## Data Flow Architecture

### Ingestion Pipeline

```
┌──────────┐
│  Config  │
│   YAML   │
└────┬─────┘
     │
     ▼
┌──────────────────┐
│ ConfigLoader     │
│ - Load YAML      │
│ - Substitute Env │
│ - Validate       │
└────┬─────────────┘
     │
     ▼
┌──────────────────┐
│ SparkManager     │
│ - Create Session │
│ - Set Config     │
└────┬─────────────┘
     │
     ▼
┌──────────────────┐      ┌──────────────┐
│ ReaderFactory    │      │ Create Reader│
│ - Determine Type │──────►              │
└────┬─────────────┘      └──────┬───────┘
     │                           │
     ▼                           ▼
┌──────────────────┐      ┌──────────────┐
│ Source Data      │      │ Read Data    │
│ (S3/GCS/DB)      │◄─────│ (CSV/JSON)   │
└────┬─────────────┘      └──────┬───────┘
     │                           │
     ▼                           │
┌──────────────────────────────────────┐
│ Transform & Validate Data            │
│ - Deduplication                      │
│ - Sampling                           │
│ - Validation                         │
│ - Caching (optional)                 │
└────┬─────────────────────────────────┘
     │
     ▼
┌──────────────────────────────────────┐
│ WriterFactory                        │
│ - Determine Target Type              │
└────┬─────────────────────────────────┘
     │
     ▼
┌──────────────────────────────────────┐
│ Write Data                           │
│ - S3/GCS/Database                    │
│ - Partitioning                       │
│ - Compression                        │
└────┬─────────────────────────────────┘
     │
     ▼
┌──────────────────┐
│ Success/Error    │
│ Logging & Cleanup│
└──────────────────┘
```

## Error Handling Strategy

### Exception Hierarchy

```
Exception
├── ConfigError
│   └── Missing or invalid configuration
├── IngestionError
│   ├── Failed to read data
│   ├── Failed to write data
│   └── Pipeline execution failed
├── ValidationError
│   └── Data quality validation failed
└── FileNotFoundError (built-in)
    └── Source file not found
```

### Error Handling Approach

```python
try:
    # Attempt operation
    df = reader.read()
except FileNotFoundError as e:
    # Specific handling for known errors
    logger.error(f"Source file not found: {e}")
    raise IngestionError(f"Failed to read source: {e}") from e
except Exception as e:
    # Generic handling with context
    logger.error(f"Unexpected error: {e}", exc_info=True)
    raise
finally:
    # Cleanup regardless of success/failure
    self.cleanup()
```

## Configuration Architecture

### Configuration Hierarchy

```yaml
ingestion:                          # Root
  source:                           # Source configuration
    type: "s3"                      # Where to read from
    format: "csv"                   # What format to read
    path: "s3://bucket/input/"      # Path or connection string
    options:                        # Format-specific options
      header: true
      delimiter: ","
    credentials:                    # Optional credentials
      access_key: "${AWS_KEY}"
  
  target:                           # Target configuration
    type: "s3"                      # Where to write to
    format: "parquet"               # What format to write
    path: "s3://bucket/output/"     # Output path
    options:
      mode: "overwrite"
      partitionBy: ["date"]
    database_config:                # For database targets
      host: "${DB_HOST}"
      credentials: "${DB_USER}"
  
  spark:                            # Spark configuration
    master: "local[*]"
    cores: 4
    memory: "4g"
  
  processing:                       # Processing options
    validate: true
    dedup: false
    cache_intermediate: false
```

### Environment Variable Substitution

```python
# In YAML
path: "${S3_BUCKET}/input/data.csv"

# Automatically becomes
path = "my-bucket/input/data.csv"  # After substitution
```

## Testing Architecture

### Test Organization

```
tests/
├── __init__.py
├── conftest.py              # Shared fixtures
├── test_readers.py          # Reader tests
├── test_writers.py          # Writer tests
├── test_config.py           # Configuration tests
├── test_facade.py           # Facade tests
└── test_integration.py      # Integration tests
```

### Test Strategy

**Unit Tests (70%)**
- Test each component in isolation
- Mock external dependencies
- Fast execution
- High coverage

**Integration Tests (20%)**
- Test component interactions
- Use temporary files/databases
- Slower than unit tests
- Real behavior testing

**End-to-End Tests (10%)**
- Test complete workflows
- Real data sources
- Slowest but most realistic
- Critical path testing

## Deployment Architecture

### Deployment Options

1. **Local Development**
   - Python virtual environment
   - Local Spark cluster
   - File system storage

2. **Docker Container**
   - Containerized application
   - Self-contained environment
   - Easy distribution

3. **Kubernetes Cluster**
   - Orchestrated deployment
   - Auto-scaling
   - High availability

4. **Cloud Providers**
   - AWS (EMR, S3)
   - Google Cloud (Dataproc, GCS)
   - Azure (Databricks, ADLS)

## Performance Considerations

### Spark Optimization

1. **Partitioning**
   - Distribute data across executors
   - Parallel processing
   - Configured via `partitionBy` option

2. **Caching**
   - Avoid recomputation
   - Memory efficient
   - Controlled via `cache_intermediate` option

3. **Repartitioning**
   - Optimize for joins
   - Reduce shuffle
   - Configured via `repartition` option

4. **Compression**
   - Reduce storage size
   - Faster I/O
   - Codec selection per format

### Configuration Tuning

```yaml
spark:
  executor_memory: "8g"          # Increase for large datasets
  driver_memory: "4g"
  cores: 16                      # Match cluster capacity
  shuffle_partitions: 200        # Adjust for cluster size

target:
  options:
    repartition: 32              # More partitions = more parallelism
    compression: "snappy"        # Trade CPU for I/O
```

## Security Architecture

### Secrets Management

```python
# ✅ Secure: Environment variables
db_password = os.getenv('DB_PASSWORD')

# ✅ Secure: Configuration with env substitution
config_yaml = """
database:
  password: "${DB_PASSWORD}"
"""

# ❌ Insecure: Hardcoded
db_password = "super-secret-123"  # Never!
```

### Input Validation

```python
# Validate all inputs
if source_type not in SUPPORTED_SOURCES:
    raise ValueError(f"Unsupported source: {source_type}")

if not isinstance(config, dict):
    raise TypeError("Config must be dictionary")

if 'path' not in config:
    raise KeyError("'path' required in config")
```

## Extensibility Architecture

### Plugin Pattern

New formats/sources added by:
1. Creating new reader/writer class
2. Inheriting from base class
3. Registering in factory dictionary
4. No modification to existing code

### Example: Adding Parquet Format

```python
# 1. Create class (new file or existing)
class ParquetReader(BaseReader):
    def read(self) -> DataFrame:
        return self.spark.read.parquet(self.path)

# 2. Register (one line in factory)
FILE_READERS = {
    'parquet': ParquetReader,  # <- Add here
}

# 3. Use (no code changes needed)
ingestion:
  source:
    format: "parquet"  # Works immediately!
```

## Summary: Key Architectural Decisions

| Decision | Pattern | Benefit |
|----------|---------|----------|
| Simplified API | Facade | Easy to use |
| Format independence | Factory | Extensible |
| Consistent interface | ABC | Type-safe |
| Configuration-driven | YAML | Flexible |
| Environment variables | Substitution | Secrets safe |
| Specific exceptions | Custom | Better debugging |
| Component separation | SRP | Testable |
| Pipeline orchestration | Facade | Coordinated |
| Error handling | Try/Except | Robust |
| Testing strategy | Pyramid | Quality |

---

**Document Version:** 1.0.0
**Last Updated:** 2024-08-24
