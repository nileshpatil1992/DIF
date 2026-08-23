# Coding Standards & Best Practices

## Overview

This document defines the coding standards, best practices, and conventions for the Data Ingestion Framework (DIF) project.

## Table of Contents

1. [Python Style Guide](#python-style-guide)
2. [Code Quality Standards](#code-quality-standards)
3. [Design Patterns](#design-patterns)
4. [Error Handling](#error-handling)
5. [Logging Standards](#logging-standards)
6. [Documentation Standards](#documentation-standards)
7. [Code Review Guidelines](#code-review-guidelines)
8. [Security Best Practices](#security-best-practices)

## Python Style Guide

### 1. PEP 8 Compliance

All code must follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) style guidelines.

#### Naming Conventions

```python
# ✅ CORRECT - Classes use PascalCase
class IngestionFacade:
    pass

class ReaderFactory:
    pass

# ✅ CORRECT - Functions and variables use snake_case
def create_reader():
    pass

config_path = "path/to/config"

# ✅ CORRECT - Constants use UPPER_CASE
MAX_RETRIES = 3
DEFAULT_TIMEOUT = 30

# ❌ INCORRECT - Inconsistent naming
def CreateReader():  # Should be create_reader
    pass

ConfigPath = "path/to/config"  # Should be config_path
```

#### Line Length

```python
# ✅ CORRECT - Lines under 88 characters (using Black formatter)
result = self.spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(self.path)

# ❌ INCORRECT - Line too long
result = self.spark.read.format("csv").option("header", True).option("inferSchema", True).csv(self.path)
```

### 2. Imports

```python
# ✅ CORRECT - Organized imports
# Standard library imports first
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any
import os
import logging

# Third-party imports
import yaml
from pyspark.sql import SparkSession, DataFrame

# Local imports
from src.config.config_loader import ConfigLoader
from src.readers.reader_factory import ReaderFactory

# ❌ INCORRECT - Random import order
from src.readers.reader_factory import ReaderFactory
import logging
from pyspark.sql import SparkSession
import os
import yaml
```

### 3. Type Hints

```python
# ✅ CORRECT - Always use type hints
def create_reader(
    spark: SparkSession,
    source_type: str,
    file_format: str,
    config: Dict[str, Any]
) -> BaseReader:
    """Create appropriate reader."""
    pass

def get_config() -> Dict[str, Any]:
    """Load configuration."""
    pass

# ❌ INCORRECT - Missing type hints
def create_reader(spark, source_type, file_format, config):
    pass
```

### 4. Docstrings

```python
# ✅ CORRECT - Google-style docstrings
class IngestionFacade:
    """Facade class providing simplified interface for data ingestion.
    
    Handles configuration loading, reader/writer creation, and data
    pipeline execution using the Facade design pattern.
    
    Attributes:
        config (Dict[str, Any]): Complete configuration dictionary
        spark (SparkSession): Spark session instance
        reader (BaseReader): Current reader instance
        writer (BaseWriter): Current writer instance
    """

    def ingest(self) -> None:
        """Execute complete ingestion pipeline.
        
        Performs the following steps:
        1. Create reader based on source configuration
        2. Read data from source
        3. Validate data (if configured)
        4. Apply transformations
        5. Create writer based on target configuration
        6. Write data to target
        
        Raises:
            FileNotFoundError: If source data not found
            ValueError: If configuration is invalid
            RuntimeError: If data validation fails
        """
        pass

# ❌ INCORRECT - Missing or poor docstrings
def ingest():
    # Do ingestion
    pass
```

## Code Quality Standards

### 1. SOLID Principles

#### Single Responsibility Principle (SRP)

```python
# ✅ CORRECT - Each class has single responsibility
class ConfigLoader:
    """Only handles configuration loading and parsing."""
    @staticmethod
    def load_config(config_path: str) -> Dict[str, Any]:
        pass

class SparkManager:
    """Only manages Spark session lifecycle."""
    def create_session(self) -> SparkSession:
        pass

# ❌ INCORRECT - Multiple responsibilities
class FrameworkManager:
    """Does everything - loading, spark, reading, writing."""
    def load_config(self):
        pass
    
    def create_spark(self):
        pass
    
    def read(self):
        pass
    
    def write(self):
        pass
```

#### Open/Closed Principle (OCP)

```python
# ✅ CORRECT - Open for extension, closed for modification
class BaseReader(ABC):
    """Abstract base for all readers."""
    @abstractmethod
    def read(self) -> DataFrame:
        pass

class CSVReader(BaseReader):
    """Extend with CSV-specific implementation."""
    def read(self) -> DataFrame:
        return self.spark.read.csv(self.path)

class JSONReader(BaseReader):
    """Extend with JSON-specific implementation."""
    def read(self) -> DataFrame:
        return self.spark.read.json(self.path)

# ❌ INCORRECT - Requires modification for each format
class Reader:
    def read(self, format):
        if format == 'csv':
            return self.spark.read.csv(self.path)
        elif format == 'json':
            return self.spark.read.json(self.path)
        elif format == 'parquet':
            return self.spark.read.parquet(self.path)
        # Need to modify this method for each new format
```

#### Liskov Substitution Principle (LSP)

```python
# ✅ CORRECT - Subclasses can replace parent class
def process_data(reader: BaseReader) -> DataFrame:
    """Works with any BaseReader subclass."""
    return reader.read()

# Can use any reader without breaking code
csv_reader = CSVReader(spark, config)
df = process_data(csv_reader)

json_reader = JSONReader(spark, config)
df = process_data(json_reader)
```

### 2. DRY (Don't Repeat Yourself)

```python
# ✅ CORRECT - Reusable methods
class BaseReader(ABC):
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.path = config.get('path')
        self.options = config.get('options', {})

    def validate(self, df: DataFrame) -> bool:
        """Shared validation logic."""
        try:
            row_count = df.count()
            col_count = len(df.columns)
            logger.info(f"Rows: {row_count}, Columns: {col_count}")
            return True
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return False

# All readers inherit this validation
class CSVReader(BaseReader):
    pass

# ❌ INCORRECT - Validation duplicated
class CSVReader(BaseReader):
    def validate(self, df):
        try:
            count = df.count()
            logger.info(f"Rows: {count}")
            return True
        except:
            return False

class JSONReader(BaseReader):
    def validate(self, df):
        try:
            count = df.count()
            logger.info(f"Rows: {count}")
            return True
        except:
            return False
```

### 3. Code Complexity

```python
# ✅ CORRECT - Keep methods focused and simple
def validate_config(config: Dict[str, Any]) -> bool:
    """Validate configuration structure."""
    required_keys = ['source', 'target', 'spark']
    return all(key in config.get('ingestion', {}) for key in required_keys)

def create_reader(spark: SparkSession, config: Dict[str, Any]) -> BaseReader:
    """Create reader using factory pattern."""
    source_config = config.get('source', {})
    return ReaderFactory.create_reader(
        spark,
        source_config.get('type'),
        source_config.get('format'),
        source_config
    )

# ❌ INCORRECT - Too complex, multiple responsibilities
def process_everything(spark, config):
    if 'source' not in config:
        raise ValueError("Missing source")
    if 'target' not in config:
        raise ValueError("Missing target")
    if 'spark' not in config:
        raise ValueError("Missing spark config")
    
    source_type = config['source']['type']
    if source_type == 's3':
        # Create S3 reader
        pass
    elif source_type == 'gcs':
        # Create GCS reader
        pass
    # ... many more conditions
```

## Design Patterns

### 1. Facade Pattern

```python
# ✅ CORRECT - Simplified interface
facade = IngestionFacade('config.yaml')
facade.ingest()  # Simple, clean API

# User doesn't need to know:
# - How to load config
# - How to create SparkSession
# - How to instantiate reader/writer
# - How to orchestrate pipeline
```

### 2. Factory Pattern

```python
# ✅ CORRECT - Object creation abstraction
reader = ReaderFactory.create_reader(
    spark,
    'csv',
    's3',
    config
)
# Factory handles:
# - Type validation
# - Appropriate class selection
# - Object instantiation
# - Error handling
```

### 3. Strategy Pattern (Implicit)

```python
# ✅ CORRECT - Different algorithms encapsulated
class BaseReader(ABC):
    @abstractmethod
    def read(self) -> DataFrame:
        pass

class CSVReader(BaseReader):
    def read(self) -> DataFrame:
        # CSV-specific reading strategy
        return self.spark.read.csv(self.path)

class JSONReader(BaseReader):
    def read(self) -> DataFrame:
        # JSON-specific reading strategy
        return self.spark.read.json(self.path)
```

## Error Handling

### 1. Exception Handling

```python
# ✅ CORRECT - Specific exceptions
try:
    df = self.spark.read.csv(self.path)
    logger.info(f"Successfully read CSV with {df.count()} rows")
    return df
except FileNotFoundError as e:
    logger.error(f"File not found: {self.path}")
    raise
except Exception as e:
    logger.error(f"Error reading CSV: {e}", exc_info=True)
    raise

# ❌ INCORRECT - Generic exception catching
try:
    df = self.spark.read.csv(self.path)
    return df
except:
    pass  # Silent failure
```

### 2. Custom Exceptions

```python
# ✅ CORRECT - Define custom exceptions
class ConfigError(Exception):
    """Raised when configuration is invalid."""
    pass

class IngestionError(Exception):
    """Raised during ingestion process."""
    pass

class ValidationError(Exception):
    """Raised when data validation fails."""
    pass

# Use in code
if not config:
    raise ConfigError("Configuration is empty")

# ❌ INCORRECT - Generic exceptions
if not config:
    raise Exception("Configuration is empty")
```

### 3. Error Propagation

```python
# ✅ CORRECT - Propagate with context
def ingest(self) -> None:
    try:
        reader = self._create_reader()
        df = reader.read()  # May raise exception
        self._write_data(df)  # May raise exception
    except FileNotFoundError as e:
        logger.error(f"Source data not found: {e}")
        raise IngestionError(f"Failed to ingest data: {e}") from e
    except Exception as e:
        logger.error(f"Unexpected error during ingestion: {e}", exc_info=True)
        raise
```

## Logging Standards

### 1. Logging Levels

```python
# ✅ CORRECT - Appropriate logging levels
logger.debug("Configuration parsed successfully")  # Development info
logger.info("Reading CSV from: /path/to/file")   # General progress
logger.warning("Null values found in column X")  # Potential issues
logger.error("Failed to write data to target")   # Error occurred
logger.critical("Spark session creation failed") # System critical
```

### 2. Logging Best Practices

```python
# ✅ CORRECT
logger.info(f"Processing file: {file_path}")
logger.error(f"Error reading file: {e}", exc_info=True)  # Include traceback
logger.info(f"Read {row_count:,} rows and {col_count} columns")

# ❌ INCORRECT
logger.info("Starting")  # Too vague
logger.error(f"Error: {e}")  # Missing context
logger.info(str(large_object))  # Logging large objects
```

### 3. Structured Logging

```python
# ✅ RECOMMENDED - Structured log entries
logger.info(
    "Ingestion started",
    extra={
        'source_type': source_type,
        'file_format': file_format,
        'row_count': row_count,
        'timestamp': datetime.now().isoformat()
    }
)
```

## Documentation Standards

### 1. Module Documentation

```python
# ✅ CORRECT
"""
Reader Factory Module

Factory pattern implementation for creating appropriate readers based on
source type and format. Supports multiple file formats and databases.

Example:
    >>> factory = ReaderFactory()
    >>> reader = factory.create_reader(spark, 'csv', 's3', config)
    >>> df = reader.read()
"""

import logging
from typing import Dict, Any
from pyspark.sql import SparkSession
```

### 2. Function Documentation

```python
# ✅ CORRECT
def create_reader(
    spark: SparkSession,
    source_type: str,
    file_format: str,
    config: Dict[str, Any]
) -> BaseReader:
    """Create appropriate reader based on source type and format.
    
    Determines the correct reader class based on source_type and
    file_format, instantiates it with the provided configuration,
    and returns the reader instance.
    
    Args:
        spark: Active SparkSession instance.
        source_type: Type of source (s3, gcs, postgres, etc.).
        file_format: File format (csv, json, parquet, etc.).
        config: Configuration dictionary with reader-specific options.
        
    Returns:
        Appropriate BaseReader subclass instance.
        
    Raises:
        ValueError: If source_type or file_format is not supported.
        ConfigError: If required configuration is missing.
        
    Example:
        >>> reader = create_reader(
        ...     spark,
        ...     's3',
        ...     'csv',
        ...     {'path': 's3://bucket/data.csv', 'options': {...}}
        ... )
        >>> df = reader.read()
    """
```

## Code Review Guidelines

### Checklist for Pull Requests

- [ ] **Code Quality**
  - [ ] Follows PEP 8 style guide
  - [ ] No code duplication
  - [ ] Appropriate abstraction level
  - [ ] No commented-out code
  - [ ] Meaningful variable/function names

- [ ] **Error Handling**
  - [ ] All exceptions are caught
  - [ ] Errors are logged appropriately
  - [ ] Error messages are descriptive
  - [ ] No silent failures

- [ ] **Testing**
  - [ ] New code has unit tests
  - [ ] All tests pass
  - [ ] Edge cases are covered
  - [ ] Test coverage > 80%

- [ ] **Documentation**
  - [ ] Functions have docstrings
  - [ ] Complex logic is explained
  - [ ] README updated if needed
  - [ ] Examples provided

- [ ] **Performance**
  - [ ] No unnecessary computations
  - [ ] Appropriate data structures
  - [ ] No N+1 problems
  - [ ] Memory efficient

- [ ] **Security**
  - [ ] No hardcoded secrets
  - [ ] Input validation
  - [ ] No SQL injection risks
  - [ ] Proper access controls

## Security Best Practices

### 1. Secrets Management

```python
# ✅ CORRECT - Use environment variables
import os

db_password = os.getenv('DB_PASSWORD')
if not db_password:
    raise ConfigError("DB_PASSWORD environment variable not set")

# Or use configuration with env var substitution
config = ConfigLoader.load_config('config.yaml')
# YAML can reference ${ENV_VAR}

# ❌ INCORRECT - Hardcoded secrets
db_password = "super_secret_password_123"  # NEVER!
connection_string = "postgres://user:pass@host:5432/db"
```

### 2. Input Validation

```python
# ✅ CORRECT - Validate all inputs
def create_reader(
    spark: SparkSession,
    source_type: str,
    file_format: str,
    config: Dict[str, Any]
) -> BaseReader:
    if not isinstance(source_type, str) or not source_type:
        raise ValueError("source_type must be non-empty string")
    
    if source_type not in ReaderFactory.SUPPORTED_SOURCES:
        raise ValueError(f"Unsupported source type: {source_type}")
    
    if not isinstance(config, dict):
        raise ValueError("config must be a dictionary")
    
    if 'path' not in config:
        raise ConfigError("'path' is required in config")
    
    # Proceed with reader creation
```

### 3. Secure File Operations

```python
# ✅ CORRECT - Secure file handling
import os
import stat

def read_config(config_path: str) -> Dict[str, Any]:
    """Read config file securely."""
    # Check file exists
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config not found: {config_path}")
    
    # Check file permissions (should not be world-readable)
    file_stat = os.stat(config_path)
    if file_stat.st_mode & stat.S_IROTH:
        logger.warning(f"Config file is world-readable: {config_path}")
    
    # Read file
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)  # Use safe_load, not load()
```

## Summary

- **Follow PEP 8** for style consistency
- **Use type hints** everywhere
- **Write docstrings** for all functions/classes
- **Apply SOLID principles** for better design
- **Handle errors explicitly** with specific exceptions
- **Log appropriately** for debugging and monitoring
- **Validate all inputs** for security
- **Keep it DRY** - no code duplication
- **Review code** before merging
- **Test thoroughly** with high coverage
