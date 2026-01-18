# Production Campaign Finance Loader

A production-ready, high-performance data loader for campaign finance data with comprehensive deduplication, error handling, and monitoring capabilities.

## 🚀 Features

### Core Features
- **Batch Processing**: Memory-efficient processing of large datasets
- **Address Deduplication**: Automatic deduplication of addresses using global caching
- **Committee Deduplication**: Automatic deduplication of committees
- **Error Handling**: Comprehensive error tracking and recovery
- **Progress Tracking**: Real-time progress bars with rich UI
- **Performance Metrics**: Detailed performance statistics
- **Logging**: Comprehensive logging to file and console
- **Transaction Safety**: Automatic rollback on errors

### Configuration Presets
- **Development**: Small batches, limited records for testing
- **Testing**: Medium batches, moderate records for validation
- **Production**: Large batches, all records for production use
- **High Performance**: Very large batches for maximum throughput
- **Safe**: Small batches, frequent commits for data safety

## 📋 Requirements

```bash
# Install dependencies
uv add rich sqlmodel sqlalchemy psycopg2-binary
```

## 🎯 Quick Start

### Basic Usage
```bash
# Use default testing preset
uv run python production_loader.py

# Use specific preset
uv run python production_loader.py production

# Use specific preset and file
uv run python production_loader.py testing oklahoma_2020
```

### Available Presets
```bash
# Development - small batches, 100 records
uv run python production_loader.py development

# Testing - medium batches, 1000 records  
uv run python production_loader.py testing

# Production - large batches, all records
uv run python production_loader.py production

# High Performance - very large batches, all records
uv run python production_loader.py high_performance

# Safe - small batches, frequent commits
uv run python production_loader.py safe
```

### Available Files
- `oklahoma_2020`: Oklahoma 2020 Contribution/Loan Extract
- `oklahoma_2021`: Oklahoma 2021 Contribution/Loan Extract  
- `texas_sample`: Texas Sample Data

## ⚙️ Configuration

### Loader Configuration
```python
@dataclass
class LoaderConfig:
    batch_size: int = 100              # Records per batch
    max_records: Optional[int] = None  # Max records to process (None = all)
    commit_frequency: int = 50         # Commit every N batches
    enable_progress: bool = True       # Show progress bar
    enable_logging: bool = True        # Enable logging
    retry_failed: bool = True          # Retry failed records
    max_retries: int = 3               # Max retry attempts
```

### Preset Details

| Preset | Batch Size | Max Records | Commit Frequency | Use Case |
|--------|------------|-------------|------------------|----------|
| Development | 50 | 100 | 5 | Quick testing |
| Testing | 100 | 1000 | 10 | Validation |
| Production | 500 | All | 20 | Production use |
| High Performance | 1000 | All | 50 | Maximum speed |
| Safe | 25 | All | 2 | Data safety |

## 📊 Performance Metrics

The loader provides comprehensive performance metrics:

- **Total Records**: Number of records processed
- **Successful**: Number of successfully processed records
- **Failed**: Number of failed records
- **Skipped**: Number of skipped records
- **Success Rate**: Percentage of successful records
- **Duration**: Total processing time
- **Records/Second**: Processing throughput
- **Address Cache**: Number of unique addresses
- **Committee Cache**: Number of unique committees

## 🔧 Advanced Usage

### Custom Configuration
```python
from loader_config import LoaderConfig
from production_loader import ProductionLoader

# Create custom configuration
config = LoaderConfig(
    batch_size=200,
    max_records=5000,
    commit_frequency=15,
    enable_progress=True,
    enable_logging=True
)

# Create and run loader
loader = ProductionLoader(config)
stats = loader.load_file(Path("path/to/file.csv"), state="oklahoma")
```

### Programmatic Usage
```python
from production_loader import ProductionLoader
from loader_config import get_config

# Get preset configuration
config = get_config("production")

# Create loader
loader = ProductionLoader(config)

# Load file
file_path = Path("tmp/oklahoma/2020_ContributionLoanExtract.csv")
stats = loader.load_file(file_path, state="oklahoma")

# Access results
print(f"Success rate: {stats.success_rate:.1f}%")
print(f"Performance: {stats.records_per_second:.1f} records/second")
```

## 🗄️ Database Schema

The loader works with the unified SQLModel schema:

### Core Tables
- `unified_transactions`: Main transaction records
- `unified_committees`: Committee information
- `unified_addresses`: Address information
- `unified_persons`: Person information

### Junction Tables
- `unified_transaction_persons`: Links transactions to persons
- `unified_committee_persons`: Links committees to persons

### Version Tables
- `unified_transaction_versions`: Transaction version history
- `unified_committee_versions`: Committee version history
- `unified_address_versions`: Address version history
- `unified_person_versions`: Person version history

## 🔍 Monitoring and Logging

### Log Files
- `campaign_finance_loader.log`: Detailed application logs

### Log Levels
- **INFO**: Batch processing information
- **ERROR**: Processing errors and failures
- **DEBUG**: Detailed debugging information

### Progress Tracking
- Real-time progress bars
- Batch completion notifications
- Performance metrics display
- Error summaries

## 🛠️ Error Handling

### Error Types
- **Validation Errors**: Data validation failures
- **Database Errors**: Database connection/transaction issues
- **Processing Errors**: Record processing failures
- **System Errors**: Memory, file, or system issues

### Error Recovery
- Automatic retry for failed records
- Transaction rollback on batch failures
- Detailed error logging and reporting
- Graceful degradation

## 📈 Performance Optimization

### Memory Management
- Batch processing to control memory usage
- Efficient caching for deduplication
- Garbage collection optimization

### Database Optimization
- Batch commits to reduce database load
- Connection pooling for efficiency
- Transaction management for consistency

### Processing Optimization
- Parallel processing capabilities
- Efficient data structures
- Optimized algorithms for deduplication

## 🔒 Data Integrity

### Deduplication Strategy
- **Address Deduplication**: Based on street, city, state, zip
- **Committee Deduplication**: Based on filer_id
- **Global Caching**: Pre-loaded existing data
- **Consistent Hashing**: Reliable duplicate detection

### Transaction Safety
- ACID compliance
- Automatic rollback on errors
- Consistent state management
- Data validation

## 🚨 Troubleshooting

### Common Issues

**Memory Issues**
```bash
# Reduce batch size
uv run python production_loader.py safe

# Or use custom configuration with smaller batches
```

**Database Connection Issues**
```bash
# Check database connection
psql campaign_finance -c "SELECT 1;"

# Verify database permissions
```

**Performance Issues**
```bash
# Use high performance preset
uv run python production_loader.py high_performance

# Disable logging for better performance
```

### Debug Mode
```python
# Enable debug logging
import logging
logging.getLogger().setLevel(logging.DEBUG)
```

## 📝 Examples

### Complete Workflow
```bash
# 1. Recreate tables (if needed)
uv run python recreate_tables.py

# 2. Load data with testing preset
uv run python production_loader.py testing

# 3. Check results
uv run python data_summary.py

# 4. Load more data with production preset
uv run python production_loader.py production
```

### Batch Processing Example
```python
# Process multiple files
files = [
    "tmp/oklahoma/2020_ContributionLoanExtract.csv",
    "tmp/oklahoma/2021_ContributionLoanExtract.csv"
]

for file_path in files:
    loader = ProductionLoader(get_config("production"))
    stats = loader.load_file(Path(file_path), state="oklahoma")
    print(f"Loaded {file_path}: {stats.successful_records} records")
```

## 🤝 Contributing

### Development Setup
```bash
# Clone repository
git clone <repository-url>
cd campaignfinance

# Install dependencies
uv sync

# Run tests
uv run python -m pytest tests/

# Run linting
uv run ruff check .
```

### Code Style
- Follow PEP 8 guidelines
- Use type hints
- Add docstrings
- Write unit tests

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
1. Check the troubleshooting section
2. Review the logs in `campaign_finance_loader.log`
3. Create an issue with detailed error information
4. Include configuration and environment details 