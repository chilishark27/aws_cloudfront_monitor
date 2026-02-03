# CloudFront Abuse Detection System - Source Code

This directory contains the improved CloudFront Abuse Detection System implementation.

## Directory Structure

```
cf_request/
├── src/                          # Source code
│   ├── __init__.py
│   ├── config.py                 # Configuration management
│   ├── dynamodb_manager.py       # DynamoDB operations (to be implemented)
│   ├── account_manager.py        # AWS account management (to be implemented)
│   ├── metrics_collector.py      # CloudWatch metrics (to be implemented)
│   ├── abuse_detector.py         # Abuse detection logic (to be implemented)
│   ├── alert_manager.py          # Alert management (to be implemented)
│   ├── observability.py          # Logging and metrics (to be implemented)
│   ├── distribution_processor.py # Distribution processing (to be implemented)
│   ├── account_processor.py      # Account processing (to be implemented)
│   └── lambda_function.py        # Lambda handler (to be implemented)
├── tests/                        # Test code
│   ├── __init__.py
│   └── test_config.py            # Configuration tests
├── requirements.txt              # Python dependencies
├── juhe_lambda_async.py          # Original Lambda function (legacy)
└── README.md                     # This file

## Configuration Module

The `config.py` module provides centralized configuration management with:

- **Type-safe configuration**: Uses Python dataclasses for type safety
- **Environment variable loading**: Loads all configuration from environment variables
- **Validation**: Comprehensive validation of all configuration values
- **Sensible defaults**: All parameters have sensible default values
- **Clear error messages**: Validation errors include specific details

### Usage

```python
from src.config import Config

# Load configuration from environment variables
config = Config.from_environment()

# Validate configuration
errors = config.validate()
if errors:
    for error in errors:
        print(f"Configuration error: {error}")
    exit(1)

# Use configuration
print(f"Region: {config.region}")
print(f"Abuse multiplier: {config.abuse_multiplier}")
```

### Configuration Parameters

#### AWS Configuration
- `AWS_REGION`: AWS region (default: us-east-1)
- `ORG_ACCESS_ROLE`: Cross-account role name (default: OrganizationAccessRole_DO_NOT_DELETE)

#### Detection Thresholds
- `ABUSE_MULTIPLIER`: Multiplier for abuse detection (default: 3.0)
- `DURATION_THRESHOLD`: Consecutive violations required (default: 1)
- `MIN_REQUESTS_THRESHOLD`: Minimum requests to trigger alert (default: 1000)
- `MIN_BYTES_THRESHOLD`: Minimum bytes to trigger alert (default: 500MB)

#### Adaptive Detection Thresholds
- `CRITICAL_REQUESTS_THRESHOLD`: Critical alert threshold for requests (default: 10000)
- `CRITICAL_BYTES_THRESHOLD`: Critical alert threshold for bytes (default: 5GB)
- `WARNING_REQUESTS_THRESHOLD`: Warning alert threshold for requests (default: 5000)
- `WARNING_BYTES_THRESHOLD`: Warning alert threshold for bytes (default: 2GB)
- `MINIMUM_BASELINE_REQUESTS`: Minimum baseline for low traffic (default: 100)
- `MINIMUM_BASELINE_BYTES`: Minimum baseline for low traffic (default: 100MB)

#### Concurrency
- `MAX_WORKERS`: Maximum parallel accounts (default: 12)
- `DIST_MAX_WORKERS`: Maximum parallel distributions per account (default: 5)
- `ALERT_MAX_WORKERS`: Maximum parallel alert sending (default: 5)

#### Telegram
- `TELEGRAM_BOT_TOKEN`: Telegram bot token (required for alerts)
- `TELEGRAM_CHAT_ID`: Telegram chat ID (required for alerts)

#### DynamoDB Tables
- `DDB_ABUSE_COUNTER_TABLE`: Abuse counter table name (default: CF_Abuse_Counter)
- `DDB_ACCOUNTS_CACHE_TABLE`: Accounts cache table name (default: CF_Accounts_Cache)
- `DDB_FAILED_ACCOUNTS_TABLE`: Failed accounts table name (default: CF_Failed_Accounts)
- `DDB_SENT_ALERTS_TABLE`: Sent alerts table name (default: CF_Sent_Alerts)

#### Cache TTLs (seconds)
- `ACCOUNTS_CACHE_TTL`: Account list cache duration (default: 86400 = 24 hours)
- `METRICS_CACHE_TTL`: Metrics cache duration (default: 21600 = 6 hours)
- `FAILED_ACCOUNTS_TTL`: Failed accounts cache duration (default: 604800 = 7 days)
- `ABUSE_COUNTER_TTL`: Abuse counter retention (default: 2592000 = 30 days)
- `SENT_ALERTS_TTL`: Sent alerts retention (default: 86400 = 24 hours)

#### Display
- `DISPLAY_TIMEZONE_OFFSET`: Timezone offset for display (default: 8 = UTC+8)

## Testing

Run tests with pytest:

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run specific test file
pytest tests/test_config.py -v
```

## Development

### Adding New Modules

1. Create the module in `src/`
2. Create corresponding tests in `tests/`
3. Update this README with module documentation
4. Ensure tests pass before committing

### Code Style

- Follow PEP 8 style guidelines
- Use type hints for all function signatures
- Write docstrings for all public functions and classes
- Keep functions focused and single-purpose
- Use descriptive variable names

### Testing Guidelines

- Write both unit tests and property-based tests
- Aim for 80%+ code coverage
- Test error handling paths
- Use mocking for external services (AWS, Telegram)
- Property tests should run minimum 100 iterations
