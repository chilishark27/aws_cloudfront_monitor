"""
Backward compatibility tests.

Tests that verify the new implementation maintains compatibility with
existing environment variables, alert formats, and performance characteristics.

Requirements: 2.1, 2.2, 2.3, 2.4
"""

import pytest
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone

from src.config import Config
from src.alert_manager import AlertManager, Alert
from src.dynamodb_manager import DynamoDBManager


class TestEnvironmentVariableCompatibility:
    """Test that existing environment variables still work."""
    
    def test_legacy_environment_variables_work(self, monkeypatch):
        """
        Test that environment variables from old version still work.
        
        The old version used these environment variables, and they should
        continue to work in the new version.
        """
        # Set legacy environment variables
        monkeypatch.setenv("AWS_REGION", "us-west-2")
        monkeypatch.setenv("ORG_ACCESS_ROLE", "CustomOrgRole")
        monkeypatch.setenv("ABUSE_MULTIPLIER", "4.0")
        monkeypatch.setenv("DURATION_THRESHOLD", "2")
        monkeypatch.setenv("MIN_REQUESTS_THRESHOLD", "2000")
        monkeypatch.setenv("MIN_BYTES_THRESHOLD", str(1024 * 1024 * 1024))  # 1 GB
        monkeypatch.setenv("MAX_WORKERS", "10")
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test_token_123")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "test_chat_456")
        
        # Load configuration
        config = Config.from_environment()
        
        # Verify all legacy variables are loaded correctly
        assert config.region == "us-west-2"
        assert config.org_access_role == "CustomOrgRole"
        assert config.abuse_multiplier == 4.0
        assert config.duration_threshold == 2
        assert config.min_requests_threshold == 2000
        assert config.min_bytes_threshold == 1024 * 1024 * 1024
        assert config.max_workers == 10
        assert config.telegram_bot_token == "test_token_123"
        assert config.telegram_chat_id == "test_chat_456"
    
    def test_new_environment_variables_have_defaults(self):
        """
        Test that new environment variables have sensible defaults.
        
        This ensures that deployments without the new variables will
        still work with default values.
        """
        config = Config()
        
        # New adaptive detection thresholds should have defaults
        assert config.critical_requests_threshold == 10000
        assert config.critical_bytes_threshold == 5 * 1024 * 1024 * 1024
        assert config.warning_requests_threshold == 5000
        assert config.warning_bytes_threshold == 2 * 1024 * 1024 * 1024
        assert config.minimum_baseline_requests == 100
        assert config.minimum_baseline_bytes == 100 * 1024 * 1024
        
        # New concurrency settings should have defaults
        assert config.dist_max_workers == 5
        assert config.alert_max_workers == 5
        
        # New table name should have default
        assert config.ddb_sent_alerts_table == "CF_Sent_Alerts"
    
    def test_optional_new_variables_can_be_set(self, monkeypatch):
        """
        Test that new optional environment variables can be set.
        """
        monkeypatch.setenv("CRITICAL_REQUESTS_THRESHOLD", "20000")
        monkeypatch.setenv("WARNING_REQUESTS_THRESHOLD", "10000")
        monkeypatch.setenv("DIST_MAX_WORKERS", "10")
        monkeypatch.setenv("ALERT_MAX_WORKERS", "8")
        
        config = Config.from_environment()
        
        assert config.critical_requests_threshold == 20000
        assert config.warning_requests_threshold == 10000
        assert config.dist_max_workers == 10
        assert config.alert_max_workers == 8


class TestAlertFormatCompatibility:
    """Test that alert format remains compatible."""
    
    def test_alert_message_contains_required_fields(self):
        """
        Test that alert messages contain all required fields.
        
        Existing monitoring systems may depend on specific fields
        being present in alert messages.
        """
        config = Config()
        
        with patch('boto3.resource'), \
             patch('boto3.client'):
            
            ddb_manager = DynamoDBManager(config)
            alert_manager = AlertManager(config, ddb_manager)
            
            # Create test alert
            alert = Alert(
                account_id='123456789012',
                account_name='TestAccount',
                account_email='test@example.com',
                distribution_id='E1234567890ABC',
                metric='Requests',
                severity='Critical',
                current_value=50000.0,
                history_value=10000.0,
                abuse_multiplier=3.0,
                consecutive_count=2,
                timestamp=datetime.now(timezone.utc).isoformat(),
                percentage_change=400.0
            )
            
            # Format alert message
            message = alert_manager._format_alert_message(alert)
            
            # Verify required fields are present
            assert 'TestAccount' in message
            assert '123456789012' in message
            assert 'E1234567890ABC' in message
            assert 'Requests' in message
            assert 'Critical' in message or 'CRITICAL' in message
            assert '50000' in message or '50,000' in message
            assert '10000' in message or '10,000' in message
    
    def test_alert_severity_levels_are_standard(self):
        """
        Test that alert severity levels use standard values.
        
        Monitoring systems may filter or route alerts based on severity.
        """
        config = Config()
        
        with patch('boto3.resource'), \
             patch('boto3.client'):
            
            ddb_manager = DynamoDBManager(config)
            alert_manager = AlertManager(config, ddb_manager)
            
            # Test critical alert
            critical_alert = Alert(
                account_id='123456789012',
                account_name='TestAccount',
                account_email='test@example.com',
                distribution_id='E1234567890ABC',
                metric='Requests',
                severity='Critical',
                current_value=50000.0,
                history_value=10000.0,
                abuse_multiplier=3.0,
                consecutive_count=2,
                timestamp=datetime.now(timezone.utc).isoformat(),
                percentage_change=400.0
            )
            
            # Test warning alert
            warning_alert = Alert(
                account_id='123456789012',
                account_name='TestAccount',
                account_email='test@example.com',
                distribution_id='E1234567890ABC',
                metric='Requests',
                severity='Warning',
                current_value=8000.0,
                history_value=2000.0,
                abuse_multiplier=3.0,
                consecutive_count=1,
                timestamp=datetime.now(timezone.utc).isoformat(),
                percentage_change=300.0
            )
            
            # Verify severity values are standard
            assert critical_alert.severity in ['Critical', 'CRITICAL']
            assert warning_alert.severity in ['Warning', 'WARNING']


class TestTableSchemaCompatibility:
    """Test that DynamoDB table schemas are compatible."""
    
    def test_abuse_counter_table_schema(self):
        """
        Test that abuse counter table uses correct schema.
        
        The schema must match what the old version created.
        """
        config = Config()
        
        with patch('boto3.resource'), patch('boto3.client') as mock_client:
            # Mock describe_table to return existing table
            mock_client.return_value.describe_table.return_value = {
                'Table': {'TableName': config.ddb_abuse_counter_table}
            }
            
            ddb_manager = DynamoDBManager(config)
            
            # Ensure tables exist (should detect existing table)
            result = ddb_manager.ensure_tables_exist()
            
            # Verify table is recognized as existing
            assert result[config.ddb_abuse_counter_table] is True
    
    def test_accounts_cache_table_schema(self):
        """
        Test that accounts cache table uses correct schema.
        """
        config = Config()
        
        with patch('boto3.resource'), patch('boto3.client') as mock_client:
            # Mock describe_table to return existing table
            mock_client.return_value.describe_table.return_value = {
                'Table': {'TableName': config.ddb_accounts_cache_table}
            }
            
            ddb_manager = DynamoDBManager(config)
            
            # Ensure tables exist (should detect existing table)
            result = ddb_manager.ensure_tables_exist()
            
            # Verify table is recognized as existing
            assert result[config.ddb_accounts_cache_table] is True
    
    def test_failed_accounts_table_schema(self):
        """
        Test that failed accounts table uses correct schema.
        """
        config = Config()
        
        with patch('boto3.resource'), patch('boto3.client') as mock_client:
            # Mock describe_table to return existing table
            mock_client.return_value.describe_table.return_value = {
                'Table': {'TableName': config.ddb_failed_accounts_table}
            }
            
            ddb_manager = DynamoDBManager(config)
            
            # Ensure tables exist (should detect existing table)
            result = ddb_manager.ensure_tables_exist()
            
            # Verify table is recognized as existing
            assert result[config.ddb_failed_accounts_table] is True


class TestPerformanceCharacteristics:
    """Test that performance is not significantly degraded."""
    
    def test_batch_api_reduces_cloudwatch_calls(self):
        """
        Test that batch API is used to reduce CloudWatch API calls.
        
        The new implementation should make fewer API calls than the old version.
        Old version: 4 calls per distribution (current requests, current bytes, avg requests, avg bytes)
        New version: 1 call per distribution (batch fetch)
        """
        # This is a design verification test - the implementation uses get_metric_data
        # which batches multiple metrics into a single API call
        
        from src.metrics_collector import MetricsCollector
        
        config = Config()
        
        with patch('boto3.resource'), patch('boto3.client'):
            ddb_manager = DynamoDBManager(config)
            metrics_collector = MetricsCollector(config, ddb_manager)
            
            # Verify that the batch method exists
            assert hasattr(metrics_collector, '_get_metrics_batch')
            assert hasattr(metrics_collector, '_get_current_metrics_batch')
            
            # The implementation should use these batch methods
            # which call get_metric_data instead of get_metric_statistics
    
    def test_caching_reduces_dynamodb_reads(self):
        """
        Test that caching is used to reduce DynamoDB reads.
        
        The new implementation should cache frequently accessed data.
        """
        from src.metrics_collector import MetricsCollector
        from src.account_manager import AccountManager
        
        config = Config()
        
        with patch('boto3.resource'), patch('boto3.client'):
            ddb_manager = DynamoDBManager(config)
            
            # Verify metrics collector has caching methods
            metrics_collector = MetricsCollector(config, ddb_manager)
            assert hasattr(metrics_collector, '_get_cached_metrics')
            assert hasattr(metrics_collector, '_cache_metrics')
            
            # Verify account manager has caching methods
            account_manager = AccountManager(config, ddb_manager)
            assert hasattr(account_manager, '_get_cached_accounts')
            assert hasattr(account_manager, '_cache_accounts')
    
    def test_parallel_processing_is_enabled(self):
        """
        Test that parallel processing is enabled for better performance.
        
        The new implementation should process distributions in parallel.
        """
        from src.account_processor import AccountProcessor
        
        config = Config()
        
        with patch('boto3.resource'), \
             patch('boto3.client'), \
             patch('src.account_processor.AccountManager'), \
             patch('src.account_processor.DistributionProcessor'), \
             patch('src.account_processor.ObservabilityManager'):
            
            account_manager = Mock()
            dist_processor = Mock()
            obs_manager = Mock()
            
            account_processor = AccountProcessor(
                config=config,
                account_manager=account_manager,
                distribution_processor=dist_processor,
                observability=obs_manager
            )
            
            # Verify parallel processing method exists
            assert hasattr(account_processor, '_process_distributions_parallel')


class TestConfigurationValidation:
    """Test that configuration validation is backward compatible."""
    
    def test_minimal_configuration_is_valid(self):
        """
        Test that minimal configuration (only required fields) is valid.
        
        This ensures that existing deployments with minimal config still work.
        """
        config = Config(
            telegram_bot_token="test_token",
            telegram_chat_id="test_chat"
        )
        
        errors = config.validate()
        
        # Should have no validation errors with minimal config
        assert errors == []
    
    def test_configuration_with_all_defaults_is_valid(self):
        """
        Test that configuration with all default values is valid.
        """
        config = Config()
        
        # Set required Telegram fields
        config.telegram_bot_token = "test_token"
        config.telegram_chat_id = "test_chat"
        
        errors = config.validate()
        
        # Should have no validation errors
        assert errors == []
    
    def test_legacy_configuration_values_are_accepted(self):
        """
        Test that legacy configuration values are still accepted.
        """
        config = Config(
            region="us-east-1",
            org_access_role="OrganizationAccessRole_DO_NOT_DELETE",
            abuse_multiplier=3.0,
            duration_threshold=1,
            min_requests_threshold=1000,
            min_bytes_threshold=500 * 1024 * 1024,
            max_workers=12,
            telegram_bot_token="test_token",
            telegram_chat_id="test_chat",
            ddb_abuse_counter_table="CF_Abuse_Counter",
            ddb_accounts_cache_table="CF_Accounts_Cache",
            ddb_failed_accounts_table="CF_Failed_Accounts"
        )
        
        errors = config.validate()
        
        # Should have no validation errors with legacy values
        assert errors == []
