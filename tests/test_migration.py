"""
Migration and backward compatibility tests.

Tests that verify the new implementation can read and work with data
created by the old version of the system.

Requirements: 2.1, 2.2, 2.3, 2.4
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, patch, MagicMock
from decimal import Decimal

from src.config import Config
from src.dynamodb_manager import DynamoDBManager
from src.abuse_detector import AbuseDetector
from src.account_manager import AccountManager
from src.metrics_collector import MetricsCollector


class TestDynamoDBDataMigration:
    """Test that existing DynamoDB data is readable by new implementation."""
    
    def test_read_old_abuse_counter_format(self):
        """
        Test that abuse counters created by old version can be read.
        
        Old format uses CounterKey as hash key with Count and LastUpdate fields.
        """
        config = Config()
        
        with patch('boto3.resource') as mock_resource, patch('boto3.client'):
            # Mock old-format counter data
            old_counter_data = {
                'CounterKey': '123456789012#E1234567890ABC#Requests',
                'Count': Decimal('3'),  # DynamoDB returns Decimal
                'LastUpdate': '2025-01-20T10:00:00+00:00',
                'TTL': int((datetime.now(timezone.utc) + timedelta(days=30)).timestamp())
            }
            
            # Mock table
            mock_table = Mock()
            mock_table.get_item.return_value = {'Item': old_counter_data}
            mock_resource.return_value.Table.return_value = mock_table
            
            # Create managers
            ddb_manager = DynamoDBManager(config)
            abuse_detector = AbuseDetector(config, ddb_manager)
            
            # Read counter using new implementation
            counter_key = '123456789012#E1234567890ABC#Requests'
            count = abuse_detector.get_abuse_counter(counter_key)
            
            # Verify we can read the old format
            assert count == 3
            mock_table.get_item.assert_called_once()
    
    def test_read_old_metrics_cache_format(self):
        """
        Test that cached metrics created by old version can be read.
        
        Old format uses CacheKey as hash key with AvgRequests, AvgBytes, and Timestamp.
        """
        config = Config()
        
        with patch('boto3.resource') as mock_resource, patch('boto3.client'):
            # Mock old-format cache data (recent timestamp to pass TTL check)
            recent_time = datetime.now(timezone.utc) - timedelta(hours=3)
            old_cache_data = {
                'CacheKey': 'metrics#123456789012#E1234567890ABC',
                'AvgRequests': Decimal('5000.5'),
                'AvgBytes': Decimal('1073741824'),  # 1 GB
                'Timestamp': recent_time.isoformat(),
                'TTL': int((datetime.now(timezone.utc) + timedelta(hours=3)).timestamp())
            }
            
            # Mock table
            mock_table = Mock()
            mock_table.get_item.return_value = {'Item': old_cache_data}
            mock_resource.return_value.Table.return_value = mock_table
            
            # Create managers
            ddb_manager = DynamoDBManager(config)
            metrics_collector = MetricsCollector(config, ddb_manager)
            
            # Read cached metrics using new implementation
            cached = metrics_collector._get_cached_metrics('123456789012', 'E1234567890ABC')
            
            # Verify we can read the old format
            assert cached is not None
            assert cached['avg_requests'] == 5000.5
            assert cached['avg_bytes'] == 1073741824
    
    def test_read_old_failed_accounts_format(self):
        """
        Test that failed accounts created by old version can be read.
        
        Old format uses AccountId as hash key with Error and Timestamp fields.
        """
        config = Config()
        
        with patch('boto3.resource') as mock_resource, patch('boto3.client'):
            # Mock old-format failed account data
            old_failed_accounts = [
                {
                    'AccountId': '123456789012',
                    'Error': 'AccessDenied: Cannot assume role',
                    'Timestamp': '2025-01-20T10:00:00+00:00',
                    'TTL': int((datetime.now(timezone.utc) + timedelta(days=7)).timestamp())
                },
                {
                    'AccountId': '987654321098',
                    'Error': 'InvalidRole: Role does not exist',
                    'Timestamp': '2025-01-20T11:00:00+00:00',
                    'TTL': int((datetime.now(timezone.utc) + timedelta(days=7)).timestamp())
                }
            ]
            
            # Mock table scan
            mock_table = Mock()
            mock_table.scan.return_value = {'Items': old_failed_accounts}
            mock_resource.return_value.Table.return_value = mock_table
            
            # Create managers
            ddb_manager = DynamoDBManager(config)
            account_manager = AccountManager(config, ddb_manager)
            
            # Read failed accounts using new implementation
            failed_accounts = account_manager.get_failed_accounts()
            
            # Verify we can read the old format
            assert '123456789012' in failed_accounts
            assert '987654321098' in failed_accounts
            assert len(failed_accounts) == 2
    
    def test_read_old_accounts_cache_format(self):
        """
        Test that accounts cache created by old version can be read.
        
        Old format uses CacheKey='ou_accounts' with AccountsData as JSON string.
        """
        config = Config()
        
        with patch('boto3.resource') as mock_resource, patch('boto3.client'):
            # Mock old-format accounts cache (recent timestamp to pass TTL check)
            import json
            recent_time = datetime.now(timezone.utc) - timedelta(hours=12)
            old_accounts = [
                {'Id': '123456789012', 'Name': 'Account1', 'Email': 'account1@example.com'},
                {'Id': '987654321098', 'Name': 'Account2', 'Email': 'account2@example.com'}
            ]
            
            old_cache_data = {
                'CacheKey': 'ou_accounts',
                'AccountsData': json.dumps(old_accounts),
                'Timestamp': recent_time.isoformat(),
                'TTL': int((datetime.now(timezone.utc) + timedelta(hours=12)).timestamp())
            }
            
            # Mock table
            mock_table = Mock()
            mock_table.get_item.return_value = {'Item': old_cache_data}
            mock_resource.return_value.Table.return_value = mock_table
            
            # Create managers
            ddb_manager = DynamoDBManager(config)
            account_manager = AccountManager(config, ddb_manager)
            
            # Read cached accounts using new implementation
            cached = account_manager._get_cached_accounts()
            
            # Verify we can read the old format
            assert cached is not None
            assert len(cached) == 2
            assert cached[0]['Id'] == '123456789012'
            assert cached[1]['Id'] == '987654321098'


class TestCounterValuePreservation:
    """Test that counter values are preserved during migration."""
    
    def test_counter_increment_preserves_existing_value(self):
        """Test that incrementing a counter preserves the existing value."""
        config = Config()
        
        with patch('boto3.resource') as mock_resource, patch('boto3.client'):
            # Mock existing counter with value 5
            existing_counter = {
                'CounterKey': '123456789012#E1234567890ABC#Requests',
                'Count': Decimal('5'),
                'LastUpdate': '2025-01-20T10:00:00+00:00',
                'TTL': int((datetime.now(timezone.utc) + timedelta(days=30)).timestamp())
            }
            
            # Mock table - first call returns no hour key (not cached), second returns existing counter
            mock_table = Mock()
            mock_table.get_item.side_effect = [
                {},  # Hour key check returns empty (not cached)
                {'Item': existing_counter},  # Main counter key returns existing value
            ]
            mock_table.put_item.return_value = {}
            mock_resource.return_value.Table.return_value = mock_table
            
            # Create managers
            ddb_manager = DynamoDBManager(config)
            abuse_detector = AbuseDetector(config, ddb_manager)
            
            # Increment counter
            counter_key = '123456789012#E1234567890ABC#Requests'
            new_count = abuse_detector.update_abuse_counter(counter_key, is_abuse=True)
            
            # Verify counter was incremented from existing value
            assert new_count == 6
            
            # Verify put_item was called with correct value
            put_calls = mock_table.put_item.call_args_list
            assert len(put_calls) >= 1
            
            # Check that at least one call has Count=6
            found_correct_count = False
            for call in put_calls:
                item = call[1]['Item']
                if item.get('Count') == 6:
                    found_correct_count = True
                    break
            
            assert found_correct_count, "Counter should be incremented to 6"
    
    def test_counter_decrement_preserves_existing_value(self):
        """Test that decrementing a counter preserves the existing value."""
        config = Config()
        
        with patch('boto3.resource') as mock_resource, patch('boto3.client'):
            # Mock existing counter with value 3
            existing_counter = {
                'CounterKey': '123456789012#E1234567890ABC#Requests',
                'Count': Decimal('3'),
                'LastUpdate': '2025-01-20T10:00:00+00:00',
                'TTL': int((datetime.now(timezone.utc) + timedelta(days=30)).timestamp())
            }
            
            # Mock table - first call returns no hour key (not cached), second returns existing counter
            mock_table = Mock()
            mock_table.get_item.side_effect = [
                {},  # Hour key check returns empty (not cached)
                {'Item': existing_counter},  # Main counter key returns existing value
            ]
            mock_table.put_item.return_value = {}
            mock_resource.return_value.Table.return_value = mock_table
            
            # Create managers
            ddb_manager = DynamoDBManager(config)
            abuse_detector = AbuseDetector(config, ddb_manager)
            
            # Decrement counter
            counter_key = '123456789012#E1234567890ABC#Requests'
            new_count = abuse_detector.update_abuse_counter(counter_key, is_abuse=False)
            
            # Verify counter was decremented from existing value
            assert new_count == 2
            
            # Verify put_item was called with correct value
            put_calls = mock_table.put_item.call_args_list
            assert len(put_calls) >= 1
            
            # Check that at least one call has Count=2
            found_correct_count = False
            for call in put_calls:
                item = call[1]['Item']
                if item.get('Count') == 2:
                    found_correct_count = True
                    break
            
            assert found_correct_count, "Counter should be decremented to 2"
    
    def test_counter_non_negative_invariant_preserved(self):
        """Test that counter never goes below zero even with existing data."""
        config = Config()
        
        with patch('boto3.resource') as mock_resource, patch('boto3.client'):
            # Mock existing counter with value 0
            existing_counter = {
                'CounterKey': '123456789012#E1234567890ABC#Requests',
                'Count': Decimal('0'),
                'LastUpdate': '2025-01-20T10:00:00+00:00',
                'TTL': int((datetime.now(timezone.utc) + timedelta(days=30)).timestamp())
            }
            
            # Mock table
            mock_table = Mock()
            mock_table.get_item.return_value = {'Item': existing_counter}
            mock_table.put_item.return_value = {}
            mock_resource.return_value.Table.return_value = mock_table
            
            # Create managers
            ddb_manager = DynamoDBManager(config)
            abuse_detector = AbuseDetector(config, ddb_manager)
            
            # Try to decrement counter at zero
            counter_key = '123456789012#E1234567890ABC#Requests'
            new_count = abuse_detector.update_abuse_counter(counter_key, is_abuse=False)
            
            # Verify counter stays at zero
            assert new_count == 0


class TestCachedDataValidity:
    """Test that cached data is still valid after migration."""
    
    def test_cached_metrics_within_ttl_are_used(self):
        """Test that cached metrics within TTL are still used."""
        config = Config()
        
        with patch('boto3.resource') as mock_resource, patch('boto3.client'):
            # Mock recent cache (within TTL)
            recent_time = datetime.now(timezone.utc) - timedelta(hours=3)
            cache_data = {
                'CacheKey': 'metrics#123456789012#E1234567890ABC',
                'AvgRequests': Decimal('5000'),
                'AvgBytes': Decimal('1073741824'),
                'Timestamp': recent_time.isoformat(),
                'TTL': int((datetime.now(timezone.utc) + timedelta(hours=3)).timestamp())
            }
            
            # Mock table
            mock_table = Mock()
            mock_table.get_item.return_value = {'Item': cache_data}
            mock_resource.return_value.Table.return_value = mock_table
            
            # Create managers
            ddb_manager = DynamoDBManager(config)
            metrics_collector = MetricsCollector(config, ddb_manager)
            
            # Get cached metrics
            cached = metrics_collector._get_cached_metrics('123456789012', 'E1234567890ABC')
            
            # Verify cache is valid and returned (note: keys are lowercase with underscores)
            assert cached is not None
            assert cached['avg_requests'] == 5000
            assert cached['avg_bytes'] == 1073741824
    
    def test_cached_metrics_expired_are_ignored(self):
        """Test that expired cached metrics are ignored."""
        config = Config()
        
        with patch('boto3.resource') as mock_resource, patch('boto3.client'):
            # Mock old cache (expired)
            old_time = datetime.now(timezone.utc) - timedelta(hours=7)
            cache_data = {
                'CacheKey': 'metrics#123456789012#E1234567890ABC',
                'AvgRequests': Decimal('5000'),
                'AvgBytes': Decimal('1073741824'),
                'Timestamp': old_time.isoformat(),
                'TTL': int(old_time.timestamp())  # Already expired
            }
            
            # Mock table
            mock_table = Mock()
            mock_table.get_item.return_value = {'Item': cache_data}
            mock_resource.return_value.Table.return_value = mock_table
            
            # Create managers
            ddb_manager = DynamoDBManager(config)
            metrics_collector = MetricsCollector(config, ddb_manager)
            
            # Get cached metrics
            cached = metrics_collector._get_cached_metrics('123456789012', 'E1234567890ABC')
            
            # Verify expired cache is not returned
            assert cached is None
    
    def test_cached_accounts_within_ttl_are_used(self):
        """Test that cached accounts within TTL are still used."""
        config = Config()
        
        with patch('boto3.resource') as mock_resource, patch('boto3.client'):
            # Mock recent accounts cache
            import json
            recent_time = datetime.now(timezone.utc) - timedelta(hours=12)
            accounts = [
                {'Id': '123456789012', 'Name': 'Account1', 'Email': 'account1@example.com'}
            ]
            
            cache_data = {
                'CacheKey': 'ou_accounts',
                'AccountsData': json.dumps(accounts),
                'Timestamp': recent_time.isoformat(),
                'TTL': int((datetime.now(timezone.utc) + timedelta(hours=12)).timestamp())
            }
            
            # Mock table
            mock_table = Mock()
            mock_table.get_item.return_value = {'Item': cache_data}
            mock_resource.return_value.Table.return_value = mock_table
            
            # Create managers
            ddb_manager = DynamoDBManager(config)
            account_manager = AccountManager(config, ddb_manager)
            
            # Get cached accounts
            cached = account_manager._get_cached_accounts()
            
            # Verify cache is valid and returned
            assert cached is not None
            assert len(cached) == 1
            assert cached[0]['Id'] == '123456789012'


class TestSchemaConsistency:
    """Test that schema is consistent between old and new versions."""
    
    def test_abuse_counter_key_structure(self):
        """Test that abuse counter keys follow the expected structure."""
        config = Config()
        
        with patch('boto3.resource') as mock_resource, patch('boto3.client'):
            # Mock table
            mock_table = Mock()
            mock_table.get_item.return_value = {}
            mock_table.put_item.return_value = {}
            mock_resource.return_value.Table.return_value = mock_table
            
            # Create managers
            ddb_manager = DynamoDBManager(config)
            abuse_detector = AbuseDetector(config, ddb_manager)
            
            # Update counter with standard key format
            counter_key = '123456789012#E1234567890ABC#Requests'
            abuse_detector.update_abuse_counter(counter_key, is_abuse=True)
            
            # Verify put_item was called with CounterKey field
            put_calls = mock_table.put_item.call_args_list
            assert len(put_calls) >= 1
            
            # Check that items have CounterKey field
            for call in put_calls:
                item = call[1]['Item']
                assert 'CounterKey' in item
                # Verify key format: AccountId#DistributionId#MetricName or with timestamp
                assert '#' in item['CounterKey']
    
    def test_metrics_cache_key_structure(self):
        """Test that metrics cache keys follow the expected structure."""
        config = Config()
        
        with patch('boto3.resource') as mock_resource, patch('boto3.client'):
            # Mock table
            mock_table = Mock()
            mock_table.get_item.return_value = {}
            mock_table.put_item.return_value = {}
            mock_resource.return_value.Table.return_value = mock_table
            
            # Create managers
            ddb_manager = DynamoDBManager(config)
            metrics_collector = MetricsCollector(config, ddb_manager)
            
            # Cache metrics
            metrics_collector._cache_metrics('123456789012', 'E1234567890ABC', 5000.0, 1073741824.0)
            
            # Verify put_item was called with CacheKey field
            mock_table.put_item.assert_called_once()
            call_args = mock_table.put_item.call_args
            item = call_args[1]['Item']
            
            assert 'CacheKey' in item
            assert item['CacheKey'] == 'metrics#123456789012#E1234567890ABC'
            assert 'AvgRequests' in item
            assert 'AvgBytes' in item
    
    def test_failed_accounts_key_structure(self):
        """Test that failed accounts keys follow the expected structure."""
        config = Config()
        
        with patch('boto3.resource') as mock_resource, patch('boto3.client'):
            # Mock table
            mock_table = Mock()
            mock_table.put_item.return_value = {}
            mock_resource.return_value.Table.return_value = mock_table
            
            # Create managers
            ddb_manager = DynamoDBManager(config)
            account_manager = AccountManager(config, ddb_manager)
            
            # Record failed account
            account_manager.record_failed_account('123456789012', 'AccessDenied')
            
            # Verify put_item was called with AccountId field
            mock_table.put_item.assert_called_once()
            call_args = mock_table.put_item.call_args
            item = call_args[1]['Item']
            
            assert 'AccountId' in item
            assert item['AccountId'] == '123456789012'
            assert 'Error' in item
            assert 'Timestamp' in item
