"""
Unit tests for DynamoDB Manager module.

Tests table creation, retry logic, and error handling.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError

from src.config import Config
from src.dynamodb_manager import DynamoDBManager


class TestDynamoDBManagerInitialization:
    """Test DynamoDB Manager initialization."""
    
    def test_initialization(self):
        """Test that DynamoDBManager initializes correctly."""
        config = Config()
        
        with patch('boto3.resource'), patch('boto3.client'):
            manager = DynamoDBManager(config)
            
            assert manager.config == config
            assert manager._table_cache == {}
            assert isinstance(manager.RETRYABLE_ERRORS, set)
            assert 'ThrottlingException' in manager.RETRYABLE_ERRORS


class TestTableExistence:
    """Test table existence checking."""
    
    def test_table_exists_cached(self):
        """Test that cached table existence is returned."""
        config = Config()
        
        with patch('boto3.resource'), patch('boto3.client'):
            manager = DynamoDBManager(config)
            manager._table_cache['test_table'] = True
            
            assert manager._table_exists('test_table') is True
    
    def test_table_not_exists_cached(self):
        """Test that non-existent table returns False from cache."""
        config = Config()
        
        with patch('boto3.resource'), patch('boto3.client'):
            manager = DynamoDBManager(config)
            
            assert manager._table_exists('test_table') is False


class TestGetItemWithRetry:
    """Test get_item_with_retry method."""
    
    def test_successful_get_item(self):
        """Test successful item retrieval."""
        config = Config()
        
        with patch('boto3.resource') as mock_resource, patch('boto3.client'):
            # Mock table and response
            mock_table = Mock()
            mock_table.get_item.return_value = {'Item': {'key': 'value'}}
            mock_resource.return_value.Table.return_value = mock_table
            
            manager = DynamoDBManager(config)
            result = manager.get_item_with_retry('test_table', {'id': '123'})
            
            assert result == {'key': 'value'}
            mock_table.get_item.assert_called_once_with(Key={'id': '123'})
    
    def test_get_item_not_found(self):
        """Test item not found returns None."""
        config = Config()
        
        with patch('boto3.resource') as mock_resource, patch('boto3.client'):
            # Mock table with no item
            mock_table = Mock()
            mock_table.get_item.return_value = {}
            mock_resource.return_value.Table.return_value = mock_table
            
            manager = DynamoDBManager(config)
            result = manager.get_item_with_retry('test_table', {'id': '123'})
            
            assert result is None
    
    def test_get_item_retryable_error_succeeds_on_retry(self):
        """Test that retryable errors are retried and eventually succeed."""
        config = Config()
        
        with patch('boto3.resource') as mock_resource, \
             patch('boto3.client'), \
             patch('time.sleep'):  # Mock sleep to speed up test
            
            # Mock table that fails once then succeeds
            mock_table = Mock()
            mock_table.get_item.side_effect = [
                ClientError(
                    {'Error': {'Code': 'ThrottlingException', 'Message': 'Rate exceeded'}},
                    'GetItem'
                ),
                {'Item': {'key': 'value'}}
            ]
            mock_resource.return_value.Table.return_value = mock_table
            
            manager = DynamoDBManager(config)
            result = manager.get_item_with_retry('test_table', {'id': '123'})
            
            assert result == {'key': 'value'}
            assert mock_table.get_item.call_count == 2
    
    def test_get_item_non_retryable_error(self):
        """Test that non-retryable errors return None immediately."""
        config = Config()
        
        with patch('boto3.resource') as mock_resource, patch('boto3.client'):
            # Mock table with access denied error
            mock_table = Mock()
            mock_table.get_item.side_effect = ClientError(
                {'Error': {'Code': 'AccessDenied', 'Message': 'Access denied'}},
                'GetItem'
            )
            mock_resource.return_value.Table.return_value = mock_table
            
            manager = DynamoDBManager(config)
            result = manager.get_item_with_retry('test_table', {'id': '123'})
            
            assert result is None
            assert mock_table.get_item.call_count == 1  # No retry


class TestPutItemWithRetry:
    """Test put_item_with_retry method."""
    
    def test_successful_put_item(self):
        """Test successful item write."""
        config = Config()
        
        with patch('boto3.resource') as mock_resource, patch('boto3.client'):
            # Mock table
            mock_table = Mock()
            mock_table.put_item.return_value = {}
            mock_resource.return_value.Table.return_value = mock_table
            
            manager = DynamoDBManager(config)
            result = manager.put_item_with_retry('test_table', {'id': '123', 'data': 'test'})
            
            assert result is True
            mock_table.put_item.assert_called_once_with(Item={'id': '123', 'data': 'test'})
    
    def test_put_item_retryable_error_succeeds_on_retry(self):
        """Test that retryable errors are retried and eventually succeed."""
        config = Config()
        
        with patch('boto3.resource') as mock_resource, \
             patch('boto3.client'), \
             patch('time.sleep'):
            
            # Mock table that fails once then succeeds
            mock_table = Mock()
            mock_table.put_item.side_effect = [
                ClientError(
                    {'Error': {'Code': 'ProvisionedThroughputExceededException', 'Message': 'Exceeded'}},
                    'PutItem'
                ),
                {}
            ]
            mock_resource.return_value.Table.return_value = mock_table
            
            manager = DynamoDBManager(config)
            result = manager.put_item_with_retry('test_table', {'id': '123'})
            
            assert result is True
            assert mock_table.put_item.call_count == 2
    
    def test_put_item_max_retries_exhausted(self):
        """Test that max retries returns False."""
        config = Config()
        
        with patch('boto3.resource') as mock_resource, \
             patch('boto3.client'), \
             patch('time.sleep'):
            
            # Mock table that always fails
            mock_table = Mock()
            mock_table.put_item.side_effect = ClientError(
                {'Error': {'Code': 'ThrottlingException', 'Message': 'Rate exceeded'}},
                'PutItem'
            )
            mock_resource.return_value.Table.return_value = mock_table
            
            manager = DynamoDBManager(config)
            result = manager.put_item_with_retry('test_table', {'id': '123'}, max_retries=3)
            
            assert result is False
            assert mock_table.put_item.call_count == 3


class TestScanWithPagination:
    """Test scan_with_pagination method."""
    
    def test_scan_single_page(self):
        """Test scanning table with single page of results."""
        config = Config()
        
        with patch('boto3.resource') as mock_resource, patch('boto3.client'):
            # Mock table with single page
            mock_table = Mock()
            mock_table.scan.return_value = {
                'Items': [{'id': '1'}, {'id': '2'}]
            }
            mock_resource.return_value.Table.return_value = mock_table
            
            manager = DynamoDBManager(config)
            result = manager.scan_with_pagination('test_table')
            
            assert len(result) == 2
            assert result[0] == {'id': '1'}
            assert result[1] == {'id': '2'}
    
    def test_scan_multiple_pages(self):
        """Test scanning table with pagination."""
        config = Config()
        
        with patch('boto3.resource') as mock_resource, patch('boto3.client'):
            # Mock table with multiple pages
            mock_table = Mock()
            mock_table.scan.side_effect = [
                {
                    'Items': [{'id': '1'}, {'id': '2'}],
                    'LastEvaluatedKey': {'id': '2'}
                },
                {
                    'Items': [{'id': '3'}, {'id': '4'}]
                }
            ]
            mock_resource.return_value.Table.return_value = mock_table
            
            manager = DynamoDBManager(config)
            result = manager.scan_with_pagination('test_table')
            
            assert len(result) == 4
            assert mock_table.scan.call_count == 2
    
    def test_scan_error_returns_empty_list(self):
        """Test that scan errors return empty list."""
        config = Config()
        
        with patch('boto3.resource') as mock_resource, patch('boto3.client'):
            # Mock table with error
            mock_table = Mock()
            mock_table.scan.side_effect = ClientError(
                {'Error': {'Code': 'AccessDenied', 'Message': 'Access denied'}},
                'Scan'
            )
            mock_resource.return_value.Table.return_value = mock_table
            
            manager = DynamoDBManager(config)
            result = manager.scan_with_pagination('test_table')
            
            assert result == []


class TestTableCreation:
    """Test table creation logic."""
    
    def test_create_table_if_not_exists_already_exists(self):
        """Test that existing table is detected and not recreated."""
        config = Config()
        
        with patch('boto3.resource'), \
             patch('boto3.client') as mock_client:
            
            # Mock describe_table to return success (table exists)
            mock_client.return_value.describe_table.return_value = {
                'Table': {'TableName': 'test_table'}
            }
            
            manager = DynamoDBManager(config)
            result = manager._create_table_if_not_exists(
                table_name='test_table',
                key_schema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
                attribute_definitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
                ttl_attribute='TTL'
            )
            
            assert result is True
            assert manager._table_cache['test_table'] is True
    
    def test_create_table_if_not_exists_creates_new(self):
        """Test that non-existent table is created."""
        config = Config()
        
        with patch('boto3.resource'), \
             patch('boto3.client') as mock_client:
            
            # Mock describe_table to raise ResourceNotFoundException
            mock_client.return_value.describe_table.side_effect = ClientError(
                {'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Not found'}},
                'DescribeTable'
            )
            
            # Mock create_table
            mock_client.return_value.create_table.return_value = {}
            
            # Mock waiter
            mock_waiter = Mock()
            mock_client.return_value.get_waiter.return_value = mock_waiter
            
            # Mock update_time_to_live
            mock_client.return_value.update_time_to_live.return_value = {}
            
            manager = DynamoDBManager(config)
            result = manager._create_table_if_not_exists(
                table_name='test_table',
                key_schema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
                attribute_definitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
                ttl_attribute='TTL'
            )
            
            assert result is True
            mock_client.return_value.create_table.assert_called_once()
            mock_waiter.wait.assert_called_once()
