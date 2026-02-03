"""
Unit tests for Scheduler Lambda Handler.

Tests the scheduler_handler module functions including:
- Account grouping logic
- Worker invocation logic
- Lambda handler entry point
"""

import json
import os
import pytest
from unittest.mock import Mock, patch, MagicMock

from scheduler_handler import (
    lambda_handler,
    get_active_accounts,
    group_accounts,
    invoke_workers_async
)


class TestGroupAccounts:
    """Test account grouping functionality."""
    
    def test_group_accounts_normal_case(self):
        """Test grouping accounts with normal input."""
        accounts = [{'Id': str(i), 'Name': f'Account {i}', 'Email': f'acc{i}@example.com'} 
                   for i in range(10)]
        groups = group_accounts(accounts, 3)
        
        assert len(groups) == 4
        assert len(groups[0]) == 3
        assert len(groups[1]) == 3
        assert len(groups[2]) == 3
        assert len(groups[3]) == 1  # Remaining accounts in last group
    
    def test_group_accounts_empty_list(self):
        """Test grouping empty account list."""
        groups = group_accounts([], 5)
        assert groups == []
    
    def test_group_accounts_exact_division(self):
        """Test grouping when accounts divide evenly."""
        accounts = [{'Id': str(i)} for i in range(9)]
        groups = group_accounts(accounts, 3)
        
        assert len(groups) == 3
        assert all(len(g) == 3 for g in groups)
    
    def test_group_accounts_single_account(self):
        """Test grouping single account."""
        accounts = [{'Id': '1', 'Name': 'Test', 'Email': 'test@example.com'}]
        groups = group_accounts(accounts, 5)
        
        assert len(groups) == 1
        assert len(groups[0]) == 1
    
    def test_group_accounts_preserves_all_accounts(self):
        """Test that no accounts are lost during grouping."""
        accounts = [{'Id': str(i)} for i in range(270)]
        groups = group_accounts(accounts, 50)
        
        total_in_groups = sum(len(g) for g in groups)
        assert total_in_groups == 270
        
        # Verify all IDs are present
        all_ids = [acc['Id'] for g in groups for acc in g]
        original_ids = [acc['Id'] for acc in accounts]
        assert sorted(all_ids) == sorted(original_ids)
    
    def test_group_accounts_group_size_one(self):
        """Test grouping with group size of 1."""
        accounts = [{'Id': str(i)} for i in range(5)]
        groups = group_accounts(accounts, 1)
        
        assert len(groups) == 5
        assert all(len(g) == 1 for g in groups)
    
    def test_group_accounts_group_size_larger_than_list(self):
        """Test grouping when group size exceeds account count."""
        accounts = [{'Id': str(i)} for i in range(3)]
        groups = group_accounts(accounts, 100)
        
        assert len(groups) == 1
        assert len(groups[0]) == 3
    
    def test_group_accounts_handles_zero_group_size(self):
        """Test that zero group size is handled (defaults to 1)."""
        accounts = [{'Id': str(i)} for i in range(5)]
        groups = group_accounts(accounts, 0)
        
        # Should default to group_size=1
        assert len(groups) == 5
    
    def test_group_accounts_handles_negative_group_size(self):
        """Test that negative group size is handled (defaults to 1)."""
        accounts = [{'Id': str(i)} for i in range(5)]
        groups = group_accounts(accounts, -5)
        
        # Should default to group_size=1
        assert len(groups) == 5


class TestInvokeWorkersAsync:
    """Test Worker Lambda invocation functionality."""
    
    @patch('scheduler_handler.boto3.client')
    def test_invoke_workers_success(self, mock_boto_client):
        """Test successful Worker invocation."""
        mock_lambda = Mock()
        mock_lambda.invoke.return_value = {'StatusCode': 202}
        mock_boto_client.return_value = mock_lambda
        
        with patch.dict(os.environ, {'WORKER_LAMBDA_NAME': 'test-worker'}):
            account_groups = [
                [{'Id': '1', 'Name': 'Acc1', 'Email': 'acc1@example.com'}],
                [{'Id': '2', 'Name': 'Acc2', 'Email': 'acc2@example.com'}]
            ]
            
            result = invoke_workers_async(
                account_groups=account_groups,
                invocation_id='test-uuid',
                timestamp='2024-01-01T00:00:00Z'
            )
            
            assert result['workers_invoked'] == 2
            assert result['errors'] == []
            assert mock_lambda.invoke.call_count == 2
    
    @patch('scheduler_handler.boto3.client')
    def test_invoke_workers_missing_lambda_name(self, mock_boto_client):
        """Test invocation fails when WORKER_LAMBDA_NAME is not set."""
        with patch.dict(os.environ, {}, clear=True):
            # Ensure WORKER_LAMBDA_NAME is not set
            if 'WORKER_LAMBDA_NAME' in os.environ:
                del os.environ['WORKER_LAMBDA_NAME']
            
            account_groups = [[{'Id': '1'}]]
            
            result = invoke_workers_async(
                account_groups=account_groups,
                invocation_id='test-uuid',
                timestamp='2024-01-01T00:00:00Z'
            )
            
            assert result['workers_invoked'] == 0
            assert len(result['errors']) == 1
            assert 'WORKER_LAMBDA_NAME' in result['errors'][0]
    
    @patch('scheduler_handler.boto3.client')
    def test_invoke_workers_partial_failure(self, mock_boto_client):
        """Test that partial failures don't stop other invocations."""
        from botocore.exceptions import ClientError
        
        mock_lambda = Mock()
        # First call succeeds, second fails, third succeeds
        mock_lambda.invoke.side_effect = [
            {'StatusCode': 202},
            ClientError({'Error': {'Code': 'ServiceException', 'Message': 'Test error'}}, 'Invoke'),
            {'StatusCode': 202}
        ]
        mock_boto_client.return_value = mock_lambda
        
        with patch.dict(os.environ, {'WORKER_LAMBDA_NAME': 'test-worker'}):
            account_groups = [
                [{'Id': '1'}],
                [{'Id': '2'}],
                [{'Id': '3'}]
            ]
            
            result = invoke_workers_async(
                account_groups=account_groups,
                invocation_id='test-uuid',
                timestamp='2024-01-01T00:00:00Z'
            )
            
            assert result['workers_invoked'] == 2
            assert len(result['errors']) == 1
    
    @patch('scheduler_handler.boto3.client')
    def test_invoke_workers_event_format(self, mock_boto_client):
        """Test that Worker events have correct format."""
        mock_lambda = Mock()
        mock_lambda.invoke.return_value = {'StatusCode': 202}
        mock_boto_client.return_value = mock_lambda
        
        with patch.dict(os.environ, {'WORKER_LAMBDA_NAME': 'test-worker'}):
            account_groups = [
                [{'Id': '1', 'Name': 'Acc1', 'Email': 'acc1@example.com'}]
            ]
            
            invoke_workers_async(
                account_groups=account_groups,
                invocation_id='test-uuid',
                timestamp='2024-01-01T00:00:00Z'
            )
            
            # Verify the invoke call
            call_args = mock_lambda.invoke.call_args
            payload = json.loads(call_args.kwargs['Payload'])
            
            assert 'accounts' in payload
            assert 'group_index' in payload
            assert 'total_groups' in payload
            assert 'invocation_id' in payload
            assert 'timestamp' in payload
            
            assert payload['group_index'] == 0
            assert payload['total_groups'] == 1
            assert payload['invocation_id'] == 'test-uuid'
            assert payload['timestamp'] == '2024-01-01T00:00:00Z'
            assert len(payload['accounts']) == 1


class TestGetActiveAccounts:
    """Test active accounts retrieval functionality."""
    
    @patch('scheduler_handler.AccountManager')
    @patch('scheduler_handler.DynamoDBManager')
    def test_get_active_accounts_success(self, mock_ddb_manager, mock_account_manager):
        """Test successful account retrieval."""
        from src.config import Config
        
        mock_am_instance = Mock()
        mock_am_instance.get_active_accounts.return_value = [
            {'Id': '123456789012', 'Name': 'Test Account', 'Email': 'test@example.com'}
        ]
        mock_account_manager.return_value = mock_am_instance
        
        config = Config()
        accounts = get_active_accounts(config)
        
        assert len(accounts) == 1
        assert accounts[0]['Id'] == '123456789012'
    
    @patch('scheduler_handler.AccountManager')
    @patch('scheduler_handler.DynamoDBManager')
    def test_get_active_accounts_error_returns_empty(self, mock_ddb_manager, mock_account_manager):
        """Test that errors return empty list."""
        from src.config import Config
        
        mock_account_manager.side_effect = Exception("Test error")
        
        config = Config()
        accounts = get_active_accounts(config)
        
        assert accounts == []


class TestLambdaHandler:
    """Test Lambda handler entry point."""
    
    @patch('scheduler_handler.invoke_workers_async')
    @patch('scheduler_handler.get_active_accounts')
    @patch('scheduler_handler.Config')
    def test_lambda_handler_success(self, mock_config_class, mock_get_accounts, mock_invoke):
        """Test successful Lambda handler execution."""
        mock_config = Mock()
        mock_config.validate.return_value = []
        mock_config_class.from_environment.return_value = mock_config
        
        mock_get_accounts.return_value = [
            {'Id': str(i), 'Name': f'Acc{i}', 'Email': f'acc{i}@example.com'}
            for i in range(100)
        ]
        
        mock_invoke.return_value = {
            'workers_invoked': 2,
            'errors': []
        }
        
        with patch.dict(os.environ, {'ACCOUNTS_PER_WORKER': '50'}):
            result = lambda_handler({}, None)
        
        assert result['statusCode'] == 200
        assert result['total_accounts'] == 100
        assert result['total_groups'] == 2
        assert result['workers_invoked'] == 2
        assert 'invocation_id' in result
        assert result['errors'] == []
    
    @patch('scheduler_handler.Config')
    def test_lambda_handler_config_validation_failure(self, mock_config_class):
        """Test Lambda handler with config validation failure."""
        mock_config = Mock()
        mock_config.validate.return_value = ['Invalid config']
        mock_config_class.from_environment.return_value = mock_config
        
        result = lambda_handler({}, None)
        
        assert result['statusCode'] == 500
        assert 'Configuration validation failed' in result['errors'][0]
    
    @patch('scheduler_handler.get_active_accounts')
    @patch('scheduler_handler.Config')
    def test_lambda_handler_no_accounts(self, mock_config_class, mock_get_accounts):
        """Test Lambda handler when no accounts are found."""
        mock_config = Mock()
        mock_config.validate.return_value = []
        mock_config_class.from_environment.return_value = mock_config
        
        mock_get_accounts.return_value = []
        
        result = lambda_handler({}, None)
        
        assert result['statusCode'] == 200
        assert result['total_accounts'] == 0
        assert result['total_groups'] == 0
        assert result['workers_invoked'] == 0
    
    @patch('scheduler_handler.Config')
    def test_lambda_handler_exception(self, mock_config_class):
        """Test Lambda handler with unexpected exception."""
        mock_config_class.from_environment.side_effect = Exception("Unexpected error")
        
        result = lambda_handler({}, None)
        
        assert result['statusCode'] == 500
        assert 'Scheduler Lambda failed' in result['errors'][0]
