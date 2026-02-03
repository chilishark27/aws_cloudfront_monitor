"""
Scheduler Lambda Handler for CloudFront Abuse Detection System.

This module implements the Scheduler Lambda function that:
1. Fetches active accounts from AWS Organizations (via AccountManager)
2. Groups accounts by configured size
3. Asynchronously invokes Worker Lambda instances to process each group

The Scheduler follows a fire-and-forget pattern - it does not wait for
Worker Lambda execution results.
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError

from src.config import Config
from src.dynamodb_manager import DynamoDBManager
from src.account_manager import AccountManager


# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create console handler if not already present
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Scheduler Lambda entry point.
    
    This function is triggered by EventBridge on a schedule. It:
    1. Loads configuration from environment variables
    2. Fetches active accounts from AWS Organizations
    3. Groups accounts by configured size
    4. Asynchronously invokes Worker Lambda for each group
    
    Args:
        event: EventBridge trigger event
        context: Lambda context object
        
    Returns:
        Dict containing scheduling results:
        - statusCode: HTTP status code (200 for success)
        - total_accounts: Total number of accounts fetched
        - total_groups: Number of account groups created
        - workers_invoked: Number of Worker Lambda invocations
        - invocation_id: Unique ID for this scheduling run
        - errors: List of any errors encountered
    """
    invocation_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()
    errors: List[str] = []
    
    logger.info(
        f"Scheduler Lambda started",
        extra={
            'invocation_id': invocation_id,
            'timestamp': timestamp,
            'event': event
        }
    )
    
    try:
        # Load configuration
        config = Config.from_environment()
        validation_errors = config.validate()
        if validation_errors:
            error_msg = f"Configuration validation failed: {validation_errors}"
            logger.error(error_msg)
            return {
                'statusCode': 500,
                'total_accounts': 0,
                'total_groups': 0,
                'workers_invoked': 0,
                'invocation_id': invocation_id,
                'errors': [error_msg]
            }
        
        # Get accounts per worker from environment (default: 50)
        accounts_per_worker = int(os.getenv('ACCOUNTS_PER_WORKER', '50'))
        
        # Fetch active accounts
        accounts = get_active_accounts(config)
        total_accounts = len(accounts)
        
        logger.info(
            f"Fetched {total_accounts} active accounts",
            extra={
                'invocation_id': invocation_id,
                'total_accounts': total_accounts
            }
        )
        
        if total_accounts == 0:
            logger.warning(
                "No active accounts found, nothing to process",
                extra={'invocation_id': invocation_id}
            )
            return {
                'statusCode': 200,
                'total_accounts': 0,
                'total_groups': 0,
                'workers_invoked': 0,
                'invocation_id': invocation_id,
                'errors': []
            }
        
        # Group accounts
        account_groups = group_accounts(accounts, accounts_per_worker)
        total_groups = len(account_groups)
        
        logger.info(
            f"Grouped {total_accounts} accounts into {total_groups} groups "
            f"(group_size={accounts_per_worker})",
            extra={
                'invocation_id': invocation_id,
                'total_accounts': total_accounts,
                'total_groups': total_groups,
                'accounts_per_worker': accounts_per_worker
            }
        )
        
        # Invoke workers asynchronously
        invocation_result = invoke_workers_async(
            account_groups=account_groups,
            invocation_id=invocation_id,
            timestamp=timestamp
        )
        
        workers_invoked = invocation_result.get('workers_invoked', 0)
        invocation_errors = invocation_result.get('errors', [])
        errors.extend(invocation_errors)
        
        logger.info(
            f"Scheduler completed: {workers_invoked}/{total_groups} workers invoked",
            extra={
                'invocation_id': invocation_id,
                'total_accounts': total_accounts,
                'total_groups': total_groups,
                'workers_invoked': workers_invoked,
                'errors_count': len(errors)
            }
        )
        
        return {
            'statusCode': 200,
            'total_accounts': total_accounts,
            'total_groups': total_groups,
            'workers_invoked': workers_invoked,
            'invocation_id': invocation_id,
            'errors': errors
        }
        
    except Exception as e:
        error_msg = f"Scheduler Lambda failed: {str(e)}"
        logger.error(
            error_msg,
            exc_info=True,
            extra={'invocation_id': invocation_id}
        )
        return {
            'statusCode': 500,
            'total_accounts': 0,
            'total_groups': 0,
            'workers_invoked': 0,
            'invocation_id': invocation_id,
            'errors': [error_msg]
        }


def get_active_accounts(config: Config) -> List[Dict[str, str]]:
    """
    Get list of active accounts from AWS Organizations.
    
    This function reuses the AccountManager module to fetch active accounts.
    It initializes the DynamoDB manager and AccountManager, then retrieves
    the list of active accounts (excluding failed accounts).
    
    Args:
        config: Configuration object with AWS settings
        
    Returns:
        List of account dictionaries with keys:
        - Id: AWS account ID
        - Name: Account name
        - Email: Account email
        
    Note:
        Returns empty list if Organizations API is unavailable or
        if all accounts have failed.
    """
    try:
        # Initialize DynamoDB manager
        ddb_manager = DynamoDBManager(config)
        
        # Initialize AccountManager
        account_manager = AccountManager(config, ddb_manager)
        
        # Get active accounts (this handles caching and failed account filtering)
        accounts = account_manager.get_active_accounts()
        
        logger.info(
            f"Retrieved {len(accounts)} active accounts via AccountManager",
            extra={'account_count': len(accounts)}
        )
        
        return accounts
        
    except Exception as e:
        logger.error(
            f"Error getting active accounts: {str(e)}",
            exc_info=True
        )
        return []


def group_accounts(accounts: List[Dict], group_size: int) -> List[List[Dict]]:
    """
    Group accounts into batches of specified size.
    
    This function divides the account list into groups for parallel processing.
    When the total number of accounts is not evenly divisible by group_size,
    the remaining accounts are placed in the last group.
    
    Args:
        accounts: List of account dictionaries to group
        group_size: Maximum number of accounts per group (must be >= 1)
        
    Returns:
        List of account groups, where each group is a list of account dicts.
        Returns empty list if accounts is empty.
        
    Examples:
        >>> group_accounts([{'Id': '1'}, {'Id': '2'}, {'Id': '3'}], 2)
        [[{'Id': '1'}, {'Id': '2'}], [{'Id': '3'}]]
        
        >>> group_accounts([], 10)
        []
    """
    if not accounts:
        return []
    
    # Ensure group_size is at least 1
    group_size = max(1, group_size)
    
    groups = []
    for i in range(0, len(accounts), group_size):
        group = accounts[i:i + group_size]
        groups.append(group)
    
    logger.info(
        f"Created {len(groups)} account groups from {len(accounts)} accounts",
        extra={
            'total_accounts': len(accounts),
            'group_size': group_size,
            'total_groups': len(groups),
            'group_sizes': [len(g) for g in groups]
        }
    )
    
    return groups


def invoke_workers_async(
    account_groups: List[List[Dict]],
    invocation_id: str,
    timestamp: str
) -> Dict[str, Any]:
    """
    Asynchronously invoke Worker Lambda for each account group.
    
    This function invokes Worker Lambda instances in fire-and-forget mode
    (InvocationType='Event'). It does not wait for Worker execution results.
    
    If a Worker invocation fails, the error is logged and the function
    continues to invoke remaining Workers.
    
    Args:
        account_groups: List of account groups to process
        invocation_id: Unique ID for this scheduling run
        timestamp: ISO format timestamp of invocation
        
    Returns:
        Dict containing:
        - workers_invoked: Number of successful Worker invocations
        - errors: List of error messages for failed invocations
        
    Event Format sent to Worker:
        {
            "accounts": [...],
            "group_index": 0,
            "total_groups": 6,
            "invocation_id": "uuid-string",
            "timestamp": "2024-01-01T00:00:00Z"
        }
    """
    # Get Worker Lambda function name from environment
    worker_lambda_name = os.getenv('WORKER_LAMBDA_NAME', '')
    
    if not worker_lambda_name:
        error_msg = "WORKER_LAMBDA_NAME environment variable not set"
        logger.error(error_msg)
        return {
            'workers_invoked': 0,
            'errors': [error_msg]
        }
    
    # Initialize Lambda client
    lambda_client = boto3.client('lambda')
    
    total_groups = len(account_groups)
    workers_invoked = 0
    errors: List[str] = []
    
    for group_index, accounts in enumerate(account_groups):
        # Build event payload for Worker
        event_payload = {
            'accounts': accounts,
            'group_index': group_index,
            'total_groups': total_groups,
            'invocation_id': invocation_id,
            'timestamp': timestamp
        }
        
        try:
            # Invoke Worker Lambda asynchronously (fire-and-forget)
            response = lambda_client.invoke(
                FunctionName=worker_lambda_name,
                InvocationType='Event',  # Async invocation
                Payload=json.dumps(event_payload)
            )
            
            status_code = response.get('StatusCode', 0)
            
            if status_code == 202:  # 202 = Accepted for async invocation
                workers_invoked += 1
                logger.info(
                    f"Successfully invoked Worker for group {group_index + 1}/{total_groups}",
                    extra={
                        'invocation_id': invocation_id,
                        'group_index': group_index,
                        'accounts_in_group': len(accounts),
                        'status_code': status_code
                    }
                )
            else:
                error_msg = (
                    f"Unexpected status code {status_code} invoking Worker "
                    f"for group {group_index}"
                )
                logger.warning(
                    error_msg,
                    extra={
                        'invocation_id': invocation_id,
                        'group_index': group_index,
                        'status_code': status_code
                    }
                )
                errors.append(error_msg)
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            error_msg = (
                f"Failed to invoke Worker for group {group_index}: "
                f"{error_code} - {error_message}"
            )
            logger.error(
                error_msg,
                extra={
                    'invocation_id': invocation_id,
                    'group_index': group_index,
                    'error_code': error_code,
                    'error_message': error_message
                }
            )
            errors.append(error_msg)
            # Continue to next group - don't fail entire scheduling
            
        except Exception as e:
            error_msg = f"Unexpected error invoking Worker for group {group_index}: {str(e)}"
            logger.error(
                error_msg,
                exc_info=True,
                extra={
                    'invocation_id': invocation_id,
                    'group_index': group_index
                }
            )
            errors.append(error_msg)
            # Continue to next group - don't fail entire scheduling
    
    logger.info(
        f"Worker invocation complete: {workers_invoked}/{total_groups} successful",
        extra={
            'invocation_id': invocation_id,
            'workers_invoked': workers_invoked,
            'total_groups': total_groups,
            'errors_count': len(errors)
        }
    )
    
    return {
        'workers_invoked': workers_invoked,
        'errors': errors
    }
