"""
Worker Lambda Handler for CloudFront Abuse Detection System.

This module implements the Worker Lambda function that:
1. Receives a list of accounts from the Scheduler Lambda
2. Validates the event format
3. Initializes all processing components
4. Processes each account using existing modules (AccountProcessor)
5. Returns processing results to the caller

The Worker Lambda directly imports and calls existing modules without
modifying their internal logic, following the design principle of
minimal code changes.

Telegram credentials are loaded from environment variables:
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError

from src.config import Config
from src.dynamodb_manager import DynamoDBManager
from src.account_manager import AccountManager
from src.metrics_collector import MetricsCollector
from src.abuse_detector import AbuseDetector
from src.alert_manager import AlertManager
from src.distribution_processor import DistributionProcessor
from src.account_processor import AccountProcessor
from src.observability import ObservabilityManager


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


class EventValidationError(Exception):
    """Exception raised when event validation fails."""
    pass


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Worker Lambda entry point.
    
    This function is invoked asynchronously by the Scheduler Lambda.
    It processes a group of accounts for abuse detection.
    
    Args:
        event: Event payload from Scheduler containing:
            - accounts: List of account dictionaries
            - group_index: Index of this account group
            - total_groups: Total number of groups
            - invocation_id: Unique ID for this scheduling run
            - timestamp: ISO format timestamp of invocation
        context: Lambda context object
        
    Returns:
        Dict containing processing results:
        - statusCode: HTTP status code (200 for success, 400/500 for errors)
        - group_index: Index of this account group
        - accounts_processed: Number of accounts successfully processed
        - accounts_failed: Number of accounts that failed processing
        - alerts_generated: Total number of alerts sent
        - execution_time_seconds: Total execution time
        - errors: List of any errors encountered
    """
    start_time = time.time()
    group_index = event.get('group_index', -1)
    invocation_id = event.get('invocation_id', 'unknown')
    errors: List[str] = []
    
    logger.info(
        f"Worker Lambda started for group {group_index}",
        extra={
            'invocation_id': invocation_id,
            'group_index': group_index,
            'event_keys': list(event.keys())
        }
    )
    
    try:
        # 1. Validate event and extract accounts
        accounts = validate_event(event)
        
        logger.info(
            f"Validated event: {len(accounts)} accounts to process",
            extra={
                'invocation_id': invocation_id,
                'group_index': group_index,
                'account_count': len(accounts)
            }
        )
        
        # 2. Process accounts
        result = process_accounts(accounts, invocation_id, group_index)
        
        # 3. Calculate execution time
        execution_time = time.time() - start_time
        
        # 4. Build response
        response = {
            'statusCode': 200,
            'group_index': group_index,
            'accounts_processed': result.get('accounts_processed', 0),
            'accounts_failed': result.get('accounts_failed', 0),
            'alerts_generated': result.get('alerts_generated', 0),
            'execution_time_seconds': round(execution_time, 2),
            'errors': result.get('errors', [])
        }
        
        logger.info(
            f"Worker Lambda completed for group {group_index}",
            extra={
                'invocation_id': invocation_id,
                'group_index': group_index,
                'accounts_processed': response['accounts_processed'],
                'accounts_failed': response['accounts_failed'],
                'alerts_generated': response['alerts_generated'],
                'execution_time_seconds': response['execution_time_seconds']
            }
        )
        
        return response
        
    except EventValidationError as e:
        # Event validation failed
        execution_time = time.time() - start_time
        error_msg = f"Event validation failed: {str(e)}"
        logger.error(
            error_msg,
            extra={
                'invocation_id': invocation_id,
                'group_index': group_index
            }
        )
        
        return {
            'statusCode': 400,
            'group_index': group_index,
            'accounts_processed': 0,
            'accounts_failed': 0,
            'alerts_generated': 0,
            'execution_time_seconds': round(execution_time, 2),
            'errors': [error_msg]
        }
        
    except Exception as e:
        # Unexpected error
        execution_time = time.time() - start_time
        error_msg = f"Worker Lambda failed: {str(e)[:100]}"
        logger.error(error_msg)
        
        return {
            'statusCode': 500,
            'group_index': group_index,
            'accounts_processed': 0,
            'accounts_failed': 0,
            'alerts_generated': 0,
            'execution_time_seconds': round(execution_time, 2),
            'errors': [error_msg]
        }


def validate_event(event: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Validate event format and extract accounts list.
    
    This function validates that the event contains all required fields
    and that the accounts list is properly formatted.
    
    Args:
        event: Event payload from Scheduler
        
    Returns:
        List of account dictionaries with keys:
        - Id: AWS account ID
        - Name: Account name
        - Email: Account email
        
    Raises:
        EventValidationError: If event format is invalid
    """
    # Check if event is a dictionary
    if not isinstance(event, dict):
        raise EventValidationError(
            f"Event must be a dictionary, got {type(event).__name__}"
        )
    
    # Check for required 'accounts' field
    if 'accounts' not in event:
        raise EventValidationError("Event missing required field: 'accounts'")
    
    accounts = event['accounts']
    
    # Check if accounts is a list
    if not isinstance(accounts, list):
        raise EventValidationError(
            f"'accounts' must be a list, got {type(accounts).__name__}"
        )
    
    # Validate each account in the list
    validated_accounts = []
    for i, account in enumerate(accounts):
        # Check if account is a dictionary
        if not isinstance(account, dict):
            raise EventValidationError(
                f"Account at index {i} must be a dictionary, got {type(account).__name__}"
            )
        
        # Check for required fields
        required_fields = ['Id', 'Name', 'Email']
        missing_fields = [f for f in required_fields if f not in account]
        
        if missing_fields:
            raise EventValidationError(
                f"Account at index {i} missing required fields: {missing_fields}"
            )
        
        # Validate field types
        if not isinstance(account['Id'], str) or not account['Id']:
            raise EventValidationError(
                f"Account at index {i} has invalid 'Id': must be a non-empty string"
            )
        
        if not isinstance(account['Name'], str):
            raise EventValidationError(
                f"Account at index {i} has invalid 'Name': must be a string"
            )
        
        if not isinstance(account['Email'], str):
            raise EventValidationError(
                f"Account at index {i} has invalid 'Email': must be a string"
            )
        
        validated_accounts.append({
            'Id': account['Id'],
            'Name': account['Name'],
            'Email': account['Email']
        })
    
    logger.info(
        f"Event validation successful: {len(validated_accounts)} accounts",
        extra={'account_count': len(validated_accounts)}
    )
    
    return validated_accounts


def process_accounts(
    accounts: List[Dict[str, str]],
    invocation_id: str,
    group_index: int
) -> Dict[str, Any]:
    """
    Process accounts list using existing AccountProcessor.
    
    This function initializes all required components and processes
    each account in the list. It directly calls existing modules
    without modifying their internal logic.
    
    Args:
        accounts: List of account dictionaries to process
        invocation_id: Unique ID for this scheduling run
        group_index: Index of this account group
        
    Returns:
        Dict containing:
        - accounts_processed: Number of accounts successfully processed
        - accounts_failed: Number of accounts that failed processing
        - alerts_generated: Total number of alerts sent
        - errors: List of error messages
    """
    accounts_processed = 0
    accounts_failed = 0
    alerts_generated = 0
    errors: List[str] = []
    
    if not accounts:
        logger.info(
            "No accounts to process",
            extra={
                'invocation_id': invocation_id,
                'group_index': group_index
            }
        )
        return {
            'accounts_processed': 0,
            'accounts_failed': 0,
            'alerts_generated': 0,
            'errors': []
        }
    
    try:
        # 1. Load configuration
        config = Config.from_environment()
        validation_errors = config.validate()
        if validation_errors:
            error_msg = f"Configuration validation failed: {validation_errors}"
            logger.error(error_msg)
            return {
                'accounts_processed': 0,
                'accounts_failed': len(accounts),
                'alerts_generated': 0,
                'errors': [error_msg]
            }
        
        # 2. Telegram credentials are loaded from environment variables via Config
        # Config.from_environment() already loads TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID
        logger.info(
            "Telegram credentials loaded from environment variables",
            extra={
                'invocation_id': invocation_id,
                'has_bot_token': bool(config.telegram_bot_token),
                'has_chat_id': bool(config.telegram_chat_id)
            }
        )
        
        # 3. Initialize components
        logger.info(
            "Initializing processing components",
            extra={
                'invocation_id': invocation_id,
                'group_index': group_index
            }
        )
        
        # Initialize DynamoDB manager
        ddb_manager = DynamoDBManager(config)
        
        # Ensure tables exist
        table_status = ddb_manager.ensure_tables_exist()
        logger.info(
            f"DynamoDB tables status: {table_status}",
            extra={
                'invocation_id': invocation_id,
                'table_status': table_status
            }
        )
        
        # Initialize observability manager
        observability = ObservabilityManager(config)
        
        # Initialize account manager
        account_manager = AccountManager(config, ddb_manager)
        
        # Initialize metrics collector
        metrics_collector = MetricsCollector(config, ddb_manager)
        
        # Initialize abuse detector
        abuse_detector = AbuseDetector(config, ddb_manager)
        
        # Initialize alert manager
        alert_manager = AlertManager(config, ddb_manager)
        alert_manager.initialize()
        
        # Initialize distribution processor
        distribution_processor = DistributionProcessor(
            config=config,
            metrics_collector=metrics_collector,
            abuse_detector=abuse_detector,
            alert_manager=alert_manager,
            observability=observability
        )
        
        # Initialize account processor
        account_processor = AccountProcessor(
            config=config,
            account_manager=account_manager,
            distribution_processor=distribution_processor,
            observability=observability
        )
        
        logger.info(
            "All components initialized successfully",
            extra={
                'invocation_id': invocation_id,
                'group_index': group_index
            }
        )
        
        # 4. Process each account
        for account in accounts:
            account_id = account['Id']
            account_name = account.get('Name', 'Unknown')
            
            try:
                logger.info(
                    f"Processing account {account_id} ({account_name})",
                    extra={
                        'invocation_id': invocation_id,
                        'group_index': group_index,
                        'account_id': account_id,
                        'account_name': account_name
                    }
                )
                
                # Process account using AccountProcessor
                account_alerts = account_processor.process_account(account)
                
                accounts_processed += 1
                alerts_generated += account_alerts
                
                logger.info(
                    f"Account {account_id} processed: {account_alerts} alert(s)",
                    extra={
                        'invocation_id': invocation_id,
                        'group_index': group_index,
                        'account_id': account_id,
                        'alerts': account_alerts
                    }
                )
                
            except Exception as e:
                accounts_failed += 1
                error_msg = f"Failed to process account {account_id}: {str(e)[:100]}"
                errors.append(error_msg)
                logger.error(f"Account {account_id} failed: {str(e)[:100]}")
                # Continue processing other accounts
                continue
        
        # 5. Shutdown alert manager (wait for pending alerts)
        logger.info(
            "Shutting down alert manager",
            extra={
                'invocation_id': invocation_id,
                'group_index': group_index
            }
        )
        alert_manager.shutdown(timeout=30)
        
        # 6. Publish metrics
        observability.record_metric('AccountsProcessed', accounts_processed)
        observability.record_metric('AccountsFailed', accounts_failed)
        observability.record_metric('AlertsGenerated', alerts_generated)
        observability.publish_metrics()
        
        logger.info(
            f"Processing complete: {accounts_processed} processed, "
            f"{accounts_failed} failed, {alerts_generated} alerts",
            extra={
                'invocation_id': invocation_id,
                'group_index': group_index,
                'accounts_processed': accounts_processed,
                'accounts_failed': accounts_failed,
                'alerts_generated': alerts_generated
            }
        )
        
        return {
            'accounts_processed': accounts_processed,
            'accounts_failed': accounts_failed,
            'alerts_generated': alerts_generated,
            'errors': errors
        }
        
    except Exception as e:
        error_msg = f"Error in process_accounts: {str(e)[:100]}"
        logger.error(error_msg)
        errors.append(error_msg)
        
        return {
            'accounts_processed': accounts_processed,
            'accounts_failed': len(accounts) - accounts_processed,
            'alerts_generated': alerts_generated,
            'errors': errors
        }



