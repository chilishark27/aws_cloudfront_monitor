"""
DynamoDB Manager module for CloudFront Abuse Detection System.

This module provides DynamoDB table lifecycle management and operations
with retry logic for handling throttling and transient errors.
"""

import time
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from .config import Config


logger = logging.getLogger(__name__)


class DynamoDBManager:
    """
    Manages DynamoDB table lifecycle and operations with retry logic.
    
    This class handles:
    - Automatic table creation with correct schemas
    - Table existence checking with caching
    - Retry logic with exponential backoff for throttling
    - Graceful degradation when tables are unavailable
    """
    
    # Retryable error codes
    RETRYABLE_ERRORS = {
        'ThrottlingException',
        'ProvisionedThroughputExceededException',
        'RequestLimitExceeded',
        'ServiceUnavailable',
        'InternalServerError',
    }
    
    def __init__(self, config: Config):
        """
        Initialize DynamoDB Manager.
        
        Args:
            config: Configuration object with DynamoDB settings
        """
        self.config = config
        self.dynamodb = boto3.resource('dynamodb', region_name=config.region)
        self.dynamodb_client = boto3.client('dynamodb', region_name=config.region)
        self._table_cache: Dict[str, bool] = {}
        
    def ensure_tables_exist(self) -> Dict[str, bool]:
        """
        Ensure all required tables exist, create if missing.
        
        This method checks for the existence of all required DynamoDB tables
        and creates them if they don't exist. It handles table creation
        asynchronously and waits for tables to become active.
        
        Returns:
            Dict[str, bool]: Dictionary mapping table names to creation status
                            (True if created/exists, False if creation failed)
        """
        results = {}
        
        # Define table schemas
        table_schemas = {
            self.config.ddb_abuse_counter_table: {
                'key_schema': [
                    {'AttributeName': 'CounterKey', 'KeyType': 'HASH'}
                ],
                'attribute_definitions': [
                    {'AttributeName': 'CounterKey', 'AttributeType': 'S'}
                ],
                'ttl_attribute': 'TTL'
            },
            self.config.ddb_accounts_cache_table: {
                'key_schema': [
                    {'AttributeName': 'CacheKey', 'KeyType': 'HASH'}
                ],
                'attribute_definitions': [
                    {'AttributeName': 'CacheKey', 'AttributeType': 'S'}
                ],
                'ttl_attribute': 'TTL'
            },
            self.config.ddb_failed_accounts_table: {
                'key_schema': [
                    {'AttributeName': 'AccountId', 'KeyType': 'HASH'}
                ],
                'attribute_definitions': [
                    {'AttributeName': 'AccountId', 'AttributeType': 'S'}
                ],
                'ttl_attribute': 'TTL'
            },
            self.config.ddb_sent_alerts_table: {
                'key_schema': [
                    {'AttributeName': 'AlertKey', 'KeyType': 'HASH'}
                ],
                'attribute_definitions': [
                    {'AttributeName': 'AlertKey', 'AttributeType': 'S'}
                ],
                'ttl_attribute': 'TTL'
            }
        }
        
        # Create each table if it doesn't exist
        for table_name, schema in table_schemas.items():
            try:
                success = self._create_table_if_not_exists(
                    table_name=table_name,
                    key_schema=schema['key_schema'],
                    attribute_definitions=schema['attribute_definitions'],
                    ttl_attribute=schema['ttl_attribute']
                )
                results[table_name] = success
                
                if success:
                    logger.info(f"Table {table_name} is ready")
                else:
                    logger.warning(f"Table {table_name} creation failed, continuing with degraded functionality")
                    
            except Exception as e:
                logger.error(
                    f"Unexpected error ensuring table {table_name} exists",
                    exc_info=True,
                    extra={'table_name': table_name, 'error': str(e)}
                )
                results[table_name] = False
        
        return results
    
    def _create_table_if_not_exists(
        self,
        table_name: str,
        key_schema: List[Dict[str, str]],
        attribute_definitions: List[Dict[str, str]],
        ttl_attribute: str
    ) -> bool:
        """
        Create a single table if it doesn't exist.
        
        Args:
            table_name: Name of the table to create
            key_schema: DynamoDB key schema definition
            attribute_definitions: DynamoDB attribute definitions
            ttl_attribute: Name of the TTL attribute
            
        Returns:
            bool: True if table exists or was created successfully, False otherwise
        """
        # Check cache first
        if self._table_exists(table_name):
            logger.info(f"Table {table_name} already exists (cached)")
            return True
        
        try:
            # Check if table actually exists
            self.dynamodb_client.describe_table(TableName=table_name)
            logger.info(f"Table {table_name} already exists")
            self._table_cache[table_name] = True
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                # Table doesn't exist, create it
                logger.info(f"Table {table_name} does not exist, creating...")
                return self._create_table(
                    table_name=table_name,
                    key_schema=key_schema,
                    attribute_definitions=attribute_definitions,
                    ttl_attribute=ttl_attribute
                )
            else:
                # Other error (e.g., access denied)
                logger.error(
                    f"Error checking if table {table_name} exists: {e.response['Error']['Code']}",
                    extra={
                        'table_name': table_name,
                        'error_code': e.response['Error']['Code'],
                        'error_message': e.response['Error']['Message']
                    }
                )
                return False
    
    def _create_table(
        self,
        table_name: str,
        key_schema: List[Dict[str, str]],
        attribute_definitions: List[Dict[str, str]],
        ttl_attribute: str
    ) -> bool:
        """
        Create a DynamoDB table with the specified schema.
        
        Args:
            table_name: Name of the table to create
            key_schema: DynamoDB key schema definition
            attribute_definitions: DynamoDB attribute definitions
            ttl_attribute: Name of the TTL attribute
            
        Returns:
            bool: True if table was created successfully, False otherwise
        """
        try:
            # Create table with on-demand billing
            self.dynamodb_client.create_table(
                TableName=table_name,
                KeySchema=key_schema,
                AttributeDefinitions=attribute_definitions,
                BillingMode='PAY_PER_REQUEST',
                Tags=[
                    {'Key': 'Purpose', 'Value': 'CloudFrontAbuseDetection'},
                    {'Key': 'Environment', 'Value': 'production'}
                ]
            )
            
            logger.info(f"Table {table_name} creation initiated, waiting for active status...")
            
            # Wait for table to become active
            waiter = self.dynamodb_client.get_waiter('table_exists')
            waiter.wait(
                TableName=table_name,
                WaiterConfig={'Delay': 2, 'MaxAttempts': 30}
            )
            
            # Enable TTL
            try:
                self.dynamodb_client.update_time_to_live(
                    TableName=table_name,
                    TimeToLiveSpecification={
                        'Enabled': True,
                        'AttributeName': ttl_attribute
                    }
                )
                logger.info(f"TTL enabled on table {table_name} for attribute {ttl_attribute}")
            except ClientError as ttl_error:
                # TTL enablement is not critical, log and continue
                logger.warning(
                    f"Failed to enable TTL on table {table_name}: {ttl_error.response['Error']['Code']}",
                    extra={
                        'table_name': table_name,
                        'ttl_attribute': ttl_attribute,
                        'error': str(ttl_error)
                    }
                )
            
            logger.info(f"Table {table_name} created successfully")
            self._table_cache[table_name] = True
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            
            if error_code == 'ResourceInUseException':
                # Table was created by another process, this is fine
                logger.info(f"Table {table_name} already exists (created by another process)")
                self._table_cache[table_name] = True
                return True
            else:
                # Creation failed
                logger.error(
                    f"Failed to create table {table_name}: {error_code}",
                    extra={
                        'table_name': table_name,
                        'error_code': error_code,
                        'error_message': e.response['Error']['Message']
                    }
                )
                return False
                
        except Exception as e:
            logger.error(
                f"Unexpected error creating table {table_name}",
                exc_info=True,
                extra={'table_name': table_name, 'error': str(e)}
            )
            return False
    
    def _table_exists(self, table_name: str) -> bool:
        """
        Check if table exists (with caching).
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            bool: True if table exists in cache, False otherwise
        """
        return self._table_cache.get(table_name, False)
    
    def get_item_with_retry(
        self,
        table_name: str,
        key: Dict[str, Any],
        max_retries: int = 3
    ) -> Optional[Dict[str, Any]]:
        """
        Get item from DynamoDB with exponential backoff retry.
        
        Args:
            table_name: Name of the table
            key: Primary key of the item to retrieve
            max_retries: Maximum number of retry attempts
            
        Returns:
            Optional[Dict]: Item data if found, None otherwise
        """
        table = self.dynamodb.Table(table_name)
        
        for attempt in range(max_retries):
            try:
                response = table.get_item(Key=key)
                return response.get('Item')
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                
                if error_code in self.RETRYABLE_ERRORS:
                    if attempt < max_retries - 1:
                        # Exponential backoff: 2^attempt seconds
                        wait_time = 2 ** attempt
                        logger.warning(
                            f"Retryable error getting item from {table_name}, "
                            f"retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})",
                            extra={
                                'table_name': table_name,
                                'error_code': error_code,
                                'attempt': attempt + 1,
                                'wait_time': wait_time
                            }
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(
                            f"Max retries exhausted getting item from {table_name}",
                            extra={
                                'table_name': table_name,
                                'error_code': error_code,
                                'max_retries': max_retries
                            }
                        )
                        return None
                else:
                    # Non-retryable error
                    logger.error(
                        f"Non-retryable error getting item from {table_name}: {error_code}",
                        extra={
                            'table_name': table_name,
                            'error_code': error_code,
                            'error_message': e.response['Error']['Message']
                        }
                    )
                    return None
                    
            except Exception as e:
                logger.error(
                    f"Unexpected error getting item from {table_name}",
                    exc_info=True,
                    extra={'table_name': table_name, 'error': str(e)}
                )
                return None
        
        return None
    
    def put_item_with_retry(
        self,
        table_name: str,
        item: Dict[str, Any],
        max_retries: int = 3
    ) -> bool:
        """
        Put item to DynamoDB with exponential backoff retry.
        
        Args:
            table_name: Name of the table
            item: Item data to write
            max_retries: Maximum number of retry attempts
            
        Returns:
            bool: True if item was written successfully, False otherwise
        """
        table = self.dynamodb.Table(table_name)
        
        for attempt in range(max_retries):
            try:
                table.put_item(Item=item)
                return True
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                
                if error_code in self.RETRYABLE_ERRORS:
                    if attempt < max_retries - 1:
                        # Exponential backoff: 2^attempt seconds
                        wait_time = 2 ** attempt
                        logger.warning(
                            f"Retryable error putting item to {table_name}, "
                            f"retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})",
                            extra={
                                'table_name': table_name,
                                'error_code': error_code,
                                'attempt': attempt + 1,
                                'wait_time': wait_time
                            }
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(
                            f"Max retries exhausted putting item to {table_name}",
                            extra={
                                'table_name': table_name,
                                'error_code': error_code,
                                'max_retries': max_retries
                            }
                        )
                        return False
                else:
                    # Non-retryable error
                    logger.error(
                        f"Non-retryable error putting item to {table_name}: {error_code}",
                        extra={
                            'table_name': table_name,
                            'error_code': error_code,
                            'error_message': e.response['Error']['Message']
                        }
                    )
                    return False
                    
            except Exception as e:
                logger.error(
                    f"Unexpected error putting item to {table_name}",
                    exc_info=True,
                    extra={'table_name': table_name, 'error': str(e)}
                )
                return False
        
        return False
    
    def scan_with_pagination(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Scan table with automatic pagination.
        
        Args:
            table_name: Name of the table to scan
            
        Returns:
            List[Dict]: List of all items in the table
        """
        table = self.dynamodb.Table(table_name)
        items = []
        
        try:
            # Initial scan
            response = table.scan()
            items.extend(response.get('Items', []))
            
            # Continue scanning if there are more items
            while 'LastEvaluatedKey' in response:
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                items.extend(response.get('Items', []))
            
            logger.info(
                f"Scanned {len(items)} items from {table_name}",
                extra={'table_name': table_name, 'item_count': len(items)}
            )
            return items
            
        except ClientError as e:
            logger.error(
                f"Error scanning table {table_name}: {e.response['Error']['Code']}",
                extra={
                    'table_name': table_name,
                    'error_code': e.response['Error']['Code'],
                    'error_message': e.response['Error']['Message']
                }
            )
            return []
            
        except Exception as e:
            logger.error(
                f"Unexpected error scanning table {table_name}",
                exc_info=True,
                extra={'table_name': table_name, 'error': str(e)}
            )
            return []
