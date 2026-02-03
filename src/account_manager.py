"""
Account Manager module for CloudFront Abuse Detection System.

This module manages AWS account discovery and failed account tracking.
It provides caching of account lists and maintains a list of accounts
that have failed to process to avoid repeated failures.
"""

import json
import logging
from typing import Dict, List, Optional, Set
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from .config import Config
from .dynamodb_manager import DynamoDBManager


logger = logging.getLogger(__name__)


class AccountManager:
    """
    Manages AWS account discovery and failed account tracking.
    
    This class handles:
    - Fetching active accounts from AWS Organizations
    - Caching account lists in DynamoDB (24-hour TTL)
    - Tracking failed accounts to skip problematic accounts
    - Proper exception handling for Organizations API
    """
    
    def __init__(self, config: Config, ddb_manager: DynamoDBManager):
        """
        Initialize Account Manager.
        
        Args:
            config: Configuration object with AWS settings
            ddb_manager: DynamoDB manager for caching operations
        """
        self.config = config
        self.ddb = ddb_manager
        self.org_client = boto3.client('organizations', region_name=config.region)
        self._failed_accounts_cache: Optional[Set[str]] = None
    
    def get_active_accounts(self) -> List[Dict[str, str]]:
        """
        Get list of active accounts from Organizations (with caching).
        
        This method first checks the DynamoDB cache for a recent account list.
        If the cache is valid (within TTL), it returns the cached data.
        Otherwise, it fetches fresh data from AWS Organizations API.
        
        Failed accounts are automatically excluded from the returned list.
        
        Returns:
            List[Dict[str, str]]: List of account dictionaries with keys:
                - Id: AWS account ID
                - Name: Account name
                - Email: Account email
                
        Note:
            Returns empty list if Organizations API is unavailable or
            if all accounts have failed.
        """
        try:
            # Try to get cached accounts first
            cached_accounts = self._get_cached_accounts()
            if cached_accounts is not None:
                logger.info(
                    f"Using cached account list ({len(cached_accounts)} accounts)",
                    extra={'account_count': len(cached_accounts), 'source': 'cache'}
                )
                # Filter out failed accounts
                failed_accounts = self.get_failed_accounts()
                active_accounts = [
                    acc for acc in cached_accounts
                    if acc['Id'] not in failed_accounts
                ]
                logger.info(
                    f"Filtered to {len(active_accounts)} active accounts "
                    f"(excluded {len(failed_accounts)} failed accounts)",
                    extra={
                        'total_accounts': len(cached_accounts),
                        'active_accounts': len(active_accounts),
                        'failed_accounts': len(failed_accounts)
                    }
                )
                return active_accounts
            
            # Cache miss or expired, fetch from Organizations
            logger.info("Account cache miss or expired, fetching from Organizations API")
            accounts = self._fetch_accounts_from_organizations()
            
            if accounts:
                # Cache the fetched accounts
                self._cache_accounts(accounts)
                
                # Filter out failed accounts
                failed_accounts = self.get_failed_accounts()
                active_accounts = [
                    acc for acc in accounts
                    if acc['Id'] not in failed_accounts
                ]
                logger.info(
                    f"Fetched {len(accounts)} accounts from Organizations, "
                    f"{len(active_accounts)} active after filtering",
                    extra={
                        'total_accounts': len(accounts),
                        'active_accounts': len(active_accounts),
                        'failed_accounts': len(failed_accounts),
                        'source': 'organizations_api'
                    }
                )
                return active_accounts
            else:
                logger.warning("No accounts fetched from Organizations API")
                return []
                
        except Exception as e:
            # Catch-all for unexpected errors
            logger.error(
                "Unexpected error in get_active_accounts",
                exc_info=True,
                extra={'error': str(e)}
            )
            return []
    
    def get_failed_accounts(self) -> Set[str]:
        """
        Get set of account IDs that have failed (with caching).
        
        This method maintains an in-memory cache of failed accounts
        to avoid repeated DynamoDB scans within the same execution.
        
        Returns:
            Set[str]: Set of account IDs that have failed to process
            
        Note:
            Returns empty set if DynamoDB scan fails.
        """
        # Return cached value if available
        if self._failed_accounts_cache is not None:
            return self._failed_accounts_cache
        
        try:
            # Scan failed accounts table
            items = self.ddb.scan_with_pagination(
                self.config.ddb_failed_accounts_table
            )
            
            # Extract account IDs
            failed_account_ids = {item['AccountId'] for item in items if 'AccountId' in item}
            
            # Cache the result
            self._failed_accounts_cache = failed_account_ids
            
            logger.info(
                f"Loaded {len(failed_account_ids)} failed accounts from DynamoDB",
                extra={'failed_account_count': len(failed_account_ids)}
            )
            
            return failed_account_ids
            
        except ClientError as e:
            # DynamoDB scan failed
            logger.error(
                f"Failed to scan failed accounts table: {e.response['Error']['Code']}",
                extra={
                    'table_name': self.config.ddb_failed_accounts_table,
                    'error_code': e.response['Error']['Code'],
                    'error_message': e.response['Error']['Message']
                }
            )
            # Return empty set as safe default
            return set()
            
        except Exception as e:
            # Unexpected error
            logger.error(
                "Unexpected error getting failed accounts",
                exc_info=True,
                extra={
                    'table_name': self.config.ddb_failed_accounts_table,
                    'error': str(e)
                }
            )
            # Return empty set as safe default
            return set()
    
    def record_failed_account(self, account_id: str, error: str) -> None:
        """
        Record an account that failed to process.
        
        This method stores the failed account in DynamoDB with a TTL
        so that it will be automatically removed after the configured
        retention period (default 7 days).
        
        Args:
            account_id: AWS account ID that failed
            error: Error message describing the failure (truncated to 200 chars)
        """
        try:
            # Truncate error message to avoid exceeding DynamoDB item size limits
            error_message = error[:200] if len(error) > 200 else error
            
            # Calculate TTL (current time + failed_accounts_ttl)
            ttl = int(datetime.now(timezone.utc).timestamp()) + self.config.failed_accounts_ttl
            
            # Create item
            item = {
                'AccountId': account_id,
                'Error': error_message,
                'Timestamp': datetime.now(timezone.utc).isoformat(),
                'TTL': ttl
            }
            
            # Write to DynamoDB
            success = self.ddb.put_item_with_retry(
                table_name=self.config.ddb_failed_accounts_table,
                item=item
            )
            
            if success:
                logger.info(
                    f"Recorded failed account {account_id}",
                    extra={
                        'account_id': account_id,
                        'error': error_message,
                        'ttl': ttl
                    }
                )
                # Update in-memory cache if it exists
                if self._failed_accounts_cache is not None:
                    self._failed_accounts_cache.add(account_id)
            else:
                logger.warning(
                    f"Failed to record failed account {account_id} in DynamoDB",
                    extra={'account_id': account_id, 'error': error_message}
                )
                
        except ClientError as e:
            # DynamoDB write failed
            logger.error(
                f"ClientError recording failed account {account_id}: {e.response['Error']['Code']}",
                extra={
                    'account_id': account_id,
                    'table_name': self.config.ddb_failed_accounts_table,
                    'error_code': e.response['Error']['Code'],
                    'error_message': e.response['Error']['Message']
                }
            )
            # Don't raise - this is not critical
            
        except Exception as e:
            # Unexpected error
            logger.error(
                f"Unexpected error recording failed account {account_id}",
                exc_info=True,
                extra={'account_id': account_id, 'error': str(e)}
            )
            # Don't raise - this is not critical
    
    def _fetch_accounts_from_organizations(self) -> List[Dict[str, str]]:
        """
        Fetch accounts from AWS Organizations API.
        
        This method uses the Organizations API to list all active accounts
        in the organization. It handles pagination automatically.
        
        Returns:
            List[Dict[str, str]]: List of account dictionaries
            
        Note:
            Returns empty list if Organizations API call fails.
        """
        accounts = []
        
        try:
            # Use paginator to handle large numbers of accounts
            paginator = self.org_client.get_paginator('list_accounts')
            page_iterator = paginator.paginate()
            
            for page in page_iterator:
                for account in page.get('Accounts', []):
                    # Only include active accounts
                    if account.get('Status') == 'ACTIVE':
                        accounts.append({
                            'Id': account['Id'],
                            'Name': account.get('Name', 'Unknown'),
                            'Email': account.get('Email', 'unknown@example.com')
                        })
            
            logger.info(
                f"Fetched {len(accounts)} active accounts from Organizations",
                extra={'account_count': len(accounts)}
            )
            return accounts
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            
            # Log specific AWS error code for troubleshooting
            logger.error(
                f"AWS Organizations API error: {error_code}",
                extra={
                    'error_code': error_code,
                    'error_message': e.response['Error']['Message'],
                    'operation': 'list_accounts'
                }
            )
            
            # Return empty list as safe default
            return []
            
        except Exception as e:
            # Unexpected error
            logger.error(
                "Unexpected error fetching accounts from Organizations",
                exc_info=True,
                extra={'error': str(e)}
            )
            # Return empty list as safe default
            return []
    
    def _cache_accounts(self, accounts: List[Dict[str, str]]) -> None:
        """
        Cache account list in DynamoDB.
        
        Args:
            accounts: List of account dictionaries to cache
        """
        try:
            # Calculate TTL (current time + accounts_cache_ttl)
            ttl = int(datetime.now(timezone.utc).timestamp()) + self.config.accounts_cache_ttl
            
            # Create cache item
            item = {
                'CacheKey': 'ou_accounts',
                'AccountsData': json.dumps(accounts),
                'Timestamp': datetime.now(timezone.utc).isoformat(),
                'TTL': ttl
            }
            
            # Write to DynamoDB
            success = self.ddb.put_item_with_retry(
                table_name=self.config.ddb_accounts_cache_table,
                item=item
            )
            
            if success:
                logger.info(
                    f"Cached {len(accounts)} accounts in DynamoDB",
                    extra={
                        'account_count': len(accounts),
                        'ttl': ttl,
                        'cache_key': 'ou_accounts'
                    }
                )
            else:
                logger.warning(
                    "Failed to cache accounts in DynamoDB",
                    extra={'account_count': len(accounts)}
                )
                
        except Exception as e:
            # Cache write failure is not critical
            logger.warning(
                "Error caching accounts in DynamoDB",
                extra={
                    'account_count': len(accounts),
                    'error': str(e)
                }
            )
            # Don't raise - caching failure should not stop processing
    
    def _get_cached_accounts(self) -> Optional[List[Dict[str, str]]]:
        """
        Retrieve cached account list from DynamoDB.
        
        Returns:
            Optional[List[Dict[str, str]]]: Cached account list if valid,
                                           None if cache miss or expired
        """
        try:
            # Get cached item
            item = self.ddb.get_item_with_retry(
                table_name=self.config.ddb_accounts_cache_table,
                key={'CacheKey': 'ou_accounts'}
            )
            
            if not item:
                logger.debug("No cached accounts found")
                return None
            
            # Check if cache is still valid
            timestamp_str = item.get('Timestamp')
            if not timestamp_str:
                logger.warning("Cached accounts missing timestamp, treating as expired")
                return None
            
            try:
                cached_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                current_time = datetime.now(timezone.utc)
                age_seconds = (current_time - cached_time).total_seconds()
                
                if age_seconds > self.config.accounts_cache_ttl:
                    logger.info(
                        f"Cached accounts expired (age: {age_seconds:.0f}s, TTL: {self.config.accounts_cache_ttl}s)",
                        extra={'cache_age_seconds': age_seconds, 'ttl_seconds': self.config.accounts_cache_ttl}
                    )
                    return None
                    
            except (ValueError, AttributeError) as e:
                logger.warning(
                    f"Error parsing cached accounts timestamp: {e}",
                    extra={'timestamp': timestamp_str, 'error': str(e)}
                )
                return None
            
            # Parse accounts data
            accounts_data = item.get('AccountsData')
            if not accounts_data:
                logger.warning("Cached accounts missing AccountsData field")
                return None
            
            try:
                accounts = json.loads(accounts_data)
                logger.debug(
                    f"Retrieved {len(accounts)} accounts from cache (age: {age_seconds:.0f}s)",
                    extra={'account_count': len(accounts), 'cache_age_seconds': age_seconds}
                )
                return accounts
                
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(
                    f"Error parsing cached accounts JSON: {e}",
                    extra={'error': str(e)}
                )
                return None
                
        except ClientError as e:
            # DynamoDB read failed
            logger.warning(
                f"Error reading cached accounts: {e.response['Error']['Code']}",
                extra={
                    'table_name': self.config.ddb_accounts_cache_table,
                    'error_code': e.response['Error']['Code']
                }
            )
            return None
            
        except Exception as e:
            # Unexpected error
            logger.warning(
                "Unexpected error getting cached accounts",
                extra={'error': str(e)}
            )
            return None
