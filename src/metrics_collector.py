"""
Metrics Collector module for CloudFront Abuse Detection System.

This module provides efficient CloudWatch metrics fetching with caching
and batch API operations to minimize API calls and improve performance.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Dict, Optional, Any

import boto3
from botocore.exceptions import ClientError

from .config import Config
from .dynamodb_manager import DynamoDBManager


logger = logging.getLogger(__name__)


@dataclass
class MetricData:
    """
    Container for CloudWatch metric data.
    
    Attributes:
        current_requests: Current 15-minute request count
        current_bytes: Current 15-minute bytes downloaded
        avg_requests: Historical average request count (per 15-min window)
        avg_bytes: Historical average bytes downloaded (per 15-min window)
    """
    current_requests: float
    current_bytes: float
    avg_requests: float
    avg_bytes: float


class MetricsCollector:
    """
    Collects CloudWatch metrics for CloudFront distributions.
    
    This class handles:
    - Batch metric fetching using get_metric_data API (1 call instead of 4)
    - Metric caching in DynamoDB with TTL
    - Proper exception handling for CloudWatch API errors
    - Consistent UTC timezone handling for all timestamps
    """
    
    # Retryable error codes for CloudWatch API
    RETRYABLE_ERRORS = {
        'ThrottlingException',
        'RequestLimitExceeded',
        'ServiceUnavailable',
        'InternalServerError',
    }
    
    def __init__(self, config: Config, ddb_manager: DynamoDBManager):
        """
        Initialize Metrics Collector.
        
        Args:
            config: Configuration object with metrics settings
            ddb_manager: DynamoDB manager for caching
        """
        self.config = config
        self.ddb = ddb_manager
    
    def get_metrics(
        self,
        cw_client,
        account_id: str,
        dist_id: str
    ) -> MetricData:
        """
        Get current and historical metrics for a distribution.
        
        This method fetches both current (last 15 minutes) and historical (24-hour average)
        metrics for a CloudFront distribution using a single batch API call.
        Historical metrics are cached in DynamoDB to reduce API calls.
        
        Args:
            cw_client: Boto3 CloudWatch client (with assumed role credentials)
            account_id: AWS account ID
            dist_id: CloudFront distribution ID
            
        Returns:
            MetricData: Current and historical metrics (zeros if unavailable)
        """
        try:
            # Get current time in UTC
            now = datetime.now(timezone.utc)
            
            # Try to get cached historical metrics first
            cached = self._get_cached_metrics(account_id, dist_id)
            
            if cached:
                # Cache hit - only fetch current metrics
                logger.info(
                    f"Using cached historical metrics for {dist_id}",
                    extra={
                        'account_id': account_id,
                        'distribution_id': dist_id,
                        'cache_hit': True
                    }
                )
                
                # Fetch current metrics only
                current_data = self._get_current_metrics_batch(
                    cw_client, dist_id, now
                )
                
                return MetricData(
                    current_requests=current_data.get('requests', 0.0),
                    current_bytes=current_data.get('bytes', 0.0),
                    avg_requests=cached.get('avg_requests', 0.0),
                    avg_bytes=cached.get('avg_bytes', 0.0)
                )
            else:
                # Cache miss - fetch both current and historical metrics
                logger.info(
                    f"Cache miss for {dist_id}, fetching all metrics",
                    extra={
                        'account_id': account_id,
                        'distribution_id': dist_id,
                        'cache_hit': False
                    }
                )
                
                # Fetch all metrics in a single batch call
                metrics = self._get_metrics_batch(
                    cw_client, dist_id, now
                )
                
                # Cache the historical metrics
                self._cache_metrics(
                    account_id,
                    dist_id,
                    metrics.get('avg_requests', 0.0),
                    metrics.get('avg_bytes', 0.0)
                )
                
                return MetricData(
                    current_requests=metrics.get('current_requests', 0.0),
                    current_bytes=metrics.get('current_bytes', 0.0),
                    avg_requests=metrics.get('avg_requests', 0.0),
                    avg_bytes=metrics.get('avg_bytes', 0.0)
                )
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.error(
                f"CloudWatch API error getting metrics for {dist_id}: {error_code}",
                extra={
                    'account_id': account_id,
                    'distribution_id': dist_id,
                    'error_code': error_code,
                    'error_message': e.response['Error']['Message']
                }
            )
            # Return zeros on error
            return MetricData(
                current_requests=0.0,
                current_bytes=0.0,
                avg_requests=0.0,
                avg_bytes=0.0
            )
            
        except Exception as e:
            logger.error(
                f"Unexpected error getting metrics for {dist_id}",
                exc_info=True,
                extra={
                    'account_id': account_id,
                    'distribution_id': dist_id,
                    'error': str(e)
                }
            )
            # Return zeros on error
            return MetricData(
                current_requests=0.0,
                current_bytes=0.0,
                avg_requests=0.0,
                avg_bytes=0.0
            )
    
    def _get_current_metrics_batch(
        self,
        cw_client,
        dist_id: str,
        end_time: datetime
    ) -> Dict[str, float]:
        """
        Fetch current metrics (last 15 minutes) using batch API.
        
        Args:
            cw_client: Boto3 CloudWatch client
            dist_id: CloudFront distribution ID
            end_time: End time for metric query (UTC)
            
        Returns:
            Dict: Dictionary with 'requests' and 'bytes' keys
        """
        # Time range: last 15 minutes
        start_time = end_time - timedelta(minutes=15)
        
        try:
            # Build metric queries for batch fetch
            # IMPORTANT: CloudFront metrics require Region=Global dimension
            metric_queries = [
                {
                    'Id': 'requests',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'AWS/CloudFront',
                            'MetricName': 'Requests',
                            'Dimensions': [
                                {'Name': 'Region', 'Value': 'Global'},
                                {'Name': 'DistributionId', 'Value': dist_id}
                            ]
                        },
                        'Period': 900,  # 15 minutes
                        'Stat': 'Sum'  # Requests uses Sum for total count
                    }
                },
                {
                    'Id': 'bytes',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'AWS/CloudFront',
                            'MetricName': 'BytesDownloaded',
                            'Dimensions': [
                                {'Name': 'Region', 'Value': 'Global'},
                                {'Name': 'DistributionId', 'Value': dist_id}
                            ]
                        },
                        'Period': 900,  # 15 minutes
                        'Stat': 'Sum'  # BytesDownloaded uses Sum
                    }
                }
            ]
            
            # Fetch metrics in a single batch call
            response = cw_client.get_metric_data(
                MetricDataQueries=metric_queries,
                StartTime=start_time,
                EndTime=end_time
            )
            
            # Parse results
            results = {}
            for result in response.get('MetricDataResults', []):
                metric_id = result['Id']
                values = result.get('Values', [])
                
                # Use the most recent value, or 0 if no data
                if values:
                    results[metric_id] = float(values[-1])
                else:
                    results[metric_id] = 0.0
                    logger.debug(
                        f"No data for metric {metric_id} in distribution {dist_id}",
                        extra={
                            'distribution_id': dist_id,
                            'metric_id': metric_id,
                            'start_time': start_time.isoformat(),
                            'end_time': end_time.isoformat()
                        }
                    )
            
            return results
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            
            if error_code in self.RETRYABLE_ERRORS:
                logger.warning(
                    f"Retryable error fetching current metrics for {dist_id}: {error_code}",
                    extra={
                        'distribution_id': dist_id,
                        'error_code': error_code
                    }
                )
            else:
                logger.error(
                    f"Non-retryable error fetching current metrics for {dist_id}: {error_code}",
                    extra={
                        'distribution_id': dist_id,
                        'error_code': error_code,
                        'error_message': e.response['Error']['Message']
                    }
                )
            
            return {'requests': 0.0, 'bytes': 0.0}
    
    def _get_metrics_batch(
        self,
        cw_client,
        dist_id: str,
        end_time: datetime
    ) -> Dict[str, float]:
        """
        Fetch multiple metrics in a single API call using get_metric_data.
        
        This method fetches both current (last 15 minutes) and historical (24-hour average)
        metrics for both Requests and BytesDownloaded in a single batch API call,
        reducing API calls from 4 to 1.
        
        Args:
            cw_client: Boto3 CloudWatch client
            dist_id: CloudFront distribution ID
            end_time: End time for metric query (UTC)
            
        Returns:
            Dict: Dictionary with current and average values for both metrics
        """
        # Time ranges
        current_start = end_time - timedelta(minutes=15)
        historical_start = end_time - timedelta(hours=24)
        
        try:
            # Build metric queries for batch fetch
            # IMPORTANT: CloudFront metrics require Region=Global dimension
            # Dimension order: Region first, then DistributionId (matching AWS console)
            # Both Requests and BytesDownloaded use Sum statistic
            metric_queries = [
                # Current requests (last 15 minutes)
                {
                    'Id': 'current_requests',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'AWS/CloudFront',
                            'MetricName': 'Requests',
                            'Dimensions': [
                                {'Name': 'Region', 'Value': 'Global'},
                                {'Name': 'DistributionId', 'Value': dist_id}
                            ]
                        },
                        'Period': 900,  # 15 minutes
                        'Stat': 'Sum'  # Requests uses Sum for total count
                    }
                },
                # Current bytes (last 15 minutes)
                {
                    'Id': 'current_bytes',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'AWS/CloudFront',
                            'MetricName': 'BytesDownloaded',
                            'Dimensions': [
                                {'Name': 'Region', 'Value': 'Global'},
                                {'Name': 'DistributionId', 'Value': dist_id}
                            ]
                        },
                        'Period': 900,  # 15 minutes
                        'Stat': 'Sum'  # BytesDownloaded uses Sum
                    }
                },
                # Historical requests (24-hour average, 15-min periods)
                {
                    'Id': 'avg_requests',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'AWS/CloudFront',
                            'MetricName': 'Requests',
                            'Dimensions': [
                                {'Name': 'Region', 'Value': 'Global'},
                                {'Name': 'DistributionId', 'Value': dist_id}
                            ]
                        },
                        'Period': 900,  # 15 minute periods
                        'Stat': 'Sum'  # Requests uses Sum for total count
                    }
                },
                # Historical bytes (24-hour average, 15-min periods)
                {
                    'Id': 'avg_bytes',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'AWS/CloudFront',
                            'MetricName': 'BytesDownloaded',
                            'Dimensions': [
                                {'Name': 'Region', 'Value': 'Global'},
                                {'Name': 'DistributionId', 'Value': dist_id}
                            ]
                        },
                        'Period': 900,  # 15 minute periods
                        'Stat': 'Sum'  # BytesDownloaded uses Sum
                    }
                }
            ]
            
            # Fetch current metrics (last hour)
            current_response = cw_client.get_metric_data(
                MetricDataQueries=metric_queries[:2],
                StartTime=current_start,
                EndTime=end_time
            )
            
            # Fetch historical metrics (last 24 hours)
            historical_response = cw_client.get_metric_data(
                MetricDataQueries=metric_queries[2:],
                StartTime=historical_start,
                EndTime=end_time
            )
            
            # Parse current metrics
            results = {}
            for result in current_response.get('MetricDataResults', []):
                metric_id = result['Id']
                values = result.get('Values', [])
                
                if values:
                    results[metric_id] = float(values[-1])
                else:
                    results[metric_id] = 0.0
                    logger.debug(
                        f"No current data for {metric_id} in distribution {dist_id}",
                        extra={
                            'distribution_id': dist_id,
                            'metric_id': metric_id
                        }
                    )
            
            # Parse historical metrics and calculate averages
            for result in historical_response.get('MetricDataResults', []):
                metric_id = result['Id']
                values = result.get('Values', [])
                
                if values:
                    # Calculate average over the 24-hour period
                    avg_value = sum(values) / len(values)
                    results[metric_id] = float(avg_value)
                else:
                    results[metric_id] = 0.0
                    logger.debug(
                        f"No historical data for {metric_id} in distribution {dist_id}",
                        extra={
                            'distribution_id': dist_id,
                            'metric_id': metric_id
                        }
                    )
            
            logger.debug(
                f"Fetched metrics for {dist_id}",
                extra={
                    'distribution_id': dist_id,
                    'current_requests': results.get('current_requests', 0.0),
                    'current_bytes': results.get('current_bytes', 0.0),
                    'avg_requests': results.get('avg_requests', 0.0),
                    'avg_bytes': results.get('avg_bytes', 0.0)
                }
            )
            
            return results
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            
            if error_code in self.RETRYABLE_ERRORS:
                logger.warning(
                    f"Retryable error fetching metrics for {dist_id}: {error_code}",
                    extra={
                        'distribution_id': dist_id,
                        'error_code': error_code
                    }
                )
            else:
                logger.error(
                    f"Non-retryable error fetching metrics for {dist_id}: {error_code}",
                    extra={
                        'distribution_id': dist_id,
                        'error_code': error_code,
                        'error_message': e.response['Error']['Message']
                    }
                )
            
            return {
                'current_requests': 0.0,
                'current_bytes': 0.0,
                'avg_requests': 0.0,
                'avg_bytes': 0.0
            }
    
    def _get_cached_metrics(
        self,
        account_id: str,
        dist_id: str
    ) -> Optional[Dict[str, float]]:
        """
        Retrieve cached historical metrics from DynamoDB.
        
        Args:
            account_id: AWS account ID
            dist_id: CloudFront distribution ID
            
        Returns:
            Optional[Dict]: Cached metrics if valid, None otherwise
        """
        try:
            cache_key = f"metrics#{account_id}#{dist_id}"
            
            item = self.ddb.get_item_with_retry(
                table_name=self.config.ddb_accounts_cache_table,
                key={'CacheKey': cache_key}
            )
            
            if not item:
                return None
            
            # Check if cache is still valid
            timestamp_str = item.get('Timestamp')
            if not timestamp_str:
                logger.debug(
                    f"Cached metrics for {dist_id} missing timestamp",
                    extra={
                        'account_id': account_id,
                        'distribution_id': dist_id
                    }
                )
                return None
            
            try:
                cached_time = datetime.fromisoformat(timestamp_str)
                # Ensure cached_time is timezone-aware (UTC)
                if cached_time.tzinfo is None:
                    cached_time = cached_time.replace(tzinfo=timezone.utc)
                
                now = datetime.now(timezone.utc)
                age_seconds = (now - cached_time).total_seconds()
                
                if age_seconds > self.config.metrics_cache_ttl:
                    logger.debug(
                        f"Cached metrics for {dist_id} expired (age: {age_seconds}s)",
                        extra={
                            'account_id': account_id,
                            'distribution_id': dist_id,
                            'age_seconds': age_seconds,
                            'ttl': self.config.metrics_cache_ttl
                        }
                    )
                    return None
                
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"Invalid timestamp in cached metrics for {dist_id}: {timestamp_str}",
                    extra={
                        'account_id': account_id,
                        'distribution_id': dist_id,
                        'timestamp': timestamp_str,
                        'error': str(e)
                    }
                )
                return None
            
            # Extract metrics
            avg_requests = item.get('AvgRequests')
            avg_bytes = item.get('AvgBytes')
            
            if avg_requests is None or avg_bytes is None:
                logger.debug(
                    f"Cached metrics for {dist_id} missing data fields",
                    extra={
                        'account_id': account_id,
                        'distribution_id': dist_id
                    }
                )
                return None
            
            return {
                'avg_requests': float(avg_requests),
                'avg_bytes': float(avg_bytes)
            }
            
        except ValueError as e:
            logger.warning(
                f"Error parsing cached metrics for {dist_id}",
                extra={
                    'account_id': account_id,
                    'distribution_id': dist_id,
                    'error': str(e)
                }
            )
            return None
            
        except Exception as e:
            logger.error(
                f"Unexpected error retrieving cached metrics for {dist_id}",
                exc_info=True,
                extra={
                    'account_id': account_id,
                    'distribution_id': dist_id,
                    'error': str(e)
                }
            )
            return None
    
    def _cache_metrics(
        self,
        account_id: str,
        dist_id: str,
        avg_requests: float,
        avg_bytes: float
    ) -> None:
        """
        Cache historical metrics in DynamoDB.
        
        Args:
            account_id: AWS account ID
            dist_id: CloudFront distribution ID
            avg_requests: Average requests per hour
            avg_bytes: Average bytes downloaded per hour
        """
        try:
            cache_key = f"metrics#{account_id}#{dist_id}"
            now = datetime.now(timezone.utc)
            ttl = int(now.timestamp()) + self.config.metrics_cache_ttl
            
            # Convert float to Decimal for DynamoDB compatibility
            item = {
                'CacheKey': cache_key,
                'AvgRequests': Decimal(str(avg_requests)),
                'AvgBytes': Decimal(str(avg_bytes)),
                'Timestamp': now.isoformat(),
                'TTL': ttl
            }
            
            success = self.ddb.put_item_with_retry(
                table_name=self.config.ddb_accounts_cache_table,
                item=item
            )
            
            if success:
                logger.debug(
                    f"Cached metrics for {dist_id}",
                    extra={
                        'account_id': account_id,
                        'distribution_id': dist_id,
                        'avg_requests': avg_requests,
                        'avg_bytes': avg_bytes
                    }
                )
            else:
                logger.warning(
                    f"Failed to cache metrics for {dist_id}",
                    extra={
                        'account_id': account_id,
                        'distribution_id': dist_id
                    }
                )
                
        except Exception as e:
            # Caching failure is not critical, just log and continue
            logger.warning(
                f"Error caching metrics for {dist_id}: {str(e)[:100]}",
                extra={
                    'account_id': account_id,
                    'distribution_id': dist_id
                }
            )
