"""
Observability module for CloudFront Abuse Detection System.

This module provides structured logging, CloudWatch metrics publishing,
and health check functionality for monitoring system operations.
"""

import logging
import json
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from .config import Config


class ObservabilityManager:
    """
    Manages observability features including structured logging,
    CloudWatch metrics, and health checks.
    
    This class handles:
    - Structured logging with context fields
    - CloudWatch metrics buffering and batch publishing
    - Health check validation of all system components
    """
    
    def __init__(self, config: Config):
        """
        Initialize Observability Manager.
        
        Args:
            config: Configuration object with observability settings
        """
        self.config = config
        self.cw_client = boto3.client('cloudwatch', region_name=config.region)
        self.logger = self._setup_logger()
        self._metrics_buffer: List[Dict[str, Any]] = []
        
    def _setup_logger(self) -> logging.Logger:
        """
        Configure structured logging.
        
        Returns:
            logging.Logger: Configured logger instance
        """
        logger = logging.getLogger('cloudfront_abuse_detection')
        
        # Only configure if not already configured
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            
            # Create console handler with structured format
            handler = logging.StreamHandler()
            handler.setLevel(logging.INFO)
            
            # Use JSON formatter for structured logging
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            
            logger.addHandler(handler)
        
        return logger
    
    def log_info(self, message: str, **context) -> None:
        """
        Log info message with structured context.
        
        Args:
            message: Log message
            **context: Additional context fields (account_id, distribution_id, etc.)
        """
        if context:
            # Add context as extra fields
            self.logger.info(message, extra=context)
        else:
            self.logger.info(message)
    
    def log_warning(self, message: str, **context) -> None:
        """
        Log warning message with structured context.
        
        Args:
            message: Log message
            **context: Additional context fields
        """
        if context:
            self.logger.warning(message, extra=context)
        else:
            self.logger.warning(message)
    
    def log_error(self, message: str, error: Optional[Exception] = None, **context) -> None:
        """
        Log error message with exception details and context.
        
        Args:
            message: Log message
            error: Exception object (optional)
            **context: Additional context fields
        """
        if error:
            # Include exception info
            context['error_type'] = type(error).__name__
            context['error_message'] = str(error)[:200]  # Truncate long error messages
            
            if isinstance(error, ClientError):
                # Add AWS-specific error details
                context['error_code'] = error.response['Error']['Code']
                context['error_details'] = error.response['Error']['Message'][:200]
        
        # Build context string for cleaner logging
        context_str = ' | '.join(f"{k}={v}" for k, v in context.items()) if context else ''
        
        if context_str:
            self.logger.error(f"{message} | {context_str}")
        else:
            self.logger.error(message)
    
    def record_metric(self, metric_name: str, value: float, unit: str = 'Count') -> None:
        """
        Buffer a CloudWatch metric for later publishing.
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            unit: CloudWatch unit (Count, Seconds, etc.)
        """
        metric_data = {
            'MetricName': metric_name,
            'Value': value,
            'Unit': unit,
            'Timestamp': datetime.now(timezone.utc)
        }
        
        self._metrics_buffer.append(metric_data)
        
        self.log_info(
            f"Recorded metric: {metric_name}={value} {unit}",
            metric_name=metric_name,
            metric_value=value,
            metric_unit=unit
        )
    
    def publish_metrics(self) -> None:
        """
        Publish all buffered metrics to CloudWatch.
        
        This method publishes metrics in batches of 20 (CloudWatch limit)
        and handles errors gracefully.
        """
        if not self._metrics_buffer:
            self.log_info("No metrics to publish")
            return
        
        namespace = 'CloudFront/AbuseDetection'
        total_metrics = len(self._metrics_buffer)
        
        self.log_info(
            f"Publishing {total_metrics} metrics to CloudWatch",
            namespace=namespace,
            metric_count=total_metrics
        )
        
        # Publish in batches of 20 (CloudWatch limit)
        batch_size = 20
        published_count = 0
        failed_count = 0
        
        for i in range(0, len(self._metrics_buffer), batch_size):
            batch = self._metrics_buffer[i:i + batch_size]
            
            try:
                self.cw_client.put_metric_data(
                    Namespace=namespace,
                    MetricData=batch
                )
                published_count += len(batch)
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                
                if error_code in ['ThrottlingException', 'RequestLimitExceeded']:
                    # Retry once after a short delay
                    self.log_warning(
                        f"CloudWatch API throttled, retrying batch",
                        error_code=error_code,
                        batch_size=len(batch)
                    )
                    
                    try:
                        import time
                        time.sleep(1)
                        self.cw_client.put_metric_data(
                            Namespace=namespace,
                            MetricData=batch
                        )
                        published_count += len(batch)
                    except Exception as retry_error:
                        self.log_error(
                            f"Failed to publish metrics batch after retry",
                            error=retry_error,
                            batch_size=len(batch)
                        )
                        failed_count += len(batch)
                else:
                    self.log_error(
                        f"Failed to publish metrics batch",
                        error=e,
                        batch_size=len(batch)
                    )
                    failed_count += len(batch)
                    
            except Exception as e:
                self.log_error(
                    f"Unexpected error publishing metrics batch",
                    error=e,
                    batch_size=len(batch)
                )
                failed_count += len(batch)
        
        self.log_info(
            f"Metrics publishing complete: {published_count} published, {failed_count} failed",
            published_count=published_count,
            failed_count=failed_count,
            total_count=total_metrics
        )
        
        # Clear the buffer
        self._metrics_buffer.clear()
    
    def health_check(
        self,
        ddb_manager: Optional[Any] = None,
        account_manager: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        Perform health check and return status.
        
        This method validates:
        - Configuration parameters
        - DynamoDB table connectivity
        - AWS Organizations access
        - Telegram API connectivity (if configured)
        
        Args:
            ddb_manager: DynamoDB manager instance (optional)
            account_manager: Account manager instance (optional)
            
        Returns:
            Dict: Health check results with status and details
        """
        health_status = {
            'status': 'healthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'checks': {}
        }
        
        # 1. Validate configuration
        self.log_info("Running health check: Configuration validation")
        config_errors = self.config.validate()
        
        if config_errors:
            health_status['checks']['configuration'] = {
                'status': 'unhealthy',
                'errors': config_errors
            }
            health_status['status'] = 'unhealthy'
            self.log_error(
                "Configuration validation failed",
                error_count=len(config_errors)
            )
        else:
            health_status['checks']['configuration'] = {
                'status': 'healthy',
                'message': 'All configuration parameters are valid'
            }
            self.log_info("Configuration validation passed")
        
        # 2. Check DynamoDB connectivity
        if ddb_manager:
            self.log_info("Running health check: DynamoDB connectivity")
            try:
                # Try to list tables to verify connectivity
                ddb_manager.dynamodb_client.list_tables(Limit=1)
                
                # Check if required tables exist
                table_status = ddb_manager.ensure_tables_exist()
                all_tables_ok = all(table_status.values())
                
                if all_tables_ok:
                    health_status['checks']['dynamodb'] = {
                        'status': 'healthy',
                        'message': 'All required tables exist and are accessible',
                        'tables': table_status
                    }
                    self.log_info("DynamoDB connectivity check passed")
                else:
                    health_status['checks']['dynamodb'] = {
                        'status': 'degraded',
                        'message': 'Some tables are unavailable',
                        'tables': table_status
                    }
                    health_status['status'] = 'degraded'
                    self.log_warning(
                        "Some DynamoDB tables are unavailable",
                        tables=table_status
                    )
                    
            except ClientError as e:
                health_status['checks']['dynamodb'] = {
                    'status': 'unhealthy',
                    'error': e.response['Error']['Code'],
                    'message': e.response['Error']['Message']
                }
                health_status['status'] = 'unhealthy'
                self.log_error(
                    "DynamoDB connectivity check failed",
                    error=e
                )
                
            except Exception as e:
                health_status['checks']['dynamodb'] = {
                    'status': 'unhealthy',
                    'error': type(e).__name__,
                    'message': str(e)
                }
                health_status['status'] = 'unhealthy'
                self.log_error(
                    "DynamoDB connectivity check failed with unexpected error",
                    error=e
                )
        
        # 3. Check AWS Organizations access
        if account_manager:
            self.log_info("Running health check: AWS Organizations access")
            try:
                # Try to list accounts to verify access
                accounts = account_manager.get_active_accounts()
                
                health_status['checks']['organizations'] = {
                    'status': 'healthy',
                    'message': f'Successfully retrieved {len(accounts)} accounts',
                    'account_count': len(accounts)
                }
                self.log_info(
                    "AWS Organizations access check passed",
                    account_count=len(accounts)
                )
                
            except ClientError as e:
                health_status['checks']['organizations'] = {
                    'status': 'unhealthy',
                    'error': e.response['Error']['Code'],
                    'message': e.response['Error']['Message']
                }
                health_status['status'] = 'unhealthy'
                self.log_error(
                    "AWS Organizations access check failed",
                    error=e
                )
                
            except Exception as e:
                health_status['checks']['organizations'] = {
                    'status': 'unhealthy',
                    'error': type(e).__name__,
                    'message': str(e)
                }
                health_status['status'] = 'unhealthy'
                self.log_error(
                    "AWS Organizations access check failed with unexpected error",
                    error=e
                )
        
        # 4. Check Telegram API connectivity (if configured)
        if self.config.telegram_bot_token and self.config.telegram_chat_id:
            self.log_info("Running health check: Telegram API connectivity")
            try:
                import urllib3
                http = urllib3.PoolManager()
                
                # Test Telegram API with getMe endpoint
                url = f"https://api.telegram.org/bot{self.config.telegram_bot_token}/getMe"
                response = http.request('GET', url, timeout=5.0)
                
                if response.status == 200:
                    health_status['checks']['telegram'] = {
                        'status': 'healthy',
                        'message': 'Telegram API is accessible'
                    }
                    self.log_info("Telegram API connectivity check passed")
                else:
                    health_status['checks']['telegram'] = {
                        'status': 'degraded',
                        'message': f'Telegram API returned status {response.status}',
                        'http_status': response.status
                    }
                    # Telegram is non-critical, so don't mark overall status as unhealthy
                    if health_status['status'] == 'healthy':
                        health_status['status'] = 'degraded'
                    self.log_warning(
                        "Telegram API connectivity check returned non-200 status",
                        http_status=response.status
                    )
                    
            except Exception as e:
                health_status['checks']['telegram'] = {
                    'status': 'degraded',
                    'error': type(e).__name__,
                    'message': str(e)
                }
                # Telegram is non-critical, so don't mark overall status as unhealthy
                if health_status['status'] == 'healthy':
                    health_status['status'] = 'degraded'
                self.log_warning(
                    "Telegram API connectivity check failed (non-critical)",
                    error=e
                )
        
        # Log final health status
        self.log_info(
            f"Health check complete: {health_status['status']}",
            status=health_status['status'],
            checks=list(health_status['checks'].keys())
        )
        
        return health_status
