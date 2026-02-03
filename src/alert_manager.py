"""
Alert Manager module for CloudFront Abuse Detection System.

This module provides alert deduplication, formatting, and delivery
with persistent storage and asynchronous sending capabilities.
"""

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional, Set, Dict, Any
from urllib.parse import urlencode

import boto3
import urllib3
from botocore.exceptions import ClientError

from .config import Config
from .dynamodb_manager import DynamoDBManager


logger = logging.getLogger(__name__)


@dataclass
class Alert:
    """
    Alert data model.
    
    Represents a CloudFront abuse alert with all necessary context
    for formatting and sending notifications.
    """
    account_id: str
    account_name: str
    account_email: str
    distribution_id: str
    metric: str  # 'Requests' or 'BytesDownloaded'
    severity: str  # 'Warning' or 'Critical'
    current_value: float
    history_value: float
    abuse_multiplier: float
    consecutive_count: int
    timestamp: str  # ISO 8601 UTC
    percentage_change: float


class AlertManager:
    """
    Manages alert deduplication, formatting, and delivery.
    
    This class handles:
    - Alert deduplication (in-memory + DynamoDB persistence)
    - Alert formatting with severity levels
    - Telegram sending with retry logic
    - Timezone conversion for display (UTC to configured timezone)
    - Thread pool management for async sending
    """
    
    # Retryable error types for Telegram API
    RETRYABLE_ERRORS = (
        urllib3.exceptions.TimeoutError,
        urllib3.exceptions.ConnectionError,
    )
    
    def __init__(self, config: Config, ddb_manager: DynamoDBManager):
        """
        Initialize Alert Manager.
        
        Args:
            config: Configuration object with alert settings
            ddb_manager: DynamoDB manager for persistent storage
        """
        self.config = config
        self.ddb = ddb_manager
        self._sent_alerts: Set[str] = set()  # In-memory deduplication
        self._alert_executor: Optional[ThreadPoolExecutor] = None
        self._payer_id: Optional[str] = None
        self._http = urllib3.PoolManager(
            timeout=urllib3.Timeout(connect=5.0, read=10.0),
            retries=False  # We handle retries manually
        )
        
    def initialize(self) -> None:
        """
        Initialize alert executor thread pool.
        
        This should be called before sending any alerts.
        """
        if self._alert_executor is None:
            self._alert_executor = ThreadPoolExecutor(
                max_workers=self.config.alert_max_workers,
                thread_name_prefix='alert-sender'
            )
            logger.info(
                f"Alert executor initialized with {self.config.alert_max_workers} workers"
            )
    
    def shutdown(self, timeout: int = 30) -> None:
        """
        Shutdown alert executor and wait for completion.
        
        Args:
            timeout: Maximum seconds to wait for pending alerts (not used in Python < 3.9)
        """
        if self._alert_executor is not None:
            logger.info("Shutting down alert executor...")
            # Note: timeout parameter only available in Python 3.9+
            # Lambda runtime may use older Python, so we just use wait=True
            self._alert_executor.shutdown(wait=True)
            self._alert_executor = None
            logger.info("Alert executor shut down successfully")
    
    def send_alert_async(self, alert: Alert) -> None:
        """
        Send alert asynchronously with deduplication.
        
        This method queues the alert for sending in a background thread.
        Duplicate alerts (within the configured time window) are filtered out.
        
        Args:
            alert: Alert object to send
        """
        if self._alert_executor is None:
            logger.error("Alert executor not initialized, cannot send alert")
            return
        
        # Submit alert to thread pool
        self._alert_executor.submit(self._send_alert_with_dedup, alert)
    
    def _send_alert_with_dedup(self, alert: Alert) -> bool:
        """
        Send alert with deduplication check (internal method).
        
        Args:
            alert: Alert object to send
            
        Returns:
            bool: True if alert was sent, False if duplicate or failed
        """
        try:
            # Check for duplicate
            if self._is_duplicate_alert(alert):
                logger.info(
                    f"Skipping duplicate alert for {alert.distribution_id} "
                    f"({alert.metric}, {alert.severity})",
                    extra={
                        'account_id': alert.account_id,
                        'distribution_id': alert.distribution_id,
                        'metric': alert.metric,
                        'severity': alert.severity
                    }
                )
                return False
            
            # Send alert
            success = self._send_alert(alert)
            
            if success:
                # Record sent alert
                self._record_sent_alert(alert)
                logger.info(
                    f"Alert sent successfully for {alert.distribution_id} "
                    f"({alert.metric}, {alert.severity})",
                    extra={
                        'account_id': alert.account_id,
                        'distribution_id': alert.distribution_id,
                        'metric': alert.metric,
                        'severity': alert.severity
                    }
                )
            else:
                logger.error(
                    f"Failed to send alert for {alert.distribution_id} "
                    f"({alert.metric}, {alert.severity})",
                    extra={
                        'account_id': alert.account_id,
                        'distribution_id': alert.distribution_id,
                        'metric': alert.metric,
                        'severity': alert.severity
                    }
                )
            
            return success
            
        except Exception as e:
            logger.error(
                f"Unexpected error sending alert for {alert.distribution_id}",
                exc_info=True,
                extra={
                    'account_id': alert.account_id,
                    'distribution_id': alert.distribution_id,
                    'metric': alert.metric,
                    'error': str(e)
                }
            )
            return False
    
    def _send_alert(self, alert: Alert) -> bool:
        """
        Send alert to Telegram (synchronous with retry).
        
        Args:
            alert: Alert object to send
            
        Returns:
            bool: True if alert was sent successfully, False otherwise
        """
        # Check if Telegram is configured
        if not self.config.telegram_bot_token or not self.config.telegram_chat_id:
            logger.warning("Telegram not configured, skipping alert")
            return False
        
        # Format alert message
        message = self._format_alert_message(alert)
        
        # Telegram API endpoint
        url = f"https://api.telegram.org/bot{self.config.telegram_bot_token}/sendMessage"
        
        # Request payload (plain text, no HTML parsing)
        payload = {
            'chat_id': self.config.telegram_chat_id,
            'text': message
        }
        
        # Retry logic
        max_retries = 2
        for attempt in range(max_retries + 1):
            try:
                # Send request
                response = self._http.request(
                    'POST',
                    url,
                    body=urlencode(payload).encode('utf-8'),
                    headers={'Content-Type': 'application/x-www-form-urlencoded'}
                )
                
                # Check response
                if response.status == 200:
                    return True
                else:
                    logger.warning(
                        f"Telegram API returned status {response.status}",
                        extra={
                            'status_code': response.status,
                            'response_body': response.data.decode('utf-8', errors='ignore')[:200],
                            'attempt': attempt + 1
                        }
                    )
                    
                    # Retry on server errors
                    if response.status >= 500 and attempt < max_retries:
                        wait_time = 2 ** attempt
                        logger.info(f"Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    
                    return False
                    
            except self.RETRYABLE_ERRORS as e:
                if attempt < max_retries:
                    wait_time = 2 ** attempt
                    logger.warning(
                        f"Retryable error sending to Telegram: {type(e).__name__}, "
                        f"retrying in {wait_time}s (attempt {attempt + 1}/{max_retries + 1})",
                        extra={
                            'error_type': type(e).__name__,
                            'error': str(e),
                            'attempt': attempt + 1,
                            'wait_time': wait_time
                        }
                    )
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(
                        f"Max retries exhausted sending to Telegram: {type(e).__name__}",
                        extra={
                            'error_type': type(e).__name__,
                            'error': str(e),
                            'max_retries': max_retries
                        }
                    )
                    return False
                    
            except Exception as e:
                logger.error(
                    f"Unexpected error sending to Telegram: {type(e).__name__}",
                    exc_info=True,
                    extra={
                        'error_type': type(e).__name__,
                        'error': str(e)
                    }
                )
                return False
        
        return False
    
    def _is_duplicate_alert(self, alert: Alert) -> bool:
        """
        Check if alert is duplicate (in-memory + DynamoDB).
        
        An alert is considered duplicate if an alert with the same
        account_id, distribution_id, and metric was sent within the
        configured time window (default 1 hour).
        
        Args:
            alert: Alert object to check
            
        Returns:
            bool: True if alert is duplicate, False otherwise
        """
        # Generate alert key
        alert_timestamp = datetime.fromisoformat(alert.timestamp.replace('Z', '+00:00'))
        hour_key = alert_timestamp.strftime('%Y%m%d%H')
        alert_key = f"{alert.account_id}#{alert.distribution_id}#{alert.metric}#{hour_key}"
        
        # Check in-memory cache first
        if alert_key in self._sent_alerts:
            return True
        
        # Check DynamoDB
        try:
            item = self.ddb.get_item_with_retry(
                table_name=self.config.ddb_sent_alerts_table,
                key={'AlertKey': alert_key}
            )
            
            if item is not None:
                # Alert was sent recently
                self._sent_alerts.add(alert_key)
                return True
            
            return False
            
        except Exception as e:
            logger.error(
                f"Error checking for duplicate alert in DynamoDB",
                exc_info=True,
                extra={
                    'alert_key': alert_key,
                    'error': str(e)
                }
            )
            # On error, check in-memory only (fail open to avoid missing alerts)
            return False
    
    def _record_sent_alert(self, alert: Alert) -> None:
        """
        Record sent alert in DynamoDB and in-memory cache.
        
        Args:
            alert: Alert object that was sent
        """
        # Generate alert key
        alert_timestamp = datetime.fromisoformat(alert.timestamp.replace('Z', '+00:00'))
        hour_key = alert_timestamp.strftime('%Y%m%d%H')
        alert_key = f"{alert.account_id}#{alert.distribution_id}#{alert.metric}#{hour_key}"
        
        # Add to in-memory cache
        self._sent_alerts.add(alert_key)
        
        # Calculate TTL (24 hours from now)
        ttl = int((datetime.now(timezone.utc) + timedelta(seconds=self.config.sent_alerts_ttl)).timestamp())
        
        # Record in DynamoDB - convert float to Decimal for DynamoDB compatibility
        item = {
            'AlertKey': alert_key,
            'AccountId': alert.account_id,
            'DistributionId': alert.distribution_id,
            'MetricName': alert.metric,
            'Severity': alert.severity,
            'CurrentValue': Decimal(str(alert.current_value)),
            'HistoryValue': Decimal(str(alert.history_value)),
            'SentTimestamp': alert.timestamp,
            'TTL': ttl
        }
        
        try:
            self.ddb.put_item_with_retry(
                table_name=self.config.ddb_sent_alerts_table,
                item=item
            )
        except Exception as e:
            logger.error(
                f"Error recording sent alert in DynamoDB: {str(e)[:100]}"
            )
            # Non-critical error, continue
    
    def _format_alert_message(self, alert: Alert) -> str:
        """
        Format alert message for Telegram using bilingual template.
        
        Args:
            alert: Alert object to format
            
        Returns:
            str: Formatted message for Telegram
        """
        # Convert timestamp to display timezone
        display_time = self._convert_to_display_timezone(alert.timestamp)
        
        # Get payer account ID
        payer_id = self._get_payer_account_id()
        
        # Format metric value based on type
        if alert.metric == 'Requests':
            current_str = f"{alert.current_value:,.0f} 请求"
            history_str = f"{alert.history_value:,.0f} 请求"
            threshold_value = alert.history_value * alert.abuse_multiplier
            threshold_str = f"{threshold_value:,.0f} 请求"
        else:  # BytesDownloaded
            current_str = self._format_bytes(alert.current_value)
            history_str = self._format_bytes(alert.history_value)
            threshold_value = alert.history_value * alert.abuse_multiplier
            threshold_str = self._format_bytes(threshold_value)
        
        # Severity header
        if alert.severity == "Critical":
            severity_header = "🔴 Critical Alert 🔴"
            severity_cn = "紧急告警"
        else:
            severity_header = "⚠️ Warning Alert ⚠️"
            severity_cn = "警告"
        
        # Build message using the template format
        message = f"""⚠️ Payer {payer_id} ⚠️
{severity_header}
⚠️ 以下Amazon CloudFront分配疑似被盗刷（流量异常） ⚠️
⚠️ The following Amazon CloudFront distribution has triggered a traffic alert ⚠️

#CDN盗刷 #流量异常 #{severity_cn} #AWS #CloudFront

帐号ID | Account ID : {alert.account_id}
帐号名称 | Account Name : {alert.account_name}
帐号电邮 | Account Email : {alert.account_email}
分配 | Distribution : {alert.distribution_id}

当前15分钟 | Current 15 min : {current_str}
过去24小时平均 | Past 24h average : {history_str}
滥用阈值 | Abuse Threshold : {threshold_str} ({alert.abuse_multiplier}x)
连续超标 | Consecutive Count : {alert.consecutive_count} 次

{display_time}"""
        
        return message
    
    def _convert_to_display_timezone(self, utc_timestamp: str) -> str:
        """
        Convert UTC timestamp to configured display timezone.
        
        Args:
            utc_timestamp: ISO 8601 UTC timestamp string
            
        Returns:
            str: Formatted timestamp in display timezone (e.g., "Tue, 03 Feb 2026 14:54:48 +0800")
        """
        try:
            # Parse UTC timestamp
            dt_utc = datetime.fromisoformat(utc_timestamp.replace('Z', '+00:00'))
            
            # Convert to display timezone
            offset_hours = self.config.display_timezone_offset
            dt_display = dt_utc + timedelta(hours=offset_hours)
            
            # Format with RFC 2822 style (e.g., "Tue, 03 Feb 2026 14:54:48 +0800")
            tz_sign = '+' if offset_hours >= 0 else '-'
            tz_str = f"{tz_sign}{abs(offset_hours):02d}00"
            return dt_display.strftime(f'%a, %d %b %Y %H:%M:%S {tz_str}')
            
        except Exception as e:
            logger.error(
                f"Error converting timestamp to display timezone",
                exc_info=True,
                extra={'timestamp': utc_timestamp, 'error': str(e)}
            )
            # Return original timestamp on error
            return utc_timestamp
    
    def _format_bytes(self, bytes_value: float) -> str:
        """
        Format bytes value in human-readable format.
        
        Args:
            bytes_value: Number of bytes
            
        Returns:
            str: Formatted string (e.g., "1.5 GB")
        """
        units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
        unit_index = 0
        value = bytes_value
        
        while value >= 1024 and unit_index < len(units) - 1:
            value /= 1024
            unit_index += 1
        
        return f"{value:.2f} {units[unit_index]}"
    
    def _get_payer_account_id(self) -> str:
        """
        Get organization payer account ID (cached).
        
        Returns:
            str: Payer account ID or "Unknown" if unavailable
        """
        if self._payer_id is not None:
            return self._payer_id
        
        try:
            org_client = boto3.client('organizations', region_name=self.config.region)
            response = org_client.describe_organization()
            self._payer_id = response['Organization']['MasterAccountId']
            return self._payer_id
            
        except ClientError as e:
            logger.warning(
                f"Failed to get payer account ID: {e.response['Error']['Code']}",
                extra={
                    'error_code': e.response['Error']['Code'],
                    'error_message': e.response['Error']['Message']
                }
            )
            self._payer_id = "Unknown"
            return self._payer_id
            
        except Exception as e:
            logger.error(
                f"Unexpected error getting payer account ID",
                exc_info=True,
                extra={'error': str(e)}
            )
            self._payer_id = "Unknown"
            return self._payer_id
