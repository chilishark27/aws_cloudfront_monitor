"""
Abuse Detector module for CloudFront Abuse Detection System.

This module provides abuse detection logic with corrected counter management,
dual-threshold evaluation (relative + absolute), and adaptive baseline handling
for low-traffic distributions.
"""

import logging
from dataclasses import dataclass
from typing import Tuple
from datetime import datetime, timezone

from botocore.exceptions import ClientError

from .config import Config
from .dynamodb_manager import DynamoDBManager


logger = logging.getLogger(__name__)


@dataclass
class AbuseEvaluation:
    """
    Result of evaluating a metric for abuse.
    
    Attributes:
        is_abuse: Whether the metric indicates abuse
        severity: Alert severity level ('Warning', 'Critical', or 'None')
        reason: Human-readable explanation of the evaluation
        percentage_change: Percentage change from baseline
        meets_critical_threshold: Whether critical (3x) threshold is met
        meets_warning_threshold: Whether warning (2x) threshold is met
        meets_absolute_threshold: Whether absolute threshold is met
        multiplier_level: The multiplier level that was exceeded (3.0, 2.0, or 0)
    """
    is_abuse: bool
    severity: str
    reason: str
    percentage_change: float
    meets_critical_threshold: bool
    meets_warning_threshold: bool
    meets_absolute_threshold: bool
    multiplier_level: float = 0.0


class AbuseDetector:
    """
    Detects abuse patterns in CloudFront metrics.
    
    This class implements:
    - Tiered threshold detection (Critical 3x immediate, Warning 2x sustained)
    - Corrected counter increment/decrement logic
    - Dual-threshold evaluation (relative + absolute)
    - Minimum baseline handling for low traffic
    - Time-windowed deduplication for counter updates
    """
    
    def __init__(self, config: Config, ddb_manager: DynamoDBManager):
        """
        Initialize Abuse Detector.
        
        Args:
            config: Configuration object with detection thresholds
            ddb_manager: DynamoDB manager for counter persistence
        """
        self.config = config
        self.ddb = ddb_manager
        
    def evaluate_metric(
        self,
        metric_name: str,
        current_value: float,
        avg_value: float
    ) -> AbuseEvaluation:
        """
        Evaluate a single metric for abuse using tiered threshold logic.
        
        This method implements tiered detection:
        1. Critical (3x): Immediate alert when current > avg * 3
        2. Warning (2x): Alert when current > avg * 2 for sustained period
        3. Both require absolute threshold to be met
        
        Args:
            metric_name: Name of the metric ('Requests' or 'BytesDownloaded')
            current_value: Current metric value
            avg_value: Historical average metric value
            
        Returns:
            AbuseEvaluation: Evaluation result with severity and reasoning
        """
        # Determine minimum baseline based on metric type
        if metric_name == 'Requests':
            minimum_baseline = self.config.minimum_baseline_requests
        else:  # BytesDownloaded
            minimum_baseline = self.config.minimum_baseline_bytes
        
        # Calculate percentage change with minimum baseline
        percentage_change = self._calculate_percentage_change(
            current_value, avg_value, minimum_baseline
        )
        
        # Check tiered relative thresholds
        meets_critical = self._check_threshold(current_value, avg_value, self.config.abuse_multiplier)
        meets_warning = self._check_threshold(current_value, avg_value, self.config.warning_multiplier)
        
        # Check absolute significance thresholds
        meets_absolute, abs_severity = self._check_absolute_threshold(metric_name, current_value)
        
        # Determine multiplier level and severity
        if meets_critical:
            multiplier_level = self.config.abuse_multiplier
            severity = 'Critical'
        elif meets_warning:
            multiplier_level = self.config.warning_multiplier
            severity = 'Warning'
        else:
            multiplier_level = 0.0
            severity = 'None'
        
        # Abuse is flagged if relative threshold AND absolute threshold are met
        is_abuse = (meets_critical or meets_warning) and meets_absolute
        
        # Build reason string
        if is_abuse:
            threshold_value = avg_value * multiplier_level
            reason = (
                f"{metric_name} abuse detected: "
                f"current={current_value:.2f}, avg={avg_value:.2f}, "
                f"threshold={threshold_value:.2f} ({multiplier_level}x), "
                f"change={percentage_change:.1f}%, severity={severity}"
            )
        elif (meets_critical or meets_warning) and not meets_absolute:
            reason = (
                f"{metric_name} exceeds relative threshold but below absolute significance: "
                f"current={current_value:.2f}, avg={avg_value:.2f}, "
                f"change={percentage_change:.1f}%"
            )
        elif meets_absolute and not (meets_critical or meets_warning):
            reason = (
                f"{metric_name} exceeds absolute threshold but not relative: "
                f"current={current_value:.2f}, avg={avg_value:.2f}, "
                f"change={percentage_change:.1f}%"
            )
        else:
            reason = (
                f"{metric_name} within normal range: "
                f"current={current_value:.2f}, avg={avg_value:.2f}, "
                f"change={percentage_change:.1f}%"
            )
        
        return AbuseEvaluation(
            is_abuse=is_abuse,
            severity=severity if is_abuse else 'None',
            reason=reason,
            percentage_change=percentage_change,
            meets_critical_threshold=meets_critical,
            meets_warning_threshold=meets_warning,
            meets_absolute_threshold=meets_absolute,
            multiplier_level=multiplier_level
        )
    
    def get_abuse_counter(self, key: str) -> int:
        """
        Get current abuse counter value.
        
        Args:
            key: Counter key (format: "{AccountId}#{DistributionId}#{MetricName}")
            
        Returns:
            int: Current counter value (0 if not found or error)
        """
        try:
            item = self.ddb.get_item_with_retry(
                table_name=self.config.ddb_abuse_counter_table,
                key={'CounterKey': key}
            )
            
            if item:
                count = item.get('Count', 0)
                logger.debug(
                    f"Retrieved abuse counter: {key} = {count}",
                    extra={'counter_key': key, 'count': count}
                )
                return int(count)
            else:
                logger.debug(
                    f"Abuse counter not found: {key}, returning 0",
                    extra={'counter_key': key}
                )
                return 0
                
        except ClientError as e:
            logger.error(
                f"Error getting abuse counter {key}: {e.response['Error']['Code']}",
                extra={
                    'counter_key': key,
                    'error_code': e.response['Error']['Code'],
                    'error_message': e.response['Error']['Message']
                }
            )
            return 0
            
        except Exception as e:
            logger.error(
                f"Unexpected error getting abuse counter {key}",
                exc_info=True,
                extra={'counter_key': key, 'error': str(e)}
            )
            return 0
    
    def update_abuse_counter(self, key: str, is_abuse: bool) -> int:
        """
        Update abuse counter with corrected increment/decrement logic.
        
        This method implements:
        1. Increment counter by 1 if is_abuse=True
        2. Decrement counter by 1 if is_abuse=False
        3. Ensure counter never goes below 0
        4. Dual write: main counter key + time-windowed key
        5. Idempotency: return cached value if already updated this hour
        
        Args:
            key: Counter key (format: "{AccountId}#{DistributionId}#{MetricName}")
            is_abuse: True to increment, False to decrement
            
        Returns:
            int: New counter value after update
        """
        # Generate time-windowed key for idempotency (15-minute windows)
        now = datetime.now(timezone.utc)
        # Use 15-minute window: floor to nearest 15 minutes
        window_minute = (now.minute // 15) * 15
        window_key = f"{key}#{now.strftime('%Y%m%d%H')}{window_minute:02d}"
        
        # Check if we already updated this 15-minute window (idempotency)
        try:
            cached_item = self.ddb.get_item_with_retry(
                table_name=self.config.ddb_abuse_counter_table,
                key={'CounterKey': window_key}
            )
            
            if cached_item:
                cached_count = int(cached_item.get('Count', 0))
                logger.debug(
                    f"Counter already updated this window: {key}, returning cached value {cached_count}",
                    extra={'counter_key': key, 'window_key': window_key, 'cached_count': cached_count}
                )
                return cached_count
                
        except Exception as e:
            # If we can't check cache, continue with update
            logger.warning(
                f"Could not check cached counter for {window_key}, continuing with update",
                extra={'counter_key': key, 'window_key': window_key, 'error': str(e)}
            )
        
        # Get current counter value
        current_count = self.get_abuse_counter(key)
        
        # Calculate new count with corrected logic
        if is_abuse:
            new_count = current_count + 1
            logger.debug(
                f"Incrementing abuse counter: {key} from {current_count} to {new_count}",
                extra={'counter_key': key, 'old_count': current_count, 'new_count': new_count}
            )
        else:
            # Decrement but never go below 0
            new_count = max(0, current_count - 1)
            logger.debug(
                f"Decrementing abuse counter: {key} from {current_count} to {new_count}",
                extra={'counter_key': key, 'old_count': current_count, 'new_count': new_count}
            )
        
        # Calculate TTL (30 days from now)
        ttl = int(now.timestamp()) + self.config.abuse_counter_ttl
        timestamp = now.isoformat()
        
        # Dual write: main counter key
        main_item = {
            'CounterKey': key,
            'Count': new_count,
            'LastUpdate': timestamp,
            'TTL': ttl
        }
        
        success_main = self.ddb.put_item_with_retry(
            table_name=self.config.ddb_abuse_counter_table,
            item=main_item
        )
        
        if not success_main:
            logger.error(
                f"Failed to update main counter key: {key}",
                extra={'counter_key': key, 'new_count': new_count}
            )
            # Return current count as fallback
            return current_count
        
        # Dual write: time-windowed key for idempotency
        window_item = {
            'CounterKey': window_key,
            'Count': new_count,
            'LastUpdate': timestamp,
            'TTL': ttl
        }
        
        success_window = self.ddb.put_item_with_retry(
            table_name=self.config.ddb_abuse_counter_table,
            item=window_item
        )
        
        if not success_window:
            logger.warning(
                f"Failed to update time-windowed counter key: {window_key}",
                extra={'counter_key': key, 'window_key': window_key, 'new_count': new_count}
            )
            # Main counter was updated, so this is not critical
        
        logger.info(
            f"Updated abuse counter: {key} = {new_count} (was {current_count})",
            extra={
                'counter_key': key,
                'old_count': current_count,
                'new_count': new_count,
                'is_abuse': is_abuse
            }
        )
        
        return new_count
    
    def _check_threshold(self, current: float, average: float, multiplier: float) -> bool:
        """
        Check if current value exceeds average by the specified multiplier.
        
        Args:
            current: Current metric value
            average: Historical average metric value
            multiplier: Threshold multiplier (e.g., 3.0 for critical, 2.0 for warning)
            
        Returns:
            bool: True if current > average * multiplier
        """
        if average == 0:
            # If average is 0, any positive current value exceeds threshold
            return current > 0
        
        threshold = average * multiplier
        return current > threshold
    
    def _check_absolute_threshold(
        self,
        metric_name: str,
        current: float
    ) -> Tuple[bool, str]:
        """
        Check if current value meets absolute significance threshold.
        
        Args:
            metric_name: Name of the metric ('Requests' or 'BytesDownloaded')
            current: Current metric value
            
        Returns:
            Tuple[bool, str]: (meets_threshold, severity_level)
                - meets_threshold: True if any absolute threshold is met
                - severity_level: 'Critical', 'Warning', or 'None'
        """
        if metric_name == 'Requests':
            if current >= self.config.critical_requests_threshold:
                return True, 'Critical'
            elif current >= self.config.warning_requests_threshold:
                return True, 'Warning'
            else:
                return False, 'None'
        else:  # BytesDownloaded
            if current >= self.config.critical_bytes_threshold:
                return True, 'Critical'
            elif current >= self.config.warning_bytes_threshold:
                return True, 'Warning'
            else:
                return False, 'None'
    
    def _calculate_percentage_change(
        self,
        current: float,
        average: float,
        minimum_baseline: float
    ) -> float:
        """
        Calculate percentage change with minimum baseline handling.
        
        For low-traffic distributions (average < minimum_baseline), use the
        minimum baseline instead of the actual average to avoid false positives
        from small absolute changes.
        
        Args:
            current: Current metric value
            average: Historical average metric value
            minimum_baseline: Minimum baseline value for this metric type
            
        Returns:
            float: Percentage change from baseline
        """
        # Use the larger of average or minimum_baseline as the denominator
        baseline = max(average, minimum_baseline)
        
        if baseline == 0:
            # Avoid division by zero
            return 0.0 if current == 0 else float('inf')
        
        percentage = ((current - average) / baseline) * 100
        return percentage
