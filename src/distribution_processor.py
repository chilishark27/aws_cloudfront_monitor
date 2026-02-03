"""
Distribution Processor module for CloudFront Abuse Detection System.

This module processes individual CloudFront distributions, coordinating
metrics collection, abuse detection, and alert generation.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from .config import Config
from .metrics_collector import MetricsCollector
from .abuse_detector import AbuseDetector
from .alert_manager import AlertManager, Alert
from .observability import ObservabilityManager


logger = logging.getLogger(__name__)


class DistributionProcessor:
    """
    Processes individual CloudFront distributions.
    
    This class coordinates:
    - Metrics collection from CloudWatch
    - Abuse detection using dual-threshold logic
    - Alert generation and sending
    - Proper exception handling for all operations
    """
    
    def __init__(
        self,
        config: Config,
        metrics_collector: MetricsCollector,
        abuse_detector: AbuseDetector,
        alert_manager: AlertManager,
        observability: ObservabilityManager
    ):
        """
        Initialize Distribution Processor.
        
        Args:
            config: Configuration object
            metrics_collector: Metrics collector instance
            abuse_detector: Abuse detector instance
            alert_manager: Alert manager instance
            observability: Observability manager instance
        """
        self.config = config
        self.metrics = metrics_collector
        self.detector = abuse_detector
        self.alerts = alert_manager
        self.obs = observability
    
    def process_distribution(
        self,
        account_id: str,
        account_name: str,
        account_email: str,
        dist_id: str,
        cw_client
    ) -> int:
        """
        Process a single distribution and return alert count.
        
        This method:
        1. Fetches current and historical metrics
        2. Evaluates each metric for abuse
        3. Updates abuse counters
        4. Sends alerts if thresholds are met
        5. Handles all exceptions gracefully
        
        Args:
            account_id: AWS account ID
            account_name: AWS account name
            account_email: AWS account email
            dist_id: CloudFront distribution ID
            cw_client: Boto3 CloudWatch client (with assumed role credentials)
            
        Returns:
            int: Number of alerts sent (0 if no abuse detected or error)
        """
        try:
            self.obs.log_info(
                f"Processing distribution {dist_id}",
                account_id=account_id,
                distribution_id=dist_id
            )
            
            # 1. Fetch metrics
            metrics = self.metrics.get_metrics(cw_client, account_id, dist_id)
            
            # 2. Log metrics for this distribution
            self._log_distribution_metrics(
                account_id=account_id,
                dist_id=dist_id,
                metrics=metrics
            )
            
            # 3. Check if distribution should be skipped (too small)
            if self._should_skip_distribution(
                metrics.current_requests,
                metrics.current_bytes
            ):
                return 0
            
            # 4. Evaluate metrics for abuse
            alerts_sent = 0
            
            # Evaluate Requests metric
            requests_eval = self.detector.evaluate_metric(
                'Requests',
                metrics.current_requests,
                metrics.avg_requests
            )
            
            # Evaluate BytesDownloaded metric
            bytes_eval = self.detector.evaluate_metric(
                'BytesDownloaded',
                metrics.current_bytes,
                metrics.avg_bytes
            )
            
            # 5. Process Requests metric
            alerts_sent += self._process_metric_evaluation(
                account_id=account_id,
                account_name=account_name,
                account_email=account_email,
                dist_id=dist_id,
                metric_name='Requests',
                evaluation=requests_eval,
                current_value=metrics.current_requests,
                history_value=metrics.avg_requests
            )
            
            # 5. Process BytesDownloaded metric
            alerts_sent += self._process_metric_evaluation(
                account_id=account_id,
                account_name=account_name,
                account_email=account_email,
                dist_id=dist_id,
                metric_name='BytesDownloaded',
                evaluation=bytes_eval,
                current_value=metrics.current_bytes,
                history_value=metrics.avg_bytes
            )
            
            # 6. Log summary
            if alerts_sent > 0:
                self.obs.log_warning(
                    f"Distribution {dist_id} triggered {alerts_sent} alert(s)",
                    account_id=account_id,
                    distribution_id=dist_id,
                    alerts_sent=alerts_sent
                )
            else:
                self.obs.log_info(
                    f"Distribution {dist_id} processed successfully - no alerts",
                    account_id=account_id,
                    distribution_id=dist_id,
                    current_requests=metrics.current_requests,
                    current_bytes=metrics.current_bytes,
                    avg_requests=metrics.avg_requests,
                    avg_bytes=metrics.avg_bytes
                )
            
            return alerts_sent
            
        except Exception as e:
            # Catch all exceptions to prevent one distribution from breaking others
            self.obs.log_error(
                f"Error processing distribution {dist_id}",
                error=e,
                account_id=account_id,
                distribution_id=dist_id
            )
            # Return 0 alerts on error
            return 0
    
    def _process_metric_evaluation(
        self,
        account_id: str,
        account_name: str,
        account_email: str,
        dist_id: str,
        metric_name: str,
        evaluation,
        current_value: float,
        history_value: float
    ) -> int:
        """
        Process a single metric evaluation result with tiered alerting.
        
        This method implements tiered alerting:
        - Critical (3x): Alert immediately (duration_threshold = 1)
        - Warning (2x): Alert after sustained detection (warning_duration_threshold = 2)
        
        Args:
            account_id: AWS account ID
            account_name: AWS account name
            account_email: AWS account email
            dist_id: CloudFront distribution ID
            metric_name: Name of the metric ('Requests' or 'BytesDownloaded')
            evaluation: AbuseEvaluation result
            current_value: Current metric value
            history_value: Historical average metric value
            
        Returns:
            int: 1 if alert was sent, 0 otherwise
        """
        # Generate counter key
        counter_key = f"{account_id}#{dist_id}#{metric_name}"
        
        # Update abuse counter
        new_count = self.detector.update_abuse_counter(
            counter_key,
            evaluation.is_abuse
        )
        
        # Log evaluation result
        if evaluation.is_abuse:
            self.obs.log_warning(
                f"Abuse detected for {dist_id} - {metric_name}: {evaluation.reason}",
                account_id=account_id,
                distribution_id=dist_id,
                metric_name=metric_name,
                severity=evaluation.severity,
                consecutive_count=new_count,
                current_value=current_value,
                history_value=history_value,
                percentage_change=evaluation.percentage_change,
                meets_critical=evaluation.meets_critical_threshold,
                meets_warning=evaluation.meets_warning_threshold,
                meets_absolute=evaluation.meets_absolute_threshold,
                multiplier_level=evaluation.multiplier_level
            )
        else:
            self.obs.log_info(
                f"No abuse for {dist_id} - {metric_name}: {evaluation.reason}",
                account_id=account_id,
                distribution_id=dist_id,
                metric_name=metric_name,
                consecutive_count=new_count,
                current_value=current_value,
                history_value=history_value,
                percentage_change=evaluation.percentage_change
            )
        
        # Determine if alert should be sent based on tiered thresholds
        should_alert = False
        if evaluation.is_abuse:
            if evaluation.meets_critical_threshold:
                # Critical (3x): Alert immediately
                should_alert = new_count >= self.config.duration_threshold
            elif evaluation.meets_warning_threshold:
                # Warning (2x): Alert after sustained detection
                should_alert = new_count >= self.config.warning_duration_threshold
        
        if should_alert:
            # Send alert with the actual multiplier level that triggered it
            alert = Alert(
                account_id=account_id,
                account_name=account_name,
                account_email=account_email,
                distribution_id=dist_id,
                metric=metric_name,
                severity=evaluation.severity,
                current_value=current_value,
                history_value=history_value,
                abuse_multiplier=evaluation.multiplier_level,
                consecutive_count=new_count,
                timestamp=datetime.now(timezone.utc).isoformat(),
                percentage_change=evaluation.percentage_change
            )
            
            self.alerts.send_alert_async(alert)
            
            self.obs.log_warning(
                f"Alert queued for {dist_id} - {metric_name}",
                account_id=account_id,
                distribution_id=dist_id,
                metric_name=metric_name,
                severity=evaluation.severity,
                consecutive_count=new_count,
                multiplier_level=evaluation.multiplier_level
            )
            
            return 1
        
        return 0
    
    def _should_skip_distribution(
        self,
        current_requests: float,
        current_bytes: float
    ) -> bool:
        """
        Determine if distribution should be skipped (too small).
        
        Distributions with very low traffic are skipped to avoid
        processing overhead and false positives.
        
        Args:
            current_requests: Current request count
            current_bytes: Current bytes downloaded
            
        Returns:
            bool: True if distribution should be skipped
        """
        # Skip if both metrics are below minimum thresholds
        below_requests = current_requests < self.config.min_requests_threshold
        below_bytes = current_bytes < self.config.min_bytes_threshold
        
        return below_requests and below_bytes
    
    def _log_distribution_metrics(
        self,
        account_id: str,
        dist_id: str,
        metrics
    ) -> None:
        """
        Log metrics for a distribution in a clean, readable format.
        
        Args:
            account_id: AWS account ID
            dist_id: CloudFront distribution ID
            metrics: DistributionMetrics object
        """
        # Format bytes in human-readable format
        def format_bytes(b: float) -> str:
            if b >= 1024**3:
                return f"{b/1024**3:.2f}GB"
            elif b >= 1024**2:
                return f"{b/1024**2:.2f}MB"
            elif b >= 1024:
                return f"{b/1024:.2f}KB"
            return f"{b:.0f}B"
        
        # Calculate percentage changes
        req_change = ((metrics.current_requests / max(metrics.avg_requests, 1)) - 1) * 100
        bytes_change = ((metrics.current_bytes / max(metrics.avg_bytes, 1)) - 1) * 100
        
        # Log in a clean single-line format using observability manager
        self.obs.log_info(
            f"[METRICS] {account_id} | {dist_id} | "
            f"Requests: {metrics.current_requests:,.0f} (avg: {metrics.avg_requests:,.0f}, {req_change:+.1f}%) | "
            f"Bytes: {format_bytes(metrics.current_bytes)} (avg: {format_bytes(metrics.avg_bytes)}, {bytes_change:+.1f}%)"
        )
