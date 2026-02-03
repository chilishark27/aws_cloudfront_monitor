"""
Configuration module for CloudFront Abuse Detection System.

This module provides centralized configuration management with validation
for all system parameters loaded from environment variables.
"""

import os
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Config:
    """
    Configuration for CloudFront Abuse Detection System.
    
    All configuration values are loaded from environment variables with
    sensible defaults. The validate() method ensures all values are valid
    before the system starts processing.
    """
    
    # AWS Configuration
    region: str = "us-east-1"
    org_access_role: str = "OrganizationAccessRole_DO_NOT_DELETE"
    
    # Detection Thresholds (existing)
    abuse_multiplier: float = 3.0  # Critical threshold: immediate alert
    warning_multiplier: float = 2.0  # Warning threshold: requires sustained detection
    duration_threshold: int = 1  # For critical (3x): immediate
    warning_duration_threshold: int = 2  # For warning (2x): requires 2 consecutive detections (30 min)
    min_requests_threshold: int = 1000
    min_bytes_threshold: int = 500 * 1024 * 1024  # 500 MB
    
    # Adaptive Detection Thresholds (15-minute window)
    # Note: These thresholds are for 15-minute detection windows
    critical_requests_threshold: int = 2500  # 10000/4 for 15-min window
    critical_bytes_threshold: int = 1280 * 1024 * 1024  # 1.25 GB (5GB/4)
    warning_requests_threshold: int = 1250  # 5000/4 for 15-min window
    warning_bytes_threshold: int = 512 * 1024 * 1024  # 512 MB (2GB/4)
    minimum_baseline_requests: int = 25  # 100/4 for 15-min window
    minimum_baseline_bytes: int = 25 * 1024 * 1024  # 25 MB (100MB/4)
    
    # Concurrency
    max_workers: int = 12
    dist_max_workers: int = 5
    alert_max_workers: int = 5
    
    # Telegram
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    
    # DynamoDB Tables
    ddb_abuse_counter_table: str = "CF_Abuse_Counter"
    ddb_accounts_cache_table: str = "CF_Accounts_Cache"
    ddb_failed_accounts_table: str = "CF_Failed_Accounts"
    ddb_sent_alerts_table: str = "CF_Sent_Alerts"
    
    # Cache TTLs (seconds)
    accounts_cache_ttl: int = 86400  # 24 hours
    metrics_cache_ttl: int = 21600  # 6 hours
    failed_accounts_ttl: int = 604800  # 7 days
    abuse_counter_ttl: int = 2592000  # 30 days
    sent_alerts_ttl: int = 86400  # 24 hours
    
    # Display Timezone
    display_timezone_offset: int = 8  # Hours from UTC (UTC+8)
    
    @classmethod
    def from_environment(cls) -> 'Config':
        """
        Load configuration from environment variables.
        
        Returns:
            Config: Configuration object with values from environment
            
        Raises:
            ValueError: If required environment variables are missing or invalid
        """
        config = cls()
        
        # AWS Configuration
        config.region = os.getenv("AWS_REGION", config.region)
        config.org_access_role = os.getenv("ORG_ACCESS_ROLE", config.org_access_role)
        
        # Detection Thresholds
        config.abuse_multiplier = cls._get_float_env(
            "ABUSE_MULTIPLIER", config.abuse_multiplier
        )
        config.warning_multiplier = cls._get_float_env(
            "WARNING_MULTIPLIER", config.warning_multiplier
        )
        config.duration_threshold = cls._get_int_env(
            "DURATION_THRESHOLD", config.duration_threshold
        )
        config.warning_duration_threshold = cls._get_int_env(
            "WARNING_DURATION_THRESHOLD", config.warning_duration_threshold
        )
        config.min_requests_threshold = cls._get_int_env(
            "MIN_REQUESTS_THRESHOLD", config.min_requests_threshold
        )
        config.min_bytes_threshold = cls._get_int_env(
            "MIN_BYTES_THRESHOLD", config.min_bytes_threshold
        )
        
        # Adaptive Detection Thresholds
        config.critical_requests_threshold = cls._get_int_env(
            "CRITICAL_REQUESTS_THRESHOLD", config.critical_requests_threshold
        )
        config.critical_bytes_threshold = cls._get_int_env(
            "CRITICAL_BYTES_THRESHOLD", config.critical_bytes_threshold
        )
        config.warning_requests_threshold = cls._get_int_env(
            "WARNING_REQUESTS_THRESHOLD", config.warning_requests_threshold
        )
        config.warning_bytes_threshold = cls._get_int_env(
            "WARNING_BYTES_THRESHOLD", config.warning_bytes_threshold
        )
        config.minimum_baseline_requests = cls._get_int_env(
            "MINIMUM_BASELINE_REQUESTS", config.minimum_baseline_requests
        )
        config.minimum_baseline_bytes = cls._get_int_env(
            "MINIMUM_BASELINE_BYTES", config.minimum_baseline_bytes
        )
        
        # Concurrency
        config.max_workers = cls._get_int_env("MAX_WORKERS", config.max_workers)
        config.dist_max_workers = cls._get_int_env(
            "DIST_MAX_WORKERS", config.dist_max_workers
        )
        config.alert_max_workers = cls._get_int_env(
            "ALERT_MAX_WORKERS", config.alert_max_workers
        )
        
        # Telegram
        config.telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        config.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        
        # DynamoDB Tables
        config.ddb_abuse_counter_table = os.getenv(
            "DDB_ABUSE_COUNTER_TABLE", config.ddb_abuse_counter_table
        )
        config.ddb_accounts_cache_table = os.getenv(
            "DDB_ACCOUNTS_CACHE_TABLE", config.ddb_accounts_cache_table
        )
        config.ddb_failed_accounts_table = os.getenv(
            "DDB_FAILED_ACCOUNTS_TABLE", config.ddb_failed_accounts_table
        )
        config.ddb_sent_alerts_table = os.getenv(
            "DDB_SENT_ALERTS_TABLE", config.ddb_sent_alerts_table
        )
        
        # Cache TTLs
        config.accounts_cache_ttl = cls._get_int_env(
            "ACCOUNTS_CACHE_TTL", config.accounts_cache_ttl
        )
        config.metrics_cache_ttl = cls._get_int_env(
            "METRICS_CACHE_TTL", config.metrics_cache_ttl
        )
        config.failed_accounts_ttl = cls._get_int_env(
            "FAILED_ACCOUNTS_TTL", config.failed_accounts_ttl
        )
        config.abuse_counter_ttl = cls._get_int_env(
            "ABUSE_COUNTER_TTL", config.abuse_counter_ttl
        )
        config.sent_alerts_ttl = cls._get_int_env(
            "SENT_ALERTS_TTL", config.sent_alerts_ttl
        )
        
        # Display Timezone
        config.display_timezone_offset = cls._get_int_env(
            "DISPLAY_TIMEZONE_OFFSET", config.display_timezone_offset
        )
        
        return config
    
    @staticmethod
    def _get_int_env(key: str, default: int) -> int:
        """
        Get integer value from environment variable.
        
        Args:
            key: Environment variable name
            default: Default value if not set
            
        Returns:
            int: Parsed integer value
            
        Raises:
            ValueError: If value cannot be parsed as integer
        """
        value = os.getenv(key)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            raise ValueError(
                f"Environment variable {key}='{value}' is not a valid integer"
            )
    
    @staticmethod
    def _get_float_env(key: str, default: float) -> float:
        """
        Get float value from environment variable.
        
        Args:
            key: Environment variable name
            default: Default value if not set
            
        Returns:
            float: Parsed float value
            
        Raises:
            ValueError: If value cannot be parsed as float
        """
        value = os.getenv(key)
        if value is None:
            return default
        try:
            return float(value)
        except ValueError:
            raise ValueError(
                f"Environment variable {key}='{value}' is not a valid number"
            )
    
    def validate(self) -> List[str]:
        """
        Validate all configuration values.
        
        Returns:
            List[str]: List of validation error messages (empty if valid)
        """
        errors = []
        
        # Validate AWS Configuration
        if not self.region:
            errors.append("AWS region cannot be empty")
        
        if not self.org_access_role:
            errors.append("Organization access role cannot be empty")
        
        # Validate Detection Thresholds
        if self.abuse_multiplier <= 0:
            errors.append(
                f"Abuse multiplier must be positive, got {self.abuse_multiplier}"
            )
        
        if self.duration_threshold < 1:
            errors.append(
                f"Duration threshold must be at least 1, got {self.duration_threshold}"
            )
        
        if self.min_requests_threshold < 0:
            errors.append(
                f"Minimum requests threshold cannot be negative, got {self.min_requests_threshold}"
            )
        
        if self.min_bytes_threshold < 0:
            errors.append(
                f"Minimum bytes threshold cannot be negative, got {self.min_bytes_threshold}"
            )
        
        # Validate Adaptive Thresholds
        if self.critical_requests_threshold < 0:
            errors.append(
                f"Critical requests threshold cannot be negative, got {self.critical_requests_threshold}"
            )
        
        if self.critical_bytes_threshold < 0:
            errors.append(
                f"Critical bytes threshold cannot be negative, got {self.critical_bytes_threshold}"
            )
        
        if self.warning_requests_threshold < 0:
            errors.append(
                f"Warning requests threshold cannot be negative, got {self.warning_requests_threshold}"
            )
        
        if self.warning_bytes_threshold < 0:
            errors.append(
                f"Warning bytes threshold cannot be negative, got {self.warning_bytes_threshold}"
            )
        
        if self.minimum_baseline_requests < 0:
            errors.append(
                f"Minimum baseline requests cannot be negative, got {self.minimum_baseline_requests}"
            )
        
        if self.minimum_baseline_bytes < 0:
            errors.append(
                f"Minimum baseline bytes cannot be negative, got {self.minimum_baseline_bytes}"
            )
        
        # Validate threshold ordering
        if self.warning_requests_threshold > self.critical_requests_threshold:
            errors.append(
                f"Warning requests threshold ({self.warning_requests_threshold}) "
                f"cannot exceed critical threshold ({self.critical_requests_threshold})"
            )
        
        if self.warning_bytes_threshold > self.critical_bytes_threshold:
            errors.append(
                f"Warning bytes threshold ({self.warning_bytes_threshold}) "
                f"cannot exceed critical threshold ({self.critical_bytes_threshold})"
            )
        
        # Validate Concurrency
        if self.max_workers < 1:
            errors.append(f"Max workers must be at least 1, got {self.max_workers}")
        
        if self.dist_max_workers < 1:
            errors.append(
                f"Distribution max workers must be at least 1, got {self.dist_max_workers}"
            )
        
        if self.alert_max_workers < 1:
            errors.append(
                f"Alert max workers must be at least 1, got {self.alert_max_workers}"
            )
        
        # Validate Telegram (optional but both must be set if either is set)
        if bool(self.telegram_bot_token) != bool(self.telegram_chat_id):
            errors.append(
                "Both TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set, or neither"
            )
        
        # Validate DynamoDB Table Names
        if not self.ddb_abuse_counter_table:
            errors.append("DynamoDB abuse counter table name cannot be empty")
        
        if not self.ddb_accounts_cache_table:
            errors.append("DynamoDB accounts cache table name cannot be empty")
        
        if not self.ddb_failed_accounts_table:
            errors.append("DynamoDB failed accounts table name cannot be empty")
        
        if not self.ddb_sent_alerts_table:
            errors.append("DynamoDB sent alerts table name cannot be empty")
        
        # Validate TTLs
        if self.accounts_cache_ttl < 0:
            errors.append(
                f"Accounts cache TTL cannot be negative, got {self.accounts_cache_ttl}"
            )
        
        if self.metrics_cache_ttl < 0:
            errors.append(
                f"Metrics cache TTL cannot be negative, got {self.metrics_cache_ttl}"
            )
        
        if self.failed_accounts_ttl < 0:
            errors.append(
                f"Failed accounts TTL cannot be negative, got {self.failed_accounts_ttl}"
            )
        
        if self.abuse_counter_ttl < 0:
            errors.append(
                f"Abuse counter TTL cannot be negative, got {self.abuse_counter_ttl}"
            )
        
        if self.sent_alerts_ttl < 0:
            errors.append(
                f"Sent alerts TTL cannot be negative, got {self.sent_alerts_ttl}"
            )
        
        # Validate Display Timezone
        if self.display_timezone_offset < -12 or self.display_timezone_offset > 14:
            errors.append(
                f"Display timezone offset must be between -12 and +14, got {self.display_timezone_offset}"
            )
        
        return errors
