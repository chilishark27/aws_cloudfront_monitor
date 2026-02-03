"""
Unit tests for configuration module.

Tests configuration loading, validation, and error handling.
"""

import os
import pytest
from src.config import Config


class TestConfigDefaults:
    """Test default configuration values."""
    
    def test_default_values(self):
        """Test that Config has sensible defaults."""
        config = Config()
        
        # AWS Configuration
        assert config.region == "us-east-1"
        assert config.org_access_role == "OrganizationAccessRole_DO_NOT_DELETE"
        
        # Detection Thresholds
        assert config.abuse_multiplier == 3.0
        assert config.duration_threshold == 1
        assert config.min_requests_threshold == 1000
        assert config.min_bytes_threshold == 500 * 1024 * 1024
        
        # Adaptive Thresholds (15-minute window)
        assert config.critical_requests_threshold == 2500
        assert config.critical_bytes_threshold == 1280 * 1024 * 1024
        assert config.warning_requests_threshold == 1250
        assert config.warning_bytes_threshold == 512 * 1024 * 1024
        
        # Concurrency
        assert config.max_workers == 12
        assert config.dist_max_workers == 5
        assert config.alert_max_workers == 5
        
        # DynamoDB Tables
        assert config.ddb_abuse_counter_table == "CF_Abuse_Counter"
        assert config.ddb_accounts_cache_table == "CF_Accounts_Cache"
        assert config.ddb_failed_accounts_table == "CF_Failed_Accounts"
        assert config.ddb_sent_alerts_table == "CF_Sent_Alerts"
        
        # TTLs
        assert config.accounts_cache_ttl == 86400
        assert config.metrics_cache_ttl == 21600
        
        # Display
        assert config.display_timezone_offset == 8


class TestConfigFromEnvironment:
    """Test loading configuration from environment variables."""
    
    def test_load_from_environment(self, monkeypatch):
        """Test loading configuration from environment variables."""
        monkeypatch.setenv("AWS_REGION", "us-west-2")
        monkeypatch.setenv("ABUSE_MULTIPLIER", "5.0")
        monkeypatch.setenv("MAX_WORKERS", "20")
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test_token")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "test_chat_id")
        
        config = Config.from_environment()
        
        assert config.region == "us-west-2"
        assert config.abuse_multiplier == 5.0
        assert config.max_workers == 20
        assert config.telegram_bot_token == "test_token"
        assert config.telegram_chat_id == "test_chat_id"
    
    def test_invalid_integer_environment_variable(self, monkeypatch):
        """Test that invalid integer values raise ValueError."""
        monkeypatch.setenv("MAX_WORKERS", "not_a_number")
        
        with pytest.raises(ValueError, match="not a valid integer"):
            Config.from_environment()
    
    def test_invalid_float_environment_variable(self, monkeypatch):
        """Test that invalid float values raise ValueError."""
        monkeypatch.setenv("ABUSE_MULTIPLIER", "not_a_number")
        
        with pytest.raises(ValueError, match="not a valid number"):
            Config.from_environment()


class TestConfigValidation:
    """Test configuration validation."""
    
    def test_valid_configuration(self):
        """Test that valid configuration passes validation."""
        config = Config()
        errors = config.validate()
        assert errors == []
    
    def test_negative_abuse_multiplier(self):
        """Test that negative abuse multiplier is invalid."""
        config = Config(abuse_multiplier=-1.0)
        errors = config.validate()
        assert any("Abuse multiplier must be positive" in err for err in errors)
    
    def test_zero_abuse_multiplier(self):
        """Test that zero abuse multiplier is invalid."""
        config = Config(abuse_multiplier=0.0)
        errors = config.validate()
        assert any("Abuse multiplier must be positive" in err for err in errors)
    
    def test_invalid_duration_threshold(self):
        """Test that duration threshold less than 1 is invalid."""
        config = Config(duration_threshold=0)
        errors = config.validate()
        assert any("Duration threshold must be at least 1" in err for err in errors)
    
    def test_negative_thresholds(self):
        """Test that negative thresholds are invalid."""
        config = Config(
            min_requests_threshold=-100,
            min_bytes_threshold=-1000,
            critical_requests_threshold=-500
        )
        errors = config.validate()
        assert any("cannot be negative" in err for err in errors)
        assert len([e for e in errors if "cannot be negative" in e]) >= 3
    
    def test_invalid_worker_counts(self):
        """Test that worker counts less than 1 are invalid."""
        config = Config(max_workers=0, dist_max_workers=0, alert_max_workers=0)
        errors = config.validate()
        assert any("Max workers must be at least 1" in err for err in errors)
        assert any("Distribution max workers must be at least 1" in err for err in errors)
        assert any("Alert max workers must be at least 1" in err for err in errors)
    
    def test_telegram_partial_configuration(self):
        """Test that partial Telegram configuration is invalid."""
        config = Config(telegram_bot_token="token", telegram_chat_id="")
        errors = config.validate()
        assert any("Both TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID" in err for err in errors)
        
        config = Config(telegram_bot_token="", telegram_chat_id="chat_id")
        errors = config.validate()
        assert any("Both TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID" in err for err in errors)
    
    def test_empty_table_names(self):
        """Test that empty table names are invalid."""
        config = Config(
            ddb_abuse_counter_table="",
            ddb_accounts_cache_table="",
            ddb_failed_accounts_table="",
            ddb_sent_alerts_table=""
        )
        errors = config.validate()
        assert any("abuse counter table name cannot be empty" in err for err in errors)
        assert any("accounts cache table name cannot be empty" in err for err in errors)
        assert any("failed accounts table name cannot be empty" in err for err in errors)
        assert any("sent alerts table name cannot be empty" in err for err in errors)
    
    def test_negative_ttls(self):
        """Test that negative TTLs are invalid."""
        config = Config(
            accounts_cache_ttl=-1,
            metrics_cache_ttl=-1,
            failed_accounts_ttl=-1,
            abuse_counter_ttl=-1,
            sent_alerts_ttl=-1
        )
        errors = config.validate()
        assert len([e for e in errors if "TTL cannot be negative" in e]) == 5
    
    def test_invalid_timezone_offset(self):
        """Test that invalid timezone offsets are rejected."""
        config = Config(display_timezone_offset=-15)
        errors = config.validate()
        assert any("timezone offset must be between -12 and +14" in err for err in errors)
        
        config = Config(display_timezone_offset=20)
        errors = config.validate()
        assert any("timezone offset must be between -12 and +14" in err for err in errors)
    
    def test_threshold_ordering(self):
        """Test that warning thresholds cannot exceed critical thresholds."""
        config = Config(
            warning_requests_threshold=20000,
            critical_requests_threshold=10000
        )
        errors = config.validate()
        assert any("Warning requests threshold" in err and "cannot exceed critical" in err for err in errors)
        
        config = Config(
            warning_bytes_threshold=10 * 1024 * 1024 * 1024,
            critical_bytes_threshold=5 * 1024 * 1024 * 1024
        )
        errors = config.validate()
        assert any("Warning bytes threshold" in err and "cannot exceed critical" in err for err in errors)
    
    def test_empty_region(self):
        """Test that empty region is invalid."""
        config = Config(region="")
        errors = config.validate()
        assert any("AWS region cannot be empty" in err for err in errors)
    
    def test_empty_org_role(self):
        """Test that empty organization role is invalid."""
        config = Config(org_access_role="")
        errors = config.validate()
        assert any("Organization access role cannot be empty" in err for err in errors)
    
    def test_multiple_validation_errors(self):
        """Test that multiple validation errors are all reported."""
        config = Config(
            abuse_multiplier=-1.0,
            duration_threshold=0,
            max_workers=0,
            region="",
            ddb_abuse_counter_table=""
        )
        errors = config.validate()
        assert len(errors) >= 5
