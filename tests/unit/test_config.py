"""Tests for src.config."""

import pytest

from src.config import load_config


class TestLoadConfig:
    """Tests for load_config()."""

    def test_raises_when_required_vars_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """load_config raises ValueError when required env vars are absent."""
        monkeypatch.delenv("RABBITMQ_URL", raising=False)
        monkeypatch.delenv("SALESFORCE_USERNAME", raising=False)
        monkeypatch.delenv("SALESFORCE_PASSWORD", raising=False)
        monkeypatch.delenv("SALESFORCE_SECURITY_TOKEN", raising=False)

        with pytest.raises(ValueError, match="RABBITMQ_URL"):
            load_config()

    def test_loads_with_all_required_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """load_config returns a Config when all required env vars are set."""
        monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
        monkeypatch.setenv("SALESFORCE_USERNAME", "test@example.com")
        monkeypatch.setenv("SALESFORCE_PASSWORD", "secret")
        monkeypatch.setenv("SALESFORCE_SECURITY_TOKEN", "token123")

        config = load_config()

        assert config.rabbitmq_url == "amqp://guest:guest@localhost:5672/"
        assert config.salesforce_username == "test@example.com"
        assert config.system_name == "CRM"
        assert config.heartbeat_interval_seconds == 1

    def test_respects_optional_overrides(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """load_config uses env var values for optional settings when provided."""
        monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
        monkeypatch.setenv("SALESFORCE_USERNAME", "test@example.com")
        monkeypatch.setenv("SALESFORCE_PASSWORD", "secret")
        monkeypatch.setenv("SALESFORCE_SECURITY_TOKEN", "token123")
        monkeypatch.setenv("HEARTBEAT_INTERVAL_SECONDS", "5")
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")

        config = load_config()

        assert config.heartbeat_interval_seconds == 5
        assert config.log_level == "DEBUG"

    def test_polling_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Polling interval defaults to 60s and state path to /tmp/..."""
        monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
        monkeypatch.setenv("SALESFORCE_USERNAME", "test@example.com")
        monkeypatch.setenv("SALESFORCE_PASSWORD", "secret")
        monkeypatch.setenv("SALESFORCE_SECURITY_TOKEN", "token123")
        monkeypatch.delenv("POLLING_INTERVAL_SECONDS", raising=False)
        monkeypatch.delenv("POLLING_STATE_PATH", raising=False)

        config = load_config()

        assert config.polling_interval_seconds == 60
        assert config.polling_state_path == "/tmp/polling_checkpoint.json"

    def test_polling_overrides(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Polling interval and state path are overridable via env."""
        monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
        monkeypatch.setenv("SALESFORCE_USERNAME", "test@example.com")
        monkeypatch.setenv("SALESFORCE_PASSWORD", "secret")
        monkeypatch.setenv("SALESFORCE_SECURITY_TOKEN", "token123")
        monkeypatch.setenv("POLLING_INTERVAL_SECONDS", "120")
        monkeypatch.setenv("POLLING_STATE_PATH", "/var/lib/crm/polling.json")

        config = load_config()

        assert config.polling_interval_seconds == 120
        assert config.polling_state_path == "/var/lib/crm/polling.json"
