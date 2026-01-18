"""Tests for Config class."""

import pytest
from src.helpers.config import Config


class TestConfigSingleton:
    """Test Config singleton pattern."""

    def test_singleton_same_instance(self, reset_config, valid_env, monkeypatch):
        """Config should return the same instance on multiple calls."""
        # reset_config and valid_env are used for side effects
        monkeypatch.setattr("code.helpers.config.load_dotenv", lambda: None)
        config1 = Config()
        config2 = Config()
        assert config1 is config2

    def test_singleton_preserves_state(self, reset_config, valid_env, monkeypatch):
        """Config should preserve state across calls."""
        # reset_config and valid_env are used for side effects
        monkeypatch.setattr("code.helpers.config.load_dotenv", lambda: None)
        config = Config()
        original_api_key = config.api_key
        config2 = Config()
        assert config2.api_key == original_api_key


class TestConfigInitialization:
    """Test Config initialization and variable loading."""

    def test_load_required_variables(self, valid_config):
        """Config should load all required environment variables."""
        assert valid_config.api_key == "test_key"
        assert valid_config.provider == "test_provider"
        assert valid_config.feed_url == "http://test.url"

    def test_load_optional_variables(self, reset_config, monkeypatch):
        """Config should load optional environment variables."""
        monkeypatch.setenv("API_KEY", "key")
        monkeypatch.setenv("PROVIDER", "provider")
        monkeypatch.setenv("FEED_URL", "http://url")
        monkeypatch.setenv("S3_BUCKET", "my-bucket")
        monkeypatch.setenv("LOG_FILE", "/var/log/app.log")
        monkeypatch.setenv("API_KEY_HEADER", "Authorization")
        monkeypatch.setenv("API_KEY_QUERY", "key_param")
        monkeypatch.setattr("code.helpers.config.load_dotenv", lambda: None)

        config = Config()

        assert config.s3_bucket == "my-bucket"
        assert config.log_file == "/var/log/app.log"
        assert config.api_key_header == "Authorization"
        assert config.api_key_query == "key_param"

    def test_default_values_for_optional_variables(self, reset_config, monkeypatch):
        """Config should apply default values for optional variables not set."""
        # reset_config is used for its setup/teardown side effects
        # Set required vars but explicitly don't set optional ones
        monkeypatch.setenv("API_KEY", "test_key")
        monkeypatch.setenv("PROVIDER", "test_provider")
        monkeypatch.setenv("FEED_URL", "http://test.url")
        monkeypatch.delenv("S3_BUCKET", raising=False)
        monkeypatch.delenv("LOG_FILE", raising=False)
        monkeypatch.delenv("API_KEY_HEADER", raising=False)
        monkeypatch.delenv("API_KEY_QUERY", raising=False)
        monkeypatch.setattr("code.helpers.config.load_dotenv", lambda: None)

        config = Config()

        assert config.log_file == "./logs/app.log"
        assert config.api_key_header == "X-API-Key"
        assert config.s3_bucket is None
        assert config.api_key_query is None

    def test_initialization_only_happens_once(
        self, reset_config, valid_env, monkeypatch
    ):
        """Config should only initialize once, even if __init__ is called again."""
        monkeypatch.setattr("code.helpers.config.load_dotenv", lambda: None)
        config = Config()
        initial_api_key = config.api_key

        # Call init again with different env var
        monkeypatch.setenv("API_KEY", "different_key")
        config.__init__()

        # API key should still be the original (initialization was skipped)
        assert config.api_key == initial_api_key


class TestConfigValidation:
    """Test Config validation of required variables."""

    def test_missing_api_key_raises_error(self, reset_config, monkeypatch):
        """Config should raise ValueError when API_KEY is missing."""
        monkeypatch.setenv("PROVIDER", "provider")
        monkeypatch.setenv("FEED_URL", "http://url")
        monkeypatch.delenv("API_KEY", raising=False)
        monkeypatch.setattr("code.helpers.config.load_dotenv", lambda: None)

        with pytest.raises(ValueError) as exc_info:
            Config()
        assert "API_KEY" in str(exc_info.value)

    def test_missing_provider_raises_error(self, reset_config, monkeypatch):
        """Config should raise ValueError when PROVIDER is missing."""
        monkeypatch.setenv("API_KEY", "key")
        monkeypatch.setenv("FEED_URL", "http://url")
        monkeypatch.delenv("PROVIDER", raising=False)
        monkeypatch.setattr("code.helpers.config.load_dotenv", lambda: None)

        with pytest.raises(ValueError) as exc_info:
            Config()
        assert "PROVIDER" in str(exc_info.value)

    def test_missing_feed_url_raises_error(self, reset_config, monkeypatch):
        """Config should raise ValueError when FEED_URL is missing."""
        monkeypatch.setenv("API_KEY", "key")
        monkeypatch.setenv("PROVIDER", "provider")
        monkeypatch.delenv("FEED_URL", raising=False)
        monkeypatch.setattr("code.helpers.config.load_dotenv", lambda: None)

        with pytest.raises(ValueError) as exc_info:
            Config()
        assert "FEED_URL" in str(exc_info.value)

    def test_multiple_missing_variables_reported(self, reset_config, monkeypatch):
        """Config should report all missing required variables in error."""
        monkeypatch.delenv("API_KEY", raising=False)
        monkeypatch.delenv("PROVIDER", raising=False)
        monkeypatch.delenv("FEED_URL", raising=False)
        monkeypatch.setattr("code.helpers.config.load_dotenv", lambda: None)

        with pytest.raises(ValueError) as exc_info:
            Config()
        error_msg = str(exc_info.value)
        assert "API_KEY" in error_msg
        assert "PROVIDER" in error_msg
        assert "FEED_URL" in error_msg


class TestConfigHeaders:
    """Test Config get_headers() method."""

    def test_get_headers_with_default_header_name(self, valid_config):
        """get_headers() should use default X-API-Key header name."""
        headers = valid_config.get_headers()
        assert headers == {"X-API-Key": "test_key"}

    def test_get_headers_with_custom_header_name(self, reset_config, monkeypatch):
        """get_headers() should use custom header name when set."""
        monkeypatch.setenv("API_KEY", "my_key")
        monkeypatch.setenv("PROVIDER", "provider")
        monkeypatch.setenv("FEED_URL", "http://url")
        monkeypatch.setenv("API_KEY_HEADER", "Authorization")
        monkeypatch.setattr("code.helpers.config.load_dotenv", lambda: None)

        config = Config()
        headers = config.get_headers()
        assert headers == {"Authorization": "my_key"}

    def test_get_headers_returns_none_when_no_header_configured(
        self, reset_config, monkeypatch
    ):
        """get_headers() should return None when API_KEY_HEADER is not set."""
        monkeypatch.setenv("API_KEY", "key")
        monkeypatch.setenv("PROVIDER", "provider")
        monkeypatch.setenv("FEED_URL", "http://url")
        monkeypatch.delenv("API_KEY_HEADER", raising=False)
        monkeypatch.setattr("code.helpers.config.load_dotenv", lambda: None)

        config = Config()
        config.api_key_header = None
        headers = config.get_headers()
        assert headers is None


class TestConfigQueryParams:
    """Test Config get_query_params() method."""

    def test_get_query_params_when_configured(self, reset_config, monkeypatch):
        """get_query_params() should return dict with API key when configured."""
        monkeypatch.setenv("API_KEY", "secret_key")
        monkeypatch.setenv("PROVIDER", "provider")
        monkeypatch.setenv("FEED_URL", "http://url")
        monkeypatch.setenv("API_KEY_QUERY", "apikey")
        monkeypatch.setattr("code.helpers.config.load_dotenv", lambda: None)

        config = Config()
        params = config.get_query_params()
        assert params == {"apikey": "secret_key"}

    def test_get_query_params_returns_none_when_not_configured(self, valid_config):
        """get_query_params() should return None when API_KEY_QUERY is not set."""
        params = valid_config.get_query_params()
        assert params is None

    def test_get_query_params_with_custom_param_name(self, reset_config, monkeypatch):
        """get_query_params() should use custom parameter name."""
        monkeypatch.setenv("API_KEY", "token123")
        monkeypatch.setenv("PROVIDER", "provider")
        monkeypatch.setenv("FEED_URL", "http://url")
        monkeypatch.setenv("API_KEY_QUERY", "token")
        monkeypatch.setattr("code.helpers.config.load_dotenv", lambda: None)

        config = Config()
        params = config.get_query_params()
        assert params == {"token": "token123"}
