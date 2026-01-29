import os
from dotenv import load_dotenv
from typing import Optional


class Config:
    """Singleton configuration class that loads environment variables from .env files."""

    _instance = None

    # Required configuration
    api_key: str
    provider: str
    feed_url: str

    # Optional configuration
    s3_bucket: Optional[str]
    log_file: Optional[str]
    api_key_header: Optional[str]
    api_key_query: Optional[str]

    def __new__(cls):
        """Ensure only one instance of Config exists."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Load and validate environment variables from .env files."""
        if self._initialized:
            return

        load_dotenv()

        # Load required variables
        self.api_key = os.getenv("API_KEY")
        self.provider = os.getenv("PROVIDER")
        self.feed_url = os.getenv("FEED_URL")

        # Load optional variables
        self.s3_bucket = os.getenv("S3_BUCKET")
        self.log_file = os.getenv("LOG_FILE", "./logs/app.log")
        self.api_key_header = os.getenv("API_KEY_HEADER", "X-API-Key")
        self.api_key_query = os.getenv("API_KEY_QUERY")

        # Validate required variables
        self._validate()
        self._initialized = True

    def _validate(self):
        """Validate that all required environment variables are set."""
        missing_vars = []

        if not self.provider:
            missing_vars.append("PROVIDER")
        if not self.feed_url:
            missing_vars.append("FEED_URL")

        # API_KEY is only required if authentication is configured
        auth_configured = self.api_key_header or self.api_key_query
        if auth_configured and not self.api_key:
            missing_vars.append("API_KEY")

        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )

    def get_headers(self) -> Optional[dict]:
        """Get headers dictionary with API key if header method is configured."""
        if self.api_key_header:
            return {self.api_key_header: self.api_key}
        return None

    def get_query_params(self) -> Optional[dict]:
        """Get query params dictionary with API key if query method is configured."""
        if self.api_key_query:
            return {self.api_key_query: self.api_key}
        return None
