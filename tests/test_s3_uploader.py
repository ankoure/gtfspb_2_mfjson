"""Tests for S3 Uploader functionality."""

import pytest
from unittest.mock import patch, MagicMock, call
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from code.helpers.s3Uploader import upload_file


class TestS3UploaderSuccess:
    """Test successful S3 upload scenarios."""

    @patch("code.helpers.s3Uploader.boto3.client")
    def test_upload_file_success(self, mock_boto3_client):
        """Test successful file upload to S3."""
        # Setup mock
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        # Test data
        test_data = "Test data content"
        bucket = "test-bucket"
        object_name = "test-key"

        # Execute
        result = upload_file(test_data, bucket, object_name)

        # Assertions
        assert result is True
        mock_boto3_client.assert_called_once_with("s3")
        mock_s3_client.put_object.assert_called_once()

        # Verify put_object was called with correct parameters
        call_kwargs = mock_s3_client.put_object.call_args[1]
        assert call_kwargs["Bucket"] == bucket
        assert call_kwargs["Key"] == object_name
        # Body should be a BytesIO object with encoded data
        assert call_kwargs["Body"].read() == test_data.encode("utf-8")

    @patch("code.helpers.s3Uploader.boto3.client")
    def test_upload_file_with_special_characters(self, mock_boto3_client):
        """Test uploading data with special characters and unicode."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        # Test data with special characters
        test_data = "Special chars: é à ñ 中文 🚀"
        bucket = "test-bucket"
        object_name = "test-key"

        result = upload_file(test_data, bucket, object_name)

        assert result is True
        call_kwargs = mock_s3_client.put_object.call_args[1]
        assert call_kwargs["Body"].read() == test_data.encode("utf-8")

    @patch("code.helpers.s3Uploader.boto3.client")
    def test_upload_file_with_complex_key(self, mock_boto3_client):
        """Test uploading with complex S3 object keys (paths)."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        test_data = "Test data"
        bucket = "test-bucket"
        object_name = "path/to/nested/object.json"

        result = upload_file(test_data, bucket, object_name)

        assert result is True
        call_kwargs = mock_s3_client.put_object.call_args[1]
        assert call_kwargs["Key"] == object_name

    @patch("code.helpers.s3Uploader.boto3.client")
    def test_upload_file_with_large_data(self, mock_boto3_client):
        """Test uploading large amounts of data."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        # Create large test data (1MB)
        test_data = "x" * (1024 * 1024)
        bucket = "test-bucket"
        object_name = "large-file"

        result = upload_file(test_data, bucket, object_name)

        assert result is True
        mock_s3_client.put_object.assert_called_once()


class TestS3UploaderCredentialErrors:
    """Test credential-related error handling."""

    @patch("code.helpers.s3Uploader.boto3.client")
    def test_upload_file_no_credentials(self, mock_boto3_client):
        """Test handling of missing AWS credentials."""
        mock_boto3_client.side_effect = NoCredentialsError()

        result = upload_file("test data", "bucket", "key")

        assert result is False

    @patch("code.helpers.s3Uploader.boto3.client")
    def test_upload_file_partial_credentials(self, mock_boto3_client):
        """Test handling of incomplete AWS credentials."""
        mock_boto3_client.side_effect = PartialCredentialsError(
            provider="aws", cred_var="AWS_ACCESS_KEY_ID"
        )

        result = upload_file("test data", "bucket", "key")

        assert result is False


class TestS3UploaderClientErrors:
    """Test boto3 ClientError handling."""

    @patch("code.helpers.s3Uploader.boto3.client")
    def test_upload_file_bucket_not_found(self, mock_boto3_client):
        """Test handling of NoSuchBucket error."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        # Create a ClientError for NoSuchBucket
        error_response = {
            "Error": {
                "Code": "NoSuchBucket",
                "Message": "The specified bucket does not exist",
            }
        }
        mock_s3_client.put_object.side_effect = ClientError(error_response, "PutObject")

        result = upload_file("test data", "nonexistent-bucket", "key")

        assert result is False

    @patch("code.helpers.s3Uploader.boto3.client")
    def test_upload_file_access_denied(self, mock_boto3_client):
        """Test handling of AccessDenied error."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        error_response = {
            "Error": {
                "Code": "AccessDenied",
                "Message": "Access Denied",
            }
        }
        mock_s3_client.put_object.side_effect = ClientError(error_response, "PutObject")

        result = upload_file("test data", "test-bucket", "key")

        assert result is False

    @patch("code.helpers.s3Uploader.boto3.client")
    def test_upload_file_invalid_bucket_name(self, mock_boto3_client):
        """Test handling of InvalidBucketName error."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        error_response = {
            "Error": {
                "Code": "InvalidBucketName",
                "Message": "The specified bucket is not valid",
            }
        }
        mock_s3_client.put_object.side_effect = ClientError(error_response, "PutObject")

        result = upload_file("test data", "invalid-bucket!", "key")

        assert result is False

    @patch("code.helpers.s3Uploader.boto3.client")
    def test_upload_file_throttling(self, mock_boto3_client):
        """Test handling of throttling/rate limiting errors."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        error_response = {
            "Error": {
                "Code": "SlowDown",
                "Message": "Please reduce your request rate",
            }
        }
        mock_s3_client.put_object.side_effect = ClientError(error_response, "PutObject")

        result = upload_file("test data", "test-bucket", "key")

        assert result is False


class TestS3UploaderUnexpectedErrors:
    """Test handling of unexpected exceptions."""

    @patch("code.helpers.s3Uploader.boto3.client")
    def test_upload_file_unexpected_exception(self, mock_boto3_client):
        """Test handling of unexpected exceptions."""
        mock_boto3_client.side_effect = RuntimeError("Unexpected error occurred")

        result = upload_file("test data", "bucket", "key")

        assert result is False

    @patch("code.helpers.s3Uploader.boto3.client")
    def test_upload_file_connection_error(self, mock_boto3_client):
        """Test handling of connection errors."""
        mock_boto3_client.side_effect = ConnectionError("Connection refused")

        result = upload_file("test data", "bucket", "key")

        assert result is False

    @patch("code.helpers.s3Uploader.boto3.client")
    def test_upload_file_timeout(self, mock_boto3_client):
        """Test handling of timeout errors."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        mock_s3_client.put_object.side_effect = TimeoutError("Request timed out")

        result = upload_file("test data", "bucket", "key")

        assert result is False


class TestS3UploaderLogging:
    """Test logging behavior during uploads."""

    @patch("code.helpers.s3Uploader.logger")
    @patch("code.helpers.s3Uploader.boto3.client")
    def test_success_logging(self, mock_boto3_client, mock_logger):
        """Test that successful upload logs debug messages."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        upload_file("test data", "test-bucket", "test-key")

        # Should have debug logs for attempt and success
        assert mock_logger.debug.call_count >= 2

    @patch("code.helpers.s3Uploader.logger")
    @patch("code.helpers.s3Uploader.boto3.client")
    def test_no_credentials_logging(self, mock_boto3_client, mock_logger):
        """Test that NoCredentialsError logs error."""
        mock_boto3_client.side_effect = NoCredentialsError()

        upload_file("test data", "bucket", "key")

        mock_logger.error.assert_called()

    @patch("code.helpers.s3Uploader.logger")
    @patch("code.helpers.s3Uploader.boto3.client")
    def test_partial_credentials_logging(self, mock_boto3_client, mock_logger):
        """Test that PartialCredentialsError logs error."""
        mock_boto3_client.side_effect = PartialCredentialsError(
            provider="aws", cred_var="AWS_ACCESS_KEY_ID"
        )

        upload_file("test data", "bucket", "key")

        mock_logger.error.assert_called()

    @patch("code.helpers.s3Uploader.logger")
    @patch("code.helpers.s3Uploader.boto3.client")
    def test_client_error_logging(self, mock_boto3_client, mock_logger):
        """Test that ClientError logs specific error messages."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        error_response = {
            "Error": {
                "Code": "NoSuchBucket",
                "Message": "The specified bucket does not exist",
            }
        }
        mock_s3_client.put_object.side_effect = ClientError(error_response, "PutObject")

        upload_file("test data", "bucket", "key")

        mock_logger.error.assert_called()

    @patch("code.helpers.s3Uploader.logger")
    @patch("code.helpers.s3Uploader.boto3.client")
    def test_unexpected_error_logging(self, mock_boto3_client, mock_logger):
        """Test that unexpected errors are logged with exception context."""
        mock_boto3_client.side_effect = RuntimeError("Test error")

        upload_file("test data", "bucket", "key")

        mock_logger.exception.assert_called()


class TestS3UploaderEdgeCases:
    """Test edge cases and boundary conditions."""

    @patch("code.helpers.s3Uploader.boto3.client")
    def test_upload_file_empty_string(self, mock_boto3_client):
        """Test uploading an empty string."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        result = upload_file("", "bucket", "key")

        assert result is True
        call_kwargs = mock_s3_client.put_object.call_args[1]
        assert call_kwargs["Body"].read() == b""

    @patch("code.helpers.s3Uploader.boto3.client")
    def test_upload_file_empty_bucket_name(self, mock_boto3_client):
        """Test behavior with empty bucket name."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        result = upload_file("test data", "", "key")

        assert result is True
        # Should still attempt upload - S3 will validate
        mock_s3_client.put_object.assert_called_once()

    @patch("code.helpers.s3Uploader.boto3.client")
    def test_upload_file_empty_object_name(self, mock_boto3_client):
        """Test behavior with empty object name."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        result = upload_file("test data", "bucket", "")

        assert result is True
        call_kwargs = mock_s3_client.put_object.call_args[1]
        assert call_kwargs["Key"] == ""

    @patch("code.helpers.s3Uploader.boto3.client")
    def test_upload_file_with_newlines(self, mock_boto3_client):
        """Test uploading data with newlines and whitespace."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        test_data = "line1\nline2\n\nline4\r\nline5"
        bucket = "test-bucket"
        object_name = "test-key"

        result = upload_file(test_data, bucket, object_name)

        assert result is True
        call_kwargs = mock_s3_client.put_object.call_args[1]
        assert call_kwargs["Body"].read() == test_data.encode("utf-8")

    @patch("code.helpers.s3Uploader.boto3.client")
    def test_upload_file_with_json_data(self, mock_boto3_client):
        """Test uploading JSON data."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        test_data = '{"key": "value", "nested": {"inner": true}}'
        bucket = "test-bucket"
        object_name = "data.json"

        result = upload_file(test_data, bucket, object_name)

        assert result is True
        call_kwargs = mock_s3_client.put_object.call_args[1]
        assert call_kwargs["Body"].read() == test_data.encode("utf-8")
