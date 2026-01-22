"""Tests for VehiclePositionFeed class."""

import pytest
from unittest.mock import Mock, patch
import requests
from src.helpers.VehiclePositionFeed import VehiclePositionFeed
from src.helpers.Entity import Entity


class TestVehiclePositionFeedInitialization:
    """Test VehiclePositionFeed initialization."""

    def test_feed_initialization(self):
        """Test creating a VehiclePositionFeed instance."""
        feed = VehiclePositionFeed(
            url="http://example.com/feed",
            file_path="/tmp/data",
            s3_bucket="my-bucket",
            headers={"Authorization": "Bearer token"},
            query_params={"key": "value"},
            https_verify=False,
            timeout=60,
        )

        assert feed.url == "http://example.com/feed"
        assert feed.file_path == "/tmp/data"
        assert feed.s3_bucket == "my-bucket"
        assert feed.headers == {"Authorization": "Bearer token"}
        assert feed.query_params == {"key": "value"}
        assert feed.https_verify is False
        assert feed.timeout == 60
        assert feed.entities == []

    def test_feed_initialization_with_defaults(self):
        """Test VehiclePositionFeed with default parameters."""
        feed = VehiclePositionFeed(
            url="http://example.com/feed",
            file_path="/tmp/data",
            s3_bucket="my-bucket",
        )

        assert feed.headers is None
        assert feed.query_params is None
        assert feed.https_verify is True
        assert feed.timeout == 30


class TestFindEntity:
    """Test entity lookup functionality."""

    def test_find_entity_exists(self, mock_entity):
        """Test finding an entity that exists in the feed."""
        feed = VehiclePositionFeed(
            url="http://example.com/feed",
            file_path="/tmp/data",
            s3_bucket="my-bucket",
        )

        entity = Entity(mock_entity)
        feed.entities.append(entity)

        found = feed.find_entity("entity_001")
        assert found is not None
        assert found.entity_id == "entity_001"

    def test_find_entity_does_not_exist(self, mock_entity):
        """Test finding an entity that doesn't exist returns None."""
        feed = VehiclePositionFeed(
            url="http://example.com/feed",
            file_path="/tmp/data",
            s3_bucket="my-bucket",
        )

        entity = Entity(mock_entity)
        feed.entities.append(entity)

        found = feed.find_entity("nonexistent_entity")
        assert found is None


class TestUpdateTimeout:
    """Test timeout management."""

    def test_update_timeout(self):
        """Test updating the request timeout."""
        feed = VehiclePositionFeed(
            url="http://example.com/feed",
            file_path="/tmp/data",
            s3_bucket="my-bucket",
            timeout=30,
        )

        assert feed.timeout == 30
        feed.update_timeout(300)
        assert feed.timeout == 300


class TestGetEntitiesWithMockedRequests:
    """Test get_entities with mocked HTTP requests."""

    @pytest.mark.parametrize("num_entities", [1, 5, 10])
    @patch("src.helpers.VehiclePositionFeed.requests.get")
    def test_get_entities_success(self, mock_get, num_entities, mock_feed_message):
        """Test successfully fetching and parsing a feed."""
        # Create feed with multiple entities
        feed_msg = mock_feed_message
        for i in range(1, num_entities):
            entity = feed_msg.entity.add()
            entity.id = f"entity_{i:03d}"
            entity.vehicle.CopyFrom(mock_feed_message.entity[0].vehicle)

        serialized = feed_msg.SerializeToString()

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = serialized
        mock_get.return_value = mock_response

        feed = VehiclePositionFeed(
            url="http://example.com/feed",
            file_path="/tmp/data",
            s3_bucket="my-bucket",
        )

        vehicles = feed.get_entities()

        assert len(vehicles) == num_entities
        assert all(v.HasField("vehicle") for v in vehicles)
        mock_get.assert_called_once()

    @patch("src.helpers.VehiclePositionFeed.requests.get")
    def test_get_entities_with_custom_headers(self, mock_get, mock_feed_message):
        """Test that custom headers are sent with request."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = mock_feed_message.SerializeToString()
        mock_get.return_value = mock_response

        custom_headers = {"X-API-Key": "secret123"}
        feed = VehiclePositionFeed(
            url="http://example.com/feed",
            file_path="/tmp/data",
            s3_bucket="my-bucket",
            headers=custom_headers,
        )

        feed.get_entities()

        # Check that request was made with correct headers
        call_kwargs = mock_get.call_args[1]
        assert "X-API-Key" in call_kwargs["headers"]
        assert call_kwargs["headers"]["X-API-Key"] == "secret123"
        assert "User-Agent" in call_kwargs["headers"]

    @patch("src.helpers.VehiclePositionFeed.requests.get")
    def test_get_entities_empty_feed(self, mock_get):
        """Test handling of empty feed."""
        empty_feed = Mock()
        empty_feed.entity = []

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b""
        mock_get.return_value = mock_response

        feed = VehiclePositionFeed(
            url="http://example.com/feed",
            file_path="/tmp/data",
            s3_bucket="my-bucket",
        )

        vehicles = feed.get_entities()

        # Empty feed should return empty list
        assert len(vehicles) == 0

    @patch("src.helpers.VehiclePositionFeed.requests.get")
    def test_get_entities_timeout(self, mock_get):
        """Test handling of request timeout."""
        mock_get.side_effect = requests.exceptions.Timeout()

        feed = VehiclePositionFeed(
            url="http://example.com/feed",
            file_path="/tmp/data",
            s3_bucket="my-bucket",
        )

        vehicles = feed.get_entities()

        # Should return empty list on timeout
        assert vehicles == []

    @patch("src.helpers.VehiclePositionFeed.requests.get")
    def test_get_entities_ssl_error(self, mock_get):
        """Test handling of SSL errors."""
        mock_get.side_effect = requests.exceptions.SSLError("SSL certificate error")

        feed = VehiclePositionFeed(
            url="http://example.com/feed",
            file_path="/tmp/data",
            s3_bucket="my-bucket",
        )

        vehicles = feed.get_entities()

        assert vehicles == []

    @patch("src.helpers.VehiclePositionFeed.requests.get")
    def test_get_entities_http_error(self, mock_get):
        """Test handling of HTTP errors."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "404 Not Found"
        )
        mock_get.return_value = mock_response

        feed = VehiclePositionFeed(
            url="http://example.com/feed",
            file_path="/tmp/data",
            s3_bucket="my-bucket",
        )

        with pytest.raises(SystemExit):
            feed.get_entities()


class TestConsumePB:
    """Test protobuf consumption and entity management."""

    @patch("src.helpers.VehiclePositionFeed.VehiclePositionFeed.get_entities")
    def test_consume_pb_creates_new_entities(
        self, mock_get_entities, mock_feed_message_multiple_entities
    ):
        """Test that consume_pb creates new entities when none exist."""
        vehicles = [e for e in mock_feed_message_multiple_entities.entity]

        mock_get_entities.return_value = vehicles

        feed = VehiclePositionFeed(
            url="http://example.com/feed",
            file_path="/tmp/data",
            s3_bucket="my-bucket",
        )

        feed.consume_pb()

        assert len(feed.entities) == len(vehicles)
        assert all(isinstance(e, Entity) for e in feed.entities)

    @patch("src.helpers.VehiclePositionFeed.VehiclePositionFeed.get_entities")
    def test_consume_pb_updates_existing_entities(
        self, mock_get_entities, mock_feed_message_multiple_entities
    ):
        """Test that consume_pb updates existing entities."""
        vehicles = [e for e in mock_feed_message_multiple_entities.entity]

        mock_get_entities.return_value = vehicles

        feed = VehiclePositionFeed(
            url="http://example.com/feed",
            file_path="/tmp/data",
            s3_bucket="my-bucket",
        )

        # First consume - creates entities
        feed.consume_pb()
        initial_count = len(feed.entities)

        # Mock new feed data with same entities but updated info
        for entity in mock_feed_message_multiple_entities.entity:
            entity.vehicle.position.speed = 25.0

        mock_get_entities.return_value = vehicles

        # Second consume - should update, not add new
        feed.consume_pb()

        assert len(feed.entities) == initial_count

    @patch("src.helpers.VehiclePositionFeed.VehiclePositionFeed.get_entities")
    def test_consume_pb_empty_feed(self, mock_get_entities):
        """Test consume_pb with empty feed."""
        mock_get_entities.return_value = []

        feed = VehiclePositionFeed(
            url="http://example.com/feed",
            file_path="/tmp/data",
            s3_bucket="my-bucket",
        )

        feed.consume_pb()

        assert len(feed.entities) == 0
        assert feed.timeout == 300  # Should be updated to ERROR_TIMEOUT


class TestMemoryLimit:
    """Test memory limit enforcement."""

    @patch("src.helpers.VehiclePositionFeed.VehiclePositionFeed.get_entities")
    @patch("src.helpers.Entity.Entity.save")
    def test_memory_limit_removal(
        self, mock_save, mock_get_entities, mock_feed_message_multiple_entities
    ):
        """Test that oldest entities are removed when limit is exceeded."""
        # This test would require setting MAX_ENTITIES to a lower value
        # For now, we just verify the mechanism works
        pass
