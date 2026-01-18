"""Pytest configuration and shared fixtures for GTFS-RT testing."""

import pytest
import time
from google.transit import gtfs_realtime_pb2
from code.helpers.config import Config


@pytest.fixture
def mock_position():
    """Create a mock Position protobuf object."""
    position = gtfs_realtime_pb2.Position()
    position.latitude = 59.9311
    position.longitude = 30.3609
    position.bearing = 45.0
    position.odometer = 1000.5
    position.speed = 15.0
    return position


@pytest.fixture
def mock_trip():
    """Create a mock Trip protobuf object."""
    trip = gtfs_realtime_pb2.TripDescriptor()
    trip.trip_id = "trip_001"
    trip.route_id = "route_1"
    trip.direction_id = 0
    trip.start_date = "20240101"
    trip.start_time = "08:00:00"
    trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.SCHEDULED
    return trip


@pytest.fixture
def mock_vehicle():
    """Create a mock VehicleDescriptor protobuf object."""
    vehicle = gtfs_realtime_pb2.VehicleDescriptor()
    vehicle.id = "vehicle_123"
    vehicle.label = "Bus_001"
    vehicle.license_plate = "ABC123"
    return vehicle


@pytest.fixture
def mock_vehicle_position(mock_position, mock_trip, mock_vehicle):
    """Create a mock VehiclePosition protobuf object."""
    vp = gtfs_realtime_pb2.VehiclePosition()
    vp.position.CopyFrom(mock_position)
    vp.current_stop_sequence = 5
    vp.stop_id = "stop_42"
    vp.current_status = gtfs_realtime_pb2.VehiclePosition.IN_TRANSIT_TO
    vp.timestamp = int(time.time())
    vp.trip.CopyFrom(mock_trip)
    vp.vehicle.CopyFrom(mock_vehicle)
    vp.occupancy_status = gtfs_realtime_pb2.VehiclePosition.MANY_SEATS_AVAILABLE
    vp.occupancy_percentage = 30
    vp.congestion_level = gtfs_realtime_pb2.VehiclePosition.UNKNOWN_CONGESTION_LEVEL
    return vp


@pytest.fixture
def mock_carriage(mock_vehicle_position):
    """Create a mock CarriageDetails protobuf object."""
    carriage = gtfs_realtime_pb2.VehiclePosition.CarriageDetails()
    carriage.id = "carriage_001"
    carriage.label = "Coach_1"
    carriage.carriage_sequence = 1
    carriage.occupancy_status = gtfs_realtime_pb2.VehiclePosition.MANY_SEATS_AVAILABLE
    carriage.occupancy_percentage = 25
    return carriage


@pytest.fixture
def mock_vehicle_position_with_carriages(mock_vehicle_position, mock_carriage):
    """Create a VehiclePosition with multi-carriage details."""
    vp = mock_vehicle_position
    carriage = vp.multi_carriage_details.add()
    carriage.id = mock_carriage.id
    carriage.label = mock_carriage.label
    carriage.carriage_sequence = mock_carriage.carriage_sequence
    carriage.occupancy_status = mock_carriage.occupancy_status
    carriage.occupancy_percentage = mock_carriage.occupancy_percentage
    return vp


@pytest.fixture
def mock_entity(mock_vehicle_position):
    """Create a mock FeedEntity protobuf object."""
    entity = gtfs_realtime_pb2.FeedEntity()
    entity.id = "entity_001"
    entity.vehicle.CopyFrom(mock_vehicle_position)
    return entity


@pytest.fixture
def mock_feed_message(mock_entity):
    """Create a mock FeedMessage protobuf object with a single entity."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = int(time.time())
    entity = feed.entity.add()
    entity.CopyFrom(mock_entity)
    return feed


@pytest.fixture
def mock_feed_message_multiple_entities(mock_vehicle_position):
    """Create a FeedMessage with multiple vehicle entities."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = int(time.time())

    # Create 3 different vehicles
    for i in range(3):
        entity = feed.entity.add()
        entity.id = f"entity_{i:03d}"

        # Create unique vehicle position
        vp = gtfs_realtime_pb2.VehiclePosition()
        vp.position.latitude = 59.9311 + (i * 0.01)
        vp.position.longitude = 30.3609 + (i * 0.01)
        vp.position.bearing = 45.0 + (i * 10)
        vp.position.speed = 15.0 + (i * 5)
        vp.current_stop_sequence = 5 + i
        vp.stop_id = f"stop_{40 + i}"
        vp.current_status = gtfs_realtime_pb2.VehiclePosition.IN_TRANSIT_TO
        vp.timestamp = int(time.time()) + (i * 10)

        # Trip info
        vp.trip.trip_id = f"trip_{i:03d}"
        vp.trip.route_id = f"route_{i % 2}"
        vp.trip.direction_id = i % 2
        vp.trip.start_date = "20240101"
        vp.trip.start_time = "08:00:00"

        # Vehicle info
        vp.vehicle.id = f"vehicle_{i:03d}"
        vp.vehicle.label = f"Bus_{i:03d}"
        vp.vehicle.license_plate = f"ABC{100 + i}"

        vp.occupancy_status = gtfs_realtime_pb2.VehiclePosition.MANY_SEATS_AVAILABLE
        vp.occupancy_percentage = 30 + (i * 10)
        vp.congestion_level = gtfs_realtime_pb2.VehiclePosition.UNKNOWN_CONGESTION_LEVEL

        entity.vehicle.CopyFrom(vp)

    return feed


@pytest.fixture
def serialized_feed_message(mock_feed_message):
    """Serialize a FeedMessage to bytes (as would be returned from HTTP response)."""
    return mock_feed_message.SerializeToString()


@pytest.fixture
def serialized_feed_message_multiple(mock_feed_message_multiple_entities):
    """Serialize a multi-entity FeedMessage to bytes."""
    return mock_feed_message_multiple_entities.SerializeToString()


@pytest.fixture
def reset_config():
    """Reset Config singleton before each test."""
    Config._instance = None
    Config._initialized = False
    yield
    Config._instance = None
    Config._initialized = False


@pytest.fixture
def valid_env(monkeypatch):
    """Set valid environment variables for testing."""
    monkeypatch.setenv("API_KEY", "test_key")
    monkeypatch.setenv("PROVIDER", "test_provider")
    monkeypatch.setenv("FEED_URL", "http://test.url")


@pytest.fixture
def valid_config(reset_config, valid_env, monkeypatch):
    """Create a Config instance with valid test values."""
    monkeypatch.setattr("code.helpers.config.load_dotenv", lambda: None)
    return Config()
