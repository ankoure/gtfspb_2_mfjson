"""Tests for Entity class."""

import json
from google.transit import gtfs_realtime_pb2
from src.helpers.Entity import Entity, Carriage


class TestEntityBasicInitialization:
    """Test Entity initialization with basic vehicle position data."""

    def test_entity_creation_from_protobuf(self, mock_entity):
        """Test creating an Entity from a mock protobuf FeedEntity."""
        entity = Entity(mock_entity)

        # Check static properties
        assert entity.entity_id == "entity_001"
        assert entity.vehicle_id == "vehicle_123"
        assert entity.vehicle_label == "Bus_001"
        assert entity.license_plate == "ABC123"
        assert entity.route_id == "route_1"
        assert entity.trip_id == "trip_001"
        assert entity.direction_id == 0
        assert entity.start_date == "20240101"
        assert entity.start_time == "08:00:00"

    def test_entity_temporal_properties(self, mock_entity):
        """Test that Entity correctly captures temporal properties."""
        entity = Entity(mock_entity)

        # Temporal properties should be lists
        assert isinstance(entity.bearing, list)
        assert isinstance(entity.speed, list)
        assert isinstance(entity.coordinates, list)
        assert isinstance(entity.updated_at, list)

        # Should have one entry initially
        assert len(entity.bearing) == 1
        assert len(entity.speed) == 1
        assert len(entity.coordinates) == 1
        assert len(entity.updated_at) == 1

        # Check values
        assert entity.bearing[0] == 45.0
        assert entity.speed[0] == 15.0
        # Use approximate equality for floating point coordinates
        assert abs(entity.coordinates[0][0] - 30.3609) < 0.001
        assert abs(entity.coordinates[0][1] - 59.9311) < 0.001

    def test_entity_occupancy_and_congestion(self, mock_entity):
        """Test occupancy and congestion level tracking."""
        entity = Entity(mock_entity)

        assert entity.occupancy_percentage[0] == 30
        assert (
            entity.occupancy_status[0]
            == gtfs_realtime_pb2.VehiclePosition.MANY_SEATS_AVAILABLE
        )
        assert (
            entity.congestion_level[0]
            == gtfs_realtime_pb2.VehiclePosition.UNKNOWN_CONGESTION_LEVEL
        )


class TestEntityUpdate:
    """Test Entity update functionality."""

    def test_entity_update_appends_temporal_data(
        self, mock_entity, mock_vehicle_position
    ):
        """Test that updating an entity appends new temporal data."""
        entity = Entity(mock_entity)

        # Create a new entity with updated position
        updated_entity = gtfs_realtime_pb2.FeedEntity()
        updated_entity.id = "entity_001"

        # Modify the vehicle position
        mock_vehicle_position.position.latitude = 59.9320
        mock_vehicle_position.position.longitude = 30.3620
        mock_vehicle_position.position.bearing = 90.0
        mock_vehicle_position.position.speed = 20.0

        updated_entity.vehicle.CopyFrom(mock_vehicle_position)

        # Update the entity
        entity.update(updated_entity)

        # Temporal data should now have 2 entries
        assert len(entity.coordinates) == 2
        assert len(entity.bearing) == 2
        assert len(entity.speed) == 2

        # New values should be appended
        assert abs(entity.coordinates[1][0] - 30.3620) < 0.001
        assert abs(entity.coordinates[1][1] - 59.9320) < 0.001
        assert entity.bearing[1] == 90.0
        assert entity.speed[1] == 20.0

    def test_entity_update_with_carriages(self, mock_vehicle_position_with_carriages):
        """Test updating entity with multi-carriage details."""
        entity_proto = gtfs_realtime_pb2.FeedEntity()
        entity_proto.id = "entity_001"
        entity_proto.vehicle.CopyFrom(mock_vehicle_position_with_carriages)

        entity = Entity(entity_proto)

        # Should have created carriages
        assert len(entity.carriages) > 0
        assert entity.carriages[0].label == "Coach_1"
        assert entity.carriages[0].carriage_sequence == 1


class TestEntityMFJSONOutput:
    """Test Entity MFJSON serialization."""

    def test_to_mfjson_returns_valid_json(self, mock_entity):
        """Test that toMFJSON returns valid JSON."""
        entity = Entity(mock_entity)
        mfjson_str = entity.toMFJSON()

        # Should be parseable as JSON
        mfjson = json.loads(mfjson_str)

        # Should have correct structure
        assert mfjson["type"] == "FeatureCollection"
        assert len(mfjson["features"]) == 1

    def test_to_mfjson_structure(self, mock_entity):
        """Test the structure of the MFJSON output."""
        entity = Entity(mock_entity)
        mfjson = json.loads(entity.toMFJSON())

        feature = mfjson["features"][0]

        # Check temporalGeometry
        assert feature["temporalGeometry"]["type"] == "MovingPoint"
        assert len(feature["temporalGeometry"]["coordinates"]) == 1
        assert len(feature["temporalGeometry"]["datetimes"]) == 1

        # Check properties
        assert feature["properties"]["entity_id"] == "entity_001"
        assert feature["properties"]["trip_id"] == "trip_001"
        assert feature["properties"]["route_id"] == "route_1"

        # Check temporalProperties
        assert len(feature["temporalProperties"]) == 1
        temporal_props = feature["temporalProperties"][0]
        assert "bearing" in temporal_props
        assert "speed" in temporal_props

    def test_to_mfjson_with_multiple_observations(
        self, mock_entity, mock_vehicle_position
    ):
        """Test MFJSON output with multiple observations."""
        entity = Entity(mock_entity)

        # Add multiple observations
        for i in range(3):
            new_entity = gtfs_realtime_pb2.FeedEntity()
            new_entity.id = "entity_001"
            mock_vehicle_position.position.latitude = 59.9311 + (i * 0.001)
            mock_vehicle_position.position.longitude = 30.3609 + (i * 0.001)
            new_entity.vehicle.CopyFrom(mock_vehicle_position)
            entity.update(new_entity)

        mfjson = json.loads(entity.toMFJSON())
        feature = mfjson["features"][0]

        # Should have 4 observations total (initial + 3 updates)
        assert len(feature["temporalGeometry"]["coordinates"]) == 4
        assert len(feature["temporalGeometry"]["datetimes"]) == 4


class TestCarriageClass:
    """Test Carriage inner class."""

    def test_carriage_creation(self, mock_carriage):
        """Test creating a Carriage object."""
        carriage = Carriage(mock_carriage)

        assert carriage.label == "Coach_1"
        assert carriage.carriage_sequence == 1
        assert isinstance(carriage.occupancy_status, list)
        assert len(carriage.occupancy_status) == 1

    def test_carriage_update(self, mock_carriage):
        """Test updating a Carriage with new occupancy status."""
        carriage = Carriage(mock_carriage)

        # Update with new status
        mock_carriage.occupancy_status = (
            gtfs_realtime_pb2.VehiclePosition.FEW_SEATS_AVAILABLE
        )
        carriage.update(mock_carriage)

        # Should have appended new status
        assert len(carriage.occupancy_status) == 2
        assert (
            carriage.occupancy_status[1]
            == gtfs_realtime_pb2.VehiclePosition.FEW_SEATS_AVAILABLE
        )
