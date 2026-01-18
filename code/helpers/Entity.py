import uuid
import json
import datetime
import os
from code.helpers.s3Uploader import upload_file
from code.helpers.setup_logger import logger


class Carriage:
    def __init__(self, carriage_details):
        self.label = carriage_details.label
        self.carriage_sequence = carriage_details.carriage_sequence
        self.occupancy_status = [carriage_details.occupancy_status]

    def update(self, carriage_details):
        self.occupancy_status.append(carriage_details.occupancy_status)


class Entity:
    @staticmethod
    def _timestamp_to_iso(timestamp):
        """Convert Unix timestamp to ISO 8601 format string."""
        return datetime.datetime.fromtimestamp(timestamp).isoformat()

    def __init__(self, entity):
        self.entity_id = entity.id

        # Static
        self.direction_id = entity.vehicle.trip.direction_id
        self.label = entity.vehicle.vehicle.label
        # TODO: self.revenue = attributes.get("revenue", None)
        self.created = datetime.datetime.now()
        self.route_id = entity.vehicle.trip.route_id
        self.trip_id = entity.vehicle.trip.trip_id
        self.schedule_relationship = entity.vehicle.trip.schedule_relationship
        self.start_date = entity.vehicle.trip.start_date
        self.start_time = entity.vehicle.trip.start_time
        self.vehicle_id = entity.vehicle.vehicle.id
        self.vehicle_label = entity.vehicle.vehicle.label
        self.license_plate = entity.vehicle.vehicle.license_plate

        # Temporal
        self.bearing = [entity.vehicle.position.bearing]
        self.current_status = [entity.vehicle.current_status]
        self.odometer = [entity.vehicle.position.odometer]
        self.speed = [entity.vehicle.position.speed]
        self.stop_id = [entity.vehicle.stop_id]
        self.updated_at = [self._timestamp_to_iso(entity.vehicle.timestamp)]
        self.current_stop_sequence = [entity.vehicle.current_stop_sequence]
        self.coordinates = [
            [entity.vehicle.position.longitude, entity.vehicle.position.latitude]
        ]
        self.occupancy_status = [entity.vehicle.occupancy_status]
        self.occupancy_percentage = [entity.vehicle.occupancy_percentage]
        self.congestion_level = [entity.vehicle.congestion_level]

        self.carriages = [Carriage(c) for c in entity.vehicle.multi_carriage_details]

    def update(self, entity):
        # Temporal
        self.bearing.append(entity.vehicle.position.bearing)
        self.current_status.append(entity.vehicle.current_status)
        self.current_stop_sequence.append(entity.vehicle.current_stop_sequence)
        self.coordinates.append(
            [entity.vehicle.position.longitude, entity.vehicle.position.latitude]
        )
        self.occupancy_status.append(entity.vehicle.occupancy_status)
        self.occupancy_percentage.append(entity.vehicle.occupancy_percentage)
        self.speed.append(entity.vehicle.position.speed)
        self.odometer.append(entity.vehicle.position.odometer)
        self.updated_at.append(self._timestamp_to_iso(entity.vehicle.timestamp))
        self.stop_id.append(entity.vehicle.stop_id)
        self.congestion_level.append(entity.vehicle.congestion_level)

        for carriage in entity.vehicle.multi_carriage_details:
            carriage_obj = next(
                (c for c in self.carriages if c.label == carriage.label), None
            )
            if carriage_obj:
                carriage_obj.update(carriage)

    def toMFJSON(self):
        dict_template = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "temporalGeometry": {
                        "type": "MovingPoint",
                        "coordinates": self.coordinates,
                        "datetimes": self.updated_at,
                        "interpolation": "Linear",
                    },
                    "properties": {
                        "trajectory_id": 0,
                        "entity_id": self.entity_id,
                        "direction_id": self.direction_id,
                        "label": self.label,
                        "trip_id": self.trip_id,
                        "route_id": self.route_id,
                        "schedule_relationship": self.schedule_relationship,
                        "trip_start_date": self.start_date,
                        "trip_start_time": self.start_time,
                        "vehicle_id": self.vehicle_id,
                        "vehicle_label": self.vehicle_label,
                        "license_plate": self.license_plate,
                    },
                    "temporalProperties": [
                        {
                            "datetimes": self.updated_at,
                            "bearing": {
                                "type": "Measure",
                                "values": self.bearing,
                                "interpolation": "Linear",
                            },
                            "current_status": {
                                "type": "Measure",
                                "values": self.current_status,
                                "interpolation": "Discrete",
                            },
                            "odometer": {
                                "type": "Measure",
                                "values": self.odometer,
                                "interpolation": "Discrete",
                            },
                            "speed": {
                                "type": "Measure",
                                "values": self.speed,
                                "interpolation": "Linear",
                            },
                            "stop_id": {
                                "type": "Measure",
                                "values": self.stop_id,
                                "interpolation": "Discrete",
                            },
                            "current_stop_sequence": {
                                "type": "Measure",
                                "values": self.current_stop_sequence,
                                "interpolation": "Discrete",
                            },
                            "occupancy_status": {
                                "type": "Measure",
                                "values": self.occupancy_status,
                                "interpolation": "Discrete",
                            },
                            "occupancy_percentage": {
                                "type": "Measure",
                                "values": self.occupancy_percentage,
                                "interpolation": "Discrete",
                            },
                            "congestion_level": {
                                "type": "Measure",
                                "values": self.congestion_level,
                                "interpolation": "Discrete",
                            },
                        }
                    ],
                }
            ],
        }
        for carriage in self.carriages:
            carriage_key = f"carriage_{carriage.carriage_sequence}_{carriage.label}"
            dict_template["features"][0]["temporalProperties"][0][carriage_key] = {
                "type": "Measure",
                "values": carriage.occupancy_status,
                "interpolation": "Discrete",
            }

        return json.dumps(
            dict_template,
            indent=4,
        )

    def save(self, file_path):
        # Extract date from first timestamp in updated_at
        date = datetime.datetime.fromisoformat(self.updated_at[0]).date()
        year = date.year
        month = date.month
        day = date.day

        # Create path with date-based subdirectories: raw/{route_id}/Year=YYYY/Month=MM/Day=DD
        route_dir = f"{file_path}/raw/{self.route_id}/Year={year}/Month={month:02d}/Day={day:02d}"
        try:
            os.makedirs(route_dir, mode=0o755, exist_ok=True)
            file_name = f"{route_dir}/{uuid.uuid4()}.mfjson"
            with open(file_name, "w") as f:
                f.write(self.toMFJSON())
            logger.debug(
                f"Entity saved: {self.entity_id} (route={self.route_id}, "
                f"observations={len(self.updated_at)}, file={file_name})"
            )
        except OSError as e:
            logger.error(f"Failed to save entity {self.entity_id} to {file_path}: {e}")

    def savetos3(self, bucket, file_path):
        s3_path = f"{file_path}/{uuid.uuid4()}.mfjson"
        logger.debug(
            f"Uploading entity {self.entity_id} to S3: s3://{bucket}/{s3_path}"
        )
        success = upload_file(self.toMFJSON(), bucket, s3_path)
        if success:
            logger.debug(
                f"Entity uploaded to S3: {self.entity_id} (bucket={bucket}, "
                f"observations={len(self.updated_at)}, path={s3_path})"
            )
        else:
            logger.error(
                f"Failed to upload entity {self.entity_id} to S3 bucket {bucket}"
            )
