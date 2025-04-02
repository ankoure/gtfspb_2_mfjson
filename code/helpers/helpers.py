import uuid
import json
import datetime

def writetojson(entity,entities,mode):
    if mode == "JSON":
        with open(f"/home/andrew/gtfspb_2_mfjson/data/{uuid.uuid4()}.json", "w") as f:
            f.write(entity.toJSON())
            entities.remove(entity)
    if mode == "MFJSON":
        with open(f"/home/andrew/gtfspb_2_mfjson/data/{uuid.uuid4()}.mfjson", "w") as f:
            f.write(entity.toJSON())
            entities.remove(entity)


            
class Entity:
    def __init__(self, entity):
        self.entity_id = entity.id

        # Static
        self.direction_id = entity.vehicle.trip.direction_id
        self.label = entity.vehicle.vehicle.label
        # TODO: self.revenue = attributes.get("revenue", None)
        self.created = datetime.now()
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
        self.updated_at = [entity.vehicle.timestamp]
        self.current_stop_sequence = [entity.vehicle.current_stop_sequence]
        self.coordinates = [
            [entity.vehicle.position.latitude, entity.vehicle.position.longitude]
        ] 
        self.occupancy_status = [entity.vehicle.occupancy_status] 
        self.occupancy_percentage = [entity.vehicle.occupancy_percentage] 
        self.congestion_level =  entity.vehicle.congestion_level

        #TODO: get multicarriage details
        # print(entity.vehicle.multi_carriage_details)
        # print(entity.vehicle.multi_carriage_details[0].label)
        # print(entity.vehicle.multi_carriage_details[0].occupancy_status)
        # print(entity.vehicle.multi_carriage_details[0].carriage_sequence)
        
     

    def update(self, entity):
        # Temporal
        self.bearing.append(entity.vehicle.position.bearing)
        self.current_status.append(entity.vehicle.current_status)
        self.current_stop_sequence.append(entity.vehicle.current_stop_sequence)
        self.coordinates.append(
            [entity.vehicle.position.latitude, entity.vehicle.position.longitude]
        )
        self.occupancy_status.append(entity.vehicle.occupancy_status)
        self.occupancy_percentage.append(entity.vehicle.occupancy_percentage)
        self.speed.append(entity.vehicle.position.speed)
        self.odometer.append(entity.vehicle.position.odometer)
        self.updated_at.append(entity.vehicle.timestamp)
        self.stop_id.append(entity.vehicle.stop_id)
    
    def checkage(self):
        #checks age of object and returns age in seconds
        return (datetime.now() - self.created).total_seconds()


    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    def toMFJSON(self):
        #TODO: Need to update properties being written out
        return json.dumps(
            {
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
                        "properties":{
                            "entity_id": self.entity_id,
                            "direction_id": self.direction_id,
                            "label": self.label,
                            "revenue": self.revenue,
                            "trip_id": self.trip_id,
                            "route_id": self.route_id

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
                                "current_stop_sequence": {
                                    "type": "Measure",
                                    "values": self.current_stop_sequence,
                                    "interpolation": "Discrete",
                                },
                                 "occupancy_status": {
                                    "type": "Measure",
                                    "values": self.current_stop_sequence,
                                    "interpolation": "Discrete",
                                },
                                 "speed": {
                                    "type": "Measure",
                                    "values": self.current_stop_sequence,
                                    "interpolation": "Linear",
                                },
                                 "stop_id": {
                                    "type": "Measure",
                                    "values": self.current_stop_sequence,
                                    "interpolation": "Discrete",
                                },
                                
                            }
                        ],
                    }
                ],
            },
            indent=4,
        )
