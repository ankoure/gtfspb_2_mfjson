from google.transit import gtfs_realtime_pb2
import requests
from helpers.Entity import Entity

class VehiclePositionFeed():
    def __init__(self, url, api_key, agency, file_path):
        self.entities = []
        self.url = url
        self.api_key = api_key
        self.agency = agency
        self.file_path = file_path
    def find_entity(self,entity_id):
        return next((e for e in self.entities if e.entity_id == entity_id), None)
    
    def get_entities(self):
        HEADERS = {"x-api-key": self.api_key}
        try:
            feed = gtfs_realtime_pb2.FeedMessage()
            response = requests.get(self.url, headers=HEADERS)
            feed.ParseFromString(response.content)
        except:
            pass
        # Returns list of feed entities
        return feed.entity
    def consume_pb(self):
        
        feed_entities = self.get_entities()
        
        if len(self.entities) == 0:
            # check if any observations exist, if none create all new objects
            for feed_entity in feed_entities:
                entity = Entity(feed_entity)
                self.entities.append(entity)
        else:
            current_ids = []
            # find and update entity
            for feed_entity in feed_entities:
                entity = self.find_entity(feed_entity.id)
                if entity:
                    # check if new direction and old direction are same
                    if entity.direction_id == feed_entity.vehicle.trip.direction_id:
                        entity.update(feed_entity)
                        current_ids.append(feed_entity.id)
                    else:
                        # first remove old
                        if len(entity.updated_at) > 1:
                            entity.save(self.file_path)
                        self.entities.remove(entity)
                        # now create new
                        entity = Entity(feed_entity)
                        self.entities.append(entity)
                        current_ids.append(feed_entity.id)
            # remove and save finished entities
            old_ids = [e.entity_id for e in self.entities]
            ids_to_remove = [x for x in old_ids if x not in current_ids]
            for id in ids_to_remove:
                # move logic onto object
                entity = self.find_entity(id)
                if entity:
                    # call save method
                    if len(entity.updated_at) > 1:
                        entity.save(self.file_path)
                    # remove from list
                    self.entities.remove(entity)