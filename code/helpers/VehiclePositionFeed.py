from google.transit import gtfs_realtime_pb2
import requests
from helpers.Entity import Entity
import logging 

class VehiclePositionFeed():
    def __init__(self, url, agency, file_path, headers=None, query_params=None,https_verify=True,timeout=30):
        self.entities = []
        self.url = url
        self.headers = headers
        self.query_params = query_params
        self.agency = agency
        self.file_path = file_path
        self.https_verify = https_verify
        self.timeout = timeout
    def find_entity(self,entity_id):
        return next((e for e in self.entities if e.entity_id == entity_id), None)
    
    def updatetimeout(self,timeout):
        self.timeout = timeout
        
    def get_entities(self):
        try:
            feed = gtfs_realtime_pb2.FeedMessage()
            # TODO: add From and User Agent Headers
            # headers = {
            #     'User-Agent': 'Your App Name/1.0',
            #     'From': 'your_email@example.com'
            # }

            if self.headers:
                if not self.query_params:
                    #Headers Yes, query params No
                    response = requests.get(self.url, headers=self.headers,verify=self.https_verify)
                    
                #Headers Yes, query params Yes
                response = requests.get(self.url, headers=self.headers,params=self.query_params,verify=self.https_verify)
                
            if self.query_params:
                if not self.headers:
                    #Headers No, query params Yes
                    response = requests.get(self.url, headers=self.headers,params=self.query_params,verify=self.https_verify)
                    
            if not self.query_params:
                if not self.headers:
                    #Headers No Query Params No
                    response = requests.get(self.url,verify=self.https_verify)
                    
                
            feed.ParseFromString(response.content)
        except:
            # TODO: update to be more fine-grained in future
            self.updatetimeout(300)
            logging.exception("message")
        # Returns list of feed entities
        vehicles = [entity for entity in feed.entity if entity.HasField('vehicle')]
        return vehicles
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