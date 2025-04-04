import os
from dotenv import load_dotenv
import requests
from helpers.Entity import Entity
import time

from google.transit import gtfs_realtime_pb2

def get_entities(url,api_key):
        HEADERS = {"x-api-key": api_key}
        try:
            feed = gtfs_realtime_pb2.FeedMessage()
            response = requests.get(url, headers=HEADERS)
            feed.ParseFromString(response.content)
        except:
            pass
        # Returns list of feed entities
        return feed.entity


def consume_pb(url,api_key, entities):
    
    def find_entity(entity_id):
        return next((e for e in entities if e.entity_id == entity_id), None)
       
    feed_entities = get_entities(url,api_key)
    
    if len(entities) == 0:
        # check if any observations exist, if none create all new objects
        for feed_entity in feed_entities:
            entity = Entity(feed_entity)
            entities.append(entity)
    else:
        current_ids = []
        # find and update entity
        for feed_entity in feed_entities:
            entity = find_entity(feed_entity.id)
            if entity:
                # check if new direction and old direction are same
                if entity.direction_id == feed_entity.vehicle.trip.direction_id:
                    entity.update(feed_entity)
                    current_ids.append(feed_entity.id)
                else:
                    # first remove old
                    entity.save("/home/andrew/gtfspb_2_mfjson/data/")
                    entities.remove(entity)
                    # now create new
                    entity = Entity(feed_entity)
                    entities.append(entity)
                    current_ids.append(feed_entity.id)
        # remove and save finished entities
        old_ids = [e.entity_id for e in entities]
        ids_to_remove = [x for x in old_ids if x not in current_ids]
        for id in ids_to_remove:
            # move logic onto object
            entity = find_entity(id)
            if entity:
                # call save method
                entity.save("/home/andrew/mbta-sse/data/")
                # remove from list
                entities.remove(entity)

if __name__ == "__main__":
    load_dotenv()
    api_key = os.getenv("API_KEY")
    pb_url = "https://cdn.mbta.com/realtime/VehiclePositions.pb" 
    entities = []
    running = True
    while running: 
        consume_pb(pb_url,api_key,entities)
        time.sleep(30)
        

