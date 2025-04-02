import os
from dotenv import load_dotenv
import requests
import json
import sseclient
import uuid
import helpers

def consume_sse(url, api_key):
    """Connects to an SSE endpoint and processes events."""
    entities = []

    def find_entity(entity_id):
        return next((e for e in entities if e.entity_id == entity_id), None)

    HEADERS = {"x-api-key": api_key, "Accept": "text/event-stream"}
    response = requests.get(url, headers=HEADERS, stream=True)

    if response.status_code != 200:
        print(f"Failed to connect: {response.status_code}, {response.text}")
        return  # Exit early if the connection fails

    client = sseclient.SSEClient(response)
    for event in client.events():
        # TODO: need to incorporate max_stop_id to prevent endless loops for busses

        try:
            event_data = json.loads(event.data)  # Use json.loads() for safe parsing
          
    
            if event.event == "reset":
                print('reset event')
                new_ids = []
                for item in event_data:
                    entity_id = item.get("id")
                    entity = find_entity(entity_id)
                    if entity:
                        entity.update(item)
                        entities.append(entity)
                        new_ids.append(entity_id)
                    else:
                        #create new and append
                        entity = helpers.Entity(entity_id, item)
                        entities.append(entity)
                        new_ids.append(entity_id)
                    # Flush out objects where the id does not appear in the reset event
                    old_ids = [e.entity_id for e in entities]
                    ids_to_remove = [x for x in old_ids if x not in new_ids]
                    for id in ids_to_remove:
                        entity = find_entity(id)
                        if entity:
                            isExist = os.path.exists(f"/home/andrew/mbta-sse/data/{entity.route_id}")
                            if isExist == False:
                                os.makedirs(f"/home/andrew/mbta-sse/data/{entity.route_id}", mode=0o777, exist_ok=False)
                            with open(
                                f"/home/andrew/mbta-sse/data/{entity.route_id}/{uuid.uuid4()}.mfjson", "w"
                            ) as f:
                                f.write(entity.toMFJSON())
                            entities.remove(entity)
                print(f"Reset Complete")
            elif event.event == "add":
                entity_id = event_data.get("id")
                new_entity = helpers.Entity(entity_id, event_data)
                entities.append(new_entity)
                print(f"Added: {entity_id}")
            elif event.event == "update":
                #print('update event')
                if event_data == None:
                    print(event_data)
                entity_id = event_data.get("id")
                entity = find_entity(entity_id)
                if entity:
                    entity.update(event_data)
                    print(f"Updated: {entity_id}")
            elif event.event == "remove":
                entity_id = event_data.get("id")
                entity = find_entity(entity_id)
                if entity:
                    isExist = os.path.exists(f"/home/andrew/mbta-sse/data/{entity.route_id}")
                    if isExist == False:
                         os.makedirs(f"/home/andrew/mbta-sse/data/{entity.route_id}", mode=0o777, exist_ok=False)
                    with open(
                        f"/home/andrew/mbta-sse/data/{entity.route_id}/{uuid.uuid4()}.mfjson", "w"
                    ) as f:
                        f.write(entity.toMFJSON())
                    entities.remove(entity)
                    print(f"Removed: {entity_id}")

        except json.JSONDecodeError:
            print(f"Failed to decode event data: {event.data}")
        except Exception as e:
            print(f"An error occurred: {e}")
            event_data = json.loads(event.data)
            print(event_data)


if __name__ == "__main__":
    load_dotenv()
    api_key = os.getenv("API_KEY")
    # sse_url = "https://api-v3.mbta.com/vehicles?filter[route]="  # Replace with your SSE endpoint
    sse_url = "https://api-v3.mbta.com/vehicles?filter[route]=57"
    consume_sse(sse_url, api_key)
    
    # routes = ['CR-Worcester','86','57']
    # sse_urls = [sse_url+x for x in routes]
    # jobs = [] # list of jobs
    # for i in sse_urls:
    #     # Declare a new process and pass arguments to it
    #     p1 = multiprocessing.Process(target=consume_sse, args=(i,api_key,))
    #     jobs.append(p1)
    #     p1.start() # starting workers
