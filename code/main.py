import os
from dotenv import load_dotenv
import time
from helpers.VehiclePositionFeed import VehiclePositionFeed
from multiprocessing import Pool
import json
import logging


def init_worker():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(processName)s - %(levelname)s - %(message)s - %(asctime)s',
        filename=f'/home/andrew/gtfspb_2_mfjson/code/logs/app.log',
        datefmt='%Y-%m-%d %H:%M:%S'
        )


def worker(VehiclePositionFeed):
    while True:
        VehiclePositionFeed.consume_pb()
        time.sleep(VehiclePositionFeed.timeout)





if __name__ == "__main__":
    load_dotenv()
    api_key = os.getenv("API_KEY")
    filepath = '/home/andrew/gtfspb_2_mfjson/code/feed.json'

    try:
        with open(filepath, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        print(f"Error: File not found: {filepath}")
   
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in file: {filepath}")
        
    test = []    
    for feed in data:
        x = VehiclePositionFeed(feed['feed_url'],feed['provider'],f'/home/andrew/gtfspb_2_mfjson/data/{feed['provider']}/')
        test.append(x)
            
    # x = VehiclePositionFeed('https://cdn.mbta.com/realtime/VehiclePositions.pb','MBTA','/home/andrew/gtfspb_2_mfjson/data/MBTA/')
    # y = VehiclePositionFeed('https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace','NYC','/home/andrew/gtfspb_2_mfjson/data/NYC/')
    # z = VehiclePositionFeed('https://cats.rideralerts.com/InfoPoint/gtfs-realtime.ashx?type=vehicleposition','Baton Rouge','/home/andrew/gtfspb_2_mfjson/data/Baton_Rouge/')
    # test = [x,z]
    pool = Pool(processes=len(data), initializer=init_worker)  # Create a multiprocessing Pool
    pool.map(worker, test)  # process data_inputs iterable with pool
            
    # running = True
    # while running: 
    #     x.consume_pb()
    #     time.sleep(30)
